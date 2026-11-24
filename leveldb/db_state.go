// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errHasFrozenMem = errors.New("has frozen mem")
)

type memDB struct {
	db *DB
	*memdb.DB
	ref int32
}

func (m *memDB) getref() int32 {
	return atomic.LoadInt32(&m.ref)
}

func (m *memDB) incref() {
	atomic.AddInt32(&m.ref, 1)
}

func (m *memDB) decref() {
	if ref := atomic.AddInt32(&m.ref, -1); ref == 0 {
		// Only put back memdb with std capacity.
		if m.Capacity() == m.db.s.o.GetWriteBuffer() {
			m.Reset()
			m.db.mpoolPut(m.DB)
		}
		m.db = nil
		m.DB = nil
	} else if ref < 0 {
		panic("negative memdb ref")
	}
}

// readState is a coherent, immutable snapshot of the set of storage
// generations that a read or iterator operates on: the effective memdb,
// possibly a frozen memdb and one version of the table set.
//
// All three are captured in a single critical section while holding memMu,
// and every captured generation carries an extra reference owned by the
// readState. A memtable rotation (or a flush commit) happening afterwards
// swaps the DB pointers but cannot change what this readState sees, nor
// reclaim the pinned generations. Callers must pair every readState with
// exactly one release.
type readState struct {
	em, fm *memDB
	v      *version
}

// release drops every reference captured by the readState. It is safe to
// call on a nil readState.
func (rs *readState) release() {
	if rs == nil {
		return
	}
	if rs.em != nil {
		rs.em.decref()
	}
	if rs.fm != nil {
		rs.fm.decref()
	}
	if rs.v != nil {
		rs.v.release()
	}
}

// readStateReleaser adapts a readState to the iterator releaser interface.
type readStateReleaser struct {
	once sync.Once
	rs   *readState
}

func (r *readStateReleaser) Release() {
	r.once.Do(func() {
		r.rs.release()
	})
}

// Get latest sequence number.
func (db *DB) getSeq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

// Atomically adds delta to seq.
func (db *DB) addSeq(delta uint64) {
	atomic.AddUint64(&db.seq, delta)
}

func (db *DB) setSeq(seq uint64) {
	atomic.StoreUint64(&db.seq, seq)
}

func (db *DB) sampleSeek(ikey internalKey) {
	v := db.s.version()
	if v.sampleSeek(ikey) {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdC)
	}
	v.release()
}

func (db *DB) mpoolPut(mem *memdb.DB) {
	if !db.isClosed() {
		select {
		case db.memPool <- mem:
		default:
		}
	}
}

func (db *DB) mpoolGet(n int) *memDB {
	var mdb *memdb.DB
	select {
	case mdb = <-db.memPool:
	default:
	}
	if mdb == nil || mdb.Capacity() < n {
		mdb = memdb.New(db.s.icmp, maxInt(db.s.o.GetWriteBuffer(), n))
	}
	return &memDB{
		db: db,
		DB: mdb,
	}
}

func (db *DB) mpoolDrain() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			select {
			case <-db.memPool:
			default:
			}
		case <-db.closeC:
			ticker.Stop()
			// Make sure the pool is drained.
			select {
			case <-db.memPool:
			case <-time.After(time.Second):
			}
			close(db.memPool)
			return
		}
	}
}

// abortNewJournal discards a freshly created journal that never became the
// active one. It closes the writer and removes the file so that no file
// descriptor nor file number leaks after a failed rotation.
func (db *DB) abortNewJournal(fd storage.FileDesc, w storage.Writer, jw *journal.Writer) {
	if err := jw.Close(); err != nil {
		db.logf("journal@abort close @%d %q", fd.Num, err)
	}
	if err := w.Close(); err != nil {
		db.logf("journal@abort close writer @%d %q", fd.Num, err)
	}
	if err := db.s.stor.Remove(fd); err != nil {
		db.logf("journal@abort remove @%d %q", fd.Num, err)
	}
	db.s.reuseFileNum(fd.Num)
}

// Create new memdb and froze the old one; need external synchronization.
// newMem only called synchronously by the writer.
//
// The rotation is all-or-nothing: it promotes exactly one generation and
// never mutates the serving state unless every step succeeds. On any error
// the previous effective memdb, frozen memdb and journal stay in place and
// continue to serve reads and writes.
func (db *DB) newMem(n int) (mem *memDB, err error) {
	db.memMu.Lock()
	defer db.memMu.Unlock()

	// Reject the rotation before allocating anything so a frozen memdb that
	// hasn't been flushed yet keeps being served and no journal is leaked.
	if db.frozenMem != nil {
		return nil, errHasFrozenMem
	}

	fd := storage.FileDesc{Type: storage.TypeJournal, Num: db.s.allocFileNum()}
	w, err := db.s.stor.Create(fd)
	if err != nil {
		db.s.reuseFileNum(fd.Num)
		return
	}

	// The new journal and memdb are staged in local variables and only
	// published to the DB once the old journal has been sealed successfully.
	newJournal := journal.NewWriter(w)
	newMem := db.mpoolGet(n)

	oldJournal := db.journal
	oldWriter := db.journalWriter
	oldJournalFd := db.journalFd
	if oldJournal != nil {
		// Seal the old journal onto the new writer. On failure the active
		// journal is untouched and the staged one is discarded.
		if err = oldJournal.Reset(w); err != nil {
			db.abortNewJournal(fd, w, newJournal)
			return
		}
		// The old journal is switched to the new writer already; closing the
		// old writer finalizes the previous file. A failure rolls the journal
		// back to the old writer before discarding the staged journal.
		if err = oldWriter.Close(); err != nil {
			if rerr := oldJournal.Reset(oldWriter); rerr != nil {
				db.logf("journal@rollback @%d %q", oldJournalFd.Num, rerr)
			}
			db.abortNewJournal(fd, w, newJournal)
			return
		}
	}

	// Commit the rotation. From here on every mutation is a pointer swap that
	// cannot fail, so the frozen memtable remains read-only and the old
	// serving state is preserved intact until the flush task takes over.
	if oldJournal == nil {
		db.journal = newJournal
	}
	db.journalWriter = w
	db.journalFd = fd
	if oldJournal != nil {
		db.frozenJournalFd = oldJournalFd
	}
	db.frozenMem = db.mem
	newMem.incref() // for self
	newMem.incref() // for caller
	db.mem = newMem
	mem = newMem
	// The seq only incremented by the writer. And whoever called newMem
	// should hold write lock, so no need additional synchronization here.
	db.frozenSeq = db.seq
	return
}

// acquireReadState pins a coherent read snapshot of the current memdbs and
// the table version. The effective and frozen memdbs are captured atomically
// under memMu so a rotation that happens concurrently cannot make a key
// appear in, or disappear from, only one of them. The version is captured
// right after; the version pointer only moves forward and a frozen memdb is
// only dropped after its flush commit, so the pinned combination can never
// miss data nor expose a state older than the captured memdbs.
//
// The returned readState owns one reference to each pinned generation and
// must be released exactly once.
func (db *DB) acquireReadState() *readState {
	rs := &readState{}

	db.memMu.RLock()
	if db.mem != nil {
		db.mem.incref()
		rs.em = db.mem
	} else if !db.isClosed() {
		db.memMu.RUnlock()
		panic("nil effective mem")
	}
	if db.frozenMem != nil {
		db.frozenMem.incref()
		rs.fm = db.frozenMem
	}
	db.memMu.RUnlock()

	rs.v = db.s.version()
	return rs
}

// Get effective memdb.
func (db *DB) getEffectiveMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mem
}

// Get frozen memdb.
func (db *DB) getFrozenMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.frozenMem != nil {
		db.frozenMem.incref()
	}
	return db.frozenMem
}

// Drop frozen memdb; assume that frozen memdb isn't nil.
func (db *DB) dropFrozenMem() {
	db.memMu.Lock()
	if err := db.s.stor.Remove(db.frozenJournalFd); err != nil {
		db.logf("journal@remove removing @%d %q", db.frozenJournalFd.Num, err)
	} else {
		db.logf("journal@remove removed @%d", db.frozenJournalFd.Num)
	}
	db.frozenJournalFd = storage.FileDesc{}
	db.frozenMem.decref()
	db.frozenMem = nil
	db.memMu.Unlock()
}

// Clear mems ptr; used by DB.Close().
func (db *DB) clearMems() {
	db.memMu.Lock()
	db.mem = nil
	db.frozenMem = nil
	db.memMu.Unlock()
}

// Set closed flag; return true if not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.closed, 0, 1)
}

// Check whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

// Check read ok status.
func (db *DB) ok() error {
	if db.isClosed() {
		return ErrClosed
	}
	return nil
}
