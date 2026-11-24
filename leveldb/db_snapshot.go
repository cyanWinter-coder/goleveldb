// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"container/list"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type snapshotElement struct {
	seq uint64
	ref int
	e   *list.Element

	// v is the version pinned on behalf of an explicit Snapshot object
	// sharing this element. The tables of the pinned version are only
	// removed after every snapshot and iterator referencing them has
	// been released. It is nil for elements that only back ephemeral
	// snapshots (e.g. the ones acquired internally by DB.Get), which
	// don't need retention beyond their own short lifetime.
	v *version
}

// Acquires a snapshot, based on latest sequence.
func (db *DB) acquireSnapshot() *snapshotElement {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	seq := db.getSeq()

	if e := db.snapsList.Back(); e != nil {
		se := e.Value.(*snapshotElement)
		if se.seq == seq {
			se.ref++
			return se
		} else if seq < se.seq {
			panic("leveldb: sequence number is not increasing")
		}
	}
	se := &snapshotElement{seq: seq, ref: 1}
	se.e = db.snapsList.PushBack(se)
	return se
}

// pinSnapshotVersion pins the current version on behalf of an explicit
// Snapshot object, so that the tables it references are kept around until
// the snapshot element is released.
func (db *DB) pinSnapshotVersion(se *snapshotElement) {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	if se.v == nil {
		se.v = db.s.version()
		db.snapsPinned++
	}
}

// Releases given snapshot element.
func (db *DB) releaseSnapshot(se *snapshotElement) {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	se.ref--
	if se.ref == 0 {
		db.snapsList.Remove(se.e)
		se.e = nil
		if se.v != nil {
			// Unpin the version; the tables it references may now be
			// removed once no iterator references them either. The
			// snapshot horizon may also have advanced, making
			// previously unproductive compactions eligible again.
			se.v.release()
			se.v = nil
			db.snapsPinned--
			db.compTrigger(db.tcompCmdC)
		}
	} else if se.ref < 0 {
		panic("leveldb: Snapshot: negative element reference")
	}
}

// Gets minimum sequence that not being snapshotted.
func (db *DB) minSeq() uint64 {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	if e := db.snapsList.Front(); e != nil {
		return e.Value.(*snapshotElement).seq
	}

	return db.getSeq()
}

// compactionHorizon returns the current snapshot horizon: the minimum
// sequence number that has to remain visible to readers, and whether that
// horizon is pinned by at least one live explicit snapshot. When no
// explicit snapshot is alive the horizon simply tracks the minimum
// sequence of the ephemeral snapshots (or the latest sequence number)
// and every table is eligible for compaction.
func (db *DB) compactionHorizon() (minSeq uint64, pinned bool) {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	if e := db.snapsList.Front(); e != nil {
		return e.Value.(*snapshotElement).seq, db.snapsPinned > 0
	}

	return db.getSeq(), false
}

// Snapshot is a DB snapshot.
type Snapshot struct {
	db       *DB
	elem     *snapshotElement
	mu       sync.RWMutex
	released bool
}

// Creates new snapshot object.
func (db *DB) newSnapshot() *Snapshot {
	snap := &Snapshot{
		db:   db,
		elem: db.acquireSnapshot(),
	}
	// Explicit snapshots pin the version of their snapshot element, so
	// that the tables visible at the snapshot horizon are not removed
	// before the snapshot (and every iterator on it) is released.
	db.pinSnapshotVersion(snap.elem)
	atomic.AddInt32(&db.aliveSnaps, 1)
	runtime.SetFinalizer(snap, (*Snapshot).Release)
	return snap
}

func (snap *Snapshot) String() string {
	return fmt.Sprintf("leveldb.Snapshot{%d}", snap.elem.seq)
}

// Get gets the value for the given key. It returns ErrNotFound if
// the DB does not contains the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (snap *Snapshot) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	snap.mu.RLock()
	defer snap.mu.RUnlock()
	if snap.released {
		err = ErrSnapshotReleased
		return
	}
	err = snap.db.ok()
	if err != nil {
		return
	}
	return snap.db.get(nil, nil, key, snap.elem.seq, ro)
}

// Has returns true if the DB does contains the given key.
//
// It is safe to modify the contents of the argument after Get returns.
func (snap *Snapshot) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	snap.mu.RLock()
	defer snap.mu.RUnlock()
	if snap.released {
		err = ErrSnapshotReleased
		return
	}
	err = snap.db.ok()
	if err != nil {
		return
	}
	return snap.db.has(nil, nil, key, snap.elem.seq, ro)
}

// NewIterator returns an iterator for the snapshot of the underlying DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. The resultant key/value pairs are guaranteed to be
// consistent.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Value() methods), its content should not be
// modified unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
// Releasing the snapshot doesn't mean releasing the iterator too, the
// iterator would be still valid until released.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (snap *Snapshot) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	snap.mu.Lock()
	defer snap.mu.Unlock()
	if snap.released {
		return iterator.NewEmptyIterator(ErrSnapshotReleased)
	}
	if err := snap.db.ok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}
	// Since iterator already hold version ref, it doesn't need to
	// hold snapshot ref.
	return snap.db.newIterator(nil, nil, snap.elem.seq, slice, ro)
}

// Release releases the snapshot. This will not release any returned
// iterators, the iterators would still be valid until released or the
// underlying DB is closed.
//
// Other methods should not be called after the snapshot has been released.
func (snap *Snapshot) Release() {
	snap.mu.Lock()
	defer snap.mu.Unlock()

	if !snap.released {
		// Clear the finalizer.
		runtime.SetFinalizer(snap, nil)

		snap.released = true
		snap.db.releaseSnapshot(snap.elem)
		atomic.AddInt32(&snap.db.aliveSnaps, -1)
		snap.db = nil
		snap.elem = nil
	}
}
