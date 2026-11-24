// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (db *DB) writeJournal(batches []*Batch, seq uint64, sync bool) error {
	wr, err := db.journal.Next()
	if err != nil {
		return err
	}
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil {
		return err
	}
	if err := db.journal.Flush(); err != nil {
		return err
	}
	if sync {
		return db.journalWriter.Sync()
	}
	return nil
}

func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	err = db.compTriggerWait(db.mcompCmdC)
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.
	mem, err = db.newMem(n)
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	if wait {
		err = db.compTriggerWait(db.mcompCmdC)
	} else {
		db.compTrigger(db.mcompCmdC)
	}
	return
}

func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {
	delayed := false
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger()
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger()
	flush := func() (retry bool) {
		mdb = db.getEffectiveMem()
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			if retry {
				mdb.decref()
				mdb = nil
			}
		}()
		tLen := db.s.tLen(0)
		mdbFree = mdb.Free()
		switch {
		case tLen >= slowdownTrigger && !delayed:
			delayed = true
			time.Sleep(time.Millisecond)
		case mdbFree >= n:
			return false
		case tLen >= pauseTrigger:
			delayed = true
			// Set the write paused flag explicitly.
			atomic.StoreInt32(&db.inWritePaused, 1)
			err = db.compTriggerWait(db.tcompCmdC)
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}
		default:
			// Allow memdb to grow if it has no entry.
			if mdb.Len() == 0 {
				mdbFree = n
			} else {
				mdb.decref()
				mdb, err = db.rotateMem(n, false)
				if err == nil {
					mdbFree = mdb.Free()
				} else {
					mdbFree = 0
				}
			}
			return false
		}
		return true
	}
	start := time.Now()
	for flush() {
	}
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

type writeMerge struct {
	sync       bool
	batch      *Batch
	keyType    keyType
	key, value []byte
}

// batchCommit records the state of a batch commit, identified by the batch
// commit identity (see Batch.CommitID). It makes a batch commit idempotent:
// a retried or replayed commit reuses the recorded sequence number and
// result instead of being applied again with a freshly generated sequence
// number.
type batchCommit struct {
	seq     uint64 // sequence number assigned to the first record of the batch
	applied bool   // whether the batch has been applied to the memdb
	err     error  // commit result
}

func (db *DB) getBatchCommit(id uint64) (batchCommit, bool) {
	db.batchCommitMu.Lock()
	defer db.batchCommitMu.Unlock()
	c, ok := db.batchCommits[id]
	if !ok {
		return batchCommit{}, false
	}
	return *c, true
}

func (db *DB) registerBatchCommit(id uint64, c *batchCommit) {
	db.batchCommitMu.Lock()
	if db.batchCommits == nil {
		db.batchCommits = make(map[uint64]*batchCommit)
	}
	db.batchCommits[id] = c
	db.batchCommitMu.Unlock()
}

// finishBatchCommit marks the commit as applied, with the given result.
func (db *DB) finishBatchCommit(id uint64, err error) {
	db.batchCommitMu.Lock()
	if c, ok := db.batchCommits[id]; ok {
		c.applied = true
		c.err = err
	}
	db.batchCommitMu.Unlock()
}

// failBatchCommit records a commit failure. The batch is already journaled,
// so a retried commit will replay it with its original sequence number.
func (db *DB) failBatchCommit(id uint64, err error) {
	db.batchCommitMu.Lock()
	if c, ok := db.batchCommits[id]; ok {
		c.err = err
	}
	db.batchCommitMu.Unlock()
}

// clearBatchCommits releases all recorded batch commits. Used by DB.Close.
func (db *DB) clearBatchCommits() {
	db.batchCommitMu.Lock()
	db.batchCommits = nil
	db.batchCommitMu.Unlock()
}

// replayBatchCommit completes an interrupted batch commit: the batch was
// journaled with its original sequence number but hasn't been applied to
// the memdb. The batch is replayed with its recorded sequence number and
// content; no new sequence number is generated.
func (db *DB) replayBatchCommit(batch *Batch, id uint64) error {
	// Acquire write lock.
	select {
	case db.writeLockC <- struct{}{}:
		// Write lock acquired.
	case err := <-db.compPerErrC:
		// Compaction error.
		return err
	case <-db.closeC:
		// Closed
		return ErrClosed
	}

	c, ok := db.getBatchCommit(id)
	if !ok {
		// The commit record is gone; this can only happen when the DB is
		// being closed.
		<-db.writeLockC
		return ErrClosed
	}
	var err error
	if c.applied {
		// Another goroutine completed the commit meanwhile.
		err = c.err
	} else {
		err = db.replayBatchCommitLocked(batch, id, c.seq)
	}
	<-db.writeLockC
	return err
}

// replayBatchCommitLocked replays a journaled batch into the memdb with its
// recorded sequence number. The write lock must be held.
func (db *DB) replayBatchCommitLocked(batch *Batch, id uint64, seq uint64) error {
	mdb, _, err := db.flush(batch.internalLen)
	if err != nil {
		return err
	}
	defer mdb.decref()

	if err := batch.putMem(seq, mdb.DB); err != nil {
		db.failBatchCommit(id, err)
		return err
	}

	// Advance the sequence number past the replayed batch; it may already
	// be beyond it if other batches were committed meanwhile.
	if nseq := seq + uint64(batch.Len()) - 1; nseq > db.seq {
		db.setSeq(nseq)
	}
	atomic.StoreUint64(&batch.commitSeq, seq)
	db.finishBatchCommit(id, nil)
	return nil
}

func (db *DB) unlockWrite(overflow bool, merged int, err error) {
	for i := 0; i < merged; i++ {
		db.writeAckC <- err
	}
	if overflow {
		// Pass lock to the next write (that failed to merge).
		db.writeMergedC <- false
	} else {
		// Release lock.
		<-db.writeLockC
	}
}

// ourBatch is batch that we can modify.
func (db *DB) writeLocked(batch, ourBatch *Batch, merge, sync bool) error {
	// Idempotent commit: if the batch carries a commit identity and a
	// commit with the same identity has been recorded, don't apply the
	// batch again; complete or return the recorded commit instead.
	if id := batch.getCommitID(); id != 0 {
		if c, ok := db.getBatchCommit(id); ok {
			var err error
			if c.applied {
				// Duplicate commit; return the recorded result without
				// re-applying the batch.
				err = c.err
			} else {
				// The previous commit was interrupted after the batch had
				// been journaled but before it was applied to the memdb.
				// Replay the batch with its original sequence number.
				err = db.replayBatchCommitLocked(batch, id, c.seq)
			}
			db.unlockWrite(false, 0, nil)
			return err
		}
	}

	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	mdb, mdbFree, err := db.flush(batch.internalLen)
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref()

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch}
	)

	if merge {
		// Merge limit.
		var mergeLimit int
		if batch.internalLen > 128<<10 {
			mergeLimit = (1 << 20) - batch.internalLen
		} else {
			mergeLimit = 128 << 10
		}
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap {
			mergeLimit = mergeCap
		}

	merge:
		for mergeLimit > 0 {
			select {
			case incoming := <-db.writeMergeC:
				if incoming.batch != nil {
					// Idempotent commit: don't merge a batch that was
					// already committed; acknowledge it with the
					// recorded result instead.
					if id := incoming.batch.getCommitID(); id != 0 {
						if c, ok := db.getBatchCommit(id); ok {
							db.writeMergedC <- true
							db.writeAckC <- c.err
							continue
						}
					}
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit {
						overflow = true
						break merge
					}
					batches = append(batches, incoming.batch)
					mergeLimit -= incoming.batch.internalLen
				} else {
					// Merge put.
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						overflow = true
						break merge
					}
					if ourBatch == nil {
						ourBatch = db.batchPool.Get().(*Batch)
						ourBatch.Reset()
						batches = append(batches, ourBatch)
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync
				merged++
				db.writeMergedC <- true

			default:
				break merge
			}
		}
	}

	// Release ourBatch if any.
	if ourBatch != nil {
		defer db.batchPool.Put(ourBatch)
	}

	// Seq number.
	seq := db.seq + 1

	// Write journal.
	if err := db.writeJournal(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}

	// Register commit identities. The batches are now durable, so a
	// retried commit with the same identity will reuse these sequence
	// numbers instead of generating new ones.
	var commitIDs []uint64
	{
		bseq := seq
		for i, b := range batches {
			if id := b.getCommitID(); id != 0 {
				if commitIDs == nil {
					commitIDs = make([]uint64, len(batches))
				}
				db.registerBatchCommit(id, &batchCommit{seq: bseq})
				commitIDs[i] = id
			}
			bseq += uint64(b.Len())
		}
	}

	// Put batches.
	for i, b := range batches {
		if err := b.putMem(seq, mdb.DB); err != nil {
			// The batch is already journaled; record the failure so that
			// a retried commit will replay the batch with its original
			// sequence number instead of committing it again.
			if commitIDs != nil && commitIDs[i] != 0 {
				db.failBatchCommit(commitIDs[i], err)
			}
			db.unlockWrite(overflow, merged, err)
			return err
		}
		if commitIDs != nil && commitIDs[i] != 0 {
			db.finishBatchCommit(commitIDs[i], nil)
		}
		atomic.StoreUint64(&b.commitSeq, seq)
		seq += uint64(b.Len())
	}

	// Incr seq number.
	db.addSeq(uint64(batchesLen(batches)))

	// Rotate memdb if it's reach the threshold.
	if batch.internalLen >= mdbFree {
		if _, err := db.rotateMem(0, false); err != nil {
			db.unlockWrite(overflow, merged, err)
			return err
		}
	}

	db.unlockWrite(overflow, merged, nil)
	return nil
}

// Write apply the given batch to the DB. The batch records will be applied
// sequentially. Write might be used concurrently, when used concurrently and
// batch is small enough, write will try to merge the batches. Set NoWriteMerge
// option to true to disable write merge.
//
// If the batch carries a commit identity (see Batch.CommitID) the commit is
// idempotent: retrying Write with the same batch after a failure reuses the
// originally assigned sequence number and content, and a duplicate commit
// returns the completed result without applying the batch again.
//
// It is safe to modify the contents of the arguments after Write returns but
// not before. Write will not modify content of the batch.
func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}

	// Idempotent commit: if the batch carries a commit identity and a
	// commit with the same identity has been recorded, don't apply the
	// batch again; complete or return the recorded commit instead.
	if id := batch.getCommitID(); id != 0 {
		if c, ok := db.getBatchCommit(id); ok {
			if c.applied {
				// Duplicate commit; return the recorded result.
				return c.err
			}
			// The previous commit was interrupted after the batch had
			// been journaled but before it was applied to the memdb.
			return db.replayBatchCommit(batch, id)
		}
	}

	// If the batch size is larger than write buffer, it may justified to write
	// using transaction instead. Using transaction the batch will be written
	// into tables directly, skipping the journaling.
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		// Sequence number assigned to the first record of the batch.
		commitSeq := tr.seq - uint64(batch.Len()) + 1
		if err := tr.Commit(); err != nil {
			return err
		}
		if id := batch.getCommitID(); id != 0 {
			db.registerBatchCommit(id, &batchCommit{seq: commitSeq, applied: true})
			atomic.StoreUint64(&batch.commitSeq, commitSeq)
		}
		return nil
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked(batch, nil, merge, sync)
}

func (db *DB) putRec(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil {
		return err
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	batch := db.batchPool.Get().(*Batch)
	batch.Reset()
	batch.appendRec(kt, key, value)
	return db.writeLocked(batch, batch, merge, sync)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map. Write merge also applies for Put, see
// Write.
//
// It is safe to modify the contents of the arguments after Put returns but not
// before.
func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeVal, key, value, wo)
}

// Delete deletes the value for the given key. Delete will not returns error if
// key doesn't exist. Write merge also applies for Delete, see Write.
//
// It is safe to modify the contents of the arguments after Delete returns but
// not before.
func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeDel, key, nil, wo)
}

func isMemOverlaps(icmp *iComparer, mem *memdb.DB, min, max []byte) bool {
	iter := mem.NewIterator(nil)
	defer iter.Release()
	return (max == nil || (iter.First() && icmp.uCompare(max, internalKey(iter.Key()).ukey()) >= 0)) &&
		(min == nil || (iter.Last() && icmp.uCompare(min, internalKey(iter.Key()).ukey()) <= 0))
}

// CompactRange compacts the underlying DB for the given key range.
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data. This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
func (db *DB) CompactRange(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdC); err != nil {
			return err
		}
	} else {
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdC, -1, r.Start, r.Limit)
}

// SetReadOnly makes DB read-only. It will stay read-only until reopened.
func (db *DB) SetReadOnly() error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		db.compWriteLocking = true
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Set compaction read-only.
	select {
	case db.compErrSetC <- ErrReadOnly:
	case perr := <-db.compPerErrC:
		return perr
	case <-db.closeC:
		return ErrClosed
	}

	return nil
}
