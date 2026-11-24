// Copyright (c) 2026, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/testutil"
)

// allTableNums returns the file numbers of all tables in the current
// version.
func allTableNums(db *DB) (nums []int64) {
	v := db.s.version()
	defer v.release()
	for _, tables := range v.levels {
		for _, t := range tables {
			nums = append(nums, t.fd.Num)
		}
	}
	return
}

// storageHasTable reports whether the storage still contains the given
// table file.
func storageHasTable(stor *testutil.Storage, num int64) bool {
	fds, err := stor.List(storage.TypeTable)
	if err != nil {
		return false
	}
	for _, fd := range fds {
		if fd.Num == num {
			return true
		}
	}
	return false
}

// waitTableRemoved waits until the given table file is removed from the
// storage.
func waitTableRemoved(t *testing.T, stor *testutil.Storage, num int64) {
	t.Helper()
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		if !storageHasTable(stor, num) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("table @%d is not removed", num)
}

// TestDB_LongLivedSnapshotPinsTables tests that a long-lived snapshot
// keeps the tables of its version alive: compactions must not remove the
// old tables until every snapshot and iterator releases them, and writes
// that happen while a compaction runs must not change the snapshot's
// view.
func TestDB_LongLivedSnapshotPinsTables(t *testing.T) {
	trun(t, func(h *dbHarness) {
		db := h.db

		h.put("foo", "v1")
		h.compactMem()

		// The snapshot pins the version containing the first table.
		nums := allTableNums(db)
		if len(nums) != 1 {
			t.Fatalf("invalid table count, want=1 got=%d", len(nums))
		}
		t1 := nums[0]
		snap := h.getSnapshot()

		// Overwrite and delete the key; each write goes into a newer
		// version and must not change the snapshot's view.
		h.put("foo", "v2")
		h.compactMem()
		h.delete("foo")
		h.compactMem()

		// Compact everything down; the old table is dropped from the
		// current version but must stay around for the snapshot.
		h.compactRange("", "")

		// The snapshot view is unchanged.
		h.getValr(snap, "foo", "v1")
		iter := snap.NewIterator(nil, nil)
		if !iter.First() {
			t.Fatal("snapshot iterator: empty")
		}
		if string(iter.Key()) != "foo" || string(iter.Value()) != "v1" {
			t.Errorf("snapshot iterator: got %q->%q, want foo->v1", iter.Key(), iter.Value())
		}
		if iter.Next() {
			t.Error("snapshot iterator: more than one entry")
		}
		if err := iter.Error(); err != nil {
			t.Error("snapshot iterator: ", err)
		}
		iter.Release()

		// The current view reflects the deletion.
		h.get("foo", false)

		// The old table is still there, pinned by the snapshot.
		if !storageHasTable(h.stor, t1) {
			t.Errorf("table @%d removed while snapshot is alive", t1)
		}

		// Releasing the snapshot unpins the version; the old table is
		// removed once no snapshot and iterator references it.
		snap.Release()
		waitTableRemoved(t, h.stor, t1)
	})
}

// TestDB_SnapshotBlocksNoProgressCompaction tests that compaction
// selection takes the snapshot horizon into account: tables that were
// already rebuilt under the current snapshot horizon are not compacted
// over and over again, and become eligible again once the snapshot is
// released.
func TestDB_SnapshotBlocksNoProgressCompaction(t *testing.T) {
	o := &opt.Options{
		DisableLargeBatchTransaction: true,
		// Keep every level oversized so that a compaction is always
		// needed score-wise.
		CompactionTotalSize:           1,
		CompactionTotalSizeMultiplier: 1,
	}
	h := newDbHarnessWopt(t, o)
	defer h.close()

	db := h.db

	h.put("foo", "v1")
	h.compactMem()
	h.put("foo", "v2")
	h.compactMem()

	// Rebuild all tables under the current horizon.
	h.compactRange("", "")

	snap := h.getSnapshot()
	defer snap.Release()

	// Wait for compactions that were already in flight when the snapshot
	// was acquired.
	h.waitCompaction()

	// All tables were rebuilt under the horizon now pinned by the
	// snapshot; there is no productive compaction to do.
	if db.tableNeedCompaction() {
		t.Error("tableNeedCompaction: got true, want false (all tables already compacted under the snapshot horizon)")
	}
	minSeq, pinned := db.compactionHorizon()
	if !pinned {
		t.Fatal("compactionHorizon: got unpinned, want pinned by live snapshot")
	}
	if c := db.s.pickCompaction(minSeq, pinned); c != nil {
		c.release()
		t.Error("pickCompaction: got a compaction, want nil (no progress possible)")
	}

	// The compaction loop must stay idle: no table may be written while
	// every candidate was already rebuilt under the pinned horizon.
	h.stor.ResetCounter(testutil.ModeWrite, storage.TypeTable)
	time.Sleep(200 * time.Millisecond)
	if n, _ := h.stor.Counter(testutil.ModeWrite, storage.TypeTable); n != 0 {
		t.Errorf("compaction made progress while blocked by snapshot: %d table(s) written", n)
	}

	// Releasing the snapshot advances the horizon; the tables become
	// eligible again.
	snap.Release()
	if !db.tableNeedCompaction() {
		t.Error("tableNeedCompaction: got false, want true after snapshot release")
	}
}

// TestDB_CompactionCommitFailureRestoresState tests that a failed
// compaction commit is a rollback-able switch: the retried commit
// succeeds with the same record, the new version is installed exactly
// once, and the table reference counts settle to exactly the tables of
// the installed version.
func TestDB_CompactionCommitFailureRestoresState(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	db := h.db

	h.put("foo", "v1")
	h.compactMem()
	h.put("foo", "v2")
	h.compactMem()

	oldNums := allTableNums(db)
	if len(oldNums) != 2 {
		t.Fatalf("invalid table count, want=2 got=%d", len(oldNums))
	}

	// Pause the background table compaction so that the compaction below
	// doesn't race with it.
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseC <- (chan<- struct{})(resumeC):
	case <-time.After(10 * time.Second):
		t.Fatal("cannot pause table compaction")
	}
	defer func() {
		// Resume the background table compaction.
		select {
		case <-resumeC:
		case <-time.After(10 * time.Second):
			t.Error("cannot resume table compaction")
		}
	}()

	// Fail the next manifest sync once; the commit transact retries and
	// the switch must complete exactly once. A sync error is used because
	// it is retryable: the journal writer latches write errors for the
	// rest of the session.
	h.stor.EmulateErrorOnce(testutil.ModeSync, storage.TypeManifest, errors.New("manifest sync error"))
	if err := db.tableRangeCompaction(0, nil, nil); err != nil {
		t.Fatal("tableRangeCompaction: got error: ", err)
	}

	// The switch is installed: old tables are gone from the version, a
	// single new table replaced them, and the data is intact.
	h.getVal("foo", "v2")
	newNums := allTableNums(db)
	if len(newNums) != 1 {
		t.Fatalf("invalid table count after compaction, want=1 got=%d", len(newNums))
	}
	for _, num := range oldNums {
		if newNums[0] == num {
			t.Fatalf("table @%d still installed after compaction", num)
		}
	}

	// The reference counts settle to exactly the installed table; the
	// failed attempt must not leak or lose references.
	until := time.Now().Add(10 * time.Second)
	for {
		refc := make(chan map[int64]int)
		db.s.fileRefCh <- refc
		refs := <-refc
		if len(refs) == 1 {
			if ref, ok := refs[newNums[0]]; ok && ref == 1 {
				break
			}
		}
		if time.Now().After(until) {
			t.Fatalf("invalid table references, want={%d:1} got=%v", newNums[0], refs)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDB_CloseWakesCompactionWaiter tests that closing the DB wakes up
// operations waiting on a compaction.
func TestDB_CloseWakesCompactionWaiter(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()

	db := h.db
	h.put("k", "v")

	// Pause table compaction so that the waiter below cannot be served.
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseC <- (chan<- struct{})(resumeC):
	case <-time.After(10 * time.Second):
		t.Fatal("cannot pause table compaction")
	}
	defer close(resumeC)

	errC := make(chan error, 1)
	go func() {
		errC <- db.compTriggerWait(db.tcompCmdC)
	}()

	// The waiter must be blocked while compaction is paused.
	select {
	case err := <-errC:
		t.Fatalf("compTriggerWait returned prematurely: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Closing the DB must wake the waiter up.
	if err := db.Close(); err != nil {
		t.Error("Close: got error: ", err)
	}
	select {
	case err := <-errC:
		if err != ErrClosed {
			t.Errorf("compTriggerWait: got %v, want %v", err, ErrClosed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("compTriggerWait is not woken by Close")
	}
}
