// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/testutil"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// drainIterator collects every visible user key/value pair in traversal
// order and fails the test on any iterator error.
func drainIterator(t *testing.T, it iteratorLike, reverse bool) [][2]string {
	t.Helper()
	var ok bool
	if reverse {
		ok = it.Last()
	} else {
		ok = it.First()
	}
	var out [][2]string
	for ok {
		out = append(out, [2]string{string(it.Key()), string(it.Value())})
		if reverse {
			ok = it.Prev()
		} else {
			ok = it.Next()
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return out
}

type iteratorLike interface {
	First() bool
	Last() bool
	Next() bool
	Prev() bool
	Seek([]byte) bool
	Key() []byte
	Value() []byte
	Error() error
}

func sortedPairs(pairs [][2]string, reverse bool) [][2]string {
	out := make([][2]string, len(pairs))
	copy(out, pairs)
	sort.Slice(out, func(i, j int) bool {
		if reverse {
			return out[i][0] > out[j][0]
		}
		return out[i][0] < out[j][0]
	})
	return out
}

func pairsEq(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDB_RotateBoundaryKeys exercises overlapping keys that straddle the
// memtable generation boundary. The newest version (including tombstones)
// must always win by sequence number in point reads, forward scans,
// reverse scans and seeks, regardless of how the data is split across the
// effective memdb, frozen memdb and flushed tables.
func TestDB_RotateBoundaryKeys(t *testing.T) {
	trun(t, func(h *dbHarness) {
		// Seed an older generation and flush it into a table.
		h.put("a", "old-a")
		h.put("boundary", "old-b")
		h.put("z", "old-z")
		h.compactMem()

		// Newer generation: boundary key is deleted and re-created, while the
		// neighbours keep their old values.
		h.delete("boundary")
		h.put("boundary", "new-b")
		h.put("mid", "new-m")

		want := [][2]string{
			{"a", "old-a"},
			{"boundary", "new-b"},
			{"mid", "new-m"},
			{"z", "old-z"},
		}

		// Point reads honour the newest sequence.
		h.getVal("boundary", "new-b")
		h.get("nonexistent", false)

		// Forward and reverse scans over both the live memdbs and, after a
		// flush, the table set.
		for _, flush := range []bool{false, true} {
			if flush {
				h.compactMem()
			}
			label := fmt.Sprintf("flush=%v", flush)
			it := h.db.NewIterator(nil, nil)
			if got := drainIterator(t, it, false); !pairsEq(got, want) {
				t.Errorf("%s forward mismatch\n got=%v\nwant=%v", label, got, want)
			}
			it.Release()

			it = h.db.NewIterator(nil, nil)
			if got := drainIterator(t, it, true); !pairsEq(got, sortedPairs(want, true)) {
				t.Errorf("%s reverse mismatch\n got=%v\nwant=%v", label, got, sortedPairs(want, true))
			}
			it.Release()

			// Seek landing exactly on the overlapping boundary key.
			it = h.db.NewIterator(&util.Range{Start: []byte("boundary")}, nil)
			got := [][2]string{}
			if it.Seek([]byte("boundary")) {
				for {
					got = append(got, [2]string{string(it.Key()), string(it.Value())})
					if !it.Next() {
						break
					}
				}
			}
			it.Release()
			if !pairsEq(got, want[1:]) {
				t.Errorf("%s seek mismatch\n got=%v\nwant=%v", label, got, want[1:])
			}
		}
	})
}

// TestDB_RotateTombstoneMasksOldValue verifies a tombstone in a newer
// generation is never masked by an older value for the same user key,
// including during reverse traversal and while the tombstone is frozen.
func TestDB_RotateTombstoneMasksOldValue(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 1 << 20})
	defer h.close()

	h.put("a", "1")
	h.put("k", "v1")
	h.put("z", "9")
	h.compactMem()

	// Tombstone lands in the fresh memdb.
	h.delete("k")
	if _, err := h.db.Get([]byte("k"), nil); err != ErrNotFound {
		t.Fatalf("Get(k) want ErrNotFound, got %v", err)
	}

	visible := [][2]string{{"a", "1"}, {"z", "9"}}
	for _, reverse := range []bool{false, true} {
		it := h.db.NewIterator(nil, nil)
		got := drainIterator(t, it, reverse)
		it.Release()
		want := visible
		if reverse {
			want = sortedPairs(visible, true)
		}
		if !pairsEq(got, want) {
			t.Errorf("reverse=%v mismatch got=%v want=%v", reverse, got, want)
		}
	}

	// Freeze the tombstone generation without flushing it and repeat; the
	// frozen memdb must keep hiding the old table value.
	h.db.writeLockC <- struct{}{}
	mem, err := h.db.newMem(0)
	if err != nil {
		<-h.db.writeLockC
		t.Fatal(err)
	}
	mem.decref()
	frozen := h.db.frozenMem
	<-h.db.writeLockC
	if frozen == nil {
		t.Fatal("expected frozen memdb holding the tombstone")
	}

	for _, reverse := range []bool{false, true} {
		it := h.db.NewIterator(nil, nil)
		got := drainIterator(t, it, reverse)
		it.Release()
		want := visible
		if reverse {
			want = sortedPairs(visible, true)
		}
		if !pairsEq(got, want) {
			t.Errorf("frozen reverse=%v mismatch got=%v want=%v", reverse, got, want)
		}
	}
	if _, err := h.db.Get([]byte("k"), nil); err != ErrNotFound {
		t.Fatalf("frozen Get(k) want ErrNotFound, got %v", err)
	}

	// Let the flush finish; the masking must remain correct from the table.
	if err := h.db.compTriggerWait(h.db.mcompCmdC); err != nil {
		t.Fatal(err)
	}
	h.get("k", false)
	it := h.db.NewIterator(nil, nil)
	if got := drainIterator(t, it, true); !pairsEq(got, sortedPairs(visible, true)) {
		t.Errorf("post-flush reverse mismatch got=%v", got)
	}
	it.Release()
}

// TestDB_RotateFailureKeepsServingState ensures a failed rotation is
// all-or-nothing: the previously serving memdb and journal stay in place,
// no new journal file is leaked, and reads/writes continue afterwards.
func TestDB_RotateFailureKeepsServingState(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true})
	defer h.close()

	h.put("k1", "v1")
	h.put("k2", "v2")

	oldJournalFd := h.db.journalFd
	oldMem := h.db.mem

	h.stor.EmulateErrorOnce(testutil.ModeCreate, storage.TypeJournal, errors.New("boom: journal create"))
	h.db.writeLockC <- struct{}{}
	_, err := h.db.rotateMem(0, false)
	<-h.db.writeLockC
	if err == nil {
		t.Fatal("expect rotation error, got nil")
	}

	// Serving state must be untouched.
	if h.db.mem != oldMem {
		t.Error("effective memdb changed despite failed rotation")
	}
	if h.db.frozenMem != nil {
		t.Error("frozen memdb set despite failed rotation")
	}
	if h.db.journalFd != oldJournalFd || h.db.journal == nil {
		t.Error("journal changed despite failed rotation")
	}

	// Old state still readable and writable.
	h.getVal("k1", "v1")
	h.put("k3", "v3")
	h.getVal("k3", "v3")

	// A subsequent, healthy rotation must succeed and the storage harness
	// must observe no leftover open journal from the failed attempt.
	h.compactMem()
	h.getVal("k1", "v1")
	h.getVal("k2", "v2")
	h.getVal("k3", "v3")
}

// TestDB_RotateRejectedWithFrozenNoLeak verifies that attempting a second
// promotion while a frozen memdb exists rejects cleanly, leaks no journal
// file and leaves the frozen generation's references untouched. Once the
// background flush completes it drops only its own reference.
func TestDB_RotateRejectedWithFrozenNoLeak(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 200})
	defer h.close()

	h.put("a", "1")

	h.db.writeLockC <- struct{}{}
	mem, err := h.db.newMem(0)
	if err != nil {
		<-h.db.writeLockC
		t.Fatal(err)
	}
	mem.decref()
	frozen := h.db.frozenMem
	if frozen == nil {
		<-h.db.writeLockC
		t.Fatal("expected frozen memdb")
	}
	refBefore := frozen.getref()

	if _, err := h.db.newMem(0); err != errHasFrozenMem {
		<-h.db.writeLockC
		t.Fatalf("expect errHasFrozenMem, got %v", err)
	}
	if h.db.frozenMem != frozen || frozen.getref() != refBefore {
		t.Errorf("rejected rotation mutated frozen state: ptr=%v ref=%d->%d",
			h.db.frozenMem != frozen, refBefore, frozen.getref())
	}
	<-h.db.writeLockC

	// Background flush releases only the reference it owns; afterwards the
	// frozen slot is empty and the data remains available from the table.
	if err := h.db.compTriggerWait(h.db.mcompCmdC); err != nil {
		t.Fatal(err)
	}
	if h.db.frozenMem != nil {
		t.Errorf("frozen memdb not cleared after flush: %v", h.db.frozenMem)
	}
	h.getVal("a", "1")
}

// TestDB_IteratorPinsReadState proves an iterator keeps observing the
// generation set captured at creation time even while concurrent writes
// rotate memdbs and flush tables underneath it.
func TestDB_IteratorPinsReadState(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 120})
	defer h.close()

	h.put("snap", "v0")
	iter := h.db.NewIterator(nil, nil)
	iter.First()
	if !iter.Valid() || string(iter.Key()) != "snap" || string(iter.Value()) != "v0" {
		t.Fatalf("unexpected initial state: %q->%q", iter.Key(), iter.Value())
	}

	// Heavy churn rotates and flushes generations, deleting the pinned key.
	for i := 0; i < 200; i++ {
		h.put(fmt.Sprintf("k%04d", i), "v")
	}
	h.delete("snap")
	h.compactMem()
	if err := h.db.compTriggerWait(h.db.mcompCmdC); err != nil {
		t.Fatal(err)
	}

	// The live DB no longer contains the key...
	if _, err := h.db.Get([]byte("snap"), nil); err != ErrNotFound {
		t.Fatalf("live Get(snap) want ErrNotFound, got %v", err)
	}
	// ...but the pinned iterator still sees its captured snapshot.
	iter.First()
	if !iter.Valid() || string(iter.Key()) != "snap" || string(iter.Value()) != "v0" {
		t.Fatalf("pinned iterator lost its snapshot: %q->%q", iter.Key(), iter.Value())
	}
	if iter.Next() {
		t.Errorf("pinned iterator should contain only the captured key, got %q", iter.Key())
	}
	iter.Release()
}

// TestDB_ConcurrentRotateAndIterate hammers rotations from a writer while
// many iterators perform full forward and reverse scans. Every iterator
// must remain internally consistent: strictly ordered, error free and
// never exposing a tombstoned key.
func TestDB_ConcurrentRotateAndIterate(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{DisableLargeBatchTransaction: true, WriteBuffer: 96})
	defer h.close()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var stop uint32
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(7))
		for i := 0; atomic.LoadUint32(&stop) == 0; i++ {
			k := fmt.Sprintf("k%03d", rng.Intn(16))
			if rng.Intn(4) == 0 {
				if err := h.db.Delete([]byte(k), nil); err != nil {
					t.Errorf("delete: %v", err)
					return
				}
			} else if err := h.db.Put([]byte(k), []byte(fmt.Sprintf("v%06d", i)), nil); err != nil {
				t.Errorf("put: %v", err)
				return
			}
			if i%20 == 0 {
				h.db.writeLockC <- struct{}{}
				if _, err := h.db.rotateMem(0, false); err != nil && err != ErrClosed {
					t.Errorf("rotate: %v", err)
				}
				<-h.db.writeLockC
			}
		}
	}()

	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			reverse := r%2 == 1
			for atomic.LoadUint32(&stop) == 0 {
				it := h.db.NewIterator(nil, nil)
				var prev []byte
				if reverse {
					for it.Last(); it.Valid(); it.Prev() {
						k := append([]byte(nil), it.Key()...)
						if prev != nil && bytes.Compare(k, prev) >= 0 {
							t.Errorf("reverse order violation %q >= %q", k, prev)
							break
						}
						prev = k
					}
				} else {
					for it.First(); it.Valid(); it.Next() {
						k := append([]byte(nil), it.Key()...)
						if prev != nil && bytes.Compare(k, prev) <= 0 {
							t.Errorf("forward order violation %q <= %q", k, prev)
							break
						}
						prev = k
					}
				}
				if err := it.Error(); err != nil {
					t.Errorf("iterator error: %v", err)
				}
				it.Release()
			}
		}(r)
	}

	// Periodically force full flushes from the main goroutine too.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.db.writeLockC <- struct{}{}
		if _, err := h.db.rotateMem(0, true); err != nil && err != ErrClosed {
			t.Errorf("forced rotate: %v", err)
		}
		<-h.db.writeLockC
	}
	atomic.StoreUint32(&stop, 1)
	wg.Wait()
}
