// Copyright (c) 2026, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func openIdempotentTestDB(t *testing.T, stor storage.Storage) *DB {
	db, err := Open(stor, &opt.Options{})
	if err != nil {
		t.Fatalf("Open: got error: %v", err)
	}
	return db
}

// TestWriteIdempotentDuplicateCommit tests that a batch carrying a commit
// identity is committed at most once: a duplicate commit returns the
// completed result without triggering further visible changes.
func TestWriteIdempotentDuplicateCommit(t *testing.T) {
	db := openIdempotentTestDB(t, storage.NewMemStorage())
	defer db.Close()

	batch := new(Batch)
	batch.Put([]byte("k1"), []byte("v1"))
	batch.Put([]byte("k2"), []byte("v2"))
	id := batch.CommitID()
	if id == 0 {
		t.Fatal("CommitID: got zero identity")
	}
	if batch.CommitID() != id {
		t.Fatal("CommitID: identity is not stable")
	}

	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("Write: got error: %v", err)
	}
	if !batch.Committed() {
		t.Fatal("batch not marked as committed")
	}
	commitSeq := batch.CommitSeq()
	if commitSeq != 1 {
		t.Fatalf("CommitSeq: got %d, want 1", commitSeq)
	}
	seq := db.getSeq()
	if seq != 2 {
		t.Fatalf("db seq: got %d, want 2", seq)
	}

	// Duplicate commit of the same batch: must return the completed result
	// without applying the batch again.
	for i := 0; i < 3; i++ {
		if err := db.Write(batch, nil); err != nil {
			t.Fatalf("duplicate Write: got error: %v", err)
		}
	}
	if got := db.getSeq(); got != seq {
		t.Fatalf("duplicate commit advanced the sequence number: got %d, want %d", got, seq)
	}
	if got := batch.CommitSeq(); got != commitSeq {
		t.Fatalf("duplicate commit changed the commit seq: got %d, want %d", got, commitSeq)
	}

	// Duplicate commit of a reconstructed batch carrying the same identity
	// (e.g. rebuilt from the journal) must also be a no-op.
	batch2 := new(Batch)
	if err := batch2.Load(batch.Dump()); err != nil {
		t.Fatalf("Load: got error: %v", err)
	}
	batch2.commitID = id
	if err := db.Write(batch2, nil); err != nil {
		t.Fatalf("reconstructed duplicate Write: got error: %v", err)
	}
	if got := db.getSeq(); got != seq {
		t.Fatalf("reconstructed duplicate commit advanced the sequence number: got %d, want %d", got, seq)
	}

	// The data must be visible exactly once.
	for k, v := range map[string]string{"k1": "v1", "k2": "v2"} {
		got, err := db.Get([]byte(k), nil)
		if err != nil {
			t.Fatalf("Get(%q): got error: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("Get(%q): got %q, want %q", k, got, v)
		}
	}

	// Modifying the batch invalidates its commit identity; the modified
	// batch is committed as a new batch.
	batch.Put([]byte("k3"), []byte("v3"))
	if batch.Committed() {
		t.Fatal("modified batch is still marked as committed")
	}
	if batch.CommitID() == id {
		t.Fatal("modified batch keeps its old commit identity")
	}
	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("Write of modified batch: got error: %v", err)
	}
	if got, want := db.getSeq(), seq+3; got != want {
		t.Fatalf("db seq after modified commit: got %d, want %d", got, want)
	}
}

// TestWriteIdempotentReplayInterruptedCommit tests that a commit interrupted
// after the journal write but before the memdb apply is completed by the
// retry with the original sequence number and content.
func TestWriteIdempotentReplayInterruptedCommit(t *testing.T) {
	db := openIdempotentTestDB(t, storage.NewMemStorage())
	defer db.Close()

	batch := new(Batch)
	batch.Put([]byte("rk1"), []byte("rv1"))
	batch.Put([]byte("rk2"), []byte("rv2"))
	id := batch.CommitID()

	// Simulate an interrupted commit: the batch was journaled with
	// sequence number 1 but never applied to the memdb.
	db.registerBatchCommit(id, &batchCommit{seq: 1})

	// The retry must replay the batch with its original sequence number
	// instead of generating a new one.
	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("Write: got error: %v", err)
	}
	if got := db.getSeq(); got != 2 {
		t.Fatalf("db seq: got %d, want 2", got)
	}
	if got := batch.CommitSeq(); got != 1 {
		t.Fatalf("CommitSeq: got %d, want 1", got)
	}
	for k, v := range map[string]string{"rk1": "rv1", "rk2": "rv2"} {
		got, err := db.Get([]byte(k), nil)
		if err != nil {
			t.Fatalf("Get(%q): got error: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("Get(%q): got %q, want %q", k, got, v)
		}
	}

	// Now the commit is completed; further retries are plain duplicates.
	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("duplicate Write: got error: %v", err)
	}
	if got := db.getSeq(); got != 2 {
		t.Fatalf("duplicate commit advanced the sequence number: got %d, want 2", got)
	}
}

// TestWriteIdempotentCrashReplay tests that a committed batch is replayed
// from the journal with its original sequence number after a crash, and
// that the DB sequence number is preserved.
func TestWriteIdempotentCrashReplay(t *testing.T) {
	stor := storage.NewMemStorage()
	db := openIdempotentTestDB(t, stor)

	b1 := new(Batch)
	b1.Put([]byte("a"), []byte("1"))
	b1.Put([]byte("b"), []byte("2"))
	b1.CommitID()
	if err := db.Write(b1, &opt.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("Write: got error: %v", err)
	}
	// Duplicate commit must not be journaled again.
	if err := db.Write(b1, &opt.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("duplicate Write: got error: %v", err)
	}
	b2 := new(Batch)
	b2.Put([]byte("c"), []byte("3"))
	if err := db.Write(b2, &opt.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("Write: got error: %v", err)
	}
	wantSeq := db.getSeq()
	if wantSeq != 3 {
		t.Fatalf("db seq: got %d, want 3", wantSeq)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: got error: %v", err)
	}

	// Reopen; the journal is replayed with the original sequence numbers.
	db = openIdempotentTestDB(t, stor)
	defer db.Close()
	for k, v := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		got, err := db.Get([]byte(k), nil)
		if err != nil {
			t.Fatalf("Get(%q): got error: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("Get(%q): got %q, want %q", k, got, v)
		}
	}
	if got := db.getSeq(); got != wantSeq {
		t.Fatalf("db seq after replay: got %d, want %d", got, wantSeq)
	}

	// New writes continue past the replayed sequence numbers.
	if err := db.Put([]byte("d"), []byte("4"), nil); err != nil {
		t.Fatalf("Put: got error: %v", err)
	}
	if got := db.getSeq(); got != wantSeq+1 {
		t.Fatalf("db seq after new write: got %d, want %d", got, wantSeq+1)
	}
}

// TestWriteIdempotentConcurrentRead tests that duplicate commits interleaved
// with concurrent reads never expose a partially applied batch, and never
// apply the batch more than once.
func TestWriteIdempotentConcurrentRead(t *testing.T) {
	db := openIdempotentTestDB(t, storage.NewMemStorage())
	defer db.Close()

	const nkeys = 20
	batch := new(Batch)
	for i := 0; i < nkeys; i++ {
		batch.Put([]byte(fmt.Sprintf("key%03d", i)), []byte("value"))
	}
	batch.CommitID()

	stop := make(chan struct{})
	errC := make(chan error, 4)
	var wg sync.WaitGroup

	// Readers: the batch must be visible atomically, either all of its
	// keys or none of them.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap, err := db.GetSnapshot()
				if err != nil {
					select {
					case errC <- fmt.Errorf("GetSnapshot: %v", err):
					default:
					}
					return
				}
				present := 0
				for i := 0; i < nkeys; i++ {
					v, err := snap.Get([]byte(fmt.Sprintf("key%03d", i)), nil)
					if err == nil {
						if !bytes.Equal(v, []byte("value")) {
							snap.Release()
							select {
							case errC <- fmt.Errorf("key%03d: got %q", i, v):
							default:
							}
							return
						}
						present++
					} else if err != ErrNotFound {
						snap.Release()
						select {
						case errC <- fmt.Errorf("Get: %v", err):
						default:
						}
						return
					}
				}
				snap.Release()
				if present != 0 && present != nkeys {
					select {
					case errC <- fmt.Errorf("partially applied batch visible: %d of %d keys", present, nkeys):
					default:
					}
					return
				}
			}
		}()
	}

	// Writer: commit the same batch over and over (duplicate commits),
	// interleaved with unique batches.
	const nuniq = 50
	for i := 0; i < nuniq; i++ {
		if err := db.Write(batch, nil); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("Write: got error: %v", err)
		}
		uniq := new(Batch)
		uniq.Put([]byte(fmt.Sprintf("uniq%03d", i)), []byte("v"))
		if err := db.Write(uniq, nil); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("Write: got error: %v", err)
		}
	}
	close(stop)
	wg.Wait()

	select {
	case err := <-errC:
		t.Fatal(err)
	default:
	}

	// The duplicate commits must not have consumed sequence numbers:
	// nkeys for the first commit plus one per unique batch.
	if got, want := db.getSeq(), uint64(nkeys+nuniq); got != want {
		t.Fatalf("db seq: got %d, want %d", got, want)
	}
}

// TestBatchCorruptedNotApplied tests that a corrupted batch is rejected with
// a clear error and is never partially applied.
func TestBatchCorruptedNotApplied(t *testing.T) {
	good := new(Batch)
	good.Put([]byte("key1"), []byte("val1"))
	good.Put([]byte("key2"), []byte("val2"))
	payload := good.Dump()

	// Offset of the second record in the payload.
	const rec1Len = 1 + 1 + 4 + 1 + 4 // type + klen + key + vlen + value

	cases := []struct {
		name     string
		payload  []byte
		seq      uint64
		batchLen int
	}{
		{"truncated", payload[:len(payload)-2], 1, 2},
		{"bad-record-type", func() []byte {
			p := append([]byte(nil), payload...)
			p[rec1Len] = 0x7f
			return p
		}(), 1, 2},
		{"bad-records-length", payload, 1, 3},
		{"invalid-seq", payload, 0, 2},
	}
	for _, c := range cases {
		mdb := memdb.New(comparer.DefaultComparer, 0)
		data := encodeBatchHeader(nil, c.seq, c.batchLen)
		data = append(data, c.payload...)
		expectSeq := uint64(0)
		if c.name == "invalid-seq" {
			expectSeq = 5
		}
		_, _, err := decodeBatchToMem(data, expectSeq, mdb)
		if err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
			continue
		}
		if !errors.IsCorrupted(err) {
			t.Errorf("%s: expected corrupted error, got %v", c.name, err)
		}
		if mdb.Len() != 0 {
			t.Errorf("%s: corrupted batch partially applied: memdb len=%d", c.name, mdb.Len())
		}
	}

	// Sanity check: the intact batch decodes and applies fully.
	mdb := memdb.New(comparer.DefaultComparer, 0)
	data := encodeBatchHeader(nil, 1, 2)
	data = append(data, payload...)
	seq, batchLen, err := decodeBatchToMem(data, 0, mdb)
	if err != nil {
		t.Fatalf("decodeBatchToMem: got error: %v", err)
	}
	if seq != 1 || batchLen != 2 {
		t.Fatalf("decodeBatchToMem: got seq=%d len=%d, want seq=1 len=2", seq, batchLen)
	}
	if mdb.Len() != 2 {
		t.Fatalf("decodeBatchToMem: memdb len=%d, want 2", mdb.Len())
	}
}

// TestWriteIdempotentCorruptedJournalReplay tests that a corrupted batch in
// the journal is skipped during replay with a clear error logged, without
// advancing the last sequence number.
func TestWriteIdempotentCorruptedJournalReplay(t *testing.T) {
	stor := storage.NewMemStorage()
	db := openIdempotentTestDB(t, stor)
	if err := db.Put([]byte("good"), []byte("1"), &opt.WriteOptions{Sync: true}); err != nil {
		t.Fatalf("Put: got error: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: got error: %v", err)
	}

	// Corrupt the first record type byte of the batch payload inside the
	// journal: 7 bytes journal chunk header + 12 bytes batch header.
	fds, err := stor.List(storage.TypeJournal)
	if err != nil || len(fds) == 0 {
		t.Fatalf("cannot list journals: fds=%v err=%v", fds, err)
	}
	sortFds(fds)
	fd := fds[len(fds)-1]
	r, err := stor.Open(fd)
	if err != nil {
		t.Fatalf("cannot open journal: %v", err)
	}
	buf, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("cannot read journal: %v", err)
	}
	const payloadOffset = 7 + 12
	if len(buf) <= payloadOffset {
		t.Fatalf("journal too small: %d bytes", len(buf))
	}
	buf[payloadOffset] ^= 0x7f
	if err := stor.Remove(fd); err != nil {
		t.Fatalf("cannot remove journal: %v", err)
	}
	w, err := stor.Create(fd)
	if err != nil {
		t.Fatalf("cannot create journal: %v", err)
	}
	if _, err := w.Write(buf); err != nil {
		t.Fatalf("cannot write journal: %v", err)
	}
	w.Close()

	// Reopen; the corrupted batch must be skipped and the sequence number
	// must not be advanced.
	db = openIdempotentTestDB(t, stor)
	defer db.Close()
	if got := db.getSeq(); got != 0 {
		t.Fatalf("db seq: got %d, want 0 (corrupted batch must not advance the sequence number)", got)
	}
	if _, err := db.Get([]byte("good"), nil); err != ErrNotFound {
		t.Fatalf("Get: got %v, want ErrNotFound", err)
	}

	// The DB keeps accepting writes.
	if err := db.Put([]byte("x"), []byte("y"), nil); err != nil {
		t.Fatalf("Put: got error: %v", err)
	}
	if got := db.getSeq(); got != 1 {
		t.Fatalf("db seq after new write: got %d, want 1", got)
	}
}

// TestBatchCommitResetAndClose tests that resetting a batch cancels its
// commit identity and that closing the DB releases the recorded batch
// commits.
func TestBatchCommitResetAndClose(t *testing.T) {
	db := openIdempotentTestDB(t, storage.NewMemStorage())

	batch := new(Batch)
	batch.Put([]byte("k"), []byte("v"))
	id := batch.CommitID()
	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("Write: got error: %v", err)
	}
	if !batch.Committed() || batch.CommitSeq() != 1 {
		t.Fatalf("batch not committed: committed=%v seq=%d", batch.Committed(), batch.CommitSeq())
	}
	seq := db.getSeq()

	// Duplicate commit returns the completed result.
	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("duplicate Write: got error: %v", err)
	}
	if got := db.getSeq(); got != seq {
		t.Fatalf("duplicate commit advanced the sequence number: got %d, want %d", got, seq)
	}

	// Reset cancels the commit identity; the batch can be reused as a
	// brand new batch.
	batch.Reset()
	if batch.getCommitID() != 0 || batch.CommitSeq() != 0 || batch.Committed() {
		t.Fatal("Reset did not cancel the commit identity")
	}
	batch.Put([]byte("k2"), []byte("v2"))
	if newID := batch.CommitID(); newID == id {
		t.Fatal("commit identity not regenerated after Reset")
	}
	if err := db.Write(batch, nil); err != nil {
		t.Fatalf("Write after Reset: got error: %v", err)
	}
	if got, want := db.getSeq(), seq+1; got != want {
		t.Fatalf("db seq after re-commit: got %d, want %d", got, want)
	}
	if got := batch.CommitSeq(); got != seq+1 {
		t.Fatalf("CommitSeq after re-commit: got %d, want %d", got, seq+1)
	}

	// Close releases the recorded batch commits.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: got error: %v", err)
	}
	if db.batchCommits != nil {
		t.Fatal("batch commits not released on Close")
	}

	// Write after close fails and touches nothing.
	if err := db.Write(batch, nil); err != ErrClosed {
		t.Fatalf("Write after Close: got %v, want ErrClosed", err)
	}
}
