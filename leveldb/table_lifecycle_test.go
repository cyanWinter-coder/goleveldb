// Copyright (c) 2022, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/testutil"
)

// openTrackingStorage wraps a storage and counts how many table readers are
// currently open. It lets the tests assert that file handles are closed
// exactly when the last reference is dropped.
type openTrackingStorage struct {
	storage.Storage
	mu     sync.Mutex
	open   map[uint64]int32 // packed fd -> open reader count
	opens  int64
	closes int64
}

func newOpenTrackingStorage(inner storage.Storage) *openTrackingStorage {
	return &openTrackingStorage{Storage: inner, open: make(map[uint64]int32)}
}

func (s *openTrackingStorage) pack(fd storage.FileDesc) uint64 {
	return uint64(fd.Num)<<4 | uint64(fd.Type)
}

func (s *openTrackingStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	r, err := s.Storage.Open(fd)
	if err != nil {
		return nil, err
	}
	x := s.pack(fd)
	s.mu.Lock()
	s.open[x]++
	s.mu.Unlock()
	atomic.AddInt64(&s.opens, 1)
	return &trackingReader{Reader: r, s: s, x: x}, nil
}

func (s *openTrackingStorage) openCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.open {
		n += int(c)
	}
	return n
}

func (s *openTrackingStorage) fdOpen(fd storage.FileDesc) int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open[s.pack(fd)]
}

type trackingReader struct {
	storage.Reader
	s *openTrackingStorage
	x uint64
}

func (r *trackingReader) Close() error {
	err := r.Reader.Close()
	r.s.mu.Lock()
	if r.s.open[r.x]--; r.s.open[r.x] <= 0 {
		delete(r.s.open, r.x)
	}
	r.s.mu.Unlock()
	atomic.AddInt64(&r.s.closes, 1)
	return err
}

// newTrackingHarness builds a DB on top of the tracking memory storage.
func newTrackingHarness(t *testing.T, o *opt.Options) (*dbHarness, *openTrackingStorage) {
	t.Helper()
	gomega.RegisterTestingT(t)
	tr := newOpenTrackingStorage(storage.NewMemStorage())
	stor := testutil.NewStorageWith(tr, "")
	stor.OnLog(testingLogger(t))
	db, err := Open(stor, o)
	if err != nil {
		stor.Close()
		t.Fatalf("Open: %v", err)
	}
	return &dbHarness{t: t, stor: stor, db: db, o: o}, tr
}

// firstTable returns any table present in the current version.
func firstTable(t *testing.T, h *dbHarness) *tFile {
	t.Helper()
	v := h.db.s.version()
	defer v.release()
	for _, tables := range v.levels {
		if len(tables) > 0 {
			return tables[0]
		}
	}
	t.Fatal("no table in current version")
	return nil
}

// TestTableOps_OpenFailureNotCached verifies that a table which fails to
// open is never published into the file cache. Repeated attempts keep the
// cache node and size counters balanced.
func TestTableOps_OpenFailureNotCached(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()
	tops := h.db.s.tops

	missing := newTableFile(storage.FileDesc{Type: storage.TypeTable, Num: 99999}, 1, nil, nil)
	for i := 0; i < 5; i++ {
		ch, err := tops.open(missing)
		if err == nil {
			ch.Release()
			t.Fatalf("attempt %d: expected open error", i)
		}
	}
	if n := tops.fileCache.Nodes(); n != 0 {
		t.Fatalf("failed opens leaked cache nodes: %d", n)
	}
	if n := tops.fileCache.Size(); n != 0 {
		t.Fatalf("failed opens leaked cache size: %d", n)
	}
}

// TestTableOps_OpenSetFuncFailureBalanced simulates setFunc returning a nil
// value repeatedly and asserts the cache stays empty and no handle leaks.
func TestTableOps_OpenSetFuncFailureBalanced(t *testing.T) {
	h := newDbHarness(t)
	defer h.close()
	tops := h.db.s.tops

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 424242}
	for i := 0; i < 20; i++ {
		ch := tops.fileCache.Get(tableCacheNS(fd), uint64(fd.Num), func() (int, cache.Value) {
			return 0, nil
		})
		if ch != nil {
			t.Fatalf("iteration %d: expected nil handle on nil value", i)
		}
	}
	if n := tops.fileCache.Nodes(); n != 0 {
		t.Fatalf("nil-value setFunc leaked nodes: %d", n)
	}
	if n := tops.fileCache.Size(); n != 0 {
		t.Fatalf("nil-value setFunc leaked size: %d", n)
	}
}

// TestTableOps_CorruptedReaderNotPublished builds a real table, corrupts its
// footer magic, and asserts the resulting half-initialized reader is closed
// rather than cached.
func TestTableOps_CorruptedReaderNotPublished(t *testing.T) {
	// Build the table against a raw memory storage so corruption can mutate
	// the in-memory bytes without fighting the test storage's open tracking.
	ms := storage.NewMemStorage()
	stor := testutil.NewStorageWith(ms, "")
	stor.OnLog(testingLogger(t))
	db, err := Open(stor, &opt.Options{DisableLargeBatchTransaction: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	h := &dbHarness{t: t, stor: stor, db: db, o: &opt.Options{DisableLargeBatchTransaction: true}}
	defer func() {
		if h.db != nil {
			_ = h.db.Close()
		}
		stor.Close()
	}()

	h.put("a", "val-a")
	h.compactMem()
	tf := firstTable(t, h)
	fd := tf.fd
	tops := h.db.s.tops

	// Populate then drop the healthy cache node.
	ch, err := tops.open(tf)
	if err != nil {
		t.Fatalf("initial open: %v", err)
	}
	ch.Release()
	if n := tops.fileCache.Nodes(); n != 1 {
		t.Fatalf("want 1 cached table, got %d", n)
	}

	// Flip the table's last byte (part of the footer magic number) directly
	// in the underlying memory storage, bypassing the open-file lock.
	mutator, ok := ms.(storage.MemFileMutator)
	if !ok {
		t.Fatal("memory storage does not implement MemFileMutator")
	}
	if err := mutator.MutateFile(fd, func(data []byte) []byte {
		data[len(data)-1] ^= 0xff
		return data
	}); err != nil {
		t.Fatalf("corrupt table: %v", err)
	}

	// Drop the healthy node so the next open rebuilds the reader.
	tops.fileCache.Evict(tableCacheNS(fd), uint64(fd.Num))

	if _, err := tops.open(tf); err == nil {
		t.Fatal("expected corrupted table open to fail")
	}
	if n := tops.fileCache.Nodes(); n != 0 {
		t.Fatalf("corrupted reader was published, nodes=%d", n)
	}
	if n := tops.fileCache.Size(); n != 0 {
		t.Fatalf("corrupted reader charged cache size=%d", n)
	}

	// A subsequent attempt must also fail and keep the counters balanced.
	if _, err := tops.open(tf); err == nil {
		t.Fatal("second corrupted table open unexpectedly succeeded")
	}
	if n := tops.fileCache.Nodes(); n != 0 {
		t.Fatalf("retry leaked cache nodes=%d", n)
	}
}

// TestTableOps_IteratorBorrowsOneReference checks every iterator owns exactly
// one cache handle and releasing it (even repeatedly) returns that handle
// once, driving the file's open-reader count back to zero after eviction.
func TestTableOps_IteratorBorrowsOneReference(t *testing.T) {
	h, tr := newTrackingHarness(t, &opt.Options{DisableLargeBatchTransaction: true})
	defer h.close()

	for i := 0; i < 20; i++ {
		h.put(string(tkey(i)), string(tval(i, 500)))
	}
	h.compactMem()
	h.waitMemCompaction()

	tf := firstTable(t, h)
	fd := tf.fd

	const n = 8
	iters := make([]iterator.Iterator, n)
	for i := range iters {
		iters[i] = h.db.s.tops.newIterator(tf, nil, nil)
	}
	// All iterators share one open file through the single cache node.
	if got := tr.fdOpen(fd); got != 1 {
		t.Fatalf("want single open reader shared via cache, got %d", got)
	}

	for i, it := range iters {
		it.First()
		_ = it.Key()
		it.Release()
		it.Release() // idempotent: reference must drop at most once
		if i%3 == 0 {
			extra, err := h.db.s.tops.open(tf)
			if err != nil {
				t.Fatalf("reopen mid-release: %v", err)
			}
			extra.Release()
		}
	}

	// Once all iterator handles are gone, eviction closes the file.
	h.db.s.tops.fileCache.Evict(tableCacheNS(fd), uint64(fd.Num))
	if got := tr.fdOpen(fd); got != 0 {
		t.Fatalf("file still open after all iterators released, got %d", got)
	}
	if got := atomic.LoadInt64(&tr.opens); got != atomic.LoadInt64(&tr.closes) {
		t.Fatalf("open/close imbalance: opens=%d closes=%d", got, atomic.LoadInt64(&tr.closes))
	}
}

// TestTableOps_RemoveWaitsForReferences verifies the delete path drops the
// table from cache visibility immediately but defers physical file removal
// until every outstanding reader reference is released.
func TestTableOps_RemoveWaitsForReferences(t *testing.T) {
	h, tr := newTrackingHarness(t, &opt.Options{DisableLargeBatchTransaction: true})
	defer h.close()

	h.put("k", "v")
	h.compactMem()
	tf := firstTable(t, h)
	fd := tf.fd

	ch, err := h.db.s.tops.open(tf)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := tr.fdOpen(fd); got != 1 {
		t.Fatalf("want 1 open reader, got %d", got)
	}

	// remove() returns once visibility is dropped; the physical file removal
	// is deferred to the cache callback that fires at ref zero.
	h.db.s.tops.remove(fd)

	// The file must still be present and open while the handle is held.
	if fds, _ := h.stor.List(storage.TypeTable); len(fds) == 0 {
		t.Fatal("table file removed while a handle was outstanding")
	}
	if got := tr.fdOpen(fd); got != 1 {
		t.Fatalf("file closed before last reference released, open=%d", got)
	}

	// Releasing the last reference unblocks the deferred physical removal.
	ch.Release()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fds, _ := h.stor.List(storage.TypeTable); len(fds) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if fds, _ := h.stor.List(storage.TypeTable); len(fds) != 0 {
		t.Fatalf("table file not removed after reference release: %v", fds)
	}
	if got := tr.fdOpen(fd); got != 0 {
		t.Fatalf("removed table still has open readers: %d", got)
	}
}

// TestDB_ConcurrentIteratorsAndCompaction drives iterators, table eviction
// via compaction and close together to surface file-handle races and leaks.
func TestDB_ConcurrentIteratorsAndCompaction(t *testing.T) {
	h, tr := newTrackingHarness(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		OpenFilesCacheCapacity:       16,
	})
	defer h.stor.Close()

	var stop int32
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(1))
		for atomic.LoadInt32(&stop) == 0 {
			h.put(string(tkey(r.Intn(500))), string(tval(r.Intn(1000), 200)))
			if r.Intn(8) == 0 {
				h.compactMem()
			}
		}
	}()

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for atomic.LoadInt32(&stop) == 0 {
				it := h.db.NewIterator(nil, nil)
				for j := 0; j < 16 && it.Next(); j++ {
					_ = it.Key()
					_ = it.Value()
				}
				if err := it.Error(); err != nil {
					t.Errorf("iterator error: %v", err)
				}
				it.Release()
				it.Release()
				time.Sleep(time.Duration(r.Intn(2)) * time.Millisecond)
			}
		}(int64(i) + 100)
	}

	time.Sleep(300 * time.Millisecond)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	if err := h.db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := tr.openCount(); got != 0 {
		t.Fatalf("leaked %d open file handles after close", got)
	}
	if got := atomic.LoadInt64(&tr.opens); got != atomic.LoadInt64(&tr.closes) {
		t.Fatalf("open/close imbalance: opens=%d closes=%d", got, atomic.LoadInt64(&tr.closes))
	}
}

// TestDB_CloseIdempotentConcurrent ensures concurrent Close calls return a
// stable result (one success, the rest ErrClosed) without racing on handles.
func TestDB_CloseIdempotentConcurrent(t *testing.T) {
	h, tr := newTrackingHarness(t, &opt.Options{DisableLargeBatchTransaction: true})
	defer h.stor.Close()
	for i := 0; i < 10; i++ {
		h.put(string(tkey(i)), string(tval(i, 300)))
	}
	h.compactMem()

	const n = 16
	var wg sync.WaitGroup
	var success, closedErr int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := h.db.Close(); {
			case err == nil:
				atomic.AddInt32(&success, 1)
			case errors.Is(err, ErrClosed):
				atomic.AddInt32(&closedErr, 1)
			default:
				t.Errorf("unexpected close error: %v", err)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&success) != 1 {
		t.Fatalf("want exactly 1 successful close, got %d", success)
	}
	if atomic.LoadInt32(&closedErr) != n-1 {
		t.Fatalf("want %d ErrClosed, got %d", n-1, closedErr)
	}
	if got := tr.openCount(); got != 0 {
		t.Fatalf("leaked %d open handles after concurrent close", got)
	}
	if got := atomic.LoadInt64(&tr.opens); got != atomic.LoadInt64(&tr.closes) {
		t.Fatalf("open/close imbalance: opens=%d closes=%d", got, atomic.LoadInt64(&tr.closes))
	}
}
