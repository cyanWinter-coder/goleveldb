// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

const ctValSize = 1000

type dbCorruptHarness struct {
	dbHarness
}

func newDbCorruptHarnessWopt(t *testing.T, o *opt.Options) *dbCorruptHarness {
	h := new(dbCorruptHarness)
	h.init(t, o)
	return h
}

func newDbCorruptHarness(t *testing.T) *dbCorruptHarness {
	return newDbCorruptHarnessWopt(t, &opt.Options{
		BlockCacheCapacity: 100,
		Strict:             opt.StrictJournalChecksum,
	})
}

func (h *dbCorruptHarness) recover() {
	p := &h.dbHarness
	t := p.t

	var err error
	p.db, err = Recover(h.stor, h.o)
	if err != nil {
		t.Fatal("Repair: got error: ", err)
	}
}

func (h *dbCorruptHarness) build(n int) {
	p := &h.dbHarness
	t := p.t
	db := p.db

	batch := new(Batch)
	for i := 0; i < n; i++ {
		batch.Reset()
		batch.Put(tkey(i), tval(i, ctValSize))
		err := db.Write(batch, p.wo)
		if err != nil {
			t.Fatal("write error: ", err)
		}
	}
}

func (h *dbCorruptHarness) buildShuffled(n int, rnd *rand.Rand) {
	p := &h.dbHarness
	t := p.t
	db := p.db

	batch := new(Batch)
	for i := range rnd.Perm(n) {
		batch.Reset()
		batch.Put(tkey(i), tval(i, ctValSize))
		err := db.Write(batch, p.wo)
		if err != nil {
			t.Fatal("write error: ", err)
		}
	}
}

func (h *dbCorruptHarness) deleteRand(n, max int, rnd *rand.Rand) {
	p := &h.dbHarness
	t := p.t
	db := p.db

	batch := new(Batch)
	for i := 0; i < n; i++ {
		batch.Reset()
		batch.Delete(tkey(rnd.Intn(max)))
		err := db.Write(batch, p.wo)
		if err != nil {
			t.Fatal("write error: ", err)
		}
	}
}

func (h *dbCorruptHarness) corrupt(ft storage.FileType, fi, offset, n int) {
	p := &h.dbHarness
	t := p.t

	fds, _ := p.stor.List(ft)
	sortFds(fds)
	if fi < 0 {
		fi = len(fds) - 1
	}
	if fi >= len(fds) {
		t.Fatalf("no such file with type %q with index %d", ft, fi)
	}

	fd := fds[fi]
	r, err := h.stor.Open(fd)
	if err != nil {
		t.Fatal("cannot open file: ", err)
	}
	x, err := r.Seek(0, 2)
	if err != nil {
		t.Fatal("cannot query file size: ", err)
	}
	m := int(x)
	if _, err := r.Seek(0, 0); err != nil {
		t.Fatal(err)
	}

	if offset < 0 {
		if -offset > m {
			offset = 0
		} else {
			offset = m + offset
		}
	}
	if offset > m {
		offset = m
	}
	if offset+n > m {
		n = m - offset
	}

	buf := make([]byte, m)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		t.Fatal("cannot read file: ", err)
	}
	r.Close()

	for i := 0; i < n; i++ {
		buf[offset+i] ^= 0x80
	}

	err = h.stor.Remove(fd)
	if err != nil {
		t.Fatal("cannot remove old file: ", err)
	}
	w, err := h.stor.Create(fd)
	if err != nil {
		t.Fatal("cannot create new file: ", err)
	}
	_, err = w.Write(buf)
	if err != nil {
		t.Fatal("cannot write new file: ", err)
	}
	w.Close()
}

func (h *dbCorruptHarness) forceRemoveAll(ft storage.FileType) {
	fds, err := h.stor.List(ft)
	if err != nil {
		h.t.Fatal("get files: ", err)
	}
	for _, fd := range fds {
		if err := h.stor.ForceRemove(fd); err != nil {
			h.t.Error("remove file: ", err)
		}
	}
}

func (h *dbCorruptHarness) removeOne(ft storage.FileType) {
	fds, err := h.stor.List(ft)
	if err != nil {
		h.t.Fatal("get files: ", err)
	}
	fd := fds[rand.Intn(len(fds))]
	h.t.Logf("removing file @%d", fd.Num)
	if err := h.stor.Remove(fd); err != nil {
		h.t.Error("remove file: ", err)
	}
}

func (h *dbCorruptHarness) check(min, max int) {
	p := &h.dbHarness
	t := p.t
	db := p.db

	var n, badk, badv, missed, good int
	iter := db.NewIterator(nil, p.ro)
	for iter.Next() {
		k := 0
		fmt.Sscanf(string(iter.Key()), "%d", &k)
		if k < n {
			badk++
			continue
		}
		missed += k - n
		n = k + 1
		if !bytes.Equal(iter.Value(), tval(k, ctValSize)) {
			badv++
		} else {
			good++
		}
	}
	err := iter.Error()
	iter.Release()
	t.Logf("want=%d..%d got=%d badkeys=%d badvalues=%d missed=%d, err=%v",
		min, max, good, badk, badv, missed, err)
	if good < min || good > max {
		t.Errorf("good entries number not in range")
	}
}

func (h *dbCorruptHarness) journalFd(fi int) storage.FileDesc {
	fds, err := h.stor.List(storage.TypeJournal)
	if err != nil {
		h.t.Fatal("cannot list journal files: ", err)
	}
	sortFds(fds)
	if fi < 0 {
		fi = len(fds) - 1
	}
	if fi < 0 || fi >= len(fds) {
		h.t.Fatalf("no journal file with index %d", fi)
	}
	return fds[fi]
}

func (h *dbCorruptHarness) readFile(fd storage.FileDesc) []byte {
	r, err := h.stor.Open(fd)
	if err != nil {
		h.t.Fatal("cannot open file: ", err)
	}
	x, err := r.Seek(0, 2)
	if err != nil {
		h.t.Fatal("cannot query file size: ", err)
	}
	if _, err := r.Seek(0, 0); err != nil {
		h.t.Fatal(err)
	}
	buf := make([]byte, int(x))
	if _, err := io.ReadFull(r, buf); err != nil {
		h.t.Fatal("cannot read file: ", err)
	}
	r.Close()
	return buf
}

func (h *dbCorruptHarness) rewriteFile(fd storage.FileDesc, data []byte) {
	if err := h.stor.Remove(fd); err != nil {
		h.t.Fatal("cannot remove old file: ", err)
	}
	w, err := h.stor.Create(fd)
	if err != nil {
		h.t.Fatal("cannot create new file: ", err)
	}
	if _, err := w.Write(data); err != nil {
		h.t.Fatal("cannot write new file: ", err)
	}
	w.Close()
}

// truncate truncates the fi'th file of the given type to size bytes,
// simulating a crash that tore the last write. A negative size is relative
// to the end of the file.
func (h *dbCorruptHarness) truncate(ft storage.FileType, fi, size int) {
	fds, _ := h.stor.List(ft)
	sortFds(fds)
	if fi < 0 {
		fi = len(fds) - 1
	}
	if fi < 0 || fi >= len(fds) {
		h.t.Fatalf("no such file with type %q with index %d", ft, fi)
	}
	fd := fds[fi]
	data := h.readFile(fd)
	if size < 0 {
		size = len(data) + size
	}
	if size < 0 {
		size = 0
	}
	if size > len(data) {
		size = len(data)
	}
	h.rewriteFile(fd, data[:size])
}

// makeJournalRecord encodes the batch as a single journal record with the
// given sequence number.
func makeJournalRecord(t *testing.T, seq uint64, batch *Batch) []byte {
	var buf bytes.Buffer
	jw := journal.NewWriter(&buf)
	w, err := jw.Next()
	if err != nil {
		t.Fatal("cannot create journal record: ", err)
	}
	if _, err := w.Write(encodeBatchHeader(nil, seq, batch.Len())); err != nil {
		t.Fatal("cannot write batch header: ", err)
	}
	if _, err := w.Write(batch.Dump()); err != nil {
		t.Fatal("cannot write batch: ", err)
	}
	if err := jw.Close(); err != nil {
		t.Fatal("cannot close journal writer: ", err)
	}
	return buf.Bytes()
}

func (h *dbCorruptHarness) assertKeys(from, to int) {
	for k := from; k < to; k++ {
		h.getVal(string(tkey(k)), string(tval(k, ctValSize)))
	}
}

func (h *dbCorruptHarness) assertNoKeys(from, to int) {
	for k := from; k < to; k++ {
		h.get(string(tkey(k)), false)
	}
}

func TestCorruptDB_Journal(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.build(100)
	h.check(100, 100)
	h.closeDB()
	// Corrupt the last journal in the middle of the second block. Replay
	// must stop at the corruption point: complete records before it are
	// recovered, bytes past it are not replayed.
	h.corrupt(storage.TypeJournal, -1, 32*1024+1000, 1)

	h.openDB()
	h.check(32, 32)
}

func TestCorruptDB_JournalTornTail(t *testing.T) {
	// Each batch holds 3 entries and is written as a single journal record
	// of 7 (chunk header) + 12 (batch header) + 3*1020 = 3079 bytes; all 10
	// records fit into a single 32KiB journal block.
	const (
		nBatch   = 10
		perBatch = 3
		recSize  = 7 + batchHeaderLen + perBatch*(1+1+16+2+ctValSize)
	)

	truncPoints := []int{
		0,                  // empty file
		1,                  // mid chunk header
		6,                  // one byte short of a full chunk header
		7,                  // chunk header only, no payload
		100,                // mid batch header
		recSize - 1,        // first record missing its last byte
		recSize,            // exactly one complete record
		recSize + 1,        // second record torn at its header
		5*recSize + 1500,   // sixth record torn mid payload
		nBatch*recSize - 1, // last record missing its last byte
		nBatch * recSize,   // intact
		nBatch*recSize + 1, // beyond EOF (no-op)
	}

	for _, trunc := range truncPoints {
		trunc := trunc
		t.Run(fmt.Sprintf("trunc=%d", trunc), func(t *testing.T) {
			h := newDbCorruptHarness(t)
			defer h.close()

			for i := 0; i < nBatch; i++ {
				batch := new(Batch)
				for j := 0; j < perBatch; j++ {
					k := i*perBatch + j
					batch.Put(tkey(k), tval(k, ctValSize))
				}
				if err := h.db.Write(batch, h.wo); err != nil {
					t.Fatal("write error: ", err)
				}
			}
			h.closeDB()
			h.truncate(storage.TypeJournal, -1, trunc)
			h.openDB()

			// Only fully committed batches may be recovered; a torn batch
			// must not enter the memtable at all.
			want := trunc / recSize * perBatch
			if want > nBatch*perBatch {
				want = nBatch * perBatch
			}
			h.assertKeys(0, want)
			h.assertNoKeys(want, nBatch*perBatch)
			// The manifest sequence must match the replayed state: replay
			// sets the next sequence to lastBatchSeq+batchLen, with batch
			// sequences starting at 1.
			wantSeq := uint64(want)
			if want > 0 {
				wantSeq++
			}
			if h.db.seq != wantSeq {
				t.Errorf("invalid seq, want=%d got=%d", wantSeq, h.db.seq)
			}
			if h.db.s.stSeqNum != wantSeq {
				t.Errorf("invalid manifest seq, want=%d got=%d", wantSeq, h.db.s.stSeqNum)
			}

			// New writes go to a fresh journal and must survive a restart;
			// the truncated tail must not interfere.
			h.put("post-recovery", "v1")
			h.reopenDB()
			h.getVal("post-recovery", "v1")
			h.assertKeys(0, want)
			h.assertNoKeys(want, nBatch*perBatch)
			if h.db.seq != wantSeq+2 {
				t.Errorf("invalid seq after restart, want=%d got=%d", wantSeq+2, h.db.seq)
			}
		})
	}
}

func TestCorruptDB_JournalTornTailStaleBytes(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.build(40)
	h.closeDB()

	// Simulate a crash that tore the write of record 31 (which spans the
	// first block boundary), leaving stale bytes behind: zero padding up to
	// the second block boundary followed by a well-formed record that was
	// never acknowledged, as could happen when a file generation reuses
	// blocks of a previous, longer one.
	fd := h.journalFd(-1)
	data := h.readFile(fd)
	torn := make([]byte, 2*32768)
	copy(torn, data[:33000])
	stale := new(Batch)
	stale.Put(tkey(999), tval(999, ctValSize))
	h.rewriteFile(fd, append(torn, makeJournalRecord(t, 40, stale)...))

	h.openDB()
	// Records before the tear are recovered; nothing past the safe
	// truncation point may resurface.
	h.assertKeys(0, 31)
	h.assertNoKeys(31, 40)
	h.get(string(tkey(999)), false)
	if h.db.seq != 32 {
		t.Errorf("invalid seq, want=32 got=%d", h.db.seq)
	}
	if h.db.s.stSeqNum != 32 {
		t.Errorf("invalid manifest seq, want=32 got=%d", h.db.s.stSeqNum)
	}

	// The stale bytes must not overwrite or pollute the new journal.
	h.put("post-recovery", "v1")
	h.reopenDB()
	h.getVal("post-recovery", "v1")
	h.assertKeys(0, 31)
	h.assertNoKeys(31, 40)
	h.get(string(tkey(999)), false)
}

func TestCorruptDB_JournalOldSegment(t *testing.T) {
	// setup builds a DB with 10 keys, then adds a second, newer journal
	// file with 10 more valid records, so that recovery sees a committed
	// segment followed by the tail segment.
	setup := func(t *testing.T) *dbCorruptHarness {
		h := newDbCorruptHarness(t)
		h.build(10)
		h.closeDB()

		fd := h.journalFd(-1)
		w, err := h.stor.Create(storage.FileDesc{Type: storage.TypeJournal, Num: fd.Num + 1})
		if err != nil {
			t.Fatal("cannot create journal file: ", err)
		}
		jw := journal.NewWriter(w)
		for i := 10; i < 20; i++ {
			batch := new(Batch)
			batch.Put(tkey(i), tval(i, ctValSize))
			rec, err := jw.Next()
			if err != nil {
				t.Fatal("cannot create journal record: ", err)
			}
			// Batch sequences start at 1, so the first 10 committed batches
			// (sequences 1..10) are followed by sequence 11.
			if _, err := rec.Write(encodeBatchHeader(nil, uint64(i+1), 1)); err != nil {
				t.Fatal("cannot write batch header: ", err)
			}
			if _, err := rec.Write(batch.Dump()); err != nil {
				t.Fatal("cannot write batch: ", err)
			}
		}
		if err := jw.Close(); err != nil {
			t.Fatal("cannot close journal writer: ", err)
		}
		w.Close()
		return h
	}

	// Both segments intact: everything is recovered.
	t.Run("intact", func(t *testing.T) {
		h := setup(t)
		defer h.close()
		h.openDB()
		h.assertKeys(0, 20)
		if h.db.seq != 21 {
			t.Errorf("invalid seq, want=21 got=%d", h.db.seq)
		}
	})

	// Corruption in the committed segment must surface as a stable error,
	// not be silently skipped.
	t.Run("corrupt_old_segment", func(t *testing.T) {
		h := setup(t)
		defer h.close()
		h.corrupt(storage.TypeJournal, 0, 100, 1)
		db, err := Open(h.stor, h.o)
		if err == nil {
			db.Close()
			t.Fatal("Open: expect error")
		}
		if !errors.IsCorrupted(err) {
			t.Errorf("Open: expect corrupted error, got: %v", err)
		}
	})

	// Corruption in the tail segment truncates it; the committed segment is
	// fully recovered.
	t.Run("corrupt_tail_segment", func(t *testing.T) {
		h := setup(t)
		defer h.close()
		h.corrupt(storage.TypeJournal, 1, 100, 1)
		h.openDB()
		h.assertKeys(0, 10)
		h.assertNoKeys(10, 20)
		if h.db.seq != 11 {
			t.Errorf("invalid seq, want=11 got=%d", h.db.seq)
		}
	})

	// A torn write in the tail segment truncates at the last complete
	// record.
	t.Run("torn_tail_segment", func(t *testing.T) {
		h := setup(t)
		defer h.close()
		// Each record is 1039 bytes; keep the first one plus a torn half of
		// the second.
		h.truncate(storage.TypeJournal, 1, 1039+500)
		h.openDB()
		h.assertKeys(0, 11)
		h.assertNoKeys(11, 20)
		if h.db.seq != 12 {
			t.Errorf("invalid seq, want=12 got=%d", h.db.seq)
		}
	})
}

func TestCorruptDB_Table(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.build(100)
	h.compactMem()
	h.compactRangeAt(0, "", "")
	h.compactRangeAt(1, "", "")
	h.closeDB()
	h.corrupt(storage.TypeTable, -1, 100, 1)

	h.openDB()
	h.check(99, 99)
}

func TestCorruptDB_TableIndex(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.build(10000)
	h.compactMem()
	h.closeDB()
	h.corrupt(storage.TypeTable, -1, -2000, 500)

	h.openDB()
	h.check(5000, 9999)
}

func TestCorruptDB_MissingManifest(t *testing.T) {
	rnd := rand.New(rand.NewSource(0x0badda7a))
	h := newDbCorruptHarnessWopt(t, &opt.Options{
		BlockCacheCapacity: 100,
		Strict:             opt.StrictJournalChecksum,
		WriteBuffer:        1000 * 60,
	})
	defer h.close()

	h.build(1000)
	h.compactMem()
	h.buildShuffled(1000, rnd)
	h.compactMem()
	h.deleteRand(500, 1000, rnd)
	h.compactMem()
	h.buildShuffled(1000, rnd)
	h.compactMem()
	h.deleteRand(500, 1000, rnd)
	h.compactMem()
	h.buildShuffled(1000, rnd)
	h.compactMem()
	h.closeDB()

	h.forceRemoveAll(storage.TypeManifest)
	h.openAssert(false)

	h.recover()
	h.check(1000, 1000)
	h.build(1000)
	h.compactMem()
	h.compactRange("", "")
	h.closeDB()

	h.recover()
	h.check(1000, 1000)
}

func TestCorruptDB_SequenceNumberRecovery(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.put("foo", "v1")
	h.put("foo", "v2")
	h.put("foo", "v3")
	h.put("foo", "v4")
	h.put("foo", "v5")
	h.closeDB()

	h.recover()
	h.getVal("foo", "v5")
	h.put("foo", "v6")
	h.getVal("foo", "v6")

	h.reopenDB()
	h.getVal("foo", "v6")
}

func TestCorruptDB_SequenceNumberRecoveryTable(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.put("foo", "v1")
	h.put("foo", "v2")
	h.put("foo", "v3")
	h.compactMem()
	h.put("foo", "v4")
	h.put("foo", "v5")
	h.compactMem()
	h.closeDB()

	h.recover()
	h.getVal("foo", "v5")
	h.put("foo", "v6")
	h.getVal("foo", "v6")

	h.reopenDB()
	h.getVal("foo", "v6")
}

func TestCorruptDB_CorruptedManifest(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.put("foo", "hello")
	h.compactMem()
	h.compactRange("", "")
	h.closeDB()
	h.corrupt(storage.TypeManifest, -1, 0, 1000)
	h.openAssert(false)

	h.recover()
	h.getVal("foo", "hello")
}

func TestCorruptDB_CompactionInputError(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.build(10)
	h.compactMem()
	h.closeDB()
	h.corrupt(storage.TypeTable, -1, 100, 1)

	h.openDB()
	h.check(9, 9)

	h.build(10000)
	h.check(10000, 10000)
}

func TestCorruptDB_UnrelatedKeys(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.build(10)
	h.compactMem()
	h.closeDB()
	h.corrupt(storage.TypeTable, -1, 100, 1)

	h.openDB()
	h.put(string(tkey(1000)), string(tval(1000, ctValSize)))
	h.getVal(string(tkey(1000)), string(tval(1000, ctValSize)))
	h.compactMem()
	h.getVal(string(tkey(1000)), string(tval(1000, ctValSize)))
}

func TestCorruptDB_Level0NewerFileHasOlderSeqnum(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.put("a", "v1")
	h.put("b", "v1")
	h.compactMem()
	h.put("a", "v2")
	h.put("b", "v2")
	h.compactMem()
	h.put("a", "v3")
	h.put("b", "v3")
	h.compactMem()
	h.put("c", "v0")
	h.put("d", "v0")
	h.compactMem()
	h.compactRangeAt(1, "", "")
	h.closeDB()

	h.recover()
	h.getVal("a", "v3")
	h.getVal("b", "v3")
	h.getVal("c", "v0")
	h.getVal("d", "v0")
}

func TestCorruptDB_RecoverInvalidSeq_Issue53(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.put("a", "v1")
	h.put("b", "v1")
	h.compactMem()
	h.put("a", "v2")
	h.put("b", "v2")
	h.compactMem()
	h.put("a", "v3")
	h.put("b", "v3")
	h.compactMem()
	h.put("c", "v0")
	h.put("d", "v0")
	h.compactMem()
	h.compactRangeAt(0, "", "")
	h.closeDB()

	h.recover()
	h.getVal("a", "v3")
	h.getVal("b", "v3")
	h.getVal("c", "v0")
	h.getVal("d", "v0")
}

func TestCorruptDB_MissingTableFiles(t *testing.T) {
	h := newDbCorruptHarness(t)
	defer h.close()

	h.put("a", "v1")
	h.put("b", "v1")
	h.compactMem()
	h.put("c", "v2")
	h.put("d", "v2")
	h.compactMem()
	h.put("e", "v3")
	h.put("f", "v3")
	h.closeDB()

	h.removeOne(storage.TypeTable)
	h.openAssert(false)
}

func TestCorruptDB_RecoverTable(t *testing.T) {
	h := newDbCorruptHarnessWopt(t, &opt.Options{
		WriteBuffer:         112 * opt.KiB,
		CompactionTableSize: 90 * opt.KiB,
		Filter:              filter.NewBloomFilter(10),
	})
	defer h.close()

	h.build(1000)
	h.compactMem()
	h.compactRangeAt(0, "", "")
	h.compactRangeAt(1, "", "")
	seq := h.db.seq
	time.Sleep(100 * time.Millisecond) // Wait lazy reference finish tasks
	h.closeDB()
	h.corrupt(storage.TypeTable, 0, 1000, 1)
	h.corrupt(storage.TypeTable, 3, 10000, 1)
	// Corrupted filter shouldn't affect recovery.
	h.corrupt(storage.TypeTable, 3, 113888, 10)
	h.corrupt(storage.TypeTable, -1, 20000, 1)

	h.recover()
	if h.db.seq != seq {
		t.Errorf("invalid seq, want=%d got=%d", seq, h.db.seq)
	}
	h.check(985, 985)
}
