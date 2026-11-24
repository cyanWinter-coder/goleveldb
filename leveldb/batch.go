// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// ErrBatchCorrupted records reason of batch corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrBatchCorrupted struct {
	Reason string
}

func (e *ErrBatchCorrupted) Error() string {
	return fmt.Sprintf("leveldb: batch corrupted: %s", e.Reason)
}

func newErrBatchCorrupted(reason string) error {
	return errors.NewErrCorrupted(storage.FileDesc{}, &ErrBatchCorrupted{reason})
}

const (
	batchHeaderLen = 8 + 4
	batchGrowLimit = 3000
)

// BatchReplay wraps basic batch operations.
type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type batchIndex struct {
	keyType            keyType
	keyPos, keyLen     int
	valuePos, valueLen int
}

func (index batchIndex) k(data []byte) []byte {
	return data[index.keyPos : index.keyPos+index.keyLen]
}

func (index batchIndex) v(data []byte) []byte {
	if index.valueLen != 0 {
		return data[index.valuePos : index.valuePos+index.valueLen]
	}
	return nil
}

// Batch is a write batch.
type Batch struct {
	// Commit identity. Need 64-bit alignment.
	commitID  uint64
	commitSeq uint64

	data  []byte
	index []batchIndex

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	internalLen int

	// growLimit is the threshold in order to slow down the memory allocation
	// for batch when the number of accumulated entries exceeds value.
	//
	// batchGrowLimit is used as the default threshold if it's not configured.
	growLimit int
}

// lastBatchCommitID generates process-wide unique batch commit identities.
var lastBatchCommitID uint64

// CommitID returns the stable commit identity of the batch, allocating one
// on the first call. The commit identity makes the batch commit idempotent:
// if Write fails after the batch has been journaled (e.g. the memdb apply
// was interrupted), retrying Write with the same batch will replay the batch
// with its originally assigned sequence number and content instead of
// generating a new one, and a duplicate commit returns the completed result
// without applying the batch again.
//
// The commit identity is bound to the batch content: modifying the batch
// (Put, Delete, Reset or Load) invalidates the identity.
//
// The returned identity is unique within the process; it is never reused,
// so it is safe to rely on it across DB sessions. Commit records are held
// in memory and are released when the DB is closed.
func (b *Batch) CommitID() uint64 {
	if id := atomic.LoadUint64(&b.commitID); id != 0 {
		return id
	}
	id := atomic.AddUint64(&lastBatchCommitID, 1)
	if !atomic.CompareAndSwapUint64(&b.commitID, 0, id) {
		id = atomic.LoadUint64(&b.commitID)
	}
	return id
}

// getCommitID returns the commit identity of the batch, or zero if the
// batch doesn't carry one.
func (b *Batch) getCommitID() uint64 {
	return atomic.LoadUint64(&b.commitID)
}

// CommitSeq returns the sequence number assigned to the first record of the
// batch when it was committed. It returns zero if the batch hasn't been
// committed yet.
func (b *Batch) CommitSeq() uint64 {
	return atomic.LoadUint64(&b.commitSeq)
}

// Committed reports whether the batch has been committed.
func (b *Batch) Committed() bool {
	return b.CommitSeq() != 0
}

// invalidateCommit clears the commit identity; it is called whenever the
// batch content changes, so that a modified batch is never mistaken for a
// duplicate of its previous content.
func (b *Batch) invalidateCommit() {
	atomic.StoreUint64(&b.commitID, 0)
	atomic.StoreUint64(&b.commitSeq, 0)
}

func (b *Batch) grow(n int) {
	o := len(b.data)
	if cap(b.data)-o < n {
		limit := batchGrowLimit
		if b.growLimit > 0 {
			limit = b.growLimit
		}
		div := 1
		if len(b.index) > limit {
			div = len(b.index) / limit
		}
		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}

func (b *Batch) appendRec(kt keyType, key, value []byte) {
	b.invalidateCommit()
	n := 1 + binary.MaxVarintLen32 + len(key)
	if kt == keyTypeVal {
		n += binary.MaxVarintLen32 + len(value)
	}
	b.grow(n)
	index := batchIndex{keyType: kt}
	o := len(b.data)
	data := b.data[:o+n]
	data[o] = byte(kt)
	o++
	o += binary.PutUvarint(data[o:], uint64(len(key)))
	index.keyPos = o
	index.keyLen = len(key)
	o += copy(data[o:], key)
	if kt == keyTypeVal {
		o += binary.PutUvarint(data[o:], uint64(len(value)))
		index.valuePos = o
		index.valueLen = len(value)
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	b.index = append(b.index, index)
	b.internalLen += index.keyLen + index.valueLen + 8
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(key, value []byte) {
	b.appendRec(keyTypeVal, key, value)
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(key []byte) {
	b.appendRec(keyTypeDel, key, nil)
}

// Dump dumps batch contents. The returned slice can be loaded into the
// batch using Load method.
// The returned slice is not its own copy, so the contents should not be
// modified.
func (b *Batch) Dump() []byte {
	return b.data
}

// Load loads given slice into the batch. Previous contents of the batch
// will be discarded.
// The given slice will not be copied and will be used as batch buffer, so
// it is not safe to modify the contents of the slice.
func (b *Batch) Load(data []byte) error {
	b.invalidateCommit()
	return b.decode(data, -1)
}

// Replay replays batch contents.
func (b *Batch) Replay(r BatchReplay) error {
	for _, index := range b.index {
		switch index.keyType {
		case keyTypeVal:
			r.Put(index.k(b.data), index.v(b.data))
		case keyTypeDel:
			r.Delete(index.k(b.data))
		}
	}
	return nil
}

// Len returns number of records in the batch.
func (b *Batch) Len() int {
	return len(b.index)
}

// Reset resets the batch. This also cancels the batch commit identity, so
// that a reused batch will not be mistaken for a duplicate commit.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.index = b.index[:0]
	b.internalLen = 0
	b.invalidateCommit()
}

func (b *Batch) replayInternal(fn func(i int, kt keyType, k, v []byte) error) error {
	for i, index := range b.index {
		if err := fn(i, index.keyType, index.k(b.data), index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) append(p *Batch) {
	b.invalidateCommit()
	ob := len(b.data)
	oi := len(b.index)
	b.data = append(b.data, p.data...)
	b.index = append(b.index, p.index...)
	b.internalLen += p.internalLen

	// Updating index offset.
	if ob != 0 {
		for ; oi < len(b.index); oi++ {
			index := &b.index[oi]
			index.keyPos += ob
			if index.valueLen != 0 {
				index.valuePos += ob
			}
		}
	}
}

func (b *Batch) decode(data []byte, expectedLen int) error {
	b.data = data
	b.index = b.index[:0]
	b.internalLen = 0
	err := decodeBatch(data, func(i int, index batchIndex) error {
		b.index = append(b.index, index)
		b.internalLen += index.keyLen + index.valueLen + 8
		return nil
	})
	if err != nil {
		return err
	}
	if expectedLen >= 0 && len(b.index) != expectedLen {
		return newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", expectedLen, len(b.index)))
	}
	return nil
}

func (b *Batch) putMem(seq uint64, mdb *memdb.DB) error {
	var ik []byte
	for i, index := range b.index {
		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.keyType)
		if err := mdb.Put(ik, index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func newBatch() interface{} {
	return &Batch{}
}

// MakeBatch returns empty batch with preallocated buffer.
func MakeBatch(n int) *Batch {
	return &Batch{data: make([]byte, 0, n)}
}

// BatchConfig contains the config options for batch.
type BatchConfig struct {
	// InitialCapacity is the batch initial capacity to preallocate.
	//
	// The default value is 0.
	InitialCapacity int

	// GrowLimit is the limit (in terms of entry) of how much buffer
	// can grow each cycle.
	//
	// Initially the buffer will grow twice its current size until
	// GrowLimit threshold is reached, after that the buffer will grow
	// up to GrowLimit each cycle. This buffer grow size in bytes is
	// loosely calculated from average entry size multiplied by GrowLimit.
	//
	// Generally, the memory allocation step is larger if this value
	// is configured large, vice versa.
	//
	// The default value is 3000.
	GrowLimit int
}

// MakeBatchWithConfig initializes a batch object with the given configs.
func MakeBatchWithConfig(config *BatchConfig) *Batch {
	var batch = new(Batch)
	if config != nil {
		if config.InitialCapacity > 0 {
			batch.data = make([]byte, 0, config.InitialCapacity)
		}
		if config.GrowLimit > 0 {
			batch.growLimit = config.GrowLimit
		}
	}
	return batch
}

func decodeBatch(data []byte, fn func(i int, index batchIndex) error) error {
	var index batchIndex
	for i, o := 0, 0; o < len(data); i++ {
		// Key type.
		index.keyType = keyType(data[o])
		if index.keyType > keyTypeVal {
			return newErrBatchCorrupted(fmt.Sprintf("bad record: invalid type %#x", uint(index.keyType)))
		}
		o++

		// Key.
		x, n := binary.Uvarint(data[o:])
		o += n
		if n <= 0 || o+int(x) > len(data) {
			return newErrBatchCorrupted("bad record: invalid key length")
		}
		index.keyPos = o
		index.keyLen = int(x)
		o += index.keyLen

		// Value.
		if index.keyType == keyTypeVal {
			x, n = binary.Uvarint(data[o:])
			o += n
			if n <= 0 || o+int(x) > len(data) {
				return newErrBatchCorrupted("bad record: invalid value length")
			}
			index.valuePos = o
			index.valueLen = int(x)
			o += index.valueLen
		} else {
			index.valuePos = 0
			index.valueLen = 0
		}

		if err := fn(i, index); err != nil {
			return err
		}
	}
	return nil
}

func decodeBatchToMem(data []byte, expectSeq uint64, mdb *memdb.DB) (seq uint64, batchLen int, err error) {
	seq, batchLen, err = decodeBatchHeader(data)
	if err != nil {
		return 0, 0, err
	}
	if seq < expectSeq {
		return 0, 0, newErrBatchCorrupted("invalid sequence number")
	}
	// Decode the entire batch before applying it. The batch is applied to
	// the memdb only after it has been fully decoded, so that a corrupted
	// batch never leaves partially applied records behind.
	var batch Batch
	if err := batch.decode(data[batchHeaderLen:], batchLen); err != nil {
		return 0, 0, err
	}
	if err := batch.putMem(seq, mdb); err != nil {
		return 0, 0, err
	}
	return seq, batchLen, nil
}

func encodeBatchHeader(dst []byte, seq uint64, batchLen int) []byte {
	dst = ensureBuffer(dst, batchHeaderLen)
	binary.LittleEndian.PutUint64(dst, seq)
	binary.LittleEndian.PutUint32(dst[8:], uint32(batchLen))
	return dst
}

func decodeBatchHeader(data []byte) (seq uint64, batchLen int, err error) {
	if len(data) < batchHeaderLen {
		return 0, 0, newErrBatchCorrupted("too short")
	}

	seq = binary.LittleEndian.Uint64(data)
	batchLen = int(binary.LittleEndian.Uint32(data[8:]))
	if batchLen < 0 {
		return 0, 0, newErrBatchCorrupted("invalid records length")
	}
	return
}

func batchesLen(batches []*Batch) int {
	batchLen := 0
	for _, batch := range batches {
		batchLen += batch.Len()
	}
	return batchLen
}

func writeBatchesWithHeader(wr io.Writer, batches []*Batch, seq uint64) error {
	if _, err := wr.Write(encodeBatchHeader(nil, seq, batchesLen(batches))); err != nil {
		return err
	}
	for _, batch := range batches {
		if _, err := wr.Write(batch.data); err != nil {
			return err
		}
	}
	return nil
}
