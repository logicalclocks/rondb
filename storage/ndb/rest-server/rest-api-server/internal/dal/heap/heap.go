/*

 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package heap

/*
#include "./../../../../data-access-rondb/src/rdrs-const.h"
#include "./../../../../data-access-rondb/src/rdrs-dal.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"

	"hopsworks.ai/rdrs/internal/config"
)

type Heap struct {
	buffers      []*NativeBuffer
	buffersStats MemoryStats
	mutex        *sync.Mutex
}

type NativeBuffer struct {
	Size   uint32
	Buffer unsafe.Pointer
}

type MemoryStats struct {
	AllocationsCount   int64
	DeallocationsCount int64
	BuffersCount       int64
	FreeBuffers        int64
}

func New() (heap *Heap, releaseBuffers func(), err error) {
	conf := config.GetAll()
	if C.ADDRESS_SIZE != 4 {
		return nil, nil, fmt.Errorf("only 4 byte address are supported; '%d' is not", C.ADDRESS_SIZE)
	} else if conf.Internal.BufferSize%C.ADDRESS_SIZE != 0 {
		return nil, nil, fmt.Errorf("buffer size must be multiple of %d", C.ADDRESS_SIZE)
	}

	preAllocatedBuffers := int64(conf.Internal.PreAllocatedBuffers)

	heap = &Heap{
		buffers: []*NativeBuffer{},
		mutex:   &sync.Mutex{},
		buffersStats: MemoryStats{
			AllocationsCount:   preAllocatedBuffers,
			BuffersCount:       preAllocatedBuffers,
			DeallocationsCount: 0,
		},
	}
	for i := int64(0); i < preAllocatedBuffers; i++ {
		heap.buffers = append(heap.buffers, allocateBuffer())
	}

	return heap, heap.releaseAllBuffers, nil
}

func allocateBuffer() *NativeBuffer {
	conf := config.GetAll()
	bufferSize := conf.Internal.BufferSize

	buff := NativeBuffer{
		Buffer: C.malloc(C.size_t(bufferSize)),
		Size:   uint32(bufferSize),
	}
	dstBuf := unsafe.Slice((*byte)(buff.Buffer), bufferSize)
	dstBuf[0] = 0x00 // reset buffer by putting null terminator in the beginning
	return &buff
}

func (heap *Heap) releaseAllBuffers() {
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	for _, buffer := range heap.buffers {
		C.free(buffer.Buffer)
	}
	heap.buffers = make([]*NativeBuffer, 0)
	heap.buffersStats = MemoryStats{}
	return
}

func (heap *Heap) GetBuffer() (buff *NativeBuffer, returnBuff func()) {
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	numBuffersLeft := len(heap.buffers)
	if numBuffersLeft > 0 {
		buff = heap.buffers[numBuffersLeft-1]
		heap.buffers = heap.buffers[:numBuffersLeft-1]
	} else {
		// we're going beyond the pre-allocated buffers here
		buff = allocateBuffer()
		heap.buffersStats.BuffersCount++
		heap.buffersStats.AllocationsCount++
	}

	return buff, func() { heap.returnBuffer(buff) }
}

func (heap *Heap) returnBuffer(buffer *NativeBuffer) {
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	// TODO: Should we not run this as well?
	// C.free(buffer.Buffer)

	// TODO: Also, shouldn't we only return if we're beneath #preAllocatedBuffers?

	heap.buffers = append(heap.buffers, buffer)
	return
}

func (heap *Heap) GetNativeBuffersStats() MemoryStats {
	// update the free buffers cound
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	heap.buffersStats.FreeBuffers = int64(len(heap.buffers))
	return heap.buffersStats
}
