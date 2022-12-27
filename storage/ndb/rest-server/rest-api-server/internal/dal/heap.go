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

package dal

/*
#include "./../../../data-access-rondb/src/rdrs-const.h"
#include "./../../../data-access-rondb/src/rdrs-dal.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"hopsworks.ai/rdrs/internal/config"
)

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

// Thread-safe buffers to save users' database permissions
var buffers []*NativeBuffer
var buffersStats MemoryStats
var initialized bool
var mutex sync.Mutex

func InitializeBuffers() error {
	mutex.Lock()
	defer mutex.Unlock()

	if initialized {
		return errors.New("Native buffers are already initialized")
	}

	if C.ADDRESS_SIZE != 4 {
		return errors.New("Only 4 byte address are supported")
	}

	conf := config.GetAll()

	if conf.Internal.BufferSize%C.ADDRESS_SIZE != 0 {
		return fmt.Errorf("Buffer size must be multiple of %d", C.ADDRESS_SIZE)
	}

	for i := uint32(0); i < conf.Internal.PreAllocatedBuffers; i++ {
		buffers = append(buffers, allocateBuffer())
	}

	buffersStats.AllocationsCount = int64(conf.Internal.PreAllocatedBuffers)
	buffersStats.BuffersCount = buffersStats.AllocationsCount
	buffersStats.DeallocationsCount = 0

	initialized = true
	return nil
}

func ReleaseAllBuffers() error {
	mutex.Lock()
	defer mutex.Unlock()

	if !initialized {
		return fmt.Errorf("Native buffers are not initialized")
	}

	for _, buffer := range buffers {
		C.free(buffer.Buffer)
	}
	buffers = make([]*NativeBuffer, 0)
	buffersStats = MemoryStats{}
	initialized = false
	return nil
}

func allocateBuffer() *NativeBuffer {
	conf := config.GetAll()
	bufferSize := conf.Internal.BufferSize

	buff := NativeBuffer{
		Buffer: C.malloc(C.size_t(bufferSize)),
		Size:   uint32(bufferSize),
	}
	dstBuf := unsafe.Slice((*byte)(buff.Buffer), bufferSize)
	dstBuf[0] = 0x00 // reset buffer by putting null terminator in the begenning
	return &buff
}

func GetBuffer() (*NativeBuffer, error) {
	if !initialized {
		return nil, fmt.Errorf("Native buffers are not initialized")
	}

	mutex.Lock()
	defer mutex.Unlock()

	var buff *NativeBuffer
	if len(buffers) > 0 {
		buff = buffers[len(buffers)-1]
		buffers = buffers[:len(buffers)-1]
	} else {
		buff = allocateBuffer()
		buffersStats.BuffersCount++
		buffersStats.AllocationsCount++
	}

	return buff, nil
}

func ReturnBuffer(buffer *NativeBuffer) error {
	if !initialized {
		return errors.New("Native buffers are not initialized")
	}

	if buffer == nil {
		return nil
	}

	mutex.Lock()
	defer mutex.Unlock()

	buffers = append(buffers, buffer)
	return nil
}

func GetNativeBuffersStats() (MemoryStats, error) {
	if !initialized {
		return buffersStats, errors.New("Native buffers are not initialized")
	}
	// update the free buffers cound
	mutex.Lock()
	defer mutex.Unlock()
	buffersStats.FreeBuffers = int64(len(buffers))
	return buffersStats, nil
}
