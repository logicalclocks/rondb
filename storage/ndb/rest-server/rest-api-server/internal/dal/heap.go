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
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"hopsworks.ai/rdrs/internal/config"
)

type NativeBuffer struct {
	Size   uint32
	Buffer unsafe.Pointer
}

func (n NativeBuffer) String() string {
	var stringify strings.Builder
	stringify.WriteString(fmt.Sprintf("Size: %d\n", n.Size))
	stringify.WriteString(fmt.Sprintf("Buffer: %v\n", n.Buffer))
	return stringify.String()
}

type MemoryStats struct {
	AllocationsCount   int64
	DeallocationsCount int64
	BuffersCount       int64
	FreeBuffers        int64
}

var buffers []*NativeBuffer
var buffersStats MemoryStats
var initialized bool
var mutex sync.Mutex

func InitializeBuffers() {
	mutex.Lock()
	defer mutex.Unlock()

	if initialized {
		panic(fmt.Sprintf("Native buffers are already initialized"))
	}

	if C.ADDRESS_SIZE != 4 {
		panic(fmt.Sprintf("Only 4 byte address are supported"))
	}

	if config.Configuration().RestServer.BufferSize%C.ADDRESS_SIZE != 0 {
		panic(fmt.Sprintf("Buffer size must be multiple of %d", C.ADDRESS_SIZE))
	}

	for i := uint32(0); i < config.Configuration().RestServer.PreAllocatedBuffers; i++ {
		buffers = append(buffers, __allocateBuffer())
	}

	buffersStats.AllocationsCount = int64(config.Configuration().RestServer.PreAllocatedBuffers)
	buffersStats.BuffersCount = buffersStats.AllocationsCount
	buffersStats.DeallocationsCount = 0

	initialized = true
}

func ReleaseAllBuffers() {
	mutex.Lock()
	defer mutex.Unlock()

	if !initialized {
		panic(fmt.Sprintf("Native buffers are not initialized"))
	}

	for _, buffer := range buffers {
		C.free(buffer.Buffer)
	}
	buffers = make([]*NativeBuffer, 0)
	buffersStats = MemoryStats{}
	initialized = false
}

func __allocateBuffer() *NativeBuffer {
	buff := NativeBuffer{Buffer: C.malloc(C.size_t(config.Configuration().RestServer.BufferSize)),
		Size: uint32(config.Configuration().RestServer.BufferSize)}
	dstBuf := unsafe.Slice((*byte)(buff.Buffer), config.Configuration().RestServer.BufferSize)
	dstBuf[0] = 0x00 // reset buffer by putting null terminator in the begenning
	return &buff
}

func GetBuffer() *NativeBuffer {
	if !initialized {
		panic(fmt.Sprintf("Native buffers are not initialized"))
	}

	mutex.Lock()
	defer mutex.Unlock()

	var buff *NativeBuffer
	if len(buffers) > 0 {
		buff = buffers[len(buffers)-1]
		buffers = buffers[:len(buffers)-1]
	} else {
		buff = __allocateBuffer()
		buffersStats.BuffersCount++
		buffersStats.AllocationsCount++
	}

	return buff
}

func ReturnBuffer(buffer *NativeBuffer) {
	if !initialized {
		panic(fmt.Sprintf("Native buffers are not initialized"))
	}

	if buffer == nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	buffers = append(buffers, buffer)
}

func GetNativeBuffersStats() MemoryStats {
	if !initialized {
		panic(fmt.Sprintf("Native buffers are not initialized"))
	}
	//update the free buffers cound
	mutex.Lock()
	defer mutex.Unlock()
	buffersStats.FreeBuffers = int64(len(buffers))
	return buffersStats
}

func BuffersInitialized() bool {
	return initialized
}
