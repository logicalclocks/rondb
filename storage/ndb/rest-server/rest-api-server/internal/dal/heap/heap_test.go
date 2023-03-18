package heap

import (
	"testing"

	"hopsworks.ai/rdrs/internal/config"
)

func TestHeap(t *testing.T) {
	conf := config.GetAll()
	preAllocatedBuffers := conf.Internal.PreAllocatedBuffers

	heap, releaseBuffers, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBuffers()

	stats := heap.GetNativeBuffersStats()
	initialTotalBuffers := stats.BuffersCount

	if stats.AllocationsCount != int64(preAllocatedBuffers) {
		t.Fatalf("Number of pre allocated buffers does not match. Expecting: %d, Got: %d ",
			preAllocatedBuffers, stats.AllocationsCount)
	}

	_, returnBuff := heap.GetBuffer()

	stats = heap.GetNativeBuffersStats()
	if stats.FreeBuffers != initialTotalBuffers-1 {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, initialTotalBuffers-1)
	}
	returnBuff()

	stats = heap.GetNativeBuffersStats()
	if stats.FreeBuffers != initialTotalBuffers {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, initialTotalBuffers)
	}

	/*
		Trying to retrieve more buffers than are initially allocated
	*/

	allocations := stats.FreeBuffers + 100
	returnBuffFuncs := []func(){}
	for i := int64(0); i < allocations; i++ {
		_, returnBuff := heap.GetBuffer()
		returnBuffFuncs = append(returnBuffFuncs, returnBuff)
	}

	stats = heap.GetNativeBuffersStats()
	if stats.FreeBuffers != 0 {
		t.Fatalf("Number of free buffers is not zero. Expecting: 0, Got: %d", stats.FreeBuffers)
	}

	if stats.BuffersCount != allocations {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			preAllocatedBuffers, stats.AllocationsCount)
	}

	for _, returnBuff := range returnBuffFuncs {
		returnBuff()
	}

	// Note that we now have more FreeBuffers than initially
	// 	TODO: Figure out whether we want this to happen
	stats = heap.GetNativeBuffersStats()
	if stats.FreeBuffers != allocations {
		t.Fatalf("Number of free buffers does not match. Expecting: %d, Got: %d",
			allocations, stats.FreeBuffers)
	}
}
