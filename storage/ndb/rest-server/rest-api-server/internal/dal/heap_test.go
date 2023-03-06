package dal

import (
	"testing"

	"hopsworks.ai/rdrs/internal/config"
)

func TestHeap(t *testing.T) {
	InitializeBuffers()

	stats := GetNativeBuffersStats()
	totalBuffers := stats.BuffersCount

	conf := config.GetAll()

	if stats.AllocationsCount != int64(conf.Internal.PreAllocatedBuffers) {
		t.Fatalf("Number of pre allocated buffers does not match. Expecting: %d, Got: %d ",
			conf.Internal.PreAllocatedBuffers, stats.AllocationsCount)
	}

	buff := GetBuffer()
	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != totalBuffers-1 {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, totalBuffers-1)
	}
	ReturnBuffer(buff)
	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != totalBuffers {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, totalBuffers)
	}

	allocations := stats.FreeBuffers + 100
	c := make(chan *NativeBuffer, 1)
	for i := int64(0); i < allocations; i++ {
		go allocateBuffTest(t, c)
	}

	myBuffers := make([]*NativeBuffer, allocations)
	for i := int64(0); i < allocations; i++ {
		myBuffers[i] = <-c
	}

	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != 0 {
		t.Fatalf("Number of free buffers is not zero. Expecting: 0, Got: %d", stats.FreeBuffers)
	}

	if stats.BuffersCount != allocations {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			conf.Internal.PreAllocatedBuffers, stats.AllocationsCount)
	}

	for i := int64(0); i < allocations; i++ {
		ReturnBuffer(myBuffers[i])
	}

	stats = GetNativeBuffersStats()
	if stats.FreeBuffers != allocations {
		t.Fatalf("Number of free buffers does not match. Expecting: %d, Got: %d",
			allocations, stats.FreeBuffers)
	}
}

func allocateBuffTest(t *testing.T, c chan *NativeBuffer) {
	b := GetBuffer()
	c <- b
}
