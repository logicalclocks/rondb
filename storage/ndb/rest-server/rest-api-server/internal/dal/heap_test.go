package dal

import (
	"testing"

	"hopsworks.ai/rdrs/internal/config"
)

func TestHeap(t *testing.T) {
	err := InitializeBuffers()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = ReleaseAllBuffers()
		if err != nil {
			t.Log(err)
		}
	}()

	stats, err := GetNativeBuffersStats()
	if err != nil {
		t.Fatal(err)
	}
	totalBuffers := stats.BuffersCount

	conf := config.GetAll()
	preAllocatedBuffers := conf.Internal.PreAllocatedBuffers

	if stats.AllocationsCount != int64(preAllocatedBuffers) {
		t.Fatalf("Number of pre allocated buffers does not match. Expecting: %d, Got: %d ",
			preAllocatedBuffers, stats.AllocationsCount)
	}

	buff, err := GetBuffer()
	if err != nil {
		t.Fatal(err)
	}
	stats, err = GetNativeBuffersStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.FreeBuffers != totalBuffers-1 {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, totalBuffers-1)
	}
	err = ReturnBuffer(buff)
	if err != nil {
		t.Fatal(err)
	}
	stats, err = GetNativeBuffersStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.FreeBuffers != totalBuffers {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			stats.FreeBuffers, totalBuffers)
	}

	allocations := stats.FreeBuffers + 100
	c := make(chan *NativeBuffer, 1)
	for i := int64(0); i < allocations; i++ {
		// TODO: Avoid using go-routine here
		go allocateBuffTest(t, c)
	}

	myBuffers := make([]*NativeBuffer, allocations)
	for i := int64(0); i < allocations; i++ {
		myBuffers[i] = <-c
	}

	stats, err = GetNativeBuffersStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.FreeBuffers != 0 {
		t.Fatalf("Number of free buffers is not zero. Expecting: 0, Got: %d", stats.FreeBuffers)
	}

	if stats.BuffersCount != allocations {
		t.Fatalf("Number of free buffers did not match. Expecting: %d, Got: %d ",
			preAllocatedBuffers, stats.AllocationsCount)
	}

	for i := int64(0); i < allocations; i++ {
		err = ReturnBuffer(myBuffers[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	stats, err = GetNativeBuffersStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.FreeBuffers != allocations {
		t.Fatalf("Number of free buffers does not match. Expecting: %d, Got: %d",
			allocations, stats.FreeBuffers)
	}
}

func allocateBuffTest(t *testing.T, c chan *NativeBuffer) {
	b, err := GetBuffer()
	if err != nil {
		t.Fatal(err)
	}
	c <- b
}
