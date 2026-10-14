/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
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
package pkread

import (
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/integrationtests/spillfixture"
	"hopsworks.ai/rdrs2/internal/testutils"
)

// RONDB-1144: blob and text values larger than the room left in a response
// buffer continue into the next buffers of the request's chain. Every size
// here is computed from the response buffer size rdrs2 reports, so the
// tests mean the same thing on a branch with other limits. After each read
// the pool must hold no buffer of ours.

const tenMiB = 10 * 1024 * 1024

func respBufferBytes(t *testing.T) int {
	return int(testutils.GetLimits(t).RespBufferBytes)
}

// The exact point where a blob stops fitting in its home buffer depends on
// the response layout; a sweep around the buffer size necessarily covers
// the fits-exactly, one-under and one-over cases.
func TestBlobSpillBoundarySweep(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	size := respBufferBytes(t)
	before := testutils.TakePoolSnapshot(t)
	for n := size - 80; n <= size+8; n++ {
		want := testutils.DeterministicBytes(n, int64(n))
		testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID, want)
		data := spillfixture.Read(t, []string{"b"})
		spillfixture.ExpectBlob(t, data, "b", want)
	}
	testutils.AssertPoolBalanced(t, before)
}

func TestBlobSpillTwoBuffersPlusOne(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	size := respBufferBytes(t)
	before := testutils.TakePoolSnapshot(t)
	want := testutils.DeterministicBytes(2*size+1, 2)
	testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID, want)
	spillfixture.ExpectBlob(t, spillfixture.Read(t, []string{"b"}), "b", want)
	testutils.AssertPoolBalanced(t, before)
}

func TestBlobSpill10MiB(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	limits := testutils.GetLimits(t)
	needed := int64(tenMiB)/limits.RespBufferBytes + 2
	if needed > limits.MaxBuffers {
		t.Skipf("a 10 MiB blob needs %d buffers, pool cap is %d", needed, limits.MaxBuffers)
	}
	before := testutils.TakePoolSnapshot(t)
	want := testutils.DeterministicBytes(tenMiB, 10)
	testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID, want)
	spillfixture.ExpectBlob(t, spillfixture.Read(t, []string{"b"}), "b", want)
	testutils.AssertPoolBalanced(t, before)
}

func TestBlobSpillNull(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	before := testutils.TakePoolSnapshot(t)
	testutils.SetBlobNull(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID)
	data := spillfixture.Read(t, []string{"b", "v"})
	if _, ok := spillfixture.Value(t, data, "b"); ok {
		t.Fatalf("b should be null")
	}
	if v, _ := spillfixture.Value(t, data, "v"); v != spillfixture.AfterBlob {
		t.Fatalf("v: got %q", v)
	}
	testutils.AssertPoolBalanced(t, before)
}

// A blob in the middle of the read set spills, and the varchar after it
// still reads correctly: blobs are written after the bounded values.
func TestBlobSpillMiddleThenVarchar(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	size := respBufferBytes(t)
	before := testutils.TakePoolSnapshot(t)
	want := testutils.DeterministicBytes(size+1, 3)
	testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID, want)
	// The blob is asked for before the varchar; the writer still emits the
	// varchar into the home buffer and the blob last (rule 4). id is the
	// filter column and may not be listed as a read column again.
	data := spillfixture.Read(t, []string{"b", "v"})
	spillfixture.ExpectBlob(t, data, "b", want)
	if v, _ := spillfixture.Value(t, data, "v"); v != spillfixture.AfterBlob {
		t.Fatalf("v after the spilled blob: got %q", v)
	}
	testutils.AssertPoolBalanced(t, before)
}

// Two spilling blobs in one row: the second starts inside a continuation
// buffer, so its column entry carries an offset past the home buffer.
func TestBlobSpillTwoBlobs(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	size := respBufferBytes(t)
	before := testutils.TakePoolSnapshot(t)
	want1 := testutils.DeterministicBytes(size+1, 4)
	want2 := testutils.DeterministicBytes(size+1, 5)
	testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b", spillfixture.RowID, want1)
	testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "b2", spillfixture.RowID, want2)
	data := spillfixture.Read(t, []string{"b", "v", "b2"})
	spillfixture.ExpectBlob(t, data, "b", want1)
	spillfixture.ExpectBlob(t, data, "b2", want2)
	if v, _ := spillfixture.Value(t, data, "v"); v != spillfixture.AfterBlob {
		t.Fatalf("v: got %q", v)
	}
	testutils.AssertPoolBalanced(t, before)
}

// TEXT takes the same path with conversion and escaping first; printable
// text grows by neither, so the sizes mean the same as for blobs.
func TestTextSpillSizes(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	limits := testutils.GetLimits(t)
	size := int(limits.RespBufferBytes)
	sizes := []int{size - 1, size, size + 1, 2*size + 1}
	if int64(tenMiB)/limits.RespBufferBytes+2 <= limits.MaxBuffers {
		sizes = append(sizes, tenMiB)
	}
	before := testutils.TakePoolSnapshot(t)
	for _, n := range sizes {
		want := testutils.PrintableBytes(n, int64(n))
		testutils.SetBlob(t, spillfixture.DB, spillfixture.Table, "t", spillfixture.RowID, want)
		data := spillfixture.Read(t, []string{"t"})
		got, ok := spillfixture.Value(t, data, "t")
		if !ok {
			t.Fatalf("size %d: t is null", n)
		}
		if got != string(want) {
			t.Fatalf("size %d: text differs (got %d bytes)", n, len(got))
		}
	}
	testutils.AssertPoolBalanced(t, before)
}

// Half a buffer of control characters escapes to three buffers of
// \u0001: the spill decision is taken on the escaped length.
func TestTextSpillEscapeGrowth(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	size := respBufferBytes(t)
	before := testutils.TakePoolSnapshot(t)
	n := size / 2
	testutils.SetRepeatedByte(t, spillfixture.DB, spillfixture.Table, "t", spillfixture.RowID, 1, n)
	data := spillfixture.Read(t, []string{"t", "v"})
	got, ok := spillfixture.Value(t, data, "t")
	if !ok {
		t.Fatalf("t is null")
	}
	if got != strings.Repeat("\x01", n) {
		t.Fatalf("escaped text differs: got %d bytes", len(got))
	}
	if v, _ := spillfixture.Value(t, data, "v"); v != spillfixture.AfterBlob {
		t.Fatalf("v: got %q", v)
	}
	testutils.AssertPoolBalanced(t, before)
}

func TestTextSpillNull(t *testing.T) {
	spillfixture.SetupSpillTable(t)
	before := testutils.TakePoolSnapshot(t)
	testutils.SetBlobNull(t, spillfixture.DB, spillfixture.Table, "t", spillfixture.RowID)
	data := spillfixture.Read(t, []string{"t", "v"})
	if _, ok := spillfixture.Value(t, data, "t"); ok {
		t.Fatalf("t should be null")
	}
	if v, _ := spillfixture.Value(t, data, "v"); v != spillfixture.AfterBlob {
		t.Fatalf("v: got %q", v)
	}
	testutils.AssertPoolBalanced(t, before)
}
