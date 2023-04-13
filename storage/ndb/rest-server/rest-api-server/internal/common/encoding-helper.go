/*

 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package common

import (
	"errors"
	"fmt"
	"strconv"
	"unsafe"

	"hopsworks.ai/rdrs/internal/dal/heap"
)

// copy a go string to the buffer at the specified location.
// NULL is appended to the string for c/c++ compatibility
func CopyGoStrToCStr(src []byte, dst *heap.NativeBuffer, offset uint32) (uint32, error) {
	dstBuf := unsafe.Slice((*byte)(dst.Buffer), dst.Size)

	if offset+uint32(len(src))+1 > dst.Size {
		return 0, errors.New("trying to write more data than the buffer capacity")
	}

	for i, j := offset, 0; i < (offset + uint32(len(src))); i, j = i+1, j+1 {
		dstBuf[i] = src[j]
	}
	dstBuf[offset+uint32(len(src))] = 0x00
	return offset + uint32(len(src)) + 1, nil
}

/*
 Copy a go string to the buffer at the specified location.
 As a failed operation may be tried multiple times we have separated
 mutable and immutable spaces for string length.
 The First two bytes are immutable and it stores the size of the string
 the next two bytes are mutalbe and it also store the size of the string.

 Depending on the types of the column the length will be written to the
 byte(s) preceeding the string in the C layer.

 NdbDictionary::Column::ArrayTypeFixed uses 0 bytes for length
 NdbDictionary::Column::ArrayTypeShortVar uses 1 byte for length
 NdbDictionary::Column::ArrayTypeMediumVar uses 2 bytes for length

 NULL is appended to the string for c/c++ compatibility.
*/

func CopyGoStrToNDBStr(src []byte, dst *heap.NativeBuffer, offset uint32) (uint32, error) {
	dstBuf := unsafe.Slice((*byte)(dst.Buffer), dst.Size)

	// remove the quotation marks from string
	str := string(src)
	if str[0:1] == "\"" && str[len(str)-1:] == "\"" {
		// it is quoted string,
		uqSrc, err := strconv.Unquote(string(src))
		if err != nil {
			return 0, fmt.Errorf("failed to unquote string. Error: %w", err)
		}
		src = []byte(uqSrc)
	}

	if offset+uint32(len(src))+1+2+2 > dst.Size {
		return 0, errors.New("trying to write more data than the buffer capacity")
	}

	// immutable length of the string
	dstBuf[offset] = byte(len(src) % 256)
	dstBuf[offset+1] = byte(len(src) / 256)
	offset += 2

	// mutable, manipulated by the c layer
	// the c layer may write the size in one or two bytes
	// depending on the type of the column
	dstBuf[offset] = byte(0)
	dstBuf[offset+1] = byte(0)
	offset += 2

	for i, j := offset, 0; i < (offset + uint32(len(src))); i, j = i+1, j+1 {
		dstBuf[i] = src[j]
	}
	dstBuf[offset+uint32(len(src))] = 0x00
	return offset + uint32(len(src)) + 1, nil
}

// WORD alignment
func AlignWord(head uint32) uint32 {
	a := head % 4
	if a != 0 {
		head += (4 - a)
	}
	return head
}
