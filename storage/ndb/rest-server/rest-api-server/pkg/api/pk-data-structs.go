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
package api

/*
#include "./../../../data-access-rondb/src/rdrs-const.h"
#include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"strings"
)

// Request
type PKReadParams struct {
	DB          *string       `json:"db"`
	Table       *string       `json:"table"`
	Filters     *[]Filter     `json:"filters"`
	ReadColumns *[]ReadColumn `json:"readColumns" binding:"omitempty"`
	OperationID *string       `json:"operationId" binding:"omitempty"`
}

// Path parameters
type PKReadPP struct {
	DB    *string `json:"db" uri:"db"  binding:"required,min=1,max=64"`
	Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"`
}

type PKReadBody struct {
	Filters     *[]Filter     `json:"filters"         form:"filters"         binding:"required,min=1,max=4096,dive"`
	ReadColumns *[]ReadColumn `json:"readColumns"    form:"read-columns"    binding:"omitempty,min=1,max=4096,unique"`
	OperationID *string       `json:"operationId"    form:"operation-id"    binding:"omitempty,min=1,max=64"`
}

type Filter struct {
	Column *string          `json:"column"   form:"column"   binding:"required,min=1,max=64"`
	Value  *json.RawMessage `json:"value"    form:"value"    binding:"required"`
}

func (f Filter) String() string {
	var stringify strings.Builder
	if f.Column != nil {
		stringify.WriteString(fmt.Sprintf("Column: %s\n", *f.Column))
	}
	if f.Value != nil {
		j, err := json.Marshal(f.Value)
		if err != nil {
			stringify.WriteString(fmt.Sprintf("Error marshaling Value: %s\n", err.Error()))
		} else {
			stringify.WriteString(fmt.Sprintf("Value: %s\n", j))
		}
	}
	return stringify.String()
}

const (
	DRT_DEFAULT = "default"
	DRT_BASE64  = "base64" // not implemented yet
	DRT_HEX     = "hex"    // not implemented yet
)

type ReadColumn struct {
	Column *string `json:"column"    form:"column"    binding:"required,min=1,max=64"`

	// You can change the return type for the column data
	// int/floats/decimal are returned as JSON Number type (default),
	// varchar/char are returned as strings (default) and varbinary as base64 (default)
	// Right now only default return type is supported
	DataReturnType *string `json:"dataReturnType"    form:"column"    binding:"Enum=default,min=1,max=64"`

	// more parameter can be added later.
}

// Response
type Column struct {
	Name  *string          `json:"name"     form:"name"     binding:"required,min=1,max=64"`
	Value *json.RawMessage `json:"value"    form:"value"    binding:"required"`
}

type PKReadResponse interface {
	Init()
	SetOperationID(opID *string)
	SetColumnData(column, value *string, valueType uint32)
}

type PKReadResponseJSON struct {
	OperationID *string                      `json:"operationId"    form:"operation-id"    binding:"omitempty,min=1,max=64"`
	Data        *map[string]*json.RawMessage `json:"data"           form:"data"            binding:"omitempty"`
}

type PKReadResponseGRPC struct {
	OperationID *string             `json:"operationId"    form:"operation-id"    binding:"omitempty,min=1,max=64"`
	Data        *map[string]*string `json:"data"           form:"data"            binding:"omitempty"`
}

func (r *PKReadResponseGRPC) Init() {
	m := make(map[string]*string)
	(*r).Data = &m
}

func (r *PKReadResponseGRPC) SetOperationID(opID *string) {
	r.OperationID = opID
}

func (r *PKReadResponseGRPC) SetColumnData(column, value *string, valueType uint32) {
	if value == nil {
		(*(*r).Data)[*column] = nil
	} else {
		(*(*r).Data)[*column] = value
	}
}

func (r *PKReadResponseJSON) Init() {
	m := make(map[string]*json.RawMessage)
	(*r).Data = &m
}

func (r *PKReadResponseJSON) SetOperationID(opID *string) {
	r.OperationID = opID
}

func (r *PKReadResponseJSON) SetColumnData(column, value *string, dataType uint32) {
	if value == nil {
		(*(*r).Data)[*column] = nil
	} else {
		if dataType == C.RDRS_INTEGER_DATATYPE || dataType == C.RDRS_FLOAT_DATATYPE {
			valueBytes := json.RawMessage(*value)
			(*(*r).Data)[*column] = &valueBytes
		} else {
			quotedString := fmt.Sprintf("\"%s\"", *value)
			valueBytes := json.RawMessage(quotedString)
			(*(*r).Data)[*column] = &valueBytes
		}
	}
}

var _ PKReadResponse = (*PKReadResponseGRPC)(nil)
var _ PKReadResponse = (*PKReadResponseJSON)(nil)

type PKReadResponseWithCode interface {
	Init()
	GetPKReadResponse() PKReadResponse
	SetCode(code *int32)
}

type PKReadResponseWithCodeJSON struct {
	Code *int32              `json:"code"    form:"code"    binding:"required"`
	Body *PKReadResponseJSON `json:"body"    form:"body"    binding:"required"`
}

type PKReadResponseWithCodeGRPC struct {
	Code *int32              `json:"code"    form:"code"    binding:"required"`
	Body *PKReadResponseGRPC `json:"body"    form:"body"    binding:"required"`
}

func (p *PKReadResponseWithCodeJSON) Init() {
	p.Body = &PKReadResponseJSON{}
	p.Body.Init()
}

func (p *PKReadResponseWithCodeJSON) GetPKReadResponse() PKReadResponse {
	return p.Body
}

func (p *PKReadResponseWithCodeJSON) SetCode(code *int32) {
	p.Code = code
}

func (p *PKReadResponseWithCodeGRPC) Init() {
	p.Body = &PKReadResponseGRPC{}
	p.Body.Init()
}

func (p *PKReadResponseWithCodeGRPC) GetPKReadResponse() PKReadResponse {
	return p.Body
}

func (p *PKReadResponseWithCodeGRPC) SetCode(code *int32) {
	p.Code = code
}

var _ PKReadResponseWithCode = (*PKReadResponseWithCodeJSON)(nil)
var _ PKReadResponseWithCode = (*PKReadResponseWithCodeGRPC)(nil)

// For testing only
type PKTestInfo struct {
	PkReq          PKReadBody
	Table          string
	Db             string
	HttpCode       int
	ErrMsgContains string
	RespKVs        []interface{}
}
