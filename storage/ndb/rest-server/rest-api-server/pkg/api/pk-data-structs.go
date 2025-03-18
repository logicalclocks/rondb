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
package api

/*
#include "./../../../data-access-rondb/src/rdrs-const.h"
#include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"bytes"
	"encoding/base64"
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

func (p PKReadParams) String() string {
	var stringify strings.Builder
	if p.DB != nil {
		stringify.WriteString(fmt.Sprintf("DB: %s\n", *p.DB))
	}
	if p.Table != nil {
		stringify.WriteString(fmt.Sprintf("Table: %s\n", *p.Table))
	}
	if p.OperationID != nil {
		stringify.WriteString(fmt.Sprintf("OperationID: %s\n", *p.OperationID))
	}
	if p.Filters != nil {
		stringify.WriteString("Filters:\n")
		for _, f := range *p.Filters {
			stringify.WriteString(f.String())
		}
	}
	if p.ReadColumns != nil {
		stringify.WriteString("ReadColumns:\n")
		for _, rc := range *p.ReadColumns {
			stringify.WriteString(fmt.Sprintf("Column: %s\n", *rc.Column))
		}
	}
	return stringify.String()
}

// Path parameters
type PKReadPP struct {
	DB    *string `json:"db" uri:"db"  binding:"required,min=1,max=64"`
	Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"`
}

type PKReadBody struct {
	Filters     *[]Filter     `json:"filters"         form:"filters"         binding:"required,min=1,max=4096,dive"`
	ReadColumns *[]ReadColumn `json:"readColumns"    form:"read-columns"    binding:"omitempty,min=1,max=4096,unique"`
	OperationID *string       `json:"operationId"    form:"operation-id"    binding:"omitempty"`
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
	SetColumnStringData(column, value *string, valueType uint32)
	SetColumnRawData(column *string, data *[]byte, dataLen uint32, valueType uint32)
	EncodeRawData()
	String() string
}

type PKReadResponseJSON struct {
	OperationID *string                      `json:"operationId"    form:"operation-id"    binding:"omitempty"`
	Data        *map[string]*json.RawMessage `json:"data"           form:"data"            binding:"omitempty"`
	RawData     *map[string]*[]byte          `json:"-"` // binary data. needs to be base64 encode and put in `Data` field before marshalling
}

type PKReadResponseGRPC struct {
	OperationID *string             `json:"operationId"    form:"operation-id"    binding:"omitempty"`
	Data        *map[string]*string `json:"data"           form:"data"            binding:"omitempty"`
	RawData     *map[string]*[]byte `json:"-"` // binary data. needs to be base64 encode and put in `Data` field before marshalling
}

func (r *PKReadResponseGRPC) Init() {
	d := make(map[string]*string)
	(*r).Data = &d
	rd := make(map[string]*[]byte)
	(*r).RawData = &rd
}

func (r *PKReadResponseGRPC) SetOperationID(opID *string) {
	r.OperationID = opID
}

func (r *PKReadResponseGRPC) SetColumnStringData(column, value *string, dataType uint32) {
	(*(*r).Data)[*column] = value
}

func (r *PKReadResponseGRPC) EncodeRawData() {
	for key, value := range *r.RawData {
		if value == nil {
			(*r.Data)[key] = nil
		} else {
			// base64 encode the value
			encoded := base64.StdEncoding.EncodeToString(*value)
			quoted := "\"" + encoded + "\""
			(*(*r).Data)[key] = &quoted
		}
	}
}

func (r *PKReadResponseGRPC) SetColumnRawData(column *string, data *[]byte, dataLen uint32, valueType uint32) {
	(*(*r).RawData)[*column] = data
}

func (r *PKReadResponseGRPC) String() string {
	var str bytes.Buffer
	str.WriteString("{ ")
	str.WriteString(fmt.Sprintf("\"OperationID\": \"%s\",", *r.OperationID))
	str.WriteString("\"Data\": {")

	if r.Data != nil {
		for key, value := range *r.Data {
			str.WriteString(fmt.Sprintf("\"%s\": \"%s\",", key, *value))
		}
	}

	str.WriteString("}")
	return str.String()
}

func (r *PKReadResponseJSON) Init() {
	d := make(map[string]*json.RawMessage)
	(*r).Data = &d
	rd := make(map[string]*[]byte)
	(*r).RawData = &rd
}

func (r *PKReadResponseJSON) SetOperationID(opID *string) {
	r.OperationID = opID
}

func (r *PKReadResponseJSON) SetColumnStringData(column, value *string, dataType uint32) {
	if value == nil {
		(*(*r).Data)[*column] = nil
	} else {
		valueBytes := json.RawMessage(*value)
		(*(*r).Data)[*column] = &valueBytes
	}
}

func (r *PKReadResponseJSON) EncodeRawData() {
	for key, value := range *r.RawData {
		if value == nil {
			(*r.Data)[key] = nil
		} else {
			// base64 encode the value
			encoded := base64.StdEncoding.EncodeToString(*value)
			quoted := "\"" + encoded + "\""
			valueBytes := json.RawMessage(quoted)
			(*(*r).Data)[key] = &valueBytes
		}
	}
}

func (r *PKReadResponseJSON) SetColumnRawData(column *string, data *[]byte, dataLen uint32, valueType uint32) {
	if data == nil {
		(*(*r).RawData)[*column] = nil
	} else {
		(*(*r).RawData)[*column] = data
	}
}

func (r *PKReadResponseJSON) String() string {
	strBytes, err := json.MarshalIndent(*r, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshar PKReadResponseJSON. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

type PKReadResponseWithCode interface {
	Init()
	GetPKReadResponse() PKReadResponse
	SetStatus(code *int32, message *string)
	EncodeRawData()
	String() string
}

type PKReadResponseWithCodeJSON struct {
	Code    *int32              `json:"code"    form:"code"    binding:"required"`
	Message *string             `json:"message" form:"message" binding:"required"`
	Body    *PKReadResponseJSON `json:"body"    form:"body"    binding:"required"`
}

type PKReadResponseWithCodeGRPC struct {
	Code    *int32              `json:"code"    form:"code"    binding:"required"`
	Message *string             `json:"message" form:"message" binding:"required"`
	Body    *PKReadResponseGRPC `json:"body"    form:"body"    binding:"required"`
}

func (p *PKReadResponseWithCodeJSON) Init() {
	p.Body = &PKReadResponseJSON{}
	p.Body.Init()
}

func (p *PKReadResponseWithCodeJSON) EncodeRawData() {
	p.Body.EncodeRawData()
}

func (p *PKReadResponseWithCodeJSON) GetPKReadResponse() PKReadResponse {
	return p.Body
}

func (p *PKReadResponseWithCodeJSON) SetStatus(code *int32, message *string) {
	p.Code = code
	p.Message = message
}

func (p *PKReadResponseWithCodeJSON) String() string {
	strBytes, err := json.MarshalIndent(*p, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshar PKReadResponseJSON. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

func (p *PKReadResponseWithCodeGRPC) Init() {
	p.Body = &PKReadResponseGRPC{}
	p.Body.Init()
}

func (p *PKReadResponseWithCodeGRPC) EncodeRawData() {
	p.Body.EncodeRawData()
}

func (p *PKReadResponseWithCodeGRPC) GetPKReadResponse() PKReadResponse {
	return p.Body
}

func (p *PKReadResponseWithCodeGRPC) SetStatus(code *int32, message *string) {
	p.Code = code
	p.Message = message
}

func (p *PKReadResponseWithCodeGRPC) String() string {
	var str bytes.Buffer
	str.WriteString("{ ")
	str.WriteString(fmt.Sprintf("\"Code\": \"%d\",", *p.Code))
	str.WriteString(fmt.Sprintf("\"Message\": \"%s\",", *p.Message))

	str.WriteString("\"Body\": { ")
	if p.Body != nil {
		str.WriteString(p.Body.String())
	}
	str.WriteString("} ") // Body

	str.WriteString("}")
	return str.String()
}

// For testing only
type PKTestInfo struct {
	PkReq          PKReadBody
	Table          string
	Db             string
	HttpCode       int
	ErrMsgContains string
	RespKVs        []interface{}
}
