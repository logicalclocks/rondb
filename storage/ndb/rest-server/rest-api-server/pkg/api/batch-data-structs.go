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

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Request
//
//easyjson:json
type BatchOpRequest struct {
	Operations *[]BatchSubOp `json:"operations" binding:"required,min=1,max=4096,unique,dive"`
}

//easyjson:json
type BatchSubOp struct {
	Method      *string     `json:"method"        binding:"required,oneof=POST"`
	RelativeURL *string     `json:"relative-url"  binding:"required,min=1"`
	Body        *PKReadBody `json:"body"          binding:"required,min=1"`
}

// Response
type BatchOpResponse interface {
	Init(numSubResponses int)
	CreateNewSubResponse() PKReadResponseWithCode
	AddSubResponse(index int, subResp PKReadResponseWithCode)
	String() string
}

type BatchResponseJSON struct {
	Result *[]*PKReadResponseWithCodeJSON `json:"result" binding:"required"`
}

type BatchResponseGRPC struct {
	Result *[]*PKReadResponseWithCodeGRPC `json:"result" binding:"required"`
}

// TODO: Why not include number of elements here?
func (b *BatchResponseJSON) Init(numSubResponses int) {
	subResponses := make([]*PKReadResponseWithCodeJSON, numSubResponses)
	b.Result = &subResponses
}

func (b *BatchResponseJSON) CreateNewSubResponse() PKReadResponseWithCode {
	subResponse := PKReadResponseWithCodeJSON{}
	subResponse.Init()
	return &subResponse
}

func (b *BatchResponseJSON) AddSubResponse(index int, subResp PKReadResponseWithCode) {
	subRespJson := subResp.(*PKReadResponseWithCodeJSON)
	(*b.Result)[index] = subRespJson
}

func (b *BatchResponseJSON) String() string {
	strBytes, err := json.MarshalIndent(*b, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshar BatchResponseJSON. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

func (b *BatchResponseGRPC) Init(numSubResponses int) {
	subResponses := make([]*PKReadResponseWithCodeGRPC, numSubResponses)
	b.Result = &subResponses
}

func (b *BatchResponseGRPC) CreateNewSubResponse() PKReadResponseWithCode {
	subResponse := PKReadResponseWithCodeGRPC{}
	subResponse.Init()
	return &subResponse
}

func (b *BatchResponseGRPC) AddSubResponse(index int, subResp PKReadResponseWithCode) {
	subRespGRPC := subResp.(*PKReadResponseWithCodeGRPC)
	(*b.Result)[index] = subRespGRPC
}

func (b *BatchResponseGRPC) String() string {
	var str bytes.Buffer
	str.WriteString("[ ")

	if b.Result != nil {
		for _, value := range *b.Result {
			str.WriteString(fmt.Sprintf("%s, ", value.String()))
		}
	}

	str.WriteString("]")
	return str.String()
}

// data structs for testing
type BatchSubOperationTestInfo struct {
	SubOperation BatchSubOp
	Table        string
	DB           string
	HttpCode     []int // for some operations there are multiple valid return codes
	RespKVs      []interface{}
}

type BatchOperationTestInfo struct {
	Operations     []BatchSubOperationTestInfo
	HttpCode       []int // for some operations there are multiple valid return codes
	ErrMsgContains string
}
