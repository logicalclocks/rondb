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
	"encoding/json"

	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/dal/heap"
)

// Converters for PK Read Request
func ConvertPKReadParams(req *PKReadParams) *PKReadRequestProto {
	pkReadRequestProto := PKReadRequestProto{}

	var filtersProto []*FilterProto
	if req.Filters != nil {
		for _, filter := range *req.Filters {
			filterProto := FilterProto{}
			filterProto.Column = filter.Column

			// remove quotes if any
			if *filter.Value != nil {
				valueStr := string([]byte(*filter.Value))
				filterProto.Value = &valueStr
			}

			filtersProto = append(filtersProto, &filterProto)
		}
	}
	pkReadRequestProto.Filters = filtersProto

	var readColumnsProto []*ReadColumnProto
	if req.ReadColumns != nil {
		for _, readColumn := range *req.ReadColumns {
			readColumnProto := ReadColumnProto{}
			readColumnProto.Column = readColumn.Column

			if readColumn.DataReturnType != nil {
				readColumnProto.DataReturnType = readColumn.DataReturnType
			}

			readColumnsProto = append(readColumnsProto, &readColumnProto)
		}
	}
	pkReadRequestProto.ReadColumns = readColumnsProto

	pkReadRequestProto.DB = req.DB
	pkReadRequestProto.Table = req.Table
	pkReadRequestProto.OperationID = req.OperationID

	return &pkReadRequestProto
}

func ConvertPKReadRequestProto(reqProto *PKReadRequestProto) *PKReadParams {
	pkReadParams := PKReadParams{}

	pkReadParams.DB = reqProto.DB
	pkReadParams.Table = reqProto.Table
	pkReadParams.OperationID = reqProto.OperationID

	var readColumns []ReadColumn
	for _, readColumnProto := range reqProto.GetReadColumns() {
		if readColumnProto != nil {
			readColumn := ReadColumn{}

			readColumn.Column = readColumnProto.Column
			readColumn.DataReturnType = readColumnProto.DataReturnType

			readColumns = append(readColumns, readColumn)
		}
	}
	if len(readColumns) > 0 {
		pkReadParams.ReadColumns = &readColumns
	} else {
		pkReadParams.ReadColumns = nil
	}

	var filters []Filter
	for _, filterProto := range reqProto.Filters {
		if filterProto != nil {
			filter := Filter{}

			filter.Column = filterProto.Column
			rawMsg := json.RawMessage([]byte(*filterProto.Value))
			filter.Value = &rawMsg

			filters = append(filters, filter)
		}
	}
	if len(filters) > 0 {
		pkReadParams.Filters = &filters
	} else {
		pkReadParams.Filters = nil
	}

	return &pkReadParams
}

// Converters for PK Read Response
func ConvertPKReadResponseProto(respProto *PKReadResponseProto) *PKReadResponseGRPC {
	resp := PKReadResponseGRPC{}

	data := make(map[string]*string)
	if respProto.Data != nil {
		for colName, colVal := range respProto.Data {
			if colVal != nil {
				data[colName] = colVal.Name
			} else {
				data[colName] = nil
			}
		}
	}
	if len(data) > 0 {
		resp.Data = &data
	} else {
		resp.Data = nil
	}

	resp.OperationID = respProto.OperationID
	return &resp
}

func ConvertPKReadResponse(resp *PKReadResponseGRPC) *PKReadResponseProto {
	respProto := PKReadResponseProto{}
	respProto.Data = make(map[string]*ColumnValueProto)
	if resp.Data != nil {
		for colName, colVal := range *resp.Data {
			if colVal != nil {
				respProto.Data[colName] = &ColumnValueProto{Name: colVal}
			} else {
				respProto.Data[colName] = nil
			}
		}
	}

	respProto.OperationID = resp.OperationID
	return &respProto
}

func ConvertBatchRequestProto(reqProto *BatchRequestProto) *[]*PKReadParams {
	operations := make([]*PKReadParams, len(reqProto.Operations))
	for i, operation := range reqProto.Operations {
		operations[i] = ConvertPKReadRequestProto(operation)
	}
	return &operations
}

func ConvertBatchOpRequest(readParams []*PKReadParams) *BatchRequestProto {
	readParamsProto := make([]*PKReadRequestProto, len(readParams))

	for i, readParam := range readParams {
		readParamsProto[i] = ConvertPKReadParams(readParam)
	}

	var batchRequestProto BatchRequestProto
	batchRequestProto.Operations = readParamsProto

	return &batchRequestProto
}

func ConvertBatchResponseProto(responsesProto *BatchResponseProto) *BatchResponseGRPC {
	pkResponsesWCode := make([]*PKReadResponseWithCodeGRPC, len(responsesProto.Responses))
	for i, respProto := range responsesProto.Responses {
		pkResponsesWCode[i] = &PKReadResponseWithCodeGRPC{Code: respProto.Code, Message: respProto.Message, Body: ConvertPKReadResponseProto(respProto)}
	}
	batchResponse := BatchResponseGRPC{Result: &pkResponsesWCode}
	return &batchResponse
}

func ConvertBatchOpResponse(responses *BatchResponseGRPC) *BatchResponseProto {
	var batchResponse BatchResponseProto
	if responses.Result != nil {
		pkReadResponsesProto := make([]*PKReadResponseProto, len(*responses.Result))
		for i, response := range *responses.Result {
			pkReadResponseProto := ConvertPKReadResponse(response.Body)
			pkReadResponseProto.Code = response.Code
			pkReadResponseProto.Message = response.Message
			pkReadResponsesProto[i] = pkReadResponseProto
		}
		batchResponse.Responses = pkReadResponsesProto
	}
	return &batchResponse
}

func ConvertStatRequest(req *StatRequest) *StatRequestProto {
	return &StatRequestProto{}
}
func ConvertStatRequestProto(reqProto *StatRequestProto) *StatRequest {
	return &StatRequest{}
}

func ConvertStatResponse(resp *StatResponse) *StatResponseProto {
	respProto := StatResponseProto{}
	memStatsProto := MemoryStatsProto{}
	rondbStatsProto := RonDBStatsProto{}

	memStatsProto.AllocationsCount = &resp.MemoryStats.AllocationsCount
	memStatsProto.DeallocationsCount = &resp.MemoryStats.DeallocationsCount
	memStatsProto.BuffersCount = &resp.MemoryStats.BuffersCount
	memStatsProto.FreeBuffers = &resp.MemoryStats.FreeBuffers

	rondbStatsProto.NdbObjectsCreationCount = &resp.RonDBStats.NdbObjectsCreationCount
	rondbStatsProto.NdbObjectsDeletionCount = &resp.RonDBStats.NdbObjectsDeletionCount
	rondbStatsProto.NdbObjectsTotalCount = &resp.RonDBStats.NdbObjectsTotalCount
	rondbStatsProto.NdbObjectsFreeCount = &resp.RonDBStats.NdbObjectsFreeCount

	respProto.RonDBStats = &rondbStatsProto
	respProto.MemoryStats = &memStatsProto
	return &respProto
}

func ConvertStatResponseProto(resp *StatResponseProto) *StatResponse {
	statResponse := StatResponse{}
	memoryStats := heap.MemoryStats{}
	ronDBStats := dal.RonDBStats{}

	memoryStats.AllocationsCount = *resp.MemoryStats.AllocationsCount
	memoryStats.DeallocationsCount = *resp.MemoryStats.DeallocationsCount
	memoryStats.BuffersCount = *resp.MemoryStats.BuffersCount
	memoryStats.FreeBuffers = *resp.MemoryStats.FreeBuffers

	ronDBStats.NdbObjectsCreationCount = *resp.RonDBStats.NdbObjectsCreationCount
	ronDBStats.NdbObjectsDeletionCount = *resp.RonDBStats.NdbObjectsDeletionCount
	ronDBStats.NdbObjectsTotalCount = *resp.RonDBStats.NdbObjectsTotalCount
	ronDBStats.NdbObjectsFreeCount = *resp.RonDBStats.NdbObjectsFreeCount

	statResponse.MemoryStats = memoryStats
	statResponse.RonDBStats = ronDBStats
	return &statResponse
}
