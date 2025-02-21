/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2025 Hopsworks AB
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

package main

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hamba/avro/v2"
)

// parser
func ConvertAvroSchemaToStruct(schema avro.Schema) (reflect.Type, error) {
	switch schema.Type() {
	case avro.Record:
		{
			switch schema.(type) {
			case *avro.RecordSchema:
				{
					rs := schema.(*avro.RecordSchema)
					var fields []reflect.StructField
					for _, field := range rs.Fields() {
						ret, err := ConvertAvroSchemaToStruct(field.Type())
						if err != nil {
							return nil, err
						} else {
							fields = append(fields,
								reflect.StructField{Name: capitalizeMember(field.Name()),
									Type: ret,
									Tag:  reflect.StructTag(fmt.Sprintf(`avro:"%s" json:"%s"`, field.Name(), field.Name()))})
						}
					}
					record := reflect.StructOf(fields)
					return record, nil

				}
			default:
				{
					return nil, errors.New(fmt.Sprintf("Unsupported Option: %v", schema.Type()))
				}
			}
		}
	case avro.Array:
		{
			as := schema.(*avro.ArraySchema)
			items := as.Items()
			ret, err := ConvertAvroSchemaToStruct(items)
			if err != nil {
				return nil, err
			} else {
				return reflect.SliceOf(ret), nil
			}
		}
	case avro.Union:
		{
			us := schema.(*avro.UnionSchema)

			if len(us.Types()) != 2 {
				return nil, errors.New("Unsupported Union in avro schema")
			}

			var toConvert avro.Schema
			if isAvroNullType(us.Types()[0]) {
				toConvert = us.Types()[1]
			} else {
				toConvert = us.Types()[0]
			}

			ret, err := ConvertAvroSchemaToStruct(toConvert)
			if err != nil {
				return nil, err
			} else {
				retPtr := reflect.PointerTo(ret)
				return retPtr, nil
			}
		}
	case avro.String:
		{
			return reflect.TypeOf(""), nil
		}
	case avro.Bytes:
		{
			return reflect.TypeOf([]byte{}), nil
		}
	case avro.Int:
		{
			if ps, ok := schema.(*avro.PrimitiveSchema); ok {
				if ps.Logical() != nil {
					switch ps.Logical().Type() {
					case avro.Date:
						return reflect.TypeOf(time.Time{}), nil
					case avro.TimeMillis:
						return reflect.TypeOf(time.Duration(0)), nil
					default:
						return nil, fmt.Errorf("unhandled logical type for avro.Int: %v", ps.Logical().Type())
					}
				}
			}
			return reflect.TypeOf(int32(0)), nil
		}
	case avro.Long:
		{
			if ps, ok := schema.(*avro.PrimitiveSchema); ok {
				if ps.Logical() != nil {
					switch ps.Logical().Type() {
					case avro.TimestampMicros:
						return reflect.TypeOf(time.Time{}), nil
					case avro.TimestampMillis:
						return reflect.TypeOf(time.Time{}), nil
					case avro.TimeMicros:
						return reflect.TypeOf(time.Duration(0)), nil
					case avro.Date:
						return reflect.TypeOf(time.Time{}), nil
					default:
						return nil, fmt.Errorf("unhandled logical type for avro.Long: %v", ps.Logical().Type())
					}
				}
			}
			return reflect.TypeOf(int64(0)), nil
		}
	case avro.Float:
		{
			return reflect.TypeOf(float32(0)), nil
		}
	case avro.Double:
		{
			return reflect.TypeOf(float64(0)), nil
		}
	case avro.Boolean:
		{
			return reflect.TypeOf(bool(false)), nil
		}
	case avro.Null:
		{
			return reflect.TypeOf((*interface{})(nil)), nil
		}
	case avro.Error:
		{
			return nil, errors.New("Unsupported Option: *avro.Error\n")
		}
	case avro.Ref:
		{
			return nil, errors.New("Unsupported Option: *avro.Ref\n")
		}
	case avro.Enum:
		{
			return nil, errors.New("Unsupported Option: *avro.Enum\n")
		}
	case avro.Map:
		{
			return nil, errors.New("Unsupported Option: *avro.Map\n")
		}
	case avro.Fixed:
		{
			return nil, errors.New("Unsupported Option: *avro.Fixed\n")
		}
	default:
		{
			return nil, errors.New(fmt.Sprintf("Unsupported Option: %v", schema.Type()))
		}
	}
}

func isAvroNullType(schema avro.Schema) bool {
	if primitiveSchema, ok := schema.(*avro.NullSchema); ok {
		return primitiveSchema.Type() == avro.Null
	}
	return false
}

// This exports Struct's members
func capitalizeMember(s string) string {
	if len(s) == 0 {
		return s
	}
	return string(s[0]-32) + s[1:]
}
