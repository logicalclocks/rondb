/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023,2025 Hopsworks AB
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

package feature_store

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hamba/avro/v2"
	"hopsworks.ai/rdrs2/internal/log"
)

// parser
func ConvertAvroSchemaToStruct(schema avro.Schema) (reflect.Type, error) {
	log.Debug(fmt.Sprintf("\n--> ConvertAvroSchemaToStruct Called %v\n", schema))
	switch schema.Type() {
	case avro.Record:
		{
			log.Debug(fmt.Sprintf("avro.Record  %T\n", schema))
			switch schema.(type) {
			case *avro.RecordSchema:
				{
					log.Debug(fmt.Sprintf("*avro.RecordSchema\n"))
					rs := schema.(*avro.RecordSchema)
					var fields []reflect.StructField
					for _, field := range rs.Fields() {
						log.Debug(fmt.Sprintf("Name: %s, Type: %T\n", field.Name(), field))
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
					log.Debug(fmt.Sprintf("RETURNING RecordSchema, Record %v\n", record))
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
			log.Debug(fmt.Sprintf("avro.Array   %T\n", schema))
			as := schema.(*avro.ArraySchema)
			items := as.Items()
			ret, err := ConvertAvroSchemaToStruct(items)
			if err != nil {
				return nil, err
			} else {
				log.Debug(fmt.Sprintf("RETURNING Array of  %v\n", ret))
				return reflect.SliceOf(ret), nil
			}
		}
	case avro.Union:
		{
			log.Debug(fmt.Sprintf("avro.Union     %T\n", schema))
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
				log.Debug(fmt.Sprintf("RETURNING UnionSchema %v\n", retPtr))
				return retPtr, nil
			}
		}
	case avro.String:
		{
			log.Debug(fmt.Sprintf("avro.String  %T\n", schema))
			return reflect.TypeOf(""), nil
		}
	case avro.Bytes:
		{
			log.Debug(fmt.Sprintf("*avro.Bytes\n"))
			return reflect.TypeOf([]byte{}), nil
		}
	case avro.Int:
		{
			log.Debug(fmt.Sprintf("*avro.Int\n"))
			if ps, ok := schema.(*avro.PrimitiveSchema); ok {
				if ps.Logical() != nil {
					log.Debug(fmt.Sprintf("*avro.Int. Logical Type: %v\n", ps.Logical().Type()))
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
			log.Debug(fmt.Sprintf("avro.Long   %T\n", schema))
			if ps, ok := schema.(*avro.PrimitiveSchema); ok {
				if ps.Logical() != nil {
					log.Debug(fmt.Sprintf("*avro.Long. Logical Type: %v\n", ps.Logical().Type()))
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
			log.Debug(fmt.Sprintf("*avro.Float\n"))
			return reflect.TypeOf(float32(0)), nil
		}
	case avro.Double:
		{
			log.Debug(fmt.Sprintf("*avro.Double\n"))
			return reflect.TypeOf(float64(0)), nil
		}
	case avro.Boolean:
		{
			log.Debug(fmt.Sprintf("*avro.Boolean\n"))
			return reflect.TypeOf(bool(false)), nil
		}
	case avro.Null:
		{
			log.Debug(fmt.Sprintf("*avro.Null\n"))
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
