package feature_store

import (
	"reflect"
	"testing"

	"hopsworks.ai/rdrs/internal/handlers/feature_store"
	"hopsworks.ai/rdrs/internal/log"

	"github.com/linkedin/goavro/v2"
)

func testConvertAvroToJson(t *testing.T, schema string, data []byte, expectedJson interface{}) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err.Error())
	}
	native, _, err := codec.NativeFromBinary(data)
	if err != nil {
		t.Fatal(err.Error())
	}

	actual := feature_store.ConvertAvroToJson(native)

	if !reflect.DeepEqual(actual, expectedJson) {
		t.Errorf("Got %s (%s) but expect %s (%s)\n", actual, reflect.TypeOf(actual), expectedJson, reflect.TypeOf(expectedJson))
	}
	log.Debugf("Got %s (%s); expect %s (%s)\n", actual, reflect.TypeOf(actual), expectedJson, reflect.TypeOf(expectedJson))
}

func TestConvertAvroToJson(t *testing.T) {
	// map
	testConvertAvroToJson(
		t,
		`["null",{"type":"record","name":"r854762204","namespace":"struct","fields":[{"name":"int1","type":["null","long"]},{"name":"int2","type":["null","long"]}]}]`,
		[]byte{0x02, 0x02, 0x56, 0x02, 0x88, 0x01},
		map[string]interface{}{"int1": int64(43), "int2": int64(68)},
	)
	// array
	testConvertAvroToJson(
		t,
		`["null",{"type":"array","items":["null","long"]}]`,
		[]byte{0x02, 0x06, 0x02, 0x02, 0x00, 0x02, 0x06, 0x00},
		[]interface{}{int64(1), nil, int64(3)},
	)
	// array of map
	testConvertAvroToJson(
		t,
		`["null",{"type":"array","items":["null",{"type":"record","name":"r854762204","namespace":"struct","fields":[{"name":"int1","type":["null","long"]},{"name":"int2","type":["null","long"]}]}]}]`,
		[]byte{0x02, 0x06, 0x02, 0x02, 0x22, 0x02, 0x48, 0x00, 0x02, 0x02, 0x24, 0x02, 0x4A, 0x00},
		[]interface{}{map[string]interface{}{"int1": int64(17), "int2": int64(36)}, nil, map[string]interface{}{"int1": int64(18), "int2": int64(37)}},
	)
	// map of array
	testConvertAvroToJson(
		t,
		`["null",{"type":"record","name":"r854762204","namespace":"struct","fields":[{"name":"int1","type":["null",{"type":"array","items":["null","long"]}]},{"name":"int2","type":["null",{"type":"array","items":["null","long"]}]}]}]`,
		[]byte{0x02, 0x02, 0x06, 0x02, 0x02, 0x02, 0x04, 0x02, 0x06, 0x00, 0x02, 0x06, 0x02, 0x06, 0x00, 0x02, 0x0a, 0x00},
		map[string]interface{}{
			"int1": []interface{}{int64(1), int64(2), int64(3)}, 
			"int2": []interface{}{int64(3), nil, int64(5)}, 
		},
	)

}
