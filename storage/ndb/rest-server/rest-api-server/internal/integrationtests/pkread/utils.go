package pkread

import (
	"testing"

	"hopsworks.ai/rdrs/pkg/api"
)

func pkTestMultiple(t *testing.T, tests map[string]api.PKTestInfo, isBinaryData bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			pkTest(t, testInfo, isBinaryData, true)
		})
	}
}

func pkTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	pkRESTTest(t, testInfo, isBinaryData, validate)
	pkGRPCTest(t, testInfo, isBinaryData, validate)
}
