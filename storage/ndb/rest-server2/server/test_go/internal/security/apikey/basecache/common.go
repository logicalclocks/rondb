package basecache

import (
	"errors"
	"strings"
)

func ValidateApiKeyFormat(apiKey *string) error {
	if apiKey == nil {
		return errors.New("the apikey is nil")
	}
	splits := strings.Split(*apiKey, ".")
	if len(splits) != 2 || len(splits[0]) != 16 || len(splits[1]) < 1 {
		return errors.New("the apikey has an incorrect format")
	}
	return nil
}
