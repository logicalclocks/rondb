package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

func GetAll() AllConfigs {
	return globalConfig
}

// TODO: Add test with embedded config_template.json

func SetFromFileIfExists(configFile string) error {
	var err error
	if configFile != "" {
		err = SetFromFile(configFile)
	} else {
		err = SetToDefaults()
	}
	return err
}

func SetFromFile(configFile string) error {
	newConfigs := newWithDefaults()
	file, err := ioutil.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("failed reading config file; error: %v", err)
	}
	err = json.Unmarshal(file, &newConfigs)
	if err != nil {
		return fmt.Errorf("failed unmarshaling config file; error: %v", err)
	}
	return SetAll(newConfigs)
}

func SetToDefaults() error {
	newConfigs := newWithDefaults()
	return SetAll(newConfigs)
}

func SetAll(newConfig AllConfigs) error {
	mutex.Lock()
	defer mutex.Unlock()
	if err := newConfig.Validate(); err != nil {
		return err
	}
	globalConfig = newConfig
	return nil
}
