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

package config

import (
	"encoding/json"
	"fmt"
	"os"
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
	file, err := os.ReadFile(configFile)
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
