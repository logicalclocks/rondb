/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
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
