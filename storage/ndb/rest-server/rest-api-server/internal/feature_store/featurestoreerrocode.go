/* Copyright (c) 2023 Hopsworks AB
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
	"encoding/json"
	"fmt"
	"net/http"
)

type RestErrorCode struct {
	code    int
	reason  string
	status  int
	message string
}

func (err *RestErrorCode) NewMessage(msg string) *RestErrorCode {
	var errCopy = *err
	errCopy.message = msg
	return &errCopy
}

func (err *RestErrorCode) GetCode() int {
	return err.code
}

func (err *RestErrorCode) GetReason() string {
	return err.reason
}

func (err *RestErrorCode) GetStatus() int {
	return err.status
}

func (err *RestErrorCode) GetMessage() string {
	return err.message
}

func (err *RestErrorCode) String() string {
	var m = map[string]interface{}{
		"code":    err.code,
		"reason":  err.reason,
		"message": err.message,
	}
	strBytes, jsonErr := json.MarshalIndent(m, "", "\t")
	if jsonErr != nil {
		return fmt.Sprintf("Failed to marshal FeatureStoreErrorResponse. Error: %s", jsonErr)
	} else {
		return fmt.Sprintf("%s", strBytes)
	}
}

func (err *RestErrorCode) Error() string {
	return err.String()
}

func (err *RestErrorCode) GetError() error {
	return fmt.Errorf(err.Error())
}

var FV_NOT_EXIST = &RestErrorCode{1, "Feature view does not exist.", http.StatusNotFound, ""}
var FS_NOT_EXIST = &RestErrorCode{2, "Feature store does not exist.", http.StatusNotFound, ""}
var FG_NOT_EXIST = &RestErrorCode{3, "Feature group does not exist.", http.StatusNotFound, ""}
var FG_READ_FAIL = &RestErrorCode{4, "Reading feature group failed.", http.StatusInternalServerError, ""}
var FS_READ_FAIL = &RestErrorCode{5, "Reading feature store failed.", http.StatusInternalServerError, ""}
var FV_READ_FAIL = &RestErrorCode{6, "Reading feature view failed.", http.StatusInternalServerError, ""}
var TD_JOIN_READ_FAIL = &RestErrorCode{7, "Reading training dataset join failed.", http.StatusInternalServerError, ""}
var TD_FEATURE_READ_FAIL = &RestErrorCode{8, "Reading training dataset feature failed.", http.StatusInternalServerError, ""}
var FETCH_METADATA_FROM_CACHE_FAIL = &RestErrorCode{9, "Fetching metadata from cache failed.", http.StatusInternalServerError, ""}
var WRONG_DATA_TYPE = &RestErrorCode{10, "Wrong data type.", http.StatusUnsupportedMediaType, ""}
var FEATURE_NOT_EXIST = &RestErrorCode{11, "Feature does not exist.", http.StatusNotFound, ""}
var INCORRECT_PRIMARY_KEY = &RestErrorCode{12, "Incorrect primary key.", http.StatusBadRequest, ""}
var INCORRECT_PASSED_FEATURE = &RestErrorCode{13, "Incorrect passed feature.", http.StatusBadRequest, ""}
var READ_FROM_DB_FAIL = &RestErrorCode{14, "Reading from db failed.", http.StatusInternalServerError, ""}
var NO_PRIMARY_KEY_GIVEN = &RestErrorCode{15, "No primary key is given.", http.StatusBadRequest, ""}
var INCORRECT_FEATURE_VALUE = &RestErrorCode{16, "Incorrect feature value.", http.StatusBadRequest, ""}
var FEATURE_STORE_NOT_SHARED = &RestErrorCode{17, "Accessing unshared feature store failed", http.StatusUnauthorized, ""}
var READ_FROM_DB_FAIL_BAD_INPUT = &RestErrorCode{14, "Reading from db failed.", http.StatusBadRequest, ""}
