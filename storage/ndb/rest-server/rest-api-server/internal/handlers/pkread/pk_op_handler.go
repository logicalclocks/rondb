/*

 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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

package pkread

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type PKRead struct{}

var _ handlers.PKReader = (*PKRead)(nil)
var pkRead PKRead

func GetPKReader() handlers.PKReader {
	return &pkRead
}

func (p *PKRead) PkReadHttpHandler(c *gin.Context) {
	pkReadParams := api.PKReadParams{}

	err := ParseRequest(c, &pkReadParams)
	if err != nil {
		if log.IsDebug() {
			msg := fmt.Sprintf("failed parsing request. Error: %v", err)
			body, err := ioutil.ReadAll(c.Request.Body)
			if err != nil {
				log.Debugf(fmt.Sprintf("%s. failed reading request body. Error: %v", msg, err))
			} else {
				log.Debugf("%s. Request body: %s", msg, body)
			}
		}
		common.SetResponseBodyError(c, http.StatusBadRequest, err)
		return
	}

	apiKey := getAPIKey(c)
	processRequestNSetStatus(c, &pkReadParams, apiKey)
}

func processRequestNSetStatus(c *gin.Context, pkReadParams *api.PKReadParams, apiKey *string) {
	var response api.PKReadResponse = (api.PKReadResponse)(&api.PKReadResponseJSON{})
	response.Init()

	status, err := pkRead.PkReadHandler(pkReadParams, apiKey, response)

	if err != nil {
		common.SetResponseBodyError(c, status, err)
		return
	}

	common.SetResponseBody(c, status, &response)
}

func (p *PKRead) PkReadHandler(pkReadParams *api.PKReadParams, apiKey *string, response api.PKReadResponse) (int, error) {
	err := checkAPIKey(apiKey, pkReadParams.DB)
	if err != nil {
		return http.StatusUnauthorized, err
	}

	reqBuff, respBuff, err := CreateNativeRequest(pkReadParams)
	defer dal.ReturnBuffer(reqBuff)
	defer dal.ReturnBuffer(respBuff)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	dalErr := dal.RonDBPKRead(reqBuff, respBuff)
	if dalErr != nil && dalErr.HttpCode != http.StatusNotFound { // any other error return immediately
		return dalErr.HttpCode, dalErr
	}

	status, err := ProcessPKReadResponse(respBuff, response)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	return int(status), nil
}

/*
	Parsing both GET and POST parameters
*/
func ParseRequest(c *gin.Context, pkReadParams *api.PKReadParams) error {

	body := api.PKReadBody{}
	pp := api.PKReadPP{}

	if err := parseURI(c, &pp); err != nil {
		return err
	}

	if err := ParseBody(c.Request, &body); err != nil {
		return err
	}

	pkReadParams.DB = pp.DB
	pkReadParams.Table = pp.Table
	pkReadParams.Filters = body.Filters
	pkReadParams.ReadColumns = body.ReadColumns
	pkReadParams.OperationID = body.OperationID

	return ValidatePKReadRequest(pkReadParams)
}

func ParseBody(req *http.Request, params *api.PKReadBody) error {

	b := binding.JSON
	err := b.Bind(req, &params)
	if err != nil {
		return err
	}
	return nil
}

func parseURI(c *gin.Context, resource *api.PKReadPP) error {
	err := c.ShouldBindUri(&resource)
	if err != nil {
		return err
	}
	return nil
}

func getAPIKey(c *gin.Context) *string {
	apiKey := c.GetHeader(config.API_KEY_NAME)
	return &apiKey
}

// TODO: Place this into middleware
func checkAPIKey(apiKey *string, db *string) error {
	// check for Hopsworks api keys
	if config.Configuration().Security.UseHopsWorksAPIKeys {
		if apiKey == nil || *apiKey == "" { // not set
			return fmt.Errorf("Unauthorized. No API key supplied")
		}
		return apikey.ValidateAPIKey(apiKey, db)
	}
	return nil
}

func ValidatePKReadRequest(req *api.PKReadParams) error {
	if err := validateDBIdentifier(*req.DB); err != nil {
		return err
	}

	if err := validateDBIdentifier(*req.Table); err != nil {
		return err
	}

	err := ValidateBody(req)
	if err != nil {
		return err
	}

	return nil
}

func validateDBIdentifier(identifier string) error {
	if len(identifier) < 1 || len(identifier) > 64 {
		return errors.New("field length validation failed")
	}

	//https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	for _, r := range identifier {
		if !((r >= rune(0x0001) && r <= rune(0x007F)) || (r >= rune(0x0080) && r <= rune(0x0FFF))) {
			return fmt.Errorf("field validation failed. Invalid character '%U' ", r)
		}
	}
	return nil
}

func ValidateBody(params *api.PKReadParams) error {

	for _, filter := range *params.Filters {
		// make sure filter columns are valid
		if err := validateDBIdentifier(*filter.Column); err != nil {
			return err
		}
	}

	// make sure that the columns are unique.
	existingFilters := make(map[string]bool)
	for _, filter := range *params.Filters {
		if _, ok := existingFilters[*filter.Column]; ok {
			return fmt.Errorf("field validation for filter failed on the 'unique' tag")
		} else {
			existingFilters[*filter.Column] = true
		}
	}

	// make sure read columns are valid
	if params.ReadColumns != nil {
		for _, col := range *params.ReadColumns {
			if err := validateDBIdentifier(*col.Column); err != nil {
				return err
			}
		}
	}

	// make sure that the filter columns and read colummns do not overlap
	// and read cols are unique
	if params.ReadColumns != nil {
		existingCols := make(map[string]bool)
		for _, readCol := range *params.ReadColumns {
			if _, ok := existingFilters[*readCol.Column]; ok {
				return fmt.Errorf("field validation for read columns faild. '%s' already included in filter", *readCol.Column)
			}

			if _, ok := existingCols[*readCol.Column]; ok {
				return fmt.Errorf("field validation for 'ReadColumns' failed on the 'unique' tag.")
			} else {
				existingCols[*readCol.Column] = true
			}
		}
	}

	return nil
}
