package rest

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (h *RouteHandler) BatchPkRead(c *gin.Context) {
	apiKey := c.GetHeader(config.API_KEY_NAME)

	operations := api.BatchOpRequest{}
	err := c.ShouldBindJSON(&operations)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	if operations.Operations == nil {
		c.AbortWithError(http.StatusBadRequest, errors.New("'operations' is missing in payload"))
		return
	}

	// TODO: Place this into Validate() of batchPkReadHandler
	pkOperations := make([]*api.PKReadParams, len(*operations.Operations))
	for i, operation := range *operations.Operations {
		pkOperations[i] = &api.PKReadParams{}
		err := parseOperation(&operation, pkOperations[i])
		if err != nil {
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	}

	var responseIntf api.BatchOpResponse = (api.BatchOpResponse)(&api.BatchResponseJSON{})
	responseIntf.Init()

	status, err := handlers.Handle(h.batchPkReadHandler, &apiKey, &pkOperations, responseIntf)
	if err != nil {
		c.AbortWithError(status, err)
		return
	}
	c.JSON(status, responseIntf.(*api.BatchResponseJSON))
}

func parseOperation(operation *api.BatchSubOp, pkReadarams *api.PKReadParams) error {
	// remove leading / character
	if strings.HasPrefix(*operation.RelativeURL, "/") {
		trimmed := strings.Trim(*operation.RelativeURL, "/")
		operation.RelativeURL = &trimmed
	}

	match, err := regexp.MatchString("^[a-zA-Z0-9$_]+/[a-zA-Z0-9$_]+/pk-read",
		*operation.RelativeURL)
	if err != nil {
		return fmt.Errorf("error parsing relative URL: %w", err)
	} else if !match {
		return fmt.Errorf("invalid relative URL: %s", *operation.RelativeURL)
	}
	return makePKReadParams(operation, pkReadarams)
}

func makePKReadParams(operation *api.BatchSubOp, pkReadarams *api.PKReadParams) error {
	if operation.Body == nil {
		return errors.New("body of operation is nil")
	}
	params := *operation.Body

	// split the relative url to extract path parameters
	splits := strings.Split(*operation.RelativeURL, "/")
	if len(splits) != 3 {
		return errors.New("Failed to extract database and table information from relative url")
	}

	pkReadarams.DB = &splits[0]
	pkReadarams.Table = &splits[1]
	pkReadarams.Filters = params.Filters
	pkReadarams.ReadColumns = params.ReadColumns
	pkReadarams.OperationID = params.OperationID

	return nil
}
