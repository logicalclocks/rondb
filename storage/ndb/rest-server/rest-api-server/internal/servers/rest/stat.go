package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (h *RouteHandler) Stat(c *gin.Context) {
	apiKey := c.GetHeader(config.API_KEY_NAME)
	statResp := api.StatResponse{}
	status, err := handlers.Handle(h.statsHandler, &apiKey, nil, &statResp)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	c.JSON(status, statResp)
}
