package api

import (
	"strings"

	"github.com/labstack/echo-contrib/jaegertracing"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type APIRestServer struct {
	echoInstance *echo.Echo
	globalGroup  *echo.Group
	port         string
	routes       func(*echo.Group)
}

func NewAPIRestServer(port, globalPrefix string, routes func(*echo.Group)) *APIRestServer {
	e := echo.New()
	return &APIRestServer{
		echoInstance: e,
		globalGroup:  e.Group(globalPrefix),
		port:         port,
		routes:       routes,
	}
}

func (api *APIRestServer) RunServer() {
	c := jaegertracing.New(api.echoInstance, func(c echo.Context) bool {
		return strings.Contains(c.Path(), "/healthz") || c.Path() == "/"
	})

	defer c.Close()

	api.echoInstance.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		CustomTimeFormat: "2006-01-02 15:04:05",
		Format:           "[${time_custom}] - method=${method}, uri=${uri}, status=${status}\n",
	}))

	api.routes(api.globalGroup)
	api.echoInstance.Logger.Fatal(api.echoInstance.Start(":" + api.port))
}
