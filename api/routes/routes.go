package routes

import (
	"github.com/jaredtokuz/market-trader/api/handlers"
	"github.com/jaredtokuz/market-trader/pkg/stocks"

	"github.com/gofiber/fiber/v2"
)

// BookRouter is the Router for GoFiber App
func StockRouter(app fiber.Router, service stocks.Service) {
	app.Post("/upload-stocks", handlers.UploadStocks(service))
	app.Get("/stocks/:symbol", handlers.GetBySymbol(service, "stocks"))
	app.Get("/day15min30/:symbol", handlers.GetBySymbol(service, "stocks"))
	app.Get("/day2min15/:symbol", handlers.GetBySymbol(service, "stocks"))
	app.Get("/min15signals/:symbol", handlers.GetBySymbol(service, "stocks"))
}
