package handlers

import (
	"mime/multipart"
	"net/http"

	"github.com/gofiber/fiber/v2"
	"github.com/jaredtokuz/market-trader/api/presenter"
	"github.com/jaredtokuz/market-trader/pkg/stocks"
)

func GetBySymbol(service stocks.Service, collectionName string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		symbol := c.Params("symbol")
		result, err := service.FindOneSymbol(collectionName, symbol)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return c.JSON(presenter.ErrorResponse(err)) 
		}
		return c.JSON(presenter.SuccessResponse(result)) 
	}
}

func UploadStocks(service stocks.Service) fiber.Handler {
	return func(c *fiber.Ctx) error {
		fileContent, err := formToContent(c,"data")
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return c.JSON(presenter.ErrorResponse(err)) 
		}
		defer fileContent.Close()
		err = service.UploadStocks(fileContent)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return c.JSON(presenter.ErrorResponse(err)) 
		}
		return c.JSON(presenter.SimpleSuccessResponse("file uploaded successfully")) 
	}
}

// generic for returning filecontent
func formToContent(c *fiber.Ctx, formName string) (multipart.File, error) {
	// Get first file from form field "data":
	file, err := c.FormFile(formName)
	if err != nil {
		return nil, err
	}
	// the actual file content to be passed to a reader
	fileContent, err := file.Open()
	if err != nil {
		return nil, err
	}
	return fileContent, nil
}
