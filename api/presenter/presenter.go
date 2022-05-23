package presenter

import (
	"github.com/gofiber/fiber/v2"
)

// Default Success
func SuccessResponse(data ...interface{}) *fiber.Map {
	return &fiber.Map{
		"status": true,
		"data":   data,
		"error":  nil,
	}
}
// Default ErrorResponse
func ErrorResponse(err error) *fiber.Map {
	return &fiber.Map{
		"status": false,
		"data":   "",
		"error":  err.Error(),
	}
}
type SimpleData struct {
	message string
}
// Simple Success
func SimpleSuccessResponse(message string) *fiber.Map {
	return &fiber.Map{
		"status": true,
		"data":   message,
		"error":  nil,
	}
}