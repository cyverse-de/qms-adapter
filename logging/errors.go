package logging

import (
	"encoding/json"
)

// ErrorResponse represents an HTTP response body containing error information. This type implements
// the error interface so that it can be returned as an error from from existing functions.
//
// swagger:response errorResponse
type ErrorResponse struct {
	Message   string                  `json:"message"`
	ErrorCode int                     `json:"error_code,omitempty"`
	Details   *map[string]interface{} `json:"details,omitempty"`
}

// ErrorBytes returns a byte-array representation of an ErrorResponse.
func (e ErrorResponse) ErrorBytes() []byte {
	bytes, err := json.Marshal(e)
	if err != nil {
		Log.Errorf("unable to marshal %+v as JSON", e)
		return make([]byte, 0)
	}
	return bytes
}

// Error returns a string representation of an ErrorResponse.
func (e ErrorResponse) Error() string {
	return string(e.ErrorBytes())
}

// NewErrorResponse constructs an ErrorResponse based on the message passed in, but does not send
// it over the wire. This is to aid in converting to labstack/echo.
func NewErrorResponse(err error) ErrorResponse {
	var errorResponse ErrorResponse
	switch val := err.(type) {
	case ErrorResponse:
		errorResponse = val
	case error:
		errorResponse = ErrorResponse{Message: val.Error()}
	}
	return errorResponse
}
