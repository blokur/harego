package harego

import (
	"github.com/pkg/errors"
)

var (
	// ErrInput is returned when an input is invalid.
	ErrInput = errors.New("invalid input")

	// ErrClosed is returned when the Client is closed and is being reused.
	ErrClosed = errors.New("exchange is already closed")
)
