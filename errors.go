package harego

import (
	"errors"
)

var (
	// ErrInput is returned when an input is invalid.
	ErrInput = errors.New("invalid input")

	// ErrNilHnadler is returned when the handler is nil.
	ErrNilHnadler = errors.New("handler can not be nil")

	// ErrClosed is returned when the Client is closed and is being reused.
	ErrClosed = errors.New("exchange is already closed")

	// ErrAlreadyConfigured is returned when an already configured client is
	// about to receive new configuration.
	ErrAlreadyConfigured = errors.New("client is already configured")
)
