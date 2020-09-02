package harego

import (
	"github.com/pkg/errors"
)

var (
	// ErrInput is returned when an input is invalid.
	ErrInput = errors.New("invalid input")

	// ErrNilHnadler is returned when the handler is nil.
	ErrNilHnadler = errors.New("handler can not be nil")

	// ErrClosed is returned when the Client is closed and is being reused.
	ErrClosed = errors.New("exchange is already closed")

	// ErrAlreadyStarted is returned when an already started client is about to
	// be configured.
	ErrAlreadyStarted = errors.New("client is already started")
)
