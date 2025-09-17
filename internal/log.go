// Package internal provides some internal functionalities for the library.
package internal

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
)

type logger interface {
	Errorf(format string, args ...any)
	Warn(args ...any)
	Warnf(format string, args ...any)
	Info(args ...any)
	Debugf(format string, args ...any)
}

// Sink represents a logging implementation to be used with the logr.Logger
// implementation.
type Sink struct {
	logger logger
	values map[any]any
	names  []string
}

// NewSink returns a Sink for using as the logr.Logger's sink.
func NewSink(l logger) *Sink {
	return &Sink{
		logger: l,
		values: make(map[any]any),
	}
}

// Init receives optional information about the logr library for LogSink
// implementations that need it.
func (s *Sink) Init(logr.RuntimeInfo) {}

// Enabled tests whether this LogSink is enabled at the specified V-level. For
// example, commandline flags might be used to set the logging verbosity and
// disable some info logs.
func (s *Sink) Enabled(int) bool { return true }

// Info logs a non-error message with the given key/value pairs as context. The
// level argument is provided for optional logging.  This method will only be
// called when Enabled(level) is true. See Logger.Info for more details.
func (s *Sink) Info(_ int, msg string, kvs ...any) {
	fields := s.getFields(kvs...)
	s.logger.Info(fmt.Sprintf("%s %s", msg, strings.Join(fields, " ")))
}

// Error logs an error, with the given message and key/value pairs as context.
// See Logger.Error for more details.
func (s *Sink) Error(err error, msg string, kvs ...any) {
	fields := s.getFields(kvs...)
	s.logger.Errorf("%v: %s %s", err, msg, strings.Join(fields, " "))
}

// WithValues returns a new LogSink with additional key/value pairs.  See
// Logger.WithValues for more details.
//
//nolint:ireturn // s implements logr.LogSink
func (s *Sink) WithValues(kv ...any) logr.LogSink {
	for i := 0; i < len(kv); i += 2 {
		s.values[kv[i]] = kv[i+1]
	}

	return s
}

// WithName returns a new LogSink with the specified name appended.  See
// Logger.WithName for more details.
//
//nolint:ireturn // Sink implements logr.LogSink.
func (s *Sink) WithName(string) logr.LogSink { return s }

func (s *Sink) getFields(kvs ...any) []string {
	fields := make([]string, 0, len(kvs)/2+len(s.names))
	for i := 0; i < len(kvs); i += 2 {
		fields = append(fields, fmt.Sprintf("%v=%v", kvs[i], kvs[i+1]))
	}

	fields = append(fields, s.names...)

	var key any

	for idx, keyOrValue := range kvs {
		if idx%2 == 0 {
			key = keyOrValue
			continue
		}

		fields = append(fields, fmt.Sprintf("%v=%v", key, keyOrValue))
	}

	return fields
}
