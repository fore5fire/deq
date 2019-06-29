package deqdb

import (
	"sync"
)

// Logger is a logger for debug information. The Logger from the standard log package implements
// this interface.
type Logger interface {
	Printf(format string, a ...interface{})
}

// NewFuncLogger returns a logger that calls a callback func.
func NewFuncLogger(callback func(format string, a ...interface{})) Logger {
	return &callbackLogger{
		callback: callback,
	}
}

type callbackLogger struct {
	callback func(string, ...interface{})
	mut      sync.Mutex
}

// Printf calls the logger's callback.
func (c *callbackLogger) Printf(format string, a ...interface{}) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.callback(format, a...)
}

type disabledLogger struct{}

func (disabledLogger) Printf(fmt string, a ...interface{}) {}

type badgerLogger struct {
	info, debug Logger
}

func (b badgerLogger) Errorf(format string, a ...interface{}) {
	b.info.Printf(format, a...)
}

func (b badgerLogger) Warningf(format string, a ...interface{}) {
	b.debug.Printf(format, a...)
}

func (b badgerLogger) Infof(format string, a ...interface{}) {
	b.debug.Printf(format, a...)
}

func (b badgerLogger) Debugf(format string, a ...interface{}) {
	b.debug.Printf(format, a...)
}
