package log

import "github.com/dgraph-io/badger/v2"

type BadgerLogger struct {
	Error, Warn, Info, Debug Logger
}

func (l *BadgerLogger) Errorf(fmt string, a ...interface{}) {
	if l.Error != nil {
		l.Error.Printf(fmt, a...)
	}
}

func (l *BadgerLogger) Warningf(fmt string, a ...interface{}) {
	if l.Warn != nil {
		l.Warn.Printf(fmt, a...)
	}
}

func (l *BadgerLogger) Infof(fmt string, a ...interface{}) {
	if l.Info != nil {
		l.Info.Printf(fmt, a...)
	}
}

func (l *BadgerLogger) Debugf(fmt string, a ...interface{}) {
	if l.Debug != nil {
		l.Debug.Printf(fmt, a...)
	}
}

var _ badger.Logger = (*BadgerLogger)(nil)
