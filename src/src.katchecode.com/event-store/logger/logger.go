package logger

import (
	"github.com/rs/zerolog"
	"os"
	"src.katchecode.com/event-store/env"
)

// Logger is a zerolog instance with server-wide configuration
var Logger = zerolog.New(os.Stderr).With().Timestamp().Str("svc", "event-store").Logger()

func init() {

	zerolog.MessageFieldName = "msg"
	zerolog.LevelFieldName = "lvl"
	zerolog.TimestampFieldName = "ts"

	if env.Debug {
		Logger = Logger.Level(zerolog.DebugLevel)
	} else {
		Logger = Logger.Level(zerolog.InfoLevel)
	}

	if env.Develop {
		Logger = Logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		zerolog.TimeFieldFormat = ""
	}
}

// With creates a child logger with fields added to its context
func With() zerolog.Context {
	return Logger.With()
}
