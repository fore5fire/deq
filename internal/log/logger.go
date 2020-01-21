package log

type Logger interface {
	Printf(fmt string, a ...interface{})
}

type NoOpLogger struct{}

func (NoOpLogger) Printf(string, ...interface{}) {}
