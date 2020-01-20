package backup

type Logger interface {
	Printf(fmt string, a ...interface{})
}

type noopLogger struct{}

func (noopLogger) Printf(string, ...interface{}) {}
