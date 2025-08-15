package pglogrepl

type Logger interface {
	LogInfo(format string, args ...any)
	LogError(format string, args ...any)
	LogWarn(format string, args ...any)
}

type LoggerFunc func(level, format string, args ...any)

func (f LoggerFunc) LogInfo(format string, args ...any) {
	f("INFO", format, args...)
}

func (f LoggerFunc) LogError(format string, args ...any) {
	f("ERROR", format, args...)
}

func (f LoggerFunc) LogWarn(format string, args ...any) {
	f("WARN", format, args...)
}
