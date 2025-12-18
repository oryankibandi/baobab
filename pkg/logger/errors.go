package logger

type LoggerError struct {
	Message string
}

func (e LoggerError) Error() string {
	return e.Message
}
