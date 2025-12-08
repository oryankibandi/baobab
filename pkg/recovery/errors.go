package recovery

type RecoveryError struct {
	Message string
}

type InvalidLogError struct {
	Message string
}

func (e RecoveryError) Error() string {
	return e.Message
}

func (e InvalidLogError) Error() string {
	return e.Message
}
