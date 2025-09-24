package helpers

type HelperError struct {
	Message string
}

func (e HelperError) Error() string {
	return e.Message
}
