package pager

type PagerError struct {
	Message string
}

func (e PagerError) Error() string {
	return e.Message
}
