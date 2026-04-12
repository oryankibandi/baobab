package pager

type PagerError struct {
	Message string
}

func (e PagerError) Error() string {
	return e.Message
}

type FreelistError struct {
	Message string
}

func (e FreelistError) Error() string {
	return e.Message
}
