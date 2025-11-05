package lrulist

type LruError struct {
	Message string
}

func (e LruError) Error() string {
	return e.Message
}
