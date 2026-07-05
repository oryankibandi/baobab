package bptree

type BTreeError struct {
	Message string
}

func (e BTreeError) Error() string {
	return e.Message
}
