package buffermanager

type BufferManagerError struct {
	Message string
}

type WTinyLFUError struct {
	Message string
}

type LRUError struct {
	Message string
}

func (e BufferManagerError) Error() string {
	return e.Message
}

func (e WTinyLFUError) Error() string {
	return e.Message
}

func (e LRUError) Error() string {
	return e.Message
}
