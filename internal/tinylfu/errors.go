package tinylfu

type TinyLFUError struct {
	Message string
}

func (e TinyLFUError) Error() string {
	return e.Message
}

type CountMinSketchError struct {
	Message string
}

func (e CountMinSketchError) Error() string {
	return e.Message
}
