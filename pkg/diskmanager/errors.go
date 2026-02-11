package diskmanager

type DiskioError struct {
	Message string
}

func (e DiskioError) Error() string {
	return e.Message
}
