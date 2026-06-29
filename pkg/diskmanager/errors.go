package diskmanager

type DiskManagerError struct {
	Message string
}

func (e DiskManagerError) Error() string {
	return e.Message
}
