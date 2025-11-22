package wal

type WalError struct {
	Message string
}

func (e WalError) Error() string {
	return e.Message
}
