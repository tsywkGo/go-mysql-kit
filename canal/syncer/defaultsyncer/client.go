package defaultsyncer

type FlushClient interface {
	Write(id int64, data []byte) error
	Read(id int64) ([]byte, error)
}
