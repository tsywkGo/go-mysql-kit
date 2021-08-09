package flusher

type IFlusher interface {
	Write(key string, data []byte) error
	Read(key string) ([]byte, error)
	Close() error
}
