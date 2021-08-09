package localflusher

import (
	"io/ioutil"
	"path/filepath"
	"time"
)

type Flusher struct {
	dir      string
	duration time.Duration
}

func New(opts ...Option) (*Flusher, error) {
	flusher := new(Flusher)
	for _, opt := range opts {
		opt(flusher)
	}
	return flusher, nil
}

func (f *Flusher) Write(key string, data []byte) error {
	return ioutil.WriteFile(f.filepath(key), data, 0666)
}

func (f *Flusher) Read(key string) ([]byte, error) {
	return ioutil.ReadFile(f.filepath(key))
}

func (f *Flusher) Close() error {
	return nil
}

func (f *Flusher) filepath(key string) string {
	return filepath.Join(f.dir, "binlog-syncer", "snapshot", key)
}
