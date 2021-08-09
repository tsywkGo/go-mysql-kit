package localflusher

const (
	_defaultDir = "./data"
)

type Option func(flusher *Flusher)

func WithDir(dir string) Option {
	return func(flusher *Flusher) {
		if len(dir) == 0 {
			dir = _defaultDir
		}
		flusher.dir = dir
	}
}
