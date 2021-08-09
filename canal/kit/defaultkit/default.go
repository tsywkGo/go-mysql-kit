package defaultkit

type Kit struct{}

func New() (*Kit, error) {
	return &Kit{}, nil
}

func (k *Kit) Sink() error {
	return nil
}
