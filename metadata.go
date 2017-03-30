package da

// metaStore stores metadata.
type metaStore struct{}

func (m *metaStore) Set(key string, data interface{}) error {
	return nil
}

func (m *metaStore) Get(key string, data interface{}) error {
	return nil
}
