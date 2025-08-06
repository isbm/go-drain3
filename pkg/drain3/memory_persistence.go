package drain3

import "context"

type MemoryPersistence struct {
	State []byte
}

func NewMemoryPersistence() *MemoryPersistence {
	return &MemoryPersistence{}
}

func (p *MemoryPersistence) Save(_ context.Context, state []byte) error {
	p.State = state
	return nil
}

func (p *MemoryPersistence) Load(_ context.Context) ([]byte, error) {
	return p.State, nil
}

func (p *MemoryPersistence) Flush() (string, error) {
	p.State = nil
	return "Memory state flushed successfully", nil
}

func (p *MemoryPersistence) Teardown() (string, error) {
	p.State = nil
	return "Teardown complete", nil
}

func (p *MemoryPersistence) Info() (PersistenceInformation, error) {
	return PersistenceInformation{
		StorageType: "memory",
		StorageName: "in-memory storage",
		MaxClusters: 0,
		RecordCount: 0,
		LastUpdated: "",
	}, nil
}

func (p *MemoryPersistence) Lock() error {
	// Memory persistence does not support locking
	return nil
}

func (p *MemoryPersistence) Unlock() error {
	// Memory persistence does not support unlocking
	return nil
}

func (p *MemoryPersistence) IsLocked() bool {
	// Memory persistence does not support locking
	return false
}
