package drain3

import "context"

type PersistenceHandler interface {
	Save(ctx context.Context, state []byte) error
	Load(ctx context.Context) ([]byte, error)
	Flush() (string, error)             // Flush clears the target storage from existing data
	Teardown(ctx context.Context) error // TearDown cleans up the storage, e.g., drops the table in Db or file etc
}
