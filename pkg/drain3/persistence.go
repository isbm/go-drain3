package drain3

import "context"

const LockID = 42

type PersistenceInformation struct {
	StorageType string `json:"storage_type"` // Type of storage, e.g., "pgx", "file", etc.
	StorageName string `json:"storage_name"` // Name of the storage, e.g., "my_table", "my_file.txt", etc.
	MaxClusters int    `json:"max_clusters"` // Number of clusters in the storage
	RecordCount int    `json:"record_count"` // Number of records in the storage
	LastUpdated string `json:"last_updated"` // Last updated timestamp in RFC3339 format
}

type PersistenceHandler interface {
	Save(ctx context.Context, state []byte) error
	Load(ctx context.Context) ([]byte, error)
	Flush() (string, error)                // Flush clears the target storage from existing data
	Teardown() (string, error)             // TearDown cleans up the storage, e.g., drops the table in Db or file etc
	Info() (PersistenceInformation, error) // Info retrieves information about the persistence state
	Lock() error                           // Lock the persistence to prevent concurrent writes
	Unlock() error                         // Unlock the persistence
	IsLocked() bool                        // Check if the persistence is locked
}
