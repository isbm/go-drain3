package drain3

import (
	"context"
	"fmt"
	"os"
	"time"
)

type FilePersistence struct {
	lockPath  string
	filePath  string
	timeStamp time.Time
}

func NewFilePersistence(filePath string) *FilePersistence {
	return &FilePersistence{filePath: filePath, lockPath: filePath + ".lock", timeStamp: time.Now()}
}

func (p *FilePersistence) Save(_ context.Context, state []byte) error {
	if err := os.WriteFile(p.filePath, state, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	p.timeStamp = time.Now()

	return nil
}

func (p *FilePersistence) Load(_ context.Context) ([]byte, error) {
	if _, err := os.Stat(p.filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file not found: %w", err)
	}

	state, err := os.ReadFile(p.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return state, nil
}

func (p *FilePersistence) Flush() (string, error) {
	file, err := os.OpenFile(p.filePath, os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to truncate file: %w", err)
	}
	defer file.Close()

	return "File flushed successfully", nil
}

func (p *FilePersistence) Teardown() (string, error) {
	if err := os.Remove(p.filePath); err != nil {
		return "", fmt.Errorf("failed to remove file during teardown: %w", err)
	}

	return "Teardown complete", nil
}

func (p *FilePersistence) Info() (PersistenceInformation, error) {
	info := PersistenceInformation{
		StorageType: "file",
		StorageName: p.filePath,
		MaxClusters: 0, // Not applicable for file storage
		RecordCount: 0, // Not applicable for file storage
		LastUpdated: p.timeStamp.Format(time.RFC3339),
	}

	return info, nil
}

// Lock the storage
func (p *FilePersistence) Lock() error {
	if _, err := os.Stat(p.lockPath); err == nil {
		return fmt.Errorf("storage is already locked: %s", p.lockPath)
	}
	return os.WriteFile(p.lockPath, []byte{}, 0600)
}

// Unlock the storage
func (p *FilePersistence) Unlock() error {
	return os.Remove(p.lockPath)
}

// IsLocked checks if the storage is locked
func (p *FilePersistence) IsLocked() bool {
	_, err := os.Stat(p.lockPath)
	return err == nil
}
