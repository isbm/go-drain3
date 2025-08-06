package drain3

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DumbPGXPersistence implements the PersistenceHandler interface
// for saving and loading state to/from a PostgreSQL database.
// It uses a single row with a fixed ID (1) to store the state.
// That's why it's called "Dumb" - it doesn't handle multiple rows or complex queries.
type DumbPGXPersistence struct {
	db        *sql.DB
	tableName string
	ts        time.Time
}

// NewDumbPGXPersistence creates a new PostgreSQL persistance
// instance with the provided database connection and table name.
func NewDumbPGXPersistence(db *sql.DB, table string) (*DumbPGXPersistence, error) {
	p := &DumbPGXPersistence{db: db, tableName: fmt.Sprintf("%s_miner", table)}
	createStmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id    INTEGER PRIMARY KEY CHECK (id = 1),
	state BYTEA NOT NULL,
	saved TIMESTAMP DEFAULT now()
)`, p.tableName)

	_, err := db.Exec(createStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to create persistence table: %w", err)
	}
	return p, nil
}

// Flush the target table from the existing data (truncate)
func (p *DumbPGXPersistence) Flush() (string, error) {
	_, err := p.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", p.tableName))
	if err != nil {
		return fmt.Sprintf("Failed to truncate table %s: %v", p.tableName, err), err
	}
	return fmt.Sprintf("Table %s truncated successfully", p.tableName), nil
}

// Teardown cleans up the storage, e.g., drops the table in Db
func (p *DumbPGXPersistence) Teardown() (string, error) {
	_, err := p.db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, p.tableName))
	if err != nil {
		return "", fmt.Errorf("failed to drop persistence table %s: %w", p.tableName, err)
	}
	return fmt.Sprintf("Teardown of table %s complete", p.tableName), nil
}

// Save saves the current state to the PostgreSQL database.
func (p *DumbPGXPersistence) Save(ctx context.Context, state []byte) error {
	_, err := p.db.ExecContext(
		ctx,
		fmt.Sprintf(`
			INSERT INTO %s (id, state, saved)
			VALUES (1, $1, now())
			ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, saved = now()
		`, p.tableName),
		state,
	)
	p.ts = time.Now()
	return err
}

// Load retrieves the saved state from the PostgreSQL database.
func (p *DumbPGXPersistence) Load(ctx context.Context) ([]byte, error) {
	var state []byte
	err := p.db.QueryRowContext(
		ctx,
		fmt.Sprintf(`SELECT state FROM %s WHERE id=1`, p.tableName),
	).Scan(&state)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return state, err
}

func (p *DumbPGXPersistence) Info() (PersistenceInformation, error) {
	var count int

	err := p.db.QueryRow(fmt.Sprintf(`SELECT count(id) FROM %s`, p.tableName)).Scan(&count)
	if err != nil {
		return PersistenceInformation{}, fmt.Errorf("failed to get record count: %w", err)
	}

	return PersistenceInformation{
		StorageType: "dumb-pgx",
		StorageName: p.tableName,
		MaxClusters: 0,     // N/A
		RecordCount: count, // Same as cluster count in this case
		LastUpdated: p.ts.Format(time.RFC3339),
	}, nil
}

func (p *DumbPGXPersistence) Lock() error {
	_, err := p.db.Exec(fmt.Sprintf(`SELECT pg_advisory_lock(%d)`, LockID))
	return err
}

func (p *DumbPGXPersistence) Unlock() error {
	_, err := p.db.Exec(fmt.Sprintf(`SELECT pg_advisory_unlock(%d)`, LockID))
	return err
}

func (p *DumbPGXPersistence) IsLocked() bool {
	var isLocked bool
	err := p.db.QueryRow(fmt.Sprintf(`SELECT pg_try_advisory_lock(%d)`, LockID)).Scan(&isLocked)
	if err != nil {
		return false
	}
	return !isLocked
}
