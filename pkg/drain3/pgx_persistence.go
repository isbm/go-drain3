package drain3

import (
	"context"
	"database/sql"
	"fmt"
)

type PGXPersistence struct {
	db        *sql.DB
	tableName string
}

// NewPGXPersistence creates a new PostgreSQL persistance
// instance with the provided database connection and table name.
func NewPGXPersistence(db *sql.DB, table string) (*PGXPersistence, error) {
	p := &PGXPersistence{db: db, tableName: fmt.Sprintf("%s_miner", table)}
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

// Save saves the current state to the PostgreSQL database.
func (p *PGXPersistence) Save(ctx context.Context, state []byte) error {
	_, err := p.db.ExecContext(
		ctx,
		fmt.Sprintf(`
			INSERT INTO %s (id, state, saved)
			VALUES (1, $1, now())
			ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, saved = now()
		`, p.tableName),
		state,
	)
	return err
}

// Load retrieves the saved state from the PostgreSQL database.
func (p *PGXPersistence) Load(ctx context.Context) ([]byte, error) {
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
