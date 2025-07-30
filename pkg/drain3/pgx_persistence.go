package drain3

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// DumbPGXPersistence implements the PersistenceHandler interface
// for saving and loading state to/from a PostgreSQL database.
// It uses a single row with a fixed ID (1) to store the state.
// That's why it's called "Dumb" - it doesn't handle multiple rows or complex queries.
type DumbPGXPersistence struct {
	db        *sql.DB
	tableName string
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

type PGXClusterPersistence struct {
	db        *sql.DB
	tableName string
}

func NewPGXClusterPersistence(db *sql.DB, table string) (*PGXClusterPersistence, error) {
	// Use a real table name (customize as needed)
	tbl := fmt.Sprintf("%s_clusters", table)
	createStmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    cluster_id BIGINT PRIMARY KEY,
    template   TEXT NOT NULL,
    size       BIGINT NOT NULL,
    updated    TIMESTAMP DEFAULT now()
)`, tbl)

	// Always try to create the table—if already exists, does nothing
	if _, err := db.Exec(createStmt); err != nil {
		return nil, fmt.Errorf("failed to create clusters table: %w", err)
	}

	return &PGXClusterPersistence{db: db, tableName: tbl}, nil
}

func (p *PGXClusterPersistence) Save(ctx context.Context, state []byte) error {
	var drain SerializableDrain
	if err := json.Unmarshal(state, &drain); err != nil {
		return fmt.Errorf("failed to unmarshal drain: %w", err)
	}

	// 1. Get all existing clusters from DB
	rows, err := p.db.QueryContext(ctx, fmt.Sprintf(`SELECT cluster_id, template, size FROM %s`, p.tableName))
	if err != nil {
		return err
	}
	defer rows.Close()

	dbClusters := make(map[int64]struct {
		template string
		size     int64
	})
	for rows.Next() {
		var clusterID int64
		var template string
		var size int64
		if err := rows.Scan(&clusterID, &template, &size); err != nil {
			return err
		}
		dbClusters[clusterID] = struct {
			template string
			size     int64
		}{template, size}
	}

	inMemIDs := make(map[int64]bool)
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare statements
	insertStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (cluster_id, template, size, updated)
		VALUES ($1, $2, $3, now())
	`, p.tableName))
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	updateStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		UPDATE %s SET template = $2, size = $3, updated = now()
		WHERE cluster_id = $1
	`, p.tableName))
	if err != nil {
		return err
	}
	defer updateStmt.Close()

	// 2. Sync new/changed clusters
	for _, cluster := range drain.Clusters {
		inMemIDs[cluster.ClusterID] = true
		dbCluster, exists := dbClusters[cluster.ClusterID]
		templateStr := strings.Join(cluster.LogTemplateTokens, " ")
		if !exists {
			_, err := insertStmt.ExecContext(ctx, cluster.ClusterID, templateStr, cluster.Size)
			if err != nil {
				return err
			}
		} else if dbCluster.template != templateStr || dbCluster.size != cluster.Size {
			// Changed cluster
			_, err := updateStmt.ExecContext(ctx, cluster.ClusterID, templateStr, cluster.Size)
			if err != nil {
				return err
			}
		}
	}

	// 3. Delete missing clusters
	for clusterID := range dbClusters {
		if !inMemIDs[clusterID] {
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE cluster_id = $1`, p.tableName), clusterID)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (p *PGXClusterPersistence) Load(ctx context.Context) ([]byte, error) {
	rows, err := p.db.QueryContext(ctx, fmt.Sprintf(`SELECT cluster_id, template, size FROM %s`, p.tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var clusters []*LogCluster
	for rows.Next() {
		var clusterID int64
		var template string
		var size int64
		if err := rows.Scan(&clusterID, &template, &size); err != nil {
			return nil, err
		}
		cluster := &LogCluster{
			ClusterID:         clusterID,
			LogTemplateTokens: strings.Split(template, " "),
			Size:              size,
		}
		clusters = append(clusters, cluster)
	}

	drain := &SerializableDrain{
		Clusters: clusters,
	}
	return json.Marshal(drain)
}
