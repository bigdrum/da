package da

import (
	"context"
	"database/sql"
)

// DB manages a database.
type DB struct {
	sqlDB     *sql.DB
	metaStore *metaStore
}

// OpenDB opens a DB.
func OpenDB(ctx context.Context, sqlDB *sql.DB) *DB {
	sqlDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS da_metadata (
		key   TEXT,
		value JSONB,
		PRIMARY KEY(key)
	)`)

	return &DB{
		sqlDB: sqlDB,
		metaStore: &metaStore{
			sqlDB:     sqlDB,
			tableName: "da_metadata",
		},
	}
}
