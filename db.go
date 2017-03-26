package da

import (
	"database/sql"
)

// DB manages a database.
type DB struct {
	sqlDB *sql.DB
}

// OpenDB opens a DB.
func OpenDB(sqlDB *sql.DB) *DB {
	return &DB{
		sqlDB: sqlDB,
	}
}
