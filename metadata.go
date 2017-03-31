package da

import (
	"context"
	"database/sql"
	"fmt"
)

// metaStore stores metadata.
type metaStore struct {
	path      string
	sqlDB     *sql.DB
	tableName string
}

func (m *metaStore) At(path string) *metaStore {
	newM := *m
	newM.path = m.path + "/" + path
	return &newM
}

func (m *metaStore) Set(ctx context.Context, data interface{}) error {
	_, err := m.sqlDB.ExecContext(
		ctx, `INSERT INTO `+m.tableName+` (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		m.path, data,
	)
	return err
}

func (m *metaStore) Get(ctx context.Context, data interface{}) error {
	err := m.sqlDB.QueryRowContext(
		ctx, `SELECT value FROM `+m.tableName+` WHERE key = $1`, m.path).Scan(data)
	if err == sql.ErrNoRows {
		fmt.Println("not found ", m.path)
		return errorf(ErrNotFound, err, `metadata not found %s`, m.path)
	}
	return err
}
