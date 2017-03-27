package da

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Table manages a table.
type Table struct {
	sqlDB *sql.DB

	name      string
	dataTable string
}

// Table returns a table of given name.
func (db *DB) Table(ctx context.Context, name string) (*Table, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	dataTable := "da_data_" + name
	_, err := db.sqlDB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		seq       BIGSERIAL,
		id        TEXT,
		version   BIGINT,
		uuid      TEXT,
		data      JSONB,
		metadata  JSONB,
		modified  TIMESTAMP,
		latest    BOOL,
		deleted   BOOL,
		PRIMARY KEY (seq)
	 )`, dataTable))
	if err != nil {
		return nil, err
	}
	return &Table{
		sqlDB:     db.sqlDB,
		name:      name,
		dataTable: dataTable,
	}, nil
}

// Put sets a document of given id.
func (tbl *Table) Put(ctx context.Context, id string, existVersion int64, data interface{}) error {
	return tbl.insertInternal(ctx, id, existVersion, data, false)
}

// Get gets a document of given id.
func (tbl *Table) Get(ctx context.Context, id string, doc *Document) error {
	err := tbl.sqlDB.QueryRowContext(ctx, `SELECT
		version, data, modified
		FROM `+tbl.dataTable+` WHERE id = $1 AND LATEST = TRUE AND DELETED != TRUE`, id).Scan(
		&doc.Version, &doc.Data, &doc.Modified)
	if err != nil {
		if err == sql.ErrNoRows {
			return errorf(ErrNotFound, nil, "record not found: %s", id)
		}
		return err
	}
	return nil
}

// Delete delets a document of given id.
func (tbl *Table) Delete(ctx context.Context, id string, version int64) error {
	return tbl.insertInternal(ctx, id, version, nil, true)
}

func (tbl *Table) insertInternal(ctx context.Context, id string, existVersion int64, data interface{}, deleted bool) error {
	var preVersion int64
	err := tbl.sqlDB.QueryRowContext(ctx, `SELECT version FROM `+tbl.dataTable+` WHERE id = $1 AND LATEST = TRUE`, id).Scan(&preVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			preVersion = 0
		} else {
			return err
		}
	}
	if existVersion != preVersion {
		return errorf(ErrConflict, nil, "version mismatched: %d vs %d", existVersion, preVersion)
	}

	_, err = tbl.sqlDB.ExecContext(ctx,
		`UPDATE `+tbl.dataTable+` SET latest=FALSE WHERE id = $1 AND latest=TRUE`, id)
	if err != nil {
		return err
	}
	_, err = tbl.sqlDB.ExecContext(ctx,
		`INSERT INTO `+tbl.dataTable+` (id, version, data, latest, modified, deleted) VALUES ($1, $2, $3, TRUE, $4, $5);`,
		id, existVersion+1, data, time.Now().UTC(), deleted)
	return err
}
