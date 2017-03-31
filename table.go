package da

import (
	"context"
	"database/sql"
	"encoding/json"
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
	read := 0
	err := tbl.ReadMulti(ctx, TableReadParams{
		ID:     id,
		Latest: true,
	}, func(docIn *Document) error {
		read++
		*doc = *docIn
		return nil
	})
	if err != nil {
		return err
	}
	if read == 0 {
		return errorf(ErrNotFound, nil, "record not found: %s", id)
	}
	if read > 1 {
		return fmt.Errorf("data inconsistent")
	}
	return nil
}

// Delete delets a document of given id.
func (tbl *Table) Delete(ctx context.Context, id string, version int64) error {
	return tbl.insertInternal(ctx, id, version, nil, true)
}

// TableReadParams contains optinal parameters for table read.
type TableReadParams struct {
	ID             string
	MinSeq         int64 // The minimum sequence number, inclusive.
	IncludeDeleted bool
	Latest         bool
	OrderBy        string
	Offset         int
	Limit          int
}

// ReadMulti reads multiple documents.
func (tbl *Table) ReadMulti(ctx context.Context, p TableReadParams, action func(r *Document) error) error {
	qb := newQueryBuilder()
	qb.Add(`SELECT seq, id, version, data, modified, deleted FROM ` + tbl.dataTable + ` WHERE TRUE`)
	if !p.IncludeDeleted {
		qb.Add(` AND deleted = FALSE`)
	}
	qb.AddIfNotZero(` AND id = $1`, p.ID)
	qb.AddIfNotZero(` AND seq >= $1`, p.MinSeq)
	qb.AddIfNotZero(` AND latest = $1`, p.Latest)
	qb.AddIfNotZero(` ORDER BY $1`, p.OrderBy)
	qb.AddIfNotZero(` OFFSET $1`, p.Offset)
	qb.AddIfNotZero(` LIMIT $1`, p.Limit)

	rows, err := qb.Query(ctx, tbl.sqlDB)
	if err != nil {
		return err
	}
	defer rows.Close()
	var docs []Document
	for rows.Next() {
		doc := Document{}
		var pData *json.RawMessage
		if err := rows.Scan(
			&doc.Seq, &doc.ID, &doc.Version, &pData, &doc.Modified, &doc.Deleted); err != nil {
			return fmt.Errorf("scan error: %v", err)
		}
		if pData != nil {
			doc.Data = *pData
		}
		docs = append(docs, doc)

	}
	rows.Close()
	for i := range docs {
		if err := action(&docs[i]); err != nil {
			return err
		}
	}
	return nil
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
