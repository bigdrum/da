package da

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// View provides a way to transform the underlying data.
type View struct {
	sqldb     *sql.DB
	config    ViewConfig
	dataTable string
}

// ViewConfig specifies the view.
type ViewConfig struct {
	Name    string
	Version string

	Inputs []ViewInput
	Mapper func(doc *Document, emit func(ve *ViewEntry)) error
}

// ViewInput specifies a view input.
type ViewInput struct {
	Table *Table
}

// ViewEntry represents a single entry of the view mapper output.
type ViewEntry struct {
	Key   string
	Value json.RawMessage
}

func (input *ViewInput) changes(ctx context.Context, minSeq int64, action func(doc *Document) error) error {
	return input.Table.ReadMulti(ctx, TableReadParams{
		MinSeq: minSeq,
		Latest: true,
	}, action)
}

// View creates a view.
func (db *DB) View(ctx context.Context, config ViewConfig) (*View, error) {
	if config.Name == "" {
		return nil, fmt.Errorf("empty view name")
	}
	if len(config.Inputs) == 0 {
		return nil, fmt.Errorf("no inputs for view")
	}
	if config.Mapper == nil {
		return nil, fmt.Errorf("mapper is not set for view")
	}

	dataTable := "da_view_" + config.Name + "_" + config.Version
	if err := checkName(dataTable); err != nil {
		return nil, err
	}
	_, err := db.sqlDB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		seq     BIGSERIAL,
		key     TEXT,
		value   JSONB,
		doc_id  TEXT,
		doc_seq BIGINT,
		PRIMARY KEY(seq))`, dataTable))
	if err != nil {
		return nil, err
	}
	return &View{
		sqldb:     db.sqlDB,
		config:    config,
		dataTable: dataTable,
	}, nil
}

// Read reads a value of given key.
func (v *View) Read(ctx context.Context, key string, each func(entry *ViewEntry) error) error {
	currentSeq, err := v.currentSeq(ctx)
	if err != nil {
		return err
	}
	for _, input := range v.config.Inputs {
		err := input.changes(ctx, currentSeq, func(doc *Document) error {
			// TODO: Need a transaction here.
			// TODO: The ops can be batched.
			_, err := v.sqldb.ExecContext(ctx, `DELETE FROM `+v.dataTable+` WHERE doc_id = $1`, doc.ID)
			if err != nil {
				return err
			}
			var emitError error
			err = v.config.Mapper(doc, func(ve *ViewEntry) {
				var value interface{}
				value = ve.Value
				_, emitError = v.sqldb.ExecContext(ctx,
					`INSERT INTO `+v.dataTable+` (key, value, doc_id, doc_seq) VALUES ($1, $2, $3, $4);`,
					ve.Key, value, doc.ID, doc.Seq)
			})
			if emitError != nil {
				return fmt.Errorf("emit error: %v", emitError)
			}
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	rows, err := v.sqldb.QueryContext(
		ctx, `SELECT value FROM `+v.dataTable+` WHERE key = $1;`, key)
	if err != nil {
		return err
	}
	defer rows.Close()

	var result []ViewEntry
	for rows.Next() {
		ve := ViewEntry{}
		if err := rows.Scan(&ve.Value); err != nil {
			return err
		}
		result = append(result, ve)
	}
	rows.Close()

	for i := range result {
		if err := each(&result[i]); err != nil {
			return err
		}
	}
	return nil
}

func (v *View) currentSeq(ctx context.Context) (int64, error) {
	return 0, nil
}
