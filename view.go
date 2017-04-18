package da

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// View provides a way to transform the underlying data.
type View struct {
	sqldb       *sql.DB
	config      ViewConfig
	mapperTable string
	metaStore   *metaStore
}

// ViewConfig specifies the view.
type ViewConfig struct {
	Name    string
	Version string

	Inputs  []ViewInput
	Mapper  func(doc *Document, emit func(ve *ViewEntry) error) error
	Reducer func(entries []ViewResultRow, rereduce bool) (json.RawMessage, error)
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

// // ViewReduceEntry represents a single entry of the view reducer input.
// type ViewReduceEntry struct {
// 	Key      string
// 	DocID    string
// 	Value    interface{}
// 	docTable string
// }

// ViewResult represents the result of view query.
type ViewResult struct {
	TotalRows int             `json:"total_rows"`
	Offset    int             `json:"offset"`
	Rows      []ViewResultRow `json:"rows"`
}

// ViewResult represents the row of the result of view query.
type ViewResultRow struct {
	Key      string          `json:"key"`
	ID       string          `json:"id"`
	Value    json.RawMessage `json:"value"`
	Doc      *Document       `json:"doc"`
	docTable string
}

// ViewQueryParam represents the query parameters.
type ViewQueryParam struct {
	Key           string
	Keys          []string
	StartKey      string
	EndKey        string
	StartKeyDocID string
	EndKeyDocID   string
	Limit         int
	Stale         string
	Descending    bool
	Skip          int
	Group         bool
	GroupLevel    int
	Reduce        bool
	IncludeDocs   bool
	// InclusiveEnd bool
	// UpdateSeq bool
}

func (input *ViewInput) changes(ctx context.Context, minSeq int64, action func(doc *Document) error) error {
	return input.Table.ReadMulti(ctx, TableReadParams{
		MinSeq:         minSeq,
		Latest:         true,
		IncludeDeleted: true,
		OrderBy:        "seq asc",
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
		seq       BIGSERIAL,
		key       TEXT,
		value     JSONB,
		doc_table TEXT,
		doc_id    TEXT,
		doc_seq   BIGINT,
		deleted   BOOL DEFAULT FALSE,
		PRIMARY KEY(seq),
		UNIQUE(doc_id))`, dataTable))
	if err != nil {
		return nil, err
	}

	return &View{
		sqldb:       db.sqlDB,
		config:      config,
		mapperTable: dataTable,
		metaStore:   db.metaStore.At("view").At(config.Name),
	}, nil
}

// Refresh ensure the view is up-to-date.
func (v *View) Refresh(ctx context.Context) error {
	for _, input := range v.config.Inputs {
		seqStore := v.metaStore.At(input.Table.name).At("last_seq")
		var lastSeq int64
		err := seqStore.Get(ctx, &lastSeq)
		if err != nil && !IsError(err, ErrNotFound) {
			return err
		}

		var seq int64
		err = input.changes(ctx, lastSeq+1, func(doc *Document) error {
			seq = doc.Seq

			// TODO: Need a transaction here.
			// TODO: The ops can be batched.
			_, err := v.sqldb.ExecContext(ctx, `UPDATE `+v.mapperTable+` SET deleted = TRUE WHERE doc_id = $1`, doc.ID)
			if err != nil {
				return err
			}
			if doc.Deleted {
				return nil
			}

			var emitError error
			err = v.config.Mapper(doc, func(ve *ViewEntry) error {
				var value interface{}
				value = ve.Value
				// key, err := json.Marshal(ve.Key)
				// if err != nil {
				// 	return err
				// }

				_, emitError = v.sqldb.ExecContext(ctx,
					`INSERT INTO `+v.mapperTable+` (key, value, doc_id, doc_table, doc_seq) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (doc_id) DO UPDATE SET
					key=excluded.key,
					value=excluded.value,
					doc_table=excluded.doc_table,
					doc_seq=excluded.doc_seq,
					deleted=false;`,
					ve.Key, value, doc.ID, input.Table.name, doc.Seq)
				return emitError
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
		err = seqStore.Set(ctx, seq)
		if err != nil {
			return err
		}
	}

	return nil
}

// Read reads a value of given key.
func (v *View) Read(ctx context.Context, key string, each func(entry *ViewEntry) error) error {
	if err := v.Refresh(ctx); err != nil {
		return err
	}

	// keyB, err := json.Marshal(key)
	// if err != nil {
	// 	return err
	// }
	rows, err := v.sqldb.QueryContext(
		ctx, `SELECT value FROM `+v.mapperTable+` WHERE key = $1 AND deleted != TRUE;`, key)
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

// https://github.com/google/btree
func (v *View) queryMap(ctx context.Context, p ViewQueryParam) ([]ViewResultRow, error) {
	var ret []ViewResultRow
	if p.Key != "" && len(p.Keys) > 0 {
		return ret, fmt.Errorf("Cannot supply both key and keys parameter. %s | %v", p.Key, p.Keys)
	}

	qb := newQueryBuilder()
	qb.Add(`SELECT key, doc_id, value, doc_table FROM ` + v.mapperTable + ` WHERE deleted != TRUE`)
	qb.AddIfNotZero(` AND key = $1`, p.Key)
	if len(p.Keys) > 0 {
		if p.Keys[0] == "" {
			return ret, fmt.Errorf("Parameter keys[0] cannot be empty string.")
		}
		qb.Add(` AND (key = $1`, p.Keys[0])
		for _, k := range p.Keys[1:] {
			qb.AddIfNotZero(` OR key = $1`, k)
		}
		qb.Add(`)`)
	}
	qb.AddIfNotZero(` AND key >= $1`, p.StartKey)
	qb.AddIfNotZero(` AND key <= $1`, p.EndKey)
	// TODO: more param

	rows, err := qb.Query(ctx, v.sqldb)
	if err != nil {
		return ret, err
	}
	for rows.Next() {
		r := ViewResultRow{}
		if err := rows.Scan(&r.Key, &r.ID, &r.Value, &r.docTable); err != nil {
			return ret, err
		}
		ret = append(ret, r)
	}

	return ret, nil
}

// // Query queries the view. If reduce == true, the return
func (v *View) Query(ctx context.Context, p ViewQueryParam) (ViewResult, error) {
	ret := ViewResult{}
	if err := v.Refresh(ctx); err != nil {
		return ret, err
	}

	rs, err := v.queryMap(ctx, p)
	if err != nil {
		return ret, err
	}

	if !p.Reduce || v.config.Reducer == nil {
		// TODO: total_rows and offset
		for _, r := range rs {
			if p.IncludeDocs {
				// TODO: polulate doc.
				r.Doc = &Document{}
			}
		}
		ret.Rows = rs
		return ret, nil
	}

	// var vres []ViewReduceEntry
	// for _, r := range rs {
	// 	var value interface{}
	// 	err := json.Unmarshal(r.Value, &value)
	// 	if err != nil {
	// 		return ret, err
	// 	}
	// 	vres = append(vres, ViewReduceEntry{
	// 		Key:      r.Key,
	// 		DocID:    r.ID,
	// 		Value:    value,
	// 		docTable: r.docTable,
	// 	})
	// }

	value, err := v.config.Reducer(rs, false)
	if err != nil {
		return ret, err
	}
	ret.Rows = []ViewResultRow{ViewResultRow{
		Value: value,
	}}

	return ret, nil
}

// Reducer
// * Scan where seq > last_seq order by key asc, doc asc
// * Run reducer of same key store at level i. Each reducer output also stores the low (key,docid).
// * When a (key,docid) input is updated, find the
// * Assumption: (key, docid) is unique.
//
// a1: 1
// a2: 2
// a3: 4
// b4: 1
// c5: 2
// c6: 3
//
// a1: 3
// a3: 4
// b4: 1
// c5: 2
// c6: 3
//
// a1: 7
// b4: 1
// c5: 5
//
// a1: 1
//
// a1,2: 5
//
