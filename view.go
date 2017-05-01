package da

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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

	// TODO: Use []Input to support multi-table mapreduce
	Input  ViewInput
	Mapper func(doc *Document, emit func(ve *ViewEntry) error) error
	// TODO: support multiple keys.
	Reducer func(keys []ViewReduceKey, values []interface{}, rereduce bool) (interface{}, error)
}

// ViewInput specifies a view input.
type ViewInput struct {
	Table *Table
}

// ViewEntry represents a single entry of the view mapper output.
type ViewEntry struct {
	Key   json.RawMessage
	Value json.RawMessage
}

// ViewReduceKey represents element of the keys parameter of reducer.
// http://docs.couchdb.org/en/latest/couchapp/ddocs.html#reduce-and-rereduce-functions
type ViewReduceKey struct {
	Key   interface{}
	DocID string
}

// ViewResult represents the result of view query.
type ViewResult struct {
	TotalRows int64           `json:"total_rows,omitempty"`
	Offset    int64           `json:"offset,omitempty"`
	UpdateSeq int64           `json:"update_seq,omitempty"`
	Rows      []ViewResultRow `json:"rows,omitempty"`
}

// ViewResultRow represents the row of the result of view query.
type ViewResultRow struct {
	Key   interface{}     `json:"key,omitempty"`
	ID    string          `json:"id,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Doc   *Document       `json:"doc,omitempty"`
}

// ViewQueryParam represents the query parameters.
type ViewQueryParam struct {
	Key           interface{}
	Keys          []interface{}
	StartKey      interface{}
	EndKey        interface{}
	StartKeyDocID string
	EndKeyDocID   string
	Limit         int
	Stale         string
	Descending    bool
	Skip          int64
	// TODO: Group and GroupLevel are stated in the CouchDB api but not yet implemented.
	// kind of tricky to do here since key is Text.
	// Group         bool
	// GroupLevel    int
	NoReduce     bool // was reduce(default true) in CouchDB api.
	IncludeDocs  bool
	ExclusiveEnd bool // was inclusive_end(default true) in CouchDB api.
	UpdateSeq    bool
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
	if config.Input.Table == nil {
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
	seqStore := v.metaStore.At(v.config.Input.Table.name).At("last_seq")
	var lastSeq int64
	err := seqStore.Get(ctx, &lastSeq)
	if err != nil && !IsError(err, ErrNotFound) {
		return err
	}

	seq := lastSeq
	err = v.config.Input.changes(ctx, lastSeq+1, func(doc *Document) error {
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

		err = v.config.Mapper(doc, func(ve *ViewEntry) error {
			var value interface{}
			value = ve.Value

			var key string
			err = json.Unmarshal(ve.Key, &key)
			if err != nil {
				return err
			}

			_, err := v.sqldb.ExecContext(ctx,
				`INSERT INTO `+v.mapperTable+` (key, value, doc_id, doc_seq) VALUES ($1, $2, $3, $4)
					ON CONFLICT (doc_id) DO UPDATE SET
					key=excluded.key,
					value=excluded.value,
					doc_seq=excluded.doc_seq,
					deleted=false;`,
				key, value, doc.ID, doc.Seq)
			return err
		})
		if err != nil {
			return fmt.Errorf("emit error: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return seqStore.Set(ctx, seq)
}

// Read reads a value of given key.
func (v *View) Read(ctx context.Context, key string, each func(entry *ViewEntry) error) error {
	if err := v.Refresh(ctx); err != nil {
		return err
	}

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

func (v *View) queryMap(ctx context.Context, p ViewQueryParam) (ViewResult, error) {
	var orderBy string
	if p.Descending {
		s := p.StartKey
		p.StartKey = p.EndKey
		p.EndKey = s
		orderBy = "key DESC, doc_id DESC"
	} else {
		orderBy = "key ASC, doc_id ASC"
	}

	ret := ViewResult{}
	if !isZero(p.Key) && len(p.Keys) > 0 {
		return ret, fmt.Errorf("cannot supply both key and keys parameter key: %s keys: %v", p.Key, p.Keys)
	}

	qb := newQueryBuilder()
	qb.Add(`SELECT key, doc_id, value FROM ` + v.mapperTable + ` WHERE deleted != TRUE`)
	qb.AddIfNotZero(` AND key = $1`, p.Key)
	if len(p.Keys) > 0 {
		if p.Keys[0] == "" {
			return ret, fmt.Errorf("parameter keys[0] cannot be empty string")
		}
		qb.Add(` AND (key = $1`, p.Keys[0])
		for _, k := range p.Keys[1:] {
			qb.AddIfNotZero(` OR key = $1`, k)
		}
		qb.Add(`)`)
	}
	qb.AddIfNotZero(` AND key >= $1`, p.StartKey)
	eqSign := "="
	if p.ExclusiveEnd {
		eqSign = ""
	}
	qb.AddIfNotZero(` AND key <`+eqSign+` $1`, p.EndKey)
	qb.AddIfNotZero(` AND doc_id >= $1`, p.StartKeyDocID)
	qb.AddIfNotZero(` AND doc_id <= $1`, p.EndKeyDocID)

	qb.Add(" ORDER BY " + orderBy)
	qb.AddIfNotZero(" LIMIT $1", p.Limit)
	qb.AddIfNotZero(" OFFSET $1", p.Skip)

	rows, err := qb.Query(ctx, v.sqldb)
	if err != nil {
		return ret, err
	}
	defer rows.Close()

	for rows.Next() {
		r := ViewResultRow{}
		if err := rows.Scan(&r.Key, &r.ID, &r.Value); err != nil {
			return ret, err
		}
		ret.Rows = append(ret.Rows, r)
	}
	rows.Close()

	r := v.sqldb.QueryRowContext(ctx, `SELECT count(*) FROM `+v.mapperTable+` WHERE deleted != TRUE`)
	if err := r.Scan(&ret.TotalRows); err != nil {
		return ret, err
	}
	ret.Offset = p.Skip

	return ret, nil
}

// Query queries the view.
func (v *View) Query(ctx context.Context, p ViewQueryParam) (ViewResult, error) {
	r := ViewResult{}
	var err error

	if p.Stale != "ok" && p.Stale != "update_after" {
		err = v.Refresh(ctx)
		if err != nil {
			return r, err
		}
	}

	r, err = v.queryMap(ctx, p)
	if err != nil {
		return r, err
	}

	if p.UpdateSeq {
		seqStore := v.metaStore.At(v.config.Input.Table.name).At("last_seq")
		var lastSeq int64
		err := seqStore.Get(ctx, &lastSeq)
		if err != nil && !IsError(err, ErrNotFound) {
			return r, err
		}
		r.UpdateSeq = lastSeq
	}

	if p.NoReduce || v.config.Reducer == nil {
		for i := range r.Rows {
			if p.IncludeDocs {
				r.Rows[i].Doc = &Document{}
				// TODO: perform batch fetch here.
				err = v.config.Input.Table.Get(ctx, r.Rows[i].ID, r.Rows[i].Doc)
				if err != nil {
					return r, err
				}
			}
		}
		return r, nil
	}

	if len(r.Rows) == 0 {
		return r, nil
	}

	// Group keys. r.Rows is sorted by key.
	type KV struct {
		k []ViewReduceKey
		v []interface{}
	}
	groupedKV := []KV{}
	tmpKey := r.Rows[0].Key
	tmpKeys := []ViewReduceKey{}
	tmpVals := []interface{}{}
	for _, r := range r.Rows {
		key := ViewReduceKey{Key: r.Key, DocID: r.ID}
		if r.Key == tmpKey {
			tmpKeys = append(tmpKeys, key)
			tmpVals = append(tmpVals, r.Value)
		} else {
			groupedKV = append(groupedKV, KV{k: tmpKeys, v: tmpVals})
			tmpKeys = []ViewReduceKey{key}
			tmpVals = []interface{}{r.Value}
			tmpKey = r.Key
		}
	}
	if len(tmpKeys) > 0 {
		groupedKV = append(groupedKV, KV{k: tmpKeys, v: tmpVals})
	}

	vs := []interface{}{}
	for _, kv := range groupedKV {
		value, err := v.config.Reducer(kv.k, kv.v, false)
		if err != nil {
			return r, err
		}
		vs = append(vs, value)
	}
	value, err := v.config.Reducer(nil, vs, true)
	if err != nil {
		return r, err
	}

	valueMsg, err := json.Marshal(value)
	if err != nil {
		return r, err
	}
	r.Rows = []ViewResultRow{ViewResultRow{
		Value: valueMsg,
	}}

	if p.Stale == "update_after" {
		v.goRefresh(ctx)
	}

	return r, nil
}

// goRefresh calls Refresh in a new goroutine.
func (v *View) goRefresh(ctx context.Context) {
	go func() {
		// TODO: avoid multiple ongoing refresh.
		err := v.Refresh(ctx)
		if err != nil {
			log.Printf("unable to refresh, error: %v", err)
		}
	}()
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
