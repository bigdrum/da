package da_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bigdrum/da"
)

func viewOneRaw(ctx context.Context, t *testing.T, view *da.View, key string) *string {
	found := 0
	value := ""
	if err := view.Read(ctx, key, func(ve *da.ViewEntry) error {
		found++
		value = string(ve.Value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		return nil
	}
	if found > 1 {
		t.Error("more than one value")
	}
	return &value
}

func TestView(t *testing.T) {
	ctx := context.Background()
	db := da.OpenDB(ctx, sqlDB)

	tbl, err := db.Table(ctx, "view_test")
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Put(ctx, "p:1", 0, json.RawMessage(`{"title": "hello world"}`))
	if err != nil {
		t.Fatal(err)
	}

	mapperRuns := 0

	view, err := db.View(ctx, da.ViewConfig{
		Name: "my_view_1",
		Input: da.ViewInput{
			Table: tbl,
		},
		Mapper: func(doc *da.Document, emit func(ve *da.ViewEntry) error) error {
			mapperRuns++
			d := map[string]interface{}{}
			if err := json.Unmarshal(doc.Data, &d); err != nil {
				return err
			}
			return emit(&da.ViewEntry{
				Key:   d["title"].(string),
				Value: json.RawMessage(`"value"`),
			})
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	viewKey := "hello world"
	value := viewOneRaw(ctx, t, view, viewKey)
	if *value != `"value"` {
		t.Error(value)
	}
	if mapperRuns != 1 {
		t.Error(mapperRuns)
	}

	value = viewOneRaw(ctx, t, view, viewKey)
	if mapperRuns != 1 {
		t.Error(mapperRuns)
	}

	if err := tbl.Put(ctx, "p:1", 1, `{"title": "hello world 2"}`); err != nil {
		t.Fatal(err)
	}

	value = viewOneRaw(ctx, t, view, viewKey)
	if mapperRuns != 2 {
		t.Error(mapperRuns)
	}
	if value != nil {
		t.Error(*value)
	}
	value = viewOneRaw(ctx, t, view, "hello world 2")
	if mapperRuns != 2 {
		t.Error(mapperRuns)
	}
	if value == nil {
		t.Error("value is nil")
	}

	// Now delete the doc, and view entry should be deleted as well.
	if err := tbl.Delete(ctx, "p:1", 2); err != nil {
		t.Fatal(err)
	}
	value = viewOneRaw(ctx, t, view, viewKey)
	if value != nil {
		t.Error(*value)
	}
}

func TestQuery(t *testing.T) {
	ctx := context.Background()
	db := da.OpenDB(ctx, sqlDB)

	tbl, err := db.Table(ctx, "view_test_query")
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Put(ctx, "p:1", 0, json.RawMessage(`{"title": "hello world", "tags": ["red", "blue"]}`))
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Put(ctx, "p:2", 0, json.RawMessage(`{"title": "hello world2", "tags": ["green"]}`))
	if err != nil {
		t.Fatal(err)
	}

	var reducerRuns int

	view, err := db.View(ctx, da.ViewConfig{
		Name:    "my_view",
		Version: "2",
		Input: da.ViewInput{
			Table: tbl,
		},
		Mapper: func(doc *da.Document, emit func(ve *da.ViewEntry) error) error {
			// emit(title, len(tags))
			d := map[string]interface{}{}
			if err := json.Unmarshal(doc.Data, &d); err != nil {
				return err
			}
			value, err := json.Marshal(len(d["tags"].([]interface{})))
			if err != nil {
				return err
			}
			return emit(&da.ViewEntry{
				Key:   d["title"].(string),
				Value: json.RawMessage(value),
			})
		},
		Reducer: func(keys []da.ViewReduceKey, values []interface{}, rereduce bool) (interface{}, error) {
			reducerRuns++
			if rereduce {
				panic("not supported yet")
			} else {
				ret := 0
				for _, value := range values {
					var v int
					if err := json.Unmarshal(value.(json.RawMessage), &v); err != nil {
						return nil, err
					}
					ret += v
				}

				return ret, nil
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	r, err := view.Query(ctx, da.ViewQueryParam{NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
    "total_rows": 2,
    "rows": [
        {
            "key": "hello world",
            "id": "p:1",
            "value": 2
        },
        {
            "key": "hello world2",
            "id": "p:2",
            "value": 1
        }
    ]
}
`)
	if reducerRuns != 0 {
		t.Fatal(reducerRuns)
	}

	r, err = view.Query(ctx, da.ViewQueryParam{})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
    "total_rows": 2,
    "rows": [
        {
            "value": 3
        }
    ]
}
`)
	if reducerRuns != 1 {
		t.Fatal(reducerRuns)
	}

	r, err = view.Query(ctx, da.ViewQueryParam{Skip: 1, NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "offset": 1,
	    "rows": [
	        {
	            "key": "hello world2",
	            "id": "p:2",
	            "value": 1
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{Limit: 1, NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{Key: "hello world", NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{Keys: []interface{}{"hello world", "hello world2"}, NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2
	        },
	        {
	            "key": "hello world2",
	            "id": "p:2",
	            "value": 1
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{StartKeyDocID: "p:2", NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world2",
	            "id": "p:2",
	            "value": 1
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{EndKeyDocID: "p:1", NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{StartKey: "hello world2", NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world2",
	            "id": "p:2",
	            "value": 1
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{EndKey: "hello world", NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{EndKey: "hello world", ExclusiveEnd: true, NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{EndKey: "hello world", Descending: true, NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world2",
	            "id": "p:2",
	            "value": 1
	        },
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{UpdateSeq: true, NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
		"update_seq": 2,
	    "rows": [
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2
	        },
	        {
	            "key": "hello world2",
	            "id": "p:2",
	            "value": 1
	        }
	    ]
	}
	`)

	r, err = view.Query(ctx, da.ViewQueryParam{IncludeDocs: true, NoReduce: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range r.Rows {
		r.Rows[i].Doc.Modified = time.Time{}
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "key": "hello world",
	            "id": "p:1",
	            "value": 2,
	            "doc": {
	                "id": "p:1",
	                "version": 1,
	                "modified": "0001-01-01T00:00:00Z",
	                "seq": 1,
	                "data": {
	                    "tags": [
	                        "red",
	                        "blue"
	                    ],
	                    "title": "hello world"
	                }
	            }
	        },
	        {
	            "key": "hello world2",
	            "id": "p:2",
	            "value": 1,
	            "doc": {
	                "id": "p:2",
	                "version": 1,
	                "modified": "0001-01-01T00:00:00Z",
	                "seq": 2,
	                "data": {
	                    "tags": [
	                        "green"
	                    ],
	                    "title": "hello world2"
	                }
	            }
	        }
	    ]
	}
	`)
	if reducerRuns != 1 {
		t.Fatal(reducerRuns)
	}

	// update table, test stale.
	tbl.Put(ctx, "p:1", 1, json.RawMessage(`{"title": "hello world", "tags": ["red"]}`))

	r, err = view.Query(ctx, da.ViewQueryParam{Stale: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "value": 3
	        }
	    ]
	}
	`)
	if reducerRuns != 1 {
		t.Fatal(reducerRuns)
	}
	r, err = view.Query(ctx, da.ViewQueryParam{Stale: "update_after"})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "value": 3
	        }
	    ]
	}
	`)
	// let refresh run.
	time.Sleep(time.Duration(1) * time.Second)
	if reducerRuns != 2 {
		t.Fatal(reducerRuns)
	}
	r, err = view.Query(ctx, da.ViewQueryParam{Stale: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	equalJSONText(t, r, `{
	    "total_rows": 2,
	    "rows": [
	        {
	            "value": 2
	        }
	    ]
	}
	`)
}
