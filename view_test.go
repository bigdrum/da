package da_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bigdrum/da"
)

func TestView(t *testing.T) {
	db := da.OpenDB(sqlDB)
	ctx := context.Background()

	tbl, err := db.Table(ctx, "view_test")
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Put(ctx, "p:1", 0, json.RawMessage(`{"title": "hello world"}`))
	if err != nil {
		t.Fatal(err)
	}

	view, err := db.View(ctx, da.ViewConfig{
		Name: "my_view_1",
		Inputs: []da.ViewInput{
			{
				Table: tbl,
			},
		},
		Mapper: func(doc *da.Document, emit func(ve *da.ViewEntry)) error {
			d := map[string]interface{}{}
			if err := json.Unmarshal(doc.Data, &d); err != nil {
				return err
			}
			emit(&da.ViewEntry{
				Key:   d["title"].(string),
				Value: json.RawMessage(`"value"`),
			})
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	found := false
	if err := view.Read(ctx, "hello world", func(ve *da.ViewEntry) error {
		found = true
		if string(ve.Value) != `"value"` {
			t.Error(string(ve.Value))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !found {
		t.Error("key not found in view")
	}
}
