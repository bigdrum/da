package da_test

import (
	"context"
	"encoding/json"
	"testing"

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
		Inputs: []da.ViewInput{
			{
				Table: tbl,
			},
		},
		Mapper: func(doc *da.Document, emit func(ve *da.ViewEntry)) error {
			mapperRuns++
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

	// Now delete the doc, and view entry should be deleted as well.
	if err := tbl.Delete(ctx, "p:1", 1); err != nil {
		t.Fatal(err)
	}
	value = viewOneRaw(ctx, t, view, viewKey)
	if value != nil {
		t.Error(value)
	}
}
