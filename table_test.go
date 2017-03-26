package da_test

import (
	"context"
	"testing"

	"encoding/json"

	"github.com/bigdrum/da"
)

func TestCRUD(t *testing.T) {
	db := da.OpenDB(sqlDB)
	ctx := context.Background()

	tbl, err := db.Table(ctx, "post")
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Put(ctx, "p:1", 0, json.RawMessage(`"hello world"`))
	if err != nil {
		t.Fatal(err)
	}

	{
		doc := da.Document{}
		err = tbl.Get(ctx, "p:1", &doc)
		if err != nil {
			t.Fatal(err)
		}
		if doc.Version != 1 {
			t.Error("unexpected version: ", doc.Version)
		}
		if string(doc.Data.([]byte)) != `"hello world"` {
			t.Error("unexpected data: ", doc.Data)
		}
		if doc.Modified.IsZero() {
			t.Error("unexpected modified: ", doc.Modified)
		}
	}

	err = tbl.Put(ctx, "p:1", 1, json.RawMessage(`"hello world 2"`))
	if err != nil {
		t.Fatal(err)
	}

	{
		doc := da.Document{}
		err = tbl.Get(ctx, "p:1", &doc)
		if err != nil {
			t.Fatal(err)
		}
		if doc.Version != 2 {
			t.Error("unexpected version: ", doc.Version)
		}
		if string(doc.Data.([]byte)) != `"hello world 2"` {
			t.Error("unexpected data: ", doc.Data)
		}
		if doc.Modified.IsZero() {
			t.Error("unexpected modified: ", doc.Modified)
		}
	}
}
