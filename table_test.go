package da_test

import (
	"context"
	"testing"

	"encoding/json"

	"github.com/bigdrum/da"
)

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	db := da.OpenDB(ctx, sqlDB)

	tbl, err := db.Table(ctx, "crud_test")
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

		data := ""
		if err := json.Unmarshal(doc.Data, &data); err != nil {
			t.Error(err)
		}
		if data != `hello world` {
			t.Error("unexpected data: ", data)
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
		data := ""
		if err := json.Unmarshal(doc.Data, &data); err != nil {
			t.Error(err)
		}
		if data != `hello world 2` {
			t.Error("unexpected data: ", doc.Data)
		}
		if doc.Modified.IsZero() {
			t.Error("unexpected modified: ", doc.Modified)
		}
	}

	{
		err = tbl.Delete(ctx, "p:1", 1)
		if err == nil {
			t.Fatal("expecting error in delete")
		}
		if err.(da.Error).Kind != da.ErrConflict {
			t.Fatal("expecting conflict error: ", err)
		}

		err = tbl.Delete(ctx, "p:1", 2)
		if err != nil {
			t.Fatal(err)
		}

		doc := da.Document{}
		err = tbl.Get(ctx, "p:1", &doc)
		if err == nil {
			t.Fatal("expecting error in get")
		}
		if derr, ok := err.(da.Error); ok {
			if derr.Kind != da.ErrNotFound {
				t.Fatal("expecting not found error: ", err)
			}
		} else {
			t.Fatal(err)
		}
	}
}
