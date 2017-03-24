package da

import "context"

// Table manages a table.
type Table struct{}

// Setup creates the table.
func (tbl *Table) Setup(ctx context.Context) error {
	return nil
}

// Update sets a document of given id.
func (tbl *Table) Update(ctx context.Context, id string, version int64, data interface{}) error {
	return nil
}
