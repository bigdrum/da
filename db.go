package da

// DB manages a database.
type DB struct{}

// OpenDB opens a DB.
func OpenDB() *DB {
	return nil
}

// Table returns a table of given name.
func (db *DB) Table(name string) *Table {
	return nil
}
