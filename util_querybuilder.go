package da

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
)

// queryBuilder helps building query with conditional arguments.
type queryBuilder struct {
	query bytes.Buffer
	vars  []interface{}
}

// newQueryBuilder creates a new QueryBuilder.
func newQueryBuilder() *queryBuilder {
	return &queryBuilder{}
}

// Query runs the given query.
func (b *queryBuilder) Query(ctx context.Context, sqlDB *sql.DB) (*sql.Rows, error) {
	return sqlDB.QueryContext(ctx, b.query.String(), b.vars...)
}

// Exec exec the given query.
func (b *queryBuilder) Exec(ctx context.Context, sqlDB *sql.DB) (sql.Result, error) {
	return sqlDB.ExecContext(ctx, b.query.String(), b.vars...)
}

var positionArgPattern = regexp.MustCompile(`\$[0-9]+\b`)

// Add append a clause with corresponding arguments.
func (b *queryBuilder) Add(clause string, vars ...interface{}) {
	largestArgIndex := 0
	rewritternClause := positionArgPattern.ReplaceAllStringFunc(clause, func(arg string) string {
		if arg[0] != '$' {
			panic("impossible pattern: " + arg)
		}
		num, err := strconv.Atoi(arg[1:])
		if err != nil {
			panic(fmt.Errorf("impossible pattern: %s, %v", arg, err))
		}
		if num > largestArgIndex {
			largestArgIndex = num
		}
		return fmt.Sprint("$", num+len(b.vars))
	})
	if largestArgIndex != len(vars) {
		panic(fmt.Errorf(
			"nubmer question mark doesn't match number of vars: %v %v", clause, vars))
	}
	b.query.WriteString(rewritternClause)
	b.vars = append(b.vars, vars...)
}

func isZero(x interface{}) bool {
	if x == nil {
		return true
	}
	t := reflect.TypeOf(x)
	kind := t.Kind()
	if kind == reflect.Slice || kind == reflect.Map {
		v := reflect.ValueOf(x)
		return v.Len() == 0
	}
	return x == reflect.Zero(reflect.TypeOf(x)).Interface()
}

// AddIfNotZero append a clause with corresponding argument, if it is a non-zero value.
func (b *queryBuilder) AddIfNotZero(clause string, v interface{}) bool {
	if isZero(v) {
		return false
	}
	b.Add(clause, v)
	return true
}
