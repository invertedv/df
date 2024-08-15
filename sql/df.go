package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	d "github.com/invertedv/df"
)

type SQLdf struct {
	n             int
	sourceSQL     string
	destTableName string
	db            *sql.DB
	orderBy       string

	*d.DFcore
}

type SQLcol struct {
	name  string
	n     int
	dType d.DataTypes
	sql   string

	catMap d.CategoryMap
}

func (df *SQLdf) Sort(keys ...string) error {
	for _, k := range keys {
		if _, e := df.Column(k); e != nil {
			return e
		}
	}

	df.orderBy = strings.Join(keys, ",")
	return nil
}

func (s *SQLcol) DataType() d.DataTypes {
	return s.dType
}

func (s *SQLcol) Len() int {
	return s.n
}

func (s *SQLcol) Data() any {
	return s.sql
}

func (s *SQLcol) Name(renameTo string) string {
	if renameTo != "" {
		s.name = renameTo
	}

	return s.name
}

func (s *SQLcol) Copy() d.Column {
	return &SQLcol{
		name:   s.name,
		n:      s.n,
		dType:  s.dType,
		sql:    s.sql,
		catMap: s.catMap,
	}
}

func NewSQLdf(query string, db *sql.DB) (*SQLdf, error) {
	var (
		err      error
		rows     *sql.Rows
		colTypes []*sql.ColumnType
		cols     []d.Column
	)

	// just get one row...TRY: just query and see if it runs one row at a time
	qry := fmt.Sprintf("WITH d AS (%s) SELECT * FROM d LIMIT 1", query)
	rows, err = db.Query(qry)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	if colTypes, err = rows.ColumnTypes(); err != nil {
		return nil, err
	}

	for ind := 0; ind < len(colTypes); ind++ {
		var dt d.DataTypes
		switch t := colTypes[ind].ScanType().Kind(); t {
		case reflect.Float64, reflect.Float32:
			dt = d.DTfloat
		case reflect.Int, reflect.Int64, reflect.Int32:
			dt = d.DTint
		case reflect.String:
			dt = d.DTstring
		case reflect.Struct:
			dt = d.DTdate
		default:
			return nil, fmt.Errorf("unsupported db field type: %v", t)
		}

		sqlCol := &SQLcol{
			name:   colTypes[ind].Name(),
			n:      1,
			dType:  dt,
			sql:    "",
			catMap: nil,
		}

		cols = append(cols, sqlCol)

	}

	var tmp *d.DFcore
	if tmp, err = d.NewDF(Run, StandardFunctions(), cols...); err != nil {
		return nil, err
	}

	df := &SQLdf{
		sourceSQL:     query,
		destTableName: "",
		db:            db,
		DFcore:        tmp,
	}

	return df, nil
}
