package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
)

type SQLdf struct {
	rowCount      int
	sourceSQL     string
	destTableName string
	db            *sql.DB
	orderBy       string

	*d.DFcore
}

type SQLcol struct {
	name     string
	rowCount int
	dType    d.DataTypes
	sql      string

	catMap d.CategoryMap
}

func (df *SQLdf) RowCount() int {
	if df.rowCount != 0 {
		return df.rowCount
	}

	var n int32
	qry := fmt.Sprintf("WITH d AS (%s) SELECT cast(count(*) AS Int32) AS n FROM d", df.sourceSQL)
	row := df.db.QueryRow(qry)
	if e := row.Scan(&n); e != nil {
		panic(e)
	}

	df.rowCount = int(n)
	return df.rowCount
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

func (df *SQLdf) MakeQuery() string {
	var fields []string
	for c := df.Next(true); c != nil; c = df.Next(false) {
		var field string
		field = c.Name("")
		if fn := c.Data().(string); fn != "" {
			field = fmt.Sprintf("%s AS %s", fn, c.Name(""))
		}

		fields = append(fields, field)
	}

	qry := fmt.Sprintf("WITH d AS (%s) SELECT %s FROM d", df.sourceSQL, strings.Join(fields, ","))

	return qry
}

func (df *SQLdf) ToMemDF() (*m.MemDF, error) {
	qry := df.MakeQuery()
	var (
		rows *sql.Rows
		err  error
	)
	if rows, err = df.db.Query(qry); err != nil {
		return nil, err
	}

	r := make([]any, df.ColumnCount())
	for ind := range r {
		var x any
		r[ind] = &x
	}

	memData := make([][]any, df.ColumnCount())
	for rows.Next() {
		rx := df.ScanSlice()
		if e := rows.Scan(rx...); e != nil {
			return nil, e
		}
		for ind := 0; ind < len(rx); ind++ {
			memData[ind] = append(memData[ind], rx[ind])
		}
	}

	colNames := df.ColumnNames()
	colTypes := df.ColumnTypes()
	var memDF *m.MemDF

	x := memData[0][0]
	y := *x.(*interface{})
	_ = y

	for ind := 0; ind < len(colNames); ind++ {
		var (
			col *m.MemCol
			e   error
		)

		if col, e = m.NewMemCol(colNames[ind], ScanInverter(memData[ind], colTypes[ind])); e != nil {
			return nil, e
		}

		if ind == 0 {
			if memDF, e = m.NewMemDF(m.Run, m.StandardFunctions(), col); e != nil {
				return nil, e
			}

			continue
		}

		if e1 := memDF.AppendColumn(col); e1 != nil {
			return nil, e1
		}

	}

	return memDF, nil
}

func ScanInverter(xScan []any, dt d.DataTypes) any {
	x := d.MakeSlice(dt, len(xScan))

	for ind, xs := range xScan {
		var (
			y any
			e error
		)

		if y, e = d.ToDataType(*xs.(*interface{}), dt, true); e != nil {
			panic(e)
		}

		switch dt {
		case d.DTfloat:
			x.([]float64)[ind] = y.(float64)
		case d.DTint:
			x.([]int)[ind] = y.(int)
		case d.DTstring:
			x.([]string)[ind] = y.(string)
		case d.DTdate:
			x.([]time.Time)[ind] = y.(time.Time)
		}
	}

	return x
}

func (df *SQLdf) ScanSlice() []any {
	var s []any

	for c := df.Next(true); c != nil; c = df.Next(false) {
		var x any

		switch c.DataType() {
		case d.DTfloat:
			x = float64(0)
		case d.DTint:
			x = int(0)
		case d.DTstring:
			x = ""
		case d.DTdate:
			x = time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)
		}

		s = append(s, &x)
	}

	return s
}

/////////// SQLcol

func (s *SQLcol) DataType() d.DataTypes {
	return s.dType
}

func (s *SQLcol) Len() int {
	return -1
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
		name: s.name,
		//		n:      s.n,
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

	df := &SQLdf{
		sourceSQL:     query,
		destTableName: "",
		db:            db,
	}

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
			name: colTypes[ind].Name(),
			//			n:      df.n,
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

	df.DFcore = tmp

	return df, nil
}
