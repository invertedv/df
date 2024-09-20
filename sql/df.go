package sql

import (
	"fmt"
	"strings"
	"time"

	d "github.com/invertedv/df"
)

type SQLdf struct {
	rowCount      int
	sourceSQL     string
	destTableName string
	orderBy       string
	where         string

	*d.DFcore
}

type SQLcol struct {
	name     string
	rowCount int
	dType    d.DataTypes
	sql      string

	catMap d.CategoryMap
}

func (df *SQLdf) DBsave(tableName string, overwrite bool, cols ...string) error {
	if cols == nil {
		cols = df.ColumnNames()
	}

	if overwrite {
		if e := df.CreateTable(tableName, "", overwrite, cols...); e != nil {
			return e
		}
	}

	return df.Dialect().Insert(tableName, df.MakeQuery(), strings.Join(cols, ","))
}

func (df *SQLdf) RowCount() int {
	if df.rowCount != 0 {
		return df.rowCount
	}

	var e error
	df.rowCount, e = df.Dialect().RowCount(df.sourceSQL)
	if e != nil {
		panic(e)
	}

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
	for cx := df.Next(true); cx != nil; cx = df.Next(false) {
		var field string
		field = cx.Name("")
		if fn := cx.Data().(string); fn != "" {
			field = fmt.Sprintf("%s AS %s", fn, cx.Name(""))
		}

		fields = append(fields, field)
	}

	qry := fmt.Sprintf("WITH d AS (%s) SELECT %s FROM d", df.sourceSQL, strings.Join(fields, ","))
	if df.where != "" {
		qry = fmt.Sprintf("%s WHERE %s", qry, df.where)
	}

	if df.orderBy != "" {
		qry = fmt.Sprintf("%s ORDER BY %s", qry, df.orderBy)
	}

	fmt.Println(qry)
	return qry
}

func (df *SQLdf) Where(col d.Column) error {
	if col == nil {
		df.where = ""
		return nil
	}

	if col.DataType() != d.DTint {
		return fmt.Errorf("where column must be tpye DTint")
	}

	df.where = fmt.Sprintf("%s > 0", col.Name(""))

	return nil
}

func (df *SQLdf) FileSave(fileName string) error {
	if e := df.Files().Create(fileName); e != nil {
		return e
	}
	defer func() { _ = df.Files().Close() }()

	qry := df.MakeQuery()
	rows, addr, fieldNames, e := df.Dialect().Rows(qry)
	if e != nil {
		return e
	}

	df.Files().FieldNames = fieldNames
	if ex := df.Files().WriteHeader(); ex != nil {
		return ex
	}

	for rows.Next() {
		if ex := rows.Scan(addr...); ex != nil {
			return ex
		}

		if ex := df.Files().WriteLine(addr); ex != nil {
			return ex
		}
	}

	return nil
}

func (df *SQLdf) MakeColumn(value any) (d.Column, error) {
	var dt d.DataTypes
	if dt = d.WhatAmI(value); dt == d.DTunknown {
		return nil, fmt.Errorf("unsupported data type")
	}

	val := fmt.Sprintf("%v", value)
	if dt == d.DTstring {
		val = df.Dialect().Quote() + val + df.Dialect().Quote()
	}

	if dt == d.DTdate {
		val = df.Dialect().Quote() + value.(time.Time).Format("2006-01-02") + df.Dialect().Quote()
	}

	cx := &SQLcol{
		name:  "",
		dType: dt,
		sql:   val,
	}

	return cx, nil
}

/////////// SQLcol

func (s *SQLcol) DataType() d.DataTypes {
	return s.dType
}

func (s *SQLcol) Len() int {
	return -1
}

func (s *SQLcol) Data() any {
	if s.sql != "" {
		return s.sql
	}

	return s.name
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

func NewSQLdf(query string, context *d.Context) (*SQLdf, error) {
	var (
		e        error
		colTypes []d.DataTypes
		colNames []string
		cols     []d.Column
	)

	if context.Dialect() == nil {
		return nil, fmt.Errorf("no DB defined in Context for NewSQLdf")
	}
	if colNames, colTypes, e = context.Dialect().Types(query); e != nil {
		return nil, e
	}

	df := &SQLdf{
		sourceSQL:     query,
		destTableName: "",
	}
	for ind := 0; ind < len(colTypes); ind++ {
		sqlCol := &SQLcol{
			name:   colNames[ind],
			dType:  colTypes[ind],
			sql:    "",
			catMap: nil,
		}

		cols = append(cols, sqlCol)
	}

	var tmp *d.DFcore
	// TODO: fix runs
	if tmp, e = d.NewDF(Run, nil, StandardFunctions(), cols...); e != nil {
		return nil, e
	}

	tmp.SetContext(context)

	df.DFcore = tmp

	return df, nil
}
