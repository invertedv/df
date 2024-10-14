package sql

import (
	"fmt"
	"strings"
	"time"

	u "github.com/invertedv/utilities"

	d "github.com/invertedv/df"
)

// TODO:
// - implement summary functions
// - implement appendDF
// - add orderBy to DBsave

const sigLen = 4

/*
 df SourceSQL
    - using NewSQLdfQry this is the sourceSQL supplied
    - using NewSQLdfCol this is the sourceSQL of the columns

col SourceSQL
    - This is the MakeSQL output of the dataframe the column is calculated from

df Signature
    - using NewSQLdfQry this is newly generated
    - using NewSQLdfCol this is the common signature of the columns

There's a new signature if:
- replace a column
- drop a column

*/

type SQLdf struct {
	rowCount int

	sourceSQL string // source SQL used to query DB

	signature string // unique 4-character signature to identify this data source
	version   int

	destTableName string
	orderBy       string
	where         string
	groupBy       string

	*d.DFcore
}

type SQLcol struct {
	name     string
	rowCount int
	dType    d.DataTypes
	sql      string

	sourceSQL string // source SQL used to query DB

	signature string // unique 4-character signature to identify this data source
	version   int

	catMap d.CategoryMap
}

func NewColSQL(name, signature, sourceSQL string, version int, dt d.DataTypes, sql string) *SQLcol {
	col := &SQLcol{
		name:      name,
		rowCount:  0,
		dType:     dt,
		sql:       sql,
		sourceSQL: sourceSQL,
		signature: signature,
		version:   version,
		catMap:    nil,
	}

	return col
}

func NewColScalar(name, sig string, version int, val any) (*SQLcol, error) {
	var dt d.DataTypes

	if dt = d.WhatAmI(val); dt != d.DTint && dt != d.DTfloat && dt != d.DTdate && dt != d.DTstring {
		return nil, fmt.Errorf("illegal input: %s", dt)
	}

	var sql string
	switch dt {
	case d.DTstring:
		sql = "'" + val.(string) + "'"
	default:
		sql = fmt.Sprintf("%v", val)
	}

	col := &SQLcol{
		name:      name,
		rowCount:  0,
		dType:     dt,
		sql:       sql,
		signature: sig,
		version:   version,
		catMap:    nil,
	}

	return col, nil
}

func (s *SQLdf) Signature() string {
	return s.signature
}

func (s *SQLdf) SourceSQL() string {
	return s.sourceSQL
}

func (s *SQLdf) AppendDF(dfNew d.DF) (d.DF, error) {
	return nil, nil
}

func (s *SQLdf) DBsave(tableName string, overwrite bool, cols ...string) error {
	if cols == nil {
		cols = s.ColumnNames()
	}

	if overwrite {
		if e := s.CreateTable(tableName, s.orderBy, overwrite, cols...); e != nil {
			return e
		}
	}

	return s.Dialect().Insert(tableName, s.MakeQuery(), strings.Join(cols, ","))
}

func (s *SQLdf) RowCount() int {
	if s.rowCount != 0 {
		return s.rowCount
	}

	var e error
	s.rowCount, e = s.Dialect().RowCount(s.MakeQuery())
	if e != nil {
		panic(e)
	}

	return s.rowCount
}

func (s *SQLdf) Sort(ascending bool, keys ...string) error {
	for _, k := range keys {
		if _, e := s.Column(k); e != nil {
			return e
		}
	}

	if !ascending {
		for ind := 0; ind < len(keys); ind++ {
			keys[ind] += " DESC"
		}
	}

	s.orderBy = strings.Join(keys, ",")
	return nil
}

func (s *SQLdf) Table(sortByRows bool, cols ...string) (d.DF, error) {
	var (
		dts   []d.DataTypes
		names []string
		cs    []*SQLcol
		e     error
	)
	for ind := 0; ind < len(cols); ind++ {
		var (
			c  d.Column
			ex error
		)
		if c, ex = s.Column(cols[ind]); ex != nil {
			return nil, ex
		}

		csql := c.(*SQLcol)
		cs = append(cs, csql)
		dt := csql.DataType()
		if dt != d.DTstring && dt != d.DTint && dt != d.DTdate {
			return nil, fmt.Errorf("cannot make table with type float")
		}

		dts = append(dts, dt)
		names = append(names, csql.Name(""))
	}

	count := NewColSQL("count", s.Signature(), s.MakeQuery(), s.Version(), d.DTint, "count(*)")
	cs = append(cs, count)
	//	rate := NewColSQL("rate", df.Signature(), df.MakeQuery(), df.Version(), d.DTfloat, "count / (S")

	ctx := d.NewContext(s.Dialect(), s.Files(), nil)
	var outDF *SQLdf

	if outDF, e = NewSQLdfCol(ctx, cs...); e != nil {
		return nil, e
	}

	outDF.groupBy = strings.Join(names, ",")

	return outDF, nil

}

func (s *SQLdf) MakeQuery() string {
	var fields []string
	for cx := s.Next(true); cx != nil; cx = s.Next(false) {
		var field string
		field = cx.Name("")
		if fn := cx.Data().(string); fn != "" {
			field = fmt.Sprintf("%s AS %s", fn, cx.Name(""))
		}

		fields = append(fields, field)
	}

	sig := u.RandomLetters(sigLen)
	qry := fmt.Sprintf("WITH %s AS (%s) SELECT %s FROM %s", sig, s.sourceSQL, strings.Join(fields, ","), sig)
	if s.where != "" {
		qry = fmt.Sprintf("%s WHERE %s", qry, s.where)
	}

	if s.groupBy != "" {
		qry = fmt.Sprintf("%s GROUP BY %s", qry, s.groupBy)
	}

	if s.orderBy != "" {
		qry = fmt.Sprintf("%s ORDER BY %s", qry, s.orderBy)
	}

	return qry
}

// TODO: overwrite Drop method and change the signature first

func (s *SQLdf) DropColumns(colNames ...string) error {
	s.signature = u.RandomLetters(sigLen)
	s.version = 0

	return s.Core().DropColumns(colNames...)
}

func (s *SQLdf) Version() int {
	return s.version
}

func (s *SQLdf) AppendColumn(col d.Column, replace bool) error {
	var (
		c  *SQLcol
		ok bool
	)

	if c, ok = col.(*SQLcol); !ok {
		return fmt.Errorf("AppendColumn requires *SQLcol")
	}

	if s.Signature() != c.Signature() {
		return fmt.Errorf("added column not from same source")
	}
	if s.Version() < c.Version() {
		return fmt.Errorf("added column from newer version")
	}

	s.version++

	if _, e := s.Column(col.Name("")); e == nil {
		s.signature = u.RandomLetters(sigLen)
		s.version = 0
	}

	return s.Core().AppendColumn(col, replace)
}

func (s *SQLdf) Where(col d.Column) (d.DF, error) {
	if col == nil {
		return nil, fmt.Errorf("where column is nil")
	}

	dfNew := s.Copy().(*SQLdf)

	if col.DataType() != d.DTint {
		return nil, fmt.Errorf("where column must be type DTint")
	}

	dfNew.where = fmt.Sprintf("%s > 0", col.Data().(string))
	if dfNew.where != "" {
		dfNew.where = fmt.Sprintf("(%s) AND (%s > 0)", dfNew.where, col.Data().(string))
	}

	return dfNew, nil
}

func (s *SQLdf) FileSave(fileName string) error {
	if e := s.Files().Create(fileName); e != nil {
		return e
	}
	defer func() { _ = s.Files().Close() }()

	qry := s.MakeQuery()
	rows, addr, fieldNames, e := s.Dialect().Rows(qry)
	if e != nil {
		return e
	}

	s.Files().FieldNames = fieldNames
	if ex := s.Files().WriteHeader(); ex != nil {
		return ex
	}

	for rows.Next() {
		if ex := rows.Scan(addr...); ex != nil {
			return ex
		}

		if ex := s.Files().WriteLine(addr); ex != nil {
			return ex
		}
	}

	return nil
}

func (s *SQLdf) Core() *d.DFcore {
	return s.DFcore
}

func (s *SQLdf) MakeColumn(value any) (d.Column, error) {
	var dt d.DataTypes
	if dt = d.WhatAmI(value); dt == d.DTunknown {
		return nil, fmt.Errorf("unsupported data type")
	}

	val := fmt.Sprintf("%v", value)
	if dt == d.DTstring {
		val = s.Dialect().Quote() + val + s.Dialect().Quote()
	}

	if dt == d.DTdate {
		val = s.Dialect().Quote() + value.(time.Time).Format("2006-01-02") + s.Dialect().Quote()
	}

	cx := &SQLcol{
		name:  "",
		dType: dt,
		sql:   val,
	}

	return cx, nil
}

func (s *SQLdf) Copy() d.DF {
	dfCore := s.Core().Copy()
	dfNew := &SQLdf{
		rowCount:      0,
		sourceSQL:     s.sourceSQL,
		destTableName: "",
		signature:     s.signature,
		version:       s.version,
		orderBy:       s.orderBy,
		where:         s.where,
		DFcore:        dfCore,
	}

	return dfNew
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
	n := &SQLcol{
		name:      s.name,
		rowCount:  0,
		dType:     s.dType,
		sql:       s.sql,
		sourceSQL: s.sourceSQL,
		signature: s.signature,
		version:   s.version,
		catMap:    s.catMap,
	}

	return n
}

func (s *SQLcol) SourceSQL() string {
	return s.sourceSQL
}

func (s *SQLcol) AppendRows(col d.Column) (d.Column, error) {
	return nil, nil
}

func (s *SQLcol) Signature() string {
	return s.signature
}

func (s *SQLcol) Version() int {
	return s.version
}

func NewSQLdfCol(context *d.Context, cols ...*SQLcol) (*SQLdf, error) {
	var (
		tmp *d.DFcore
		e   error
	)
	mk := cols[0].SourceSQL()
	sig := cols[0].Signature()
	version := cols[0].Version()
	for ind := 0; ind < len(cols); ind++ {
		if cols[ind].Signature() != sig {
			return nil, fmt.Errorf("incompatable columns to NewSQLdfCol")
		}
		if v := cols[ind].Version(); v > version {
			version = v
			mk = cols[0].SourceSQL()
		}
	}
	// TODO: fix runs

	df := &SQLdf{
		rowCount:      0,
		sourceSQL:     mk,
		signature:     sig,
		version:       version,
		destTableName: "",
		orderBy:       "",
		where:         "",
		DFcore:        nil,
	}

	var cstd []d.Column
	for ind := 0; ind < len(cols); ind++ {
		cstd = append(cstd, cols[ind])
	}

	if tmp, e = d.NewDF(RunDFfn, StandardFunctions(), cstd...); e != nil {
		return nil, e
	}

	// TODO: think about: should SetContext copy context?
	ctx := d.NewContext(context.Dialect(), context.Files(), df)
	tmp.SetContext(ctx)

	df.DFcore = tmp

	// populate sourceSQL for each column
	qry := df.SourceSQL() // this will be the make query from the columns
	for c := df.Next(true); c != nil; c = df.Next(false) {
		c1 := c.(*SQLcol)
		c1.sourceSQL = qry
	}

	return df, nil
}

func NewSQLdfQry(context *d.Context, query string) (*SQLdf, error) {
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
		signature:     u.RandomLetters(sigLen),
		version:       0,
		destTableName: "",
	}
	for ind := 0; ind < len(colTypes); ind++ {
		sqlCol := &SQLcol{
			name:      colNames[ind],
			dType:     colTypes[ind],
			signature: df.signature,
			version:   0,
			sql:       "",
			catMap:    nil,
		}

		cols = append(cols, sqlCol)
	}

	var tmp *d.DFcore
	// TODO: fix runs
	if tmp, e = d.NewDF(RunDFfn, StandardFunctions(), cols...); e != nil {
		return nil, e
	}
	// TODO: think about: should SetContext copy context?
	tmp.SetContext(context)
	tmp.Context.SetSelf(df)

	df.DFcore = tmp

	// populate sourceSQL for each column
	qry := df.SourceSQL()
	for c := df.Next(true); c != nil; c = df.Next(false) {
		c1 := c.(*SQLcol)
		c1.sourceSQL = qry
	}

	return df, nil
}
