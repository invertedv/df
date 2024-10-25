package sql

import (
	"fmt"
	"maps"
	"strings"
	"time"

	m "github.com/invertedv/df/mem"

	u "github.com/invertedv/utilities"

	d "github.com/invertedv/df"
)

/*
 df SourceSQL
    - using DBload this is the sourceSQL supplied
    - using NewDFcol this is the sourceSQL of the columns

col SourceSQL
    - This is the MakeSQL output of the dataframe the column is calculated from

df Signature
    - using DBload this is newly generated
    - using NewDFcol this is the common signature of the columns

There's a new signature if:
- replace a column
- drop a column

*/

// ***************** SQLdf *****************

// SQLdf is the implementation of DF for SQL.
//
// signature is the unique identifier of this dataframe.  It is reset if
//   - a column is dropped
//
// version is the version number of this dataframe.  It is incremented if
//   - a column is added
type SQLdf struct {
	//	rowCount int

	sourceSQL string // source SQL used to query DB

	signature string // unique 4-character signature to identify this data source
	version   int    // version of this dataframe.  The version is incremented when columns are added.

	orderBy string
	where   string
	groupBy string

	*d.DFcore
}

type SQLcol struct {
	name string
	//	rowCountX int
	dType d.DataTypes
	sql   string // SQL to generate this column

	sourceSQL string // SQL that produces the result set that populates this column

	signature string // unique 4-character signature to identify this data source
	version   int    // version of the dataframe that existed when this column was added

	catMap    d.CategoryMap
	catCounts d.CategoryMap
	rawType   d.DataTypes

	scalarValue any // This is for keeping the actual value of constants rather than SQL version
}

func NewDFcol(runDF d.RunFn, funcs d.Fns, context *d.Context, cols ...*SQLcol) (*SQLdf, error) {
	if runDF == nil {
		runDF = RunDFfn
	}

	if funcs == nil {
		funcs = StandardFunctions()
	}

	//func NewDFcol(context *d.Context, cols ...*SQLcol) (*SQLdf, error) {
	var (
		tmp *d.DFcore
		e   error
	)
	mk := cols[0].SourceSQL()
	sig := cols[0].Signature()
	version := cols[0].Version()
	for ind := 0; ind < len(cols); ind++ {
		if cols[ind].Signature() != sig {
			return nil, fmt.Errorf("incompatable columns to NewDFcol")
		}
		if v := cols[ind].Version(); v > version {
			version = v
			mk = cols[0].SourceSQL()
		}
	}
	// TODO: fix runs ??

	df := &SQLdf{
		sourceSQL: mk,
		signature: sig,
		version:   version,
		orderBy:   "",
		where:     "",
		DFcore:    nil,
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

func DBload(query string, context *d.Context) (*SQLdf, error) {
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
		sourceSQL: query,
		signature: newSignature(),
		version:   0,
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

	// increment version # if append is a new column or an existing column of the same type
	exists, sameType := false, false
	if cx, e := s.Column(col.Name("")); e == nil {
		exists = true
		if cx.DataType() == col.DataType() {
			sameType = true
		}
	}

	if exists && sameType {
		s.version++
	}

	if !exists {
		s.version++
	}

	// create a new signature if the append is replacing an existing column but is not the same type
	if exists && !sameType {
		s.signature = newSignature()
		s.version = 0
	}

	return s.Core().AppendColumn(col, replace)
}

func (s *SQLdf) AppendDF(dfNew d.DF) (d.DF, error) {
	n1 := s.ColumnNames()

	if len(n1) != len(dfNew.ColumnNames()) {
		return nil, fmt.Errorf("dataframes cannot be appended")
	}

	for c := s.Next(true); c != nil; c = s.Next(false) {
		var (
			cNew d.Column
			e    error
		)
		if cNew, e = dfNew.Column(c.Name("")); e != nil {
			return nil, fmt.Errorf("missing column %s in AppendDF", c.Name(""))
		}

		if c.DataType() != cNew.DataType() {
			return nil, fmt.Errorf("column %s has differing data types in AppendDF", c.Name(""))
		}
	}

	var (
		sql string
		e   error
	)
	if sql, e = s.Context.Dialect().Union(s.MakeQuery(n1...), dfNew.(*SQLdf).MakeQuery(n1...), n1...); e != nil {
		return nil, e
	}

	var (
		dfOut *SQLdf
		eOut  error
	)
	ctx := d.NewContext(s.Context.Dialect(), nil, nil)
	if dfOut, eOut = DBload(sql, ctx); eOut != nil {
		return nil, eOut
	}

	dfOut.SetSelf(dfOut)

	return dfOut, nil
}

func (s *SQLdf) Copy() d.DF {
	dfCore := s.Core().Copy()
	dfNew := &SQLdf{
		sourceSQL: s.sourceSQL,
		signature: s.signature,
		version:   s.version,
		orderBy:   s.orderBy,
		groupBy:   s.groupBy,
		where:     s.where,
		DFcore:    dfCore,
	}

	dfNew.SetSelf(dfNew)
	return dfNew
}

func (s *SQLdf) Core() *d.DFcore {
	return s.DFcore
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

// TODO: overwrite Drop method and change the signature first
func (s *SQLdf) DropColumns(colNames ...string) error {
	s.signature = newSignature()
	s.version = 0

	return s.Core().DropColumns(colNames...)
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

func (s *SQLdf) MakeQuery(colNames ...string) string {
	var fields []string
	if colNames == nil {
		colNames = s.ColumnNames()
	}

	for _, cn := range colNames {
		var (
			cx d.Column
			e  error
		)

		if cx, e = s.Column(cn); e != nil {
			panic(e)
		}

		var field string
		field = cx.Name("")
		if fn := cx.Data().(string); fn != "" {
			// Need to Cast to required type here o.w. DB may default to an unsupported type
			fnc, _ := s.Context.Dialect().CastField(fn, cx.DataType(), cx.DataType())
			field = fmt.Sprintf("%s AS %s", fnc, cx.Name(""))
		}

		fields = append(fields, field)
	}

	sig := newSignature()
	qry := fmt.Sprintf("WITH %s AS (%s) SELECT\n%s FROM %s", sig, s.sourceSQL, strings.Join(fields, ",\n"), sig)
	if s.where != "" {
		qry = fmt.Sprintf("%s WHERE %s\n", qry, s.where)
	}

	if s.groupBy != "" {
		qry = fmt.Sprintf("%s GROUP BY %s\n", qry, s.groupBy)
	}

	if s.orderBy != "" {
		qry = fmt.Sprintf("%s ORDER BY %s\n", qry, s.orderBy)
	}

	return qry
}

func (s *SQLdf) RowCount() int {
	var (
		rowCount int
		e        error
	)
	if rowCount, e = s.Dialect().RowCount(s.MakeQuery()); e != nil {
		panic(e)
	}

	return rowCount
}

func (s *SQLdf) Signature() string {
	return s.signature
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

func (s *SQLdf) SourceSQL() string {
	return s.sourceSQL
}

func (s *SQLdf) Categorical(colName string, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (d.Column, error) {
	var (
		col d.Column
		e   error
	)
	if col, e = s.Column(colName); e != nil {
		return nil, e
	}

	nextInt := 0
	for k, v := range catMap {
		if k != nil && d.WhatAmI(k) != col.DataType() {
			return nil, fmt.Errorf("map and column not same data types")
		}

		if v >= nextInt {
			nextInt = v + 1
		}
	}

	toMap := make(d.CategoryMap)
	maps.Copy(toMap, catMap)

	if _, ok := toMap[defaultVal]; !ok {
		toMap[defaultVal] = -1
	}

	cn := col.Name("")
	var (
		tabl d.DF
		e4   error
	)
	if tabl, e4 = s.Table(true, cn); e4 != nil {
		return nil, e4
	}

	x := tabl.(*SQLdf).MakeQuery()
	var (
		mDF *m.MemDF
		e1  error
	)
	if mDF, e1 = m.DBLoad(x, s.Context); e1 != nil {
		return nil, e
	}

	_ = mDF.Sort(true, cn)

	var (
		inCol d.Column
		e2    error
	)
	if inCol, e2 = mDF.Column(cn); e2 != nil {
		return nil, e2
	}

	var (
		counts d.Column
		e3     error
	)
	if counts, e3 = mDF.Column("count"); e3 != nil {
		return nil, e3
	}

	cnts := make(d.CategoryMap)
	caseNo := 0
	var whens, equalTo []string
	for ind := 0; ind < inCol.Len(); ind++ {
		outVal := caseNo
		val := inCol.(*m.MemCol).Element(ind)
		ct := counts.(*m.MemCol).Element(ind).(int)
		catVal := val

		if fuzz > 1 && ct < fuzz {
			outVal = -1
		}

		if levels != nil && !d.In(val, levels) {
			if v, ok := toMap[defaultVal]; ok {
				outVal = v
			}

			catVal = defaultVal
		}

		if v, ok := toMap[val]; ok {
			outVal = v
		}

		toMap[val] = outVal

		cnts[catVal] += ct

		whens = append(whens, fmt.Sprintf("%s = %s", cn, s.Dialect().ToString(val)))
		equalTo = append(equalTo, fmt.Sprintf("%d", outVal))
		if outVal == caseNo {
			caseNo++
		}
	}

	var (
		sql1 string
		ex   error
	)
	if sql1, ex = s.Dialect().Case(whens, equalTo); ex != nil {
		return nil, ex
	}
	if sql1, ex = s.Dialect().CastField(sql1, d.DTint, d.DTint); ex != nil {
		return nil, ex
	}

	outCol := NewColSQL("", s.Signature(), s.MakeQuery(), s.Version(), d.DTcategorical, sql1)
	outCol.rawType = col.DataType()
	outCol.catMap, outCol.catCounts = toMap, cnts

	return outCol, nil
}

func (s *SQLdf) Table(sortByRows bool, cols ...string) (d.DF, error) {
	var (
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
		if dt != d.DTstring && dt != d.DTint && dt != d.DTdate && dt != d.DTcategorical {
			return nil, fmt.Errorf("cannot make table with type float")
		}

		names = append(names, csql.Name(""))
	}

	var (
		cc, cf string
		ex     error
	)
	if cc, ex = s.Dialect().CastField("count(*)", d.DTint, d.DTint); ex != nil {
		return nil, ex
	}
	if cf, ex = s.Dialect().CastField("count(*) / (SELECT count(*) FROM (%s))", d.DTfloat, d.DTfloat); ex != nil {
		return nil, ex
	}

	count := NewColSQL("count", s.Signature(), s.MakeQuery(), s.Version(), d.DTint, cc)
	cs = append(cs, count)
	rateSQL := fmt.Sprintf(cf, s.MakeQuery())
	rate := NewColSQL("rate", s.Signature(), s.MakeQuery(), s.Version(), d.DTfloat, rateSQL)
	cs = append(cs, rate)

	ctx := d.NewContext(s.Dialect(), s.Files(), nil)
	var outDF *SQLdf

	if outDF, e = NewDFcol(s.Runner(), s.Fns(), ctx, cs...); e != nil {
		return nil, e
	}

	outDF.groupBy = strings.Join(names, ",")
	outDF.orderBy = "count DESC"
	if sortByRows {
		outDF.orderBy = outDF.groupBy
	}

	return outDF, nil
}

func (s *SQLdf) Version() int {
	return s.version
}

func (s *SQLdf) Where(col d.Column) (d.DF, error) {
	if col == nil {
		return nil, fmt.Errorf("where column is nil")
	}

	dfNew := s.Copy().(*SQLdf)
	dfNew.signature += "W"
	dfNew.version = 0

	if col.DataType() != d.DTint {
		return nil, fmt.Errorf("where column must be type DTint")
	}

	dfNew.where = fmt.Sprintf("%s > 0", col.Data().(string))
	if dfNew.where != "" {
		dfNew.where = fmt.Sprintf("(%s) AND (%s > 0)", dfNew.where, col.Data().(string))
	}

	return dfNew, nil
}

func (s *SQLdf) MakeColumnXX(value any) (d.Column, error) {
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

// ***************** SQLcol *****************

func NewColSQL(name, signature, sourceSQL string, version int, dt d.DataTypes, sql string) *SQLcol {
	col := &SQLcol{
		name:      name,
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
		dType:     dt,
		sql:       sql,
		signature: sig,
		version:   version,
	}

	return col, nil
}

func (s *SQLcol) AppendRows(col d.Column) (d.Column, error) {
	return nil, nil
}

func (s *SQLcol) Copy() d.Column {
	n := &SQLcol{
		name:      s.name,
		dType:     s.dType,
		sql:       s.sql,
		sourceSQL: s.sourceSQL,
		signature: s.signature,
		version:   s.version,
		catMap:    s.catMap,
	}

	return n
}

func (s *SQLcol) Data() any {
	if s.sql != "" {
		return s.sql
	}

	return s.name
}

func (s *SQLcol) DataType() d.DataTypes {
	return s.dType
}

func (s *SQLcol) Len() int {
	return -1
}

func (s *SQLcol) Name(renameTo string) string {
	if renameTo != "" {
		s.name = renameTo
	}

	return s.name
}

func (s *SQLcol) Signature() string {
	return s.signature
}

func (s *SQLcol) SourceSQL() string {
	return s.sourceSQL
}

func (s *SQLcol) Version() int {
	return s.version
}

// helpers

func newSignature() string {
	const sigLen = 4
	return u.RandomLetters(sigLen)
}

func (s *SQLcol) RawType() d.DataTypes {
	return s.rawType
}
