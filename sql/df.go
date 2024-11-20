package sql

import (
	"database/sql"
	"fmt"
	"io"
	"maps"
	"strings"

	m "github.com/invertedv/df/mem"

	d "github.com/invertedv/df"
)

// TODO: make mem work like this

// DF is the implementation of DF for SQL.
//
// signature is the unique identifier of this dataframe.  It is reset if
//   - a column is dropped
//
// version is the version number of this dataframe.  It is incremented if
//   - a column is added
type DF struct {
	//	rowCount int

	sourceSQL string // source SQL used to query DB

	//	signature string // unique 4-character signature to identify this data source
	//	version   int    // version of this dataframe.  The version is incremented when columns are added.

	orderBy string
	where   string
	groupBy string

	*d.DFcore

	rows *sql.Rows
	row  []any
}

type Col struct {
	name  string
	dType d.DataTypes
	sql   string // SQL to generate this column

	ctx *d.Context

	//	signature string // unique 4-character signature to identify this data source
	//	version   int    // version of the dataframe that existed when this column was added

	catMap    d.CategoryMap
	catCounts d.CategoryMap
	rawType   d.DataTypes

	scalarValue any // This is for keeping the actual value of constants rather than SQL version

	dependencies []string
}

// ***************** DF - Create *****************

func NewDFcol(runDF d.RunFn, funcs d.Fns, context *d.Context, cols ...*Col) (*DF, error) {
	if runDF == nil {
		runDF = RunDFfn
	}

	if funcs == nil {
		funcs = StandardFunctions()
	}

	for ind := 0; ind < len(cols); ind++ {
		cols[ind].ctx = context
	}
	// TODO: fix runs ??

	r := cols[0].Context().Self()
	_ = r
	df := &DF{
		sourceSQL: cols[0].Context().Self().MakeQuery(), // TODO: check
		orderBy:   "",
		where:     "",
		DFcore:    nil,
	}

	var cstd []d.Column
	for ind := 0; ind < len(cols); ind++ {
		cstd = append(cstd, cols[ind])
	}

	var (
		tmp *d.DFcore
		e   error
	)
	if tmp, e = d.NewDF(runDF, funcs, cstd...); e != nil {
		return nil, e
	}

	// TODO: think about: should SetContext copy context?
	ctx := d.NewContext(context.Dialect(), df, context.Unassigned()...)
	tmp.SetContext(ctx)

	df.DFcore = tmp

	return df, nil
}

func NewDFseq(runDF d.RunFn, funcs d.Fns, context *d.Context, n int) *DF {
	if runDF == nil {
		runDF = RunDFfn
	}

	if funcs == nil {
		funcs = StandardFunctions()
	}

	dlct := context.Dialect()
	seqSQL := fmt.Sprintf("SELECT %s AS seq", dlct.Seq(n))

	col := &Col{
		name:         "seq",
		dType:        d.DTint,
		sql:          "",
		ctx:          nil,
		catMap:       nil,
		catCounts:    nil,
		rawType:      0,
		scalarValue:  nil,
		dependencies: nil,
	}

	dfc, ex := d.NewDF(runDF, funcs, col)
	if ex != nil {
		panic(ex)
	}

	df := &DF{
		sourceSQL: seqSQL,
		orderBy:   "",
		where:     "",
		groupBy:   "",
		DFcore:    dfc,

		rows: nil,
		row:  nil,
	}
	ctx := d.NewContext(context.Dialect(), df)
	df.SetContext(ctx)

	return df
}

func DBload(query string, context *d.Context) (*DF, error) {
	var (
		e        error
		colTypes []d.DataTypes
		colNames []string
		cols     []d.Column
	)

	dlct := context.Dialect()
	if dlct == nil {
		return nil, fmt.Errorf("no DB defined in Context for NewSQLdf")
	}
	if colNames, colTypes, e = context.Dialect().Types(query); e != nil {
		return nil, e
	}

	df := &DF{
		sourceSQL: query,
	}

	for ind := 0; ind < len(colTypes); ind++ {
		sqlCol := &Col{
			name:   colNames[ind],
			dType:  colTypes[ind],
			ctx:    context,
			sql:    "",
			catMap: nil,
		}

		cols = append(cols, sqlCol)
	}

	var tmp *d.DFcore
	// TODO: fix runs
	if tmp, e = d.NewDF(RunDFfn, StandardFunctions(), cols...); e != nil {
		return nil, e
	}
	// TODO: think about: should SetContext copy context?
	df.DFcore = tmp
	df.SetContext(d.NewContext(dlct, df))

	return df, nil
}

// ***************** DF - Methods *****************

func (s *DF) AppendColumn(col d.Column, replace bool) error {
	panicer(col)

	if !sameSource(s, col) {
		return fmt.Errorf("added column not from same source")
	}

	return s.Core().AppendColumn(col, replace)
}

func (s *DF) AppendDF(dfNew d.DF) (d.DF, error) {
	n1 := s.ColumnNames()

	if len(n1) != len(dfNew.ColumnNames()) {
		return nil, fmt.Errorf("dataframes cannot be appended")
	}

	for c := s.Next(true); c != nil; c = s.Next(false) {
		var cNew d.Column
		if cNew = dfNew.Column(c.Name("")); c == nil {
			return nil, fmt.Errorf("missing column %s in AppendDF", c.Name(""))
		}

		if c.DataType() != cNew.DataType() {
			return nil, fmt.Errorf("column %s has differing data types in AppendDF", c.Name(""))
		}
	}

	var (
		sqlx string
		e    error
	)
	if sqlx, e = s.Context().Dialect().Union(s.MakeQuery(), dfNew.(*DF).MakeQuery(), n1...); e != nil {
		return nil, e
	}

	var (
		dfOut *DF
		eOut  error
	)
	ctx := d.NewContext(s.Context().Dialect(), nil, nil)

	if dfOut, eOut = DBload(sqlx, ctx); eOut != nil {
		return nil, eOut
	}

	dfOut.Context().SetSelf(dfOut)

	return dfOut, nil
}

func (s *DF) Categorical(colName string, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (d.Column, error) {
	var col d.Column
	if col = s.Column(colName); col == nil {
		return nil, fmt.Errorf("column %s not found", col)
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

	x := tabl.(*DF).MakeQuery()
	var (
		mDF *m.DF
		e1  error
	)
	if mDF, e1 = m.DBLoad(x, s.Context().Dialect()); e1 != nil {
		return nil, e1
	}

	_ = mDF.Sort(true, cn)

	var inCol d.Column
	if inCol = mDF.Column(cn); inCol == nil {
		return nil, fmt.Errorf("column %s not found", cn)
	}

	var counts d.Column
	if counts = mDF.Column("count"); counts == nil {
		return nil, fmt.Errorf("column count not found")
	}

	cnts := make(d.CategoryMap)
	caseNo := 0
	var whens, equalTo []string
	for ind := 0; ind < inCol.Len(); ind++ {
		outVal := caseNo
		val := inCol.(*m.Col).Element(ind)
		ct := counts.(*m.Col).Element(ind).(int)
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

		whens = append(whens, fmt.Sprintf("%s = %s", cn, s.Context().Dialect().ToString(val)))
		equalTo = append(equalTo, fmt.Sprintf("%d", outVal))
		if outVal == caseNo {
			caseNo++
		}
	}

	var (
		sql1 string
		ex   error
	)
	if sql1, ex = s.Context().Dialect().Case(whens, equalTo); ex != nil {
		return nil, ex
	}
	if sql1, ex = s.Context().Dialect().CastField(sql1, d.DTint, d.DTint); ex != nil {
		return nil, ex
	}

	outCol := NewColSQL("", s.Context(), d.DTcategorical, sql1)
	outCol.rawType = col.DataType()
	outCol.catMap, outCol.catCounts = toMap, cnts

	return outCol, nil
}

func (s *DF) Copy() d.DF {
	dfCore := s.Core().Copy()
	dfNew := &DF{
		sourceSQL: s.sourceSQL,
		orderBy:   s.orderBy,
		groupBy:   s.groupBy,
		where:     s.where,
		DFcore:    dfCore,
	}

	//dfNew.Context().SetSelf(dfNew)
	ctx := d.NewContext(s.Context().Dialect(), dfNew)
	dfNew.SetContext(ctx)
	return dfNew
}

// TODO: check
func (s *DF) DropColumns(colNames ...string) error {
	return s.Core().DropColumns(colNames...)
}

func (s *DF) Iter(reset bool) (row []any, err error) {
	if reset {
		qry := s.MakeQuery()
		var e error
		s.rows, s.row, _, e = s.Context().Dialect().Rows(qry)
		if e != nil {
			_ = s.rows.Close()
			return nil, e
		}
	}

	if ok := s.rows.Next(); !ok {
		return nil, io.EOF
	}

	if ex := s.rows.Scan(s.row...); ex != nil {
		_ = s.rows.Close()
		return nil, io.EOF
	}

	return s.row, nil
}

func (s *DF) MakeQuery(colNames ...string) string {
	var fields []string

	if colNames == nil {
		colNames = s.ColumnNames()
	}

	for ind := 0; ind < len(colNames); ind++ {
		var cx d.Column
		if cx = s.Column(colNames[ind]); cx == nil {
			panic(fmt.Errorf("missing name %s", cx.Name("")))
		}

		var field string
		field = cx.Name("")
		if fn := cx.(*Col).SQL().(string); fn != "" {
			// Need to Cast to required type here o.w. DB may default to an unsupported type
			fnc, _ := s.Context().Dialect().CastField(fn, cx.DataType(), cx.DataType())
			field = fmt.Sprintf("%s AS %s", fnc, cx.Name(""))
		}

		fields = append(fields, field)
	}

	with := d.RandomLetters(4)
	qry := fmt.Sprintf("WITH %s AS (%s) SELECT\n%s FROM %s", with, s.sourceSQL, strings.Join(fields, ",\n"), with)
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

func (s *DF) RowCount() int {
	var (
		rowCount int
		e        error
	)
	if rowCount, e = s.Context().Dialect().RowCount(s.MakeQuery()); e != nil {
		panic(e)
	}

	return rowCount
}

func (s *DF) Sort(ascending bool, keys ...string) error {
	for _, k := range keys {
		if c := s.Column(k); c == nil {
			return fmt.Errorf("missing column %s", k)
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

func (s *DF) SourceSQL() string {
	return s.sourceSQL
}

func (s *DF) String() string {
	var sx string
	for c := s.Next(true); c != nil; c = s.Next(false) {
		sx += c.String() + "\n"
	}

	return sx
}

func (s *DF) Table(sortByRows bool, cols ...string) (d.DF, error) {
	var (
		names []string
		cs    []*Col
	)
	for ind := 0; ind < len(cols); ind++ {
		var c d.Column
		if c = s.Column(cols[ind]); c == nil {
			return nil, fmt.Errorf("missing column %s", cols[ind])
		}

		csql := c.(*Col)
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
	if cc, ex = s.Context().Dialect().CastField("count(*)", d.DTint, d.DTint); ex != nil {
		return nil, ex
	}

	if cf, ex = s.Context().Dialect().CastField("count(*) / (SELECT count(*) FROM (%s))", d.DTfloat, d.DTfloat); ex != nil {
		return nil, ex
	}

	count := NewColSQL("count", s.Context(), d.DTint, cc)
	cs = append(cs, count)
	rateSQL := fmt.Sprintf(cf, s.MakeQuery())
	rate := NewColSQL("rate", s.Context(), d.DTfloat, rateSQL)
	cs = append(cs, rate)

	var (
		dfc *d.DFcore
		e   error
	)
	if dfc, e = s.KeepColumns(names...); e != nil {
		return nil, e
	}

	outDF := &DF{
		sourceSQL: s.sourceSQL,
		orderBy:   "",
		where:     s.where,
		groupBy:   "",
		DFcore:    dfc,
		rows:      nil,
		row:       nil,
	}

	outDF.groupBy = strings.Join(names, ",")
	outDF.orderBy = "count DESC"
	if sortByRows {
		outDF.orderBy = outDF.groupBy
	}

	if e1 := outDF.AppendColumn(count, false); e1 != nil {
		return nil, e1
	}

	if e1 := outDF.AppendColumn(rate, false); e1 != nil {
		return nil, e1
	}

	ctx := d.NewContext(s.Context().Dialect(), outDF)
	outDF.SetContext(ctx)

	return outDF, nil
}

func (s *DF) Where(col d.Column) (d.DF, error) {
	panicer(col)
	if col == nil {
		return nil, fmt.Errorf("where column is nil")
	}

	// TODO: this should update self + columns
	dfNew := s.Copy().(*DF)

	if col.DataType() != d.DTint {
		return nil, fmt.Errorf("where column must be type DTint")
	}

	dfNew.where = fmt.Sprintf("%s > 0", col.(*Col).SQL().(string))
	if dfNew.where != "" {
		dfNew.where = fmt.Sprintf("(%s) AND (%s > 0)", dfNew.where, col.(*Col).SQL().(string))
	}

	//dfNew.SetContext(d.NewContext(s.Context().Dialect(), dfNew))

	return dfNew, nil
}

// ***************** Col - Create *****************

func NewColSQL(name string, context *d.Context, dt d.DataTypes, sqlx string) *Col {
	col := &Col{
		name:   name,
		dType:  dt,
		sql:    sqlx,
		ctx:    context,
		catMap: nil,
	}

	return col
}

func NewColScalar(name string, val any) (*Col, error) {
	var dt d.DataTypes

	if dt = d.WhatAmI(val); dt != d.DTint && dt != d.DTfloat && dt != d.DTdate && dt != d.DTstring {
		return nil, fmt.Errorf("illegal input: %s", dt)
	}

	var sqlx string
	switch dt {
	case d.DTstring:
		sqlx = "'" + val.(string) + "'"
	default:
		sqlx = fmt.Sprintf("%v", val)
	}

	col := &Col{
		name:  name,
		dType: dt,
		sql:   sqlx,
	}

	return col, nil
}

// ***************** SQLCol - Methods *****************

func (s *Col) AppendRows(col d.Column) (d.Column, error) {
	panicer(col)
	if s.DataType() != col.DataType() {
		return nil, fmt.Errorf("incompatible columns in AppendRows")
	}
	q1 := s.MakeQuery()
	c := col.Copy()
	c.Name(s.Name(""))
	q2 := s.MakeQuery()

	var (
		source string
		e      error
	)
	if source, e = s.Context().Dialect().Union(q1, q2, s.Name("")); e != nil {
		return nil, e
	}
	_ = source
	// TODO: this work?
	outCol := &Col{
		name:  s.Name(""),
		dType: s.DataType(),
		sql:   "",
		ctx:   s.Context(),
	}
	return outCol, nil
}

func (s *Col) CategoryMap() d.CategoryMap {
	return s.catMap
}

func (s *Col) Copy() d.Column {
	n := &Col{
		name:  s.name,
		dType: s.dType,
		sql:   s.sql,
		ctx:   s.ctx,

		catMap:      s.catMap,
		catCounts:   s.catCounts,
		rawType:     s.rawType,
		scalarValue: s.scalarValue,
	}

	return n
}

func (s *Col) Data() any {
	var (
		df *m.DF
		e  error
	)

	// give it a random name if it does not have one
	if s.Name("") == "" {
		s.Name(d.RandomLetters(5))
	}

	if df, e = m.DBLoad(s.MakeQuery(), s.Context().Dialect()); e != nil {
		panic(e)
	}

	var col d.Column
	if col = df.Column(s.Name("")); col == nil {
		panic(fmt.Errorf("missing column?"))
	}

	return col.(*m.Col).Data()
}

func (s *Col) SQL() any {
	if s.sql != "" {
		return s.sql
	}

	return s.name
}

func (s *Col) DataType() d.DataTypes {
	return s.dType
}

func (s *Col) Context() *d.Context {
	return s.ctx
}

func (s *Col) Len() int {
	var (
		n  int
		ex error
	)
	if n, ex = s.Context().Dialect().RowCount(s.MakeQuery()); ex != nil {
		panic(ex)
	}

	return n
}

func (s *Col) MakeQuery() string {
	if s.Context().Self() == nil {
		panic("oh no")
	}

	df := s.Context().Self().(*DF)

	field := s.Name("")
	// give it a random name if it does not have one
	if field == "" || (field != "" && !d.Has(field, "", df.ColumnNames()...)) {
		field = s.SQL().(string)
	}

	field, _ = s.Context().Dialect().CastField(field, s.DataType(), s.DataType())
	deps := s.Dependencies()

	//	field := s.Name("")
	//	if fn := s.SQL().(string); fn != "abcd" {
	// Need to Cast to required type here o.w. DB may default to an unsupported type
	//		field, _ = s.Context().Dialect().CastField(fn, s.DataType(), s.DataType())
	//	}
	w := d.RandomLetters(4)
	t := df.MakeQuery(deps...)
	_ = t
	qry := fmt.Sprintf("WITH %s AS (%s) SELECT %s AS %s FROM %s", w, df.MakeQuery(deps...), field, s.Name(""), w)

	return qry

}

func (s *Col) Name(renameTo string) string {
	if renameTo != "" {
		s.name = renameTo
	}

	return s.name
}

func (s *Col) RawType() d.DataTypes {
	return s.rawType
}

func (s *Col) Replace(indicator, replacement d.Column) (d.Column, error) {
	panicer(indicator, replacement)

	if !sameSource(s, indicator) || !sameSource(s, replacement) {
		return nil, fmt.Errorf("columns not from same DF in Replace")
	}

	if s.DataType() != replacement.DataType() {
		return nil, fmt.Errorf("incompatible columns in Replace")
	}

	// TODO: why unused?
	//tmpDF, _ := NewDFcol(nil, nil, s.ctx, indicator.(*Col), replacement.(*Col), s)

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("indicator not type DTint in Replace")
	}

	whens := []string{fmt.Sprintf("%s > 0", indicator.Name("")),
		fmt.Sprintf("%s <= 0", indicator.Name(""))}
	equalTo := []string{replacement.Name(""), s.Name("")}

	var (
		sqlx string
		e    error
	)
	if sqlx, e = s.Context().Dialect().Case(whens, equalTo); e != nil {
		return nil, e
	}
	outCol := NewColSQL("", s.Context(), s.DataType(), sqlx)

	return outCol, nil
}

func (s *Col) SetContext(ctx *d.Context) {
	s.ctx = ctx
}

func (s *Col) String() string {
	if s.Name("") == "" {
		panic("column has no name")
	}

	t := fmt.Sprintf("column: %s\ntype: %s\n", s.Name(""), s.DataType())

	if s.CategoryMap() != nil {
		var keys []string
		var vals []int
		for k, v := range s.CategoryMap() {
			if k == nil {
				k = "Other"
			}
			x, _ := d.ToString(k, true)

			keys = append(keys, x.(string))
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + d.PrettyPrint(header, keys, vals) + "\n"
	}

	if s.DataType() != d.DTfloat {
		ctx := d.NewContext(s.Context().Dialect(), nil, nil)
		_ = ctx
		df, ex := NewDFcol(nil, nil, s.ctx, s)
		_ = ex
		tab, _ := df.Table(false, s.Name(""))

		var (
			vals *m.DF
			e    error
		)
		if vals, e = m.DBLoad(tab.MakeQuery(), tab.Context().Dialect()); e != nil {
			panic(e)
		}

		l := vals.Column(s.Name(""))
		c := vals.Column("count")

		header := []string{l.Name(""), c.Name("")}
		return t + d.PrettyPrint(header, l.(*m.Col).Data(), c.(*m.Col).Data())
	}

	cols := []string{"min", "lq", "median", "mean", "uq", "max", "n"}

	header := []string{"metric", "value"}
	vals, _ := s.Context().Dialect().Summary(s.MakeQuery(), s.Name(""))
	return t + d.PrettyPrint(header, cols, vals)
}

func (s *Col) Dependencies() []string {
	return s.dependencies
}

func (s *Col) SetDependencies(dep []string) {
	s.dependencies = dep
}

// ***************** Helpers *****************

func panicer(cols ...d.Column) {
	for _, c := range cols {
		if _, ok := c.(*Col); !ok {
			panic("non sql.*Col argument")
		}
	}
}

func sameSource(s1, s2 any) bool {
	sql1, sql2 := "No", "Match"
	if df1, ok := s1.(*DF); ok {
		sql1 = df1.SourceSQL()
	}

	if c1, ok := s1.(*Col); ok {
		sql1 = c1.Context().Self().(*DF).SourceSQL()
	}

	if df2, ok := s2.(*DF); ok {
		sql2 = df2.SourceSQL()
	}

	if c2, ok := s2.(*Col); ok {
		sql2 = c2.Context().Self().(*DF).SourceSQL()
	}

	return sql1 == sql2
}
