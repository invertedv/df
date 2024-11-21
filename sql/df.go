package sql

// TODO: change SQL() to return string

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
	sql string // SQL to generate this column

	//	signature string // unique 4-character signature to identify this data source
	//	version   int    // version of the dataframe that existed when this column was added

	//	catMap    d.CategoryMap
	//	catCounts d.CategoryMap
	//	rawType   d.DataTypes

	scalarValue any // This is for keeping the actual value of constants rather than SQL version

	*d.ColCore
}

// ***************** DF - Create *****************

func NewDFcol(runDF d.RunFn, funcs d.Fns, context *d.Context, cols ...*Col) (*DF, error) {
	for ind := 1; ind < len(cols); ind++ {
		if !sameSource(cols[ind-1], cols[ind]) {
			return nil, fmt.Errorf("incompatible columns in NewDFcol %s %s", cols[ind-1].Name(), cols[ind].Name())
		}
	}

	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDFcol")
	}

	if runDF == nil {
		runDF = RunDFfn
	}

	if funcs == nil {
		funcs = StandardFunctions()
	}

	for ind := 0; ind < len(cols); ind++ {
		d.ColContext(context)(cols[ind].ColCore)
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
		sql:         "",
		scalarValue: nil,
		ColCore:     d.NewColCore(d.DTint, d.ColRename("seq")),
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
	if colNames, colTypes, _, e = context.Dialect().Types(query); e != nil {
		return nil, e
	}

	df := &DF{
		sourceSQL: query,
	}

	for ind := 0; ind < len(colTypes); ind++ {
		sqlCol := &Col{
			sql:     "",
			ColCore: d.NewColCore(colTypes[ind], d.ColRename(colNames[ind]), d.ColContext(context)),
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

func (f *DF) AppendColumn(col d.Column, replace bool) error {
	panicer(col)

	if !sameSource(f, col) {
		return fmt.Errorf("added column not from same source")
	}

	return f.Core().AppendColumn(col, replace)
}

func (f *DF) AppendDF(dfNew d.DF) (d.DF, error) {
	n1 := f.ColumnNames()

	if len(n1) != len(dfNew.ColumnNames()) {
		return nil, fmt.Errorf("dataframes cannot be appended")
	}

	for c := f.Next(true); c != nil; c = f.Next(false) {
		var cNew d.Column
		if cNew = dfNew.Column(c.Name()); c == nil {
			return nil, fmt.Errorf("missing column %s in AppendDF", c.Name())
		}

		if c.DataType() != cNew.DataType() {
			return nil, fmt.Errorf("column %s has differing data types in AppendDF", c.Name())
		}
	}

	var (
		sqlx string
		e    error
	)
	if sqlx, e = f.Context().Dialect().Union(f.MakeQuery(), dfNew.(*DF).MakeQuery(), n1...); e != nil {
		return nil, e
	}

	var (
		dfOut *DF
		eOut  error
	)
	ctx := d.NewContext(f.Context().Dialect(), nil, nil)

	if dfOut, eOut = DBload(sqlx, ctx); eOut != nil {
		return nil, eOut
	}

	dfOut.Context().SetSelf(dfOut)

	return dfOut, nil
}

func (f *DF) Categorical(colName string, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (d.Column, error) {
	var col d.Column
	if col = f.Column(colName); col == nil {
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

	cn := col.Name()
	var (
		tabl d.DF
		e4   error
	)
	if tabl, e4 = f.Table(true, cn); e4 != nil {
		return nil, e4
	}

	x := tabl.(*DF).MakeQuery()
	var (
		mDF *m.DF
		e1  error
	)
	if mDF, e1 = m.DBLoad(x, f.Context().Dialect()); e1 != nil {
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

		whens = append(whens, fmt.Sprintf("%s = %s", cn, f.Context().Dialect().ToString(val)))
		equalTo = append(equalTo, fmt.Sprintf("%d", outVal))
		if outVal == caseNo {
			caseNo++
		}
	}

	// o.w. the result is nullable
	whens[len(whens)-1] = "ELSE"

	var (
		sql1 string
		ex   error
	)
	if sql1, ex = f.Context().Dialect().Case(whens, equalTo); ex != nil {
		return nil, ex
	}

	outCol := NewColSQL("", f.Context(), d.DTcategorical, sql1)
	d.ColRawType(col.DataType())(outCol.ColCore)
	d.ColCatCounts(cnts)(outCol.ColCore)
	d.ColCatMap(toMap)(outCol.ColCore)

	return outCol, nil
}

func (f *DF) Copy() d.DF {
	dfCore := f.Core().Copy()
	dfNew := &DF{
		sourceSQL: f.sourceSQL,
		orderBy:   f.orderBy,
		groupBy:   f.groupBy,
		where:     f.where,
		DFcore:    dfCore,
	}

	ctx := d.NewContext(f.Context().Dialect(), dfNew)
	dfNew.SetContext(ctx)
	return dfNew
}

// TODO: check
func (f *DF) DropColumns(colNames ...string) error {
	return f.Core().DropColumns(colNames...)
}

func (f *DF) Iter(reset bool) (row []any, err error) {
	if reset {
		qry := f.MakeQuery()
		var e error
		f.rows, f.row, _, e = f.Context().Dialect().Rows(qry)
		if e != nil {
			_ = f.rows.Close()
			return nil, e
		}
	}

	if ok := f.rows.Next(); !ok {
		return nil, io.EOF
	}

	if ex := f.rows.Scan(f.row...); ex != nil {
		_ = f.rows.Close()
		return nil, io.EOF
	}

	return f.row, nil
}

func (f *DF) MakeQuery(colNames ...string) string {
	var fields []string

	if colNames == nil {
		colNames = f.ColumnNames()
	}

	for ind := 0; ind < len(colNames); ind++ {
		var cx d.Column
		if cx = f.Column(colNames[ind]); cx == nil {
			panic(fmt.Errorf("missing name %s", cx.Name()))
		}

		var field string
		field = cx.Name()
		if fn := cx.(*Col).SQL().(string); fn != "" {
			field = fmt.Sprintf("%s AS %s", fn, cx.Name())
		}

		fields = append(fields, field)
	}

	with := d.RandomLetters(4)
	qry := fmt.Sprintf("WITH %s AS (%s) SELECT\n%s FROM %s", with, f.sourceSQL, strings.Join(fields, ",\n"), with)
	if f.where != "" {
		qry = fmt.Sprintf("%s WHERE %s\n", qry, f.where)
	}

	if f.groupBy != "" {
		qry = fmt.Sprintf("%s GROUP BY %s\n", qry, f.groupBy)
	}

	if f.orderBy != "" {
		qry = fmt.Sprintf("%s ORDER BY %s\n", qry, f.orderBy)
	}

	return qry
}

func (f *DF) RowCount() int {
	var (
		rowCount int
		e        error
	)
	if rowCount, e = f.Context().Dialect().RowCount(f.MakeQuery()); e != nil {
		panic(e)
	}

	return rowCount
}

func (f *DF) Sort(ascending bool, keys ...string) error {
	for _, k := range keys {
		if c := f.Column(k); c == nil {
			return fmt.Errorf("missing column %s", k)
		}
	}

	if !ascending {
		for ind := 0; ind < len(keys); ind++ {
			keys[ind] += " DESC"
		}
	}

	f.orderBy = strings.Join(keys, ",")

	return nil
}

func (f *DF) SourceSQL() string {
	return f.sourceSQL
}

func (f *DF) String() string {
	var sx string
	for c := f.Next(true); c != nil; c = f.Next(false) {
		sx += c.String() + "\n"
	}

	return sx
}

func (f *DF) Table(sortByRows bool, cols ...string) (d.DF, error) {
	var names []string

	for ind := 0; ind < len(cols); ind++ {
		var c d.Column
		if c = f.Column(cols[ind]); c == nil {
			return nil, fmt.Errorf("missing column %s", cols[ind])
		}

		dt := c.DataType()
		if dt != d.DTstring && dt != d.DTint && dt != d.DTdate && dt != d.DTcategorical {
			return nil, fmt.Errorf("cannot make table with type float")
		}

		names = append(names, c.Name())
	}

	// this requires a cast o.w. it's nullable
	var (
		cf string
		ex error
	)
	if cf, ex = f.Context().Dialect().CastField("count(*) / (SELECT count(*) FROM (%s))", d.DTfloat, d.DTfloat); ex != nil {
		return nil, ex
	}

	count := NewColSQL("count", f.Context(), d.DTint, "count(*)")

	rateSQL := fmt.Sprintf(cf, f.MakeQuery())
	rate := NewColSQL("rate", f.Context(), d.DTfloat, rateSQL)

	var (
		dfc *d.DFcore
		e   error
	)
	if dfc, e = f.KeepColumns(names...); e != nil {
		return nil, e
	}

	outDF := &DF{
		sourceSQL: f.sourceSQL,
		orderBy:   "",
		where:     f.where,
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

	ctx := d.NewContext(f.Context().Dialect(), outDF)
	outDF.SetContext(ctx)

	return outDF, nil
}

func (f *DF) Where(col d.Column) (d.DF, error) {
	panicer(col)
	if col == nil {
		return nil, fmt.Errorf("where column is nil")
	}

	// TODO: this should update self + columns
	dfNew := f.Copy().(*DF)

	if col.DataType() != d.DTint {
		return nil, fmt.Errorf("where column must be type DTint")
	}

	dfNew.where = fmt.Sprintf("%s > 0", col.(*Col).SQL().(string))
	if dfNew.where != "" {
		dfNew.where = fmt.Sprintf("(%s) AND (%s > 0)", dfNew.where, col.(*Col).SQL().(string))
	}

	return dfNew, nil
}

// ***************** Col - Create *****************

func NewColSQL(name string, context *d.Context, dt d.DataTypes, sqlx string) *Col {
	col := &Col{
		sql:     sqlx,
		ColCore: d.NewColCore(dt, d.ColRename(name), d.ColContext(context)),
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
		sql:     sqlx,
		ColCore: d.NewColCore(dt, d.ColRename(name)),
	}

	return col, nil
}

// ***************** SQLCol - Methods *****************

// TODO: test this, doesn't look right
func (c *Col) AppendRows(col d.Column) (d.Column, error) {
	panicer(col)
	if c.DataType() != col.DataType() {
		return nil, fmt.Errorf("incompatible columns in AppendRows")
	}
	q1 := c.MakeQuery()
	cx := col.Copy()
	cx.Rename(c.Name())
	q2 := c.MakeQuery()

	var (
		source string
		e      error
	)
	if source, e = c.Context().Dialect().Union(q1, q2, cx.Name()); e != nil {
		return nil, e
	}
	_ = source
	outCol := &Col{
		sql:     "",
		ColCore: d.NewColCore(c.DataType(), d.ColRename(c.Name()), d.ColContext(c.Context())),
	}

	return outCol, nil
}

//func (c *Col) CategoryMap() d.CategoryMap {
//	return c.catMap
//}

func (c *Col) Copy() d.Column {
	n := &Col{
		sql:         c.sql,
		scalarValue: c.scalarValue,
		ColCore:     c.ColCore,
	}

	return n
}

func (c *Col) Data() any {
	var (
		df *m.DF
		e  error
	)

	// give it a random name if it does not have one
	if c.Name() == "" {
		c.Rename(d.RandomLetters(5))
		d.ColRename(d.RandomLetters(5))(c.ColCore)
	}

	if df, e = m.DBLoad(c.MakeQuery(), c.Context().Dialect()); e != nil {
		panic(e)
	}

	var col d.Column
	if col = df.Column(c.Name()); col == nil {
		panic(fmt.Errorf("missing column?"))
	}

	return col.(*m.Col).Data()
}

func (c *Col) SQL() any {
	if c.sql != "" {
		return c.sql
	}

	return c.Name()
}

func (c *Col) Len() int {
	var (
		n  int
		ex error
	)
	if n, ex = c.Context().Dialect().RowCount(c.MakeQuery()); ex != nil {
		panic(ex)
	}

	return n
}

func (c *Col) MakeQuery() string {
	if c.Context().Self() == nil {
		panic("oh no")
	}

	df := c.Context().Self().(*DF)

	field := c.Name()
	if field == "" || (field != "" && !d.Has(field, "", df.ColumnNames()...)) {
		field = c.SQL().(string)
	}

	deps := c.Dependencies()

	w := d.RandomLetters(4)
	qry := fmt.Sprintf("WITH %s AS (%s) SELECT %s AS %s FROM %s", w, df.MakeQuery(deps...), field, c.Name(), w)

	return qry
}

//func (c *Col) Name() string {
//
//	return c.name
//}

//func (c *Col) RawType() d.DataTypes {
//	return c.rawType
//}

func (c *Col) Replace(indicator, replacement d.Column) (d.Column, error) {
	panicer(indicator, replacement)

	if !sameSource(c, indicator) || !sameSource(c, replacement) {
		return nil, fmt.Errorf("columns not from same DF in Replace")
	}

	if c.DataType() != replacement.DataType() {
		return nil, fmt.Errorf("incompatible columns in Replace")
	}

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("indicator not type DTint in Replace")
	}

	whens := []string{fmt.Sprintf("%s > 0", indicator.Name()), "ELSE"}
	equalTo := []string{replacement.Name(), c.Name()}

	var (
		sqlx string
		e    error
	)
	if sqlx, e = c.Context().Dialect().Case(whens, equalTo); e != nil {
		return nil, e
	}
	outCol := NewColSQL("", c.Context(), c.DataType(), sqlx)

	return outCol, nil
}

func (c *Col) Rename(newName string) {
	if !d.ValidName(c.Name()) {
		panic(fmt.Errorf("illegal name: %s", c.Name()))
	}

	d.ColRename(newName)(c.ColCore)
}

// TODO: delete this
func (c *Col) SetContext(ctx *d.Context) {
	d.ColContext(ctx)(c.ColCore)
}

func (c *Col) String() string {
	if c.Name() == "" {
		panic("column has no name")
	}

	t := fmt.Sprintf("column: %s\ntype: %s\n", c.Name(), c.DataType())

	if c.CategoryMap() != nil {
		var keys []string
		var vals []int
		for k, v := range c.CategoryMap() {
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

	if c.DataType() != d.DTfloat {
		df, ex := NewDFcol(nil, nil, c.Context(), c)
		_ = ex
		tab, _ := df.Table(false, c.Name())

		var (
			vals *m.DF
			e    error
		)
		if vals, e = m.DBLoad(tab.MakeQuery(), tab.Context().Dialect()); e != nil {
			panic(e)
		}

		l := vals.Column(c.Name())
		c := vals.Column("count")

		header := []string{l.Name(), c.Name()}
		return t + d.PrettyPrint(header, l.(*m.Col).Data(), c.(*m.Col).Data())
	}

	cols := []string{"min", "lq", "median", "mean", "uq", "max", "n"}

	header := []string{"metric", "value"}
	vals, _ := c.Context().Dialect().Summary(c.MakeQuery(), c.Name())
	return t + d.PrettyPrint(header, cols, vals)
}

func (c *Col) SetDependencies(dep []string) {
	d.ColSetDependencies(dep)(c.ColCore)
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
