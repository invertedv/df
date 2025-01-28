package sql

import (
	"database/sql"
	"fmt"
	"io"
	"maps"
	"strings"

	d "github.com/invertedv/df"

	// TODO: change SQL() to return string

	m "github.com/invertedv/df/mem"
)

func StandardFunctions(dlct *d.Dialect) d.Fns {
	fns := d.Fns{applyCat,
		sortDF, table, toCat, where}
	fns = append(fns, fnDefs(dlct)...)

	return fns
}

// TODO: make mem work like this

// DF is the implementation of DF for SQL.
//
// signature is the unique identifier of this dataframe.  It is reset if
//   - a column is dropped
//
// version is the version number of this dataframe.  It is incremented if
//   - a column is added
type DF struct {
	sourceSQL string // source SQL used to query DB

	orderBy string
	where   string
	groupBy string

	*d.DFcore

	rows *sql.Rows
	row  []any
}

// ***************** DF - Create *****************

func NewDFcol(funcs d.Fns, dlct *d.Dialect, cols ...*Col) (*DF, error) {
	for ind := 1; ind < len(cols); ind++ {
		if !sameSource(cols[ind-1], cols[ind]) {
			return nil, fmt.Errorf("incompatible columns in NewDFcol %s %s", cols[ind-1].Name(), cols[ind].Name())
		}
	}

	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDFcol")
	}

	if funcs == nil {
		funcs = StandardFunctions(dlct)
	}

	// HERE
	//	for ind := 0; ind < len(cols); ind++ {
	//		d.ColContext(context)(cols[ind].Core())
	//	}
	// TODO: fix runs ??

	df := &DF{
		sourceSQL: cols[0].Parent().MakeQuery(), // TODO: check
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
	if tmp, e = d.NewDF(funcs, cstd); e != nil {
		return nil, e
	}

	df.DFcore = tmp

	_ = d.DFdialect(dlct)(df)

	if ex := df.SetParent(); ex != nil {
		return nil, ex
	}

	return df, nil
}

func NewDFseq(funcs d.Fns, dlct *d.Dialect, n int) (*DF, error) {
	if funcs == nil {
		funcs = StandardFunctions(dlct)
	}

	seqSQL := fmt.Sprintf("SELECT %s AS seq", dlct.Seq(n))

	var (
		cc *d.ColCore
		e  error
	)
	if cc, e = d.NewColCore(d.DTint, d.ColName("seq")); e != nil {
		return nil, e
	}

	col := &Col{
		sql:     "",
		ColCore: cc,
	}

	dfc, ex := d.NewDF(funcs, []d.Column{col})
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

	_ = d.DFdialect(dlct)(df)

	if ey := df.SetParent(); ey != nil {
		return nil, ey
	}

	return df, nil
}

// TODO: needs runDF, fns as parameters...
func DBload(query string, dlct *d.Dialect) (*DF, error) {
	var (
		e        error
		colTypes []d.DataTypes
		colNames []string
		cols     []d.Column
	)

	if colNames, colTypes, _, e = dlct.Types(query); e != nil {
		return nil, e
	}

	df := &DF{
		sourceSQL: query,
	}

	for ind := 0; ind < len(colTypes); ind++ {
		var (
			cc *d.ColCore
			e1 error
		)
		if cc, e1 = d.NewColCore(colTypes[ind], d.ColName(colNames[ind])); e1 != nil {
			return nil, e1
		}

		sqlCol := &Col{
			sql:     "",
			ColCore: cc,
		}

		cols = append(cols, sqlCol)
	}

	var tmp *d.DFcore
	// TODO: fix runs
	if tmp, e = d.NewDF(StandardFunctions(dlct), cols); e != nil {
		return nil, e
	}
	// TODO: think about: should SetContext copy context?
	df.DFcore = tmp

	_ = d.DFdialect(dlct)(df)

	if ex := df.SetParent(); ex != nil {
		return nil, ex
	}

	return df, nil
}

// ***************** DF - Methods *****************

func (f *DF) AppendColumn(col d.Column, replace bool) error {
	panicer(col)

	if !sameSource(f, col) {
		return fmt.Errorf("added column not from same source")
	}

	if f.RowCount() != col.Len() {
		return fmt.Errorf("added column has differing # of rows")
	}

	return f.Core().AppendColumn(col, replace)
}

// TODO: think about this and ways it could fail
func (f *DF) AppendDF(dfNew d.DF) (d.DF, error) {
	n1 := f.ColumnNames()

	if len(n1) != len(dfNew.ColumnNames()) {
		return nil, fmt.Errorf("dataframes cannot be appended")
	}

	for c := f.First(); c != nil; c = f.Next() {
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
	if sqlx, e = f.Dialect().Union(f.MakeQuery(), dfNew.(*DF).MakeQuery(), n1...); e != nil {
		return nil, e
	}

	var (
		dfOut *DF
		eOut  error
	)
	if dfOut, eOut = DBload(sqlx, f.Dialect()); eOut != nil {
		return nil, eOut
	}

	_ = d.DFdialect(f.Dialect())(dfOut)

	if ex := dfOut.SetParent(); ex != nil {
		return nil, ex
	}

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
	if mDF, e1 = m.DBLoad(x, f.Dialect()); e1 != nil {
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

		if levels != nil && !d.Has(val, levels) {
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

		whens = append(whens, fmt.Sprintf("%s = %s", cn, f.Dialect().ToString(val)))
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
	if sql1, ex = f.Dialect().Case(whens, equalTo); ex != nil {
		return nil, ex
	}

	outCol, _ := NewColSQL(d.DTcategorical, f.Dialect(), sql1)
	_ = d.ColRawType(col.DataType())(outCol.Core())
	_ = d.ColCatCounts(cnts)(outCol.Core())
	_ = d.ColCatMap(toMap)(outCol.Core())

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

	_ = dfNew.SetParent()

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
		f.rows, f.row, _, e = f.Dialect().Rows(qry)
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
		if fn := cx.(*Col).SQL(); fn != "" {
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
	if rowCount, e = f.Dialect().RowCount(f.MakeQuery()); e != nil {
		panic(e)
	}

	return rowCount
}

func (f *DF) SetParent() error {
	for c := f.First(); c != nil; c = f.Next() {
		if e := d.ColParent(f)(c); e != nil {
			return e
		}

		_ = d.ColDialect(f.Dialect())(c)
	}

	return nil
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
	for c := f.First(); c != nil; c = f.Next() {
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
	if cf, ex = f.Dialect().CastField("count(*) / (SELECT count(*) FROM (%s))", d.DTfloat, d.DTfloat); ex != nil {
		return nil, ex
	}

	count, _ := NewColSQL(d.DTint, f.Dialect(), "count(*)", d.ColName("count"))

	rateSQL := fmt.Sprintf(cf, f.MakeQuery())
	rate, _ := NewColSQL(d.DTfloat, f.Dialect(), rateSQL, d.ColName("rate"))

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

	_ = d.DFdialect(f.Dialect())(outDF)

	if ex := outDF.SetParent(); ex != nil {
		return nil, ex
	}

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

	dfNew.where = fmt.Sprintf("%s > 0", col.(*Col).SQL())
	if dfNew.where != "" {
		dfNew.where = fmt.Sprintf("(%s) AND (%s > 0)", dfNew.where, col.(*Col).SQL())
	}

	return dfNew, nil
}

// ***************** Helpers *****************

func sameSource(s1, s2 any) bool {
	sql1, sql2 := "No", "Match"
	if df1, ok := s1.(*DF); ok {
		sql1 = df1.SourceSQL()
	}

	if c1, ok := s1.(*Col); ok {
		sql1 = c1.Parent().(*DF).SourceSQL()
	}

	if df2, ok := s2.(*DF); ok {
		sql2 = df2.SourceSQL()
	}

	if c2, ok := s2.(*Col); ok {
		sql2 = c2.Parent().(*DF).SourceSQL()
	}

	return sql1 == sql2
}

func panicer(cols ...d.Column) {
	for _, c := range cols {
		if _, ok := c.(*Col); !ok {
			panic("non sql.*Col argument")
		}
	}
}
