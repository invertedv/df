package sql

import (
	"database/sql"
	"fmt"
	"iter"
	"maps"
	"strings"

	d "github.com/invertedv/df"

	m "github.com/invertedv/df/mem"
)

// StandardFunctions returns the built-in functions for in-memory data to be used by Parser.
func StandardFunctions(dlct *d.Dialect) d.Fns {
	fns := d.Fns{applyCat, global, toCat} //, varying("greatest", "greatest")}
	fns = append(fns, fnDefs(dlct)...)

	return fns
}

// DF is the implementation of DF for SQL.
type DF struct {
	sourceSQL string // source SQL used to query DB

	orderBy string
	where   string
	groupBy string

	*d.DFcore
}

// ***************** DF - Create *****************

// NewDF creates a *DF from input.
//
// if input is a *DF, a copy is returned.
// Otherwise, NewDF saves the data to a temp table and returns a *DF based on that.
func NewDF(dlct *d.Dialect, input d.HasIter, opts ...d.DFopt) (*DF, error) {
	switch inp := input.(type) {
	case *DF:
		if dlct.DialectName() != inp.Dialect().DialectName() {
			return nil, fmt.Errorf("conflicting dialects in NewDF")
		}

		return inp.Copy().(*DF), nil
	case d.HasIter:
		if dlct == nil {
			return nil, fmt.Errorf("missing dialect sql NewDF")
		}

		tn := dlct.WithName()
		if e := dlct.Save(tn, "", true, true, inp); e != nil {
			return nil, e
		}

		qry := fmt.Sprintf("SELECT * FROM %s", tn)
		return DBload(qry, dlct, opts...)
	default:
		return nil, fmt.Errorf("unsupported input to sql NewDF")
	}
}

// NewDFseq creates a *DF with a single column, "seq". That column is a DTint sequence
// from 0 to n-1.
func NewDFseq(dlct *d.Dialect, n int, name string, opts ...d.DFopt) (*DF, error) {
	seqSQL := fmt.Sprintf("SELECT %s AS seq", dlct.Seq(n))

	var (
		cc *d.ColCore
		e  error
	)
	if cc, e = d.NewColCore(d.ColDataType(d.DTint), d.ColName(name)); e != nil {
		return nil, e
	}

	col := &Col{
		sql:     "",
		ColCore: cc,
	}

	dfc, ex := d.NewDFcore([]d.Column{col})
	if ex != nil {
		panic(ex)
	}

	df := &DF{
		sourceSQL: seqSQL,
		orderBy:   "",
		where:     "",
		groupBy:   "",
		DFcore:    dfc,
	}

	for _, opt := range opts {
		if ex := opt(df); ex != nil {
			return nil, ex
		}
	}

	if df.Fns() == nil {
		_ = d.DFsetFns(StandardFunctions(dlct))(df)
	}

	_ = d.DFdialect(dlct)(df)

	if ey := df.SetParent(); ey != nil {
		return nil, ey
	}

	return df, nil
}

// DBload creates a *DF from a query. Note: the data is not loaded to memory.
func DBload(query string, dlct *d.Dialect, opts ...d.DFopt) (*DF, error) {
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

	for ind := range len(colTypes) {
		var (
			cc *d.ColCore
			e1 error
		)
		if cc, e1 = d.NewColCore(d.ColDataType(colTypes[ind]), d.ColName(colNames[ind])); e1 != nil {
			return nil, e1
		}

		sqlCol := &Col{
			sql:     "",
			ColCore: cc,
		}

		cols = append(cols, sqlCol)
	}

	var tmp *d.DFcore
	if tmp, e = d.NewDFcore(cols); e != nil {
		return nil, e
	}
	df.DFcore = tmp

	_ = d.DFdialect(dlct)(df)

	for _, opt := range opts {
		if ex := opt(df); ex != nil {
			return nil, ex
		}
	}

	if df.Fns() == nil {
		_ = d.DFsetFns(StandardFunctions(dlct))(df)
	}

	if ex := df.SetParent(); ex != nil {
		return nil, ex
	}

	return df, nil
}

// ***************** DF - Methods *****************

func (f *DF) AllRows() iter.Seq2[int, []any] {
	return func(yield func(int, []any) bool) {

		qry := f.MakeQuery()
		var (
			rows     *sql.Rows
			row2Read []any
			e        error
		)
		if rows, row2Read, _, e = f.Dialect().Rows(qry); e != nil {
			panic(e)
		}
		defer func() {
			_ = rows.Close()
		}()

		rowNum := 0
		for rows.Next() {
			if ex := rows.Scan(row2Read...); ex != nil {
				_ = rows.Close()
				return
			}

			// f.row elements are pointers to interface, remove the "pointer" part
			row := make([]any, len(row2Read))
			for ind, x := range row2Read {
				var z any = *x.(*any)
				row[ind] = f.Dialect().Convert(z)
			}

			if !yield(rowNum, row) {
				return
			}

			rowNum++
		}
	}
}

// AppendColumn makses the DFcore version to check that f and col
// come from the same source.
func (f *DF) AppendColumn(col d.Column, replace bool) error {
	// toCol allows us to append constants
	colx := toCol(f, col)

	if !sameSource(f, colx) {
		return fmt.Errorf("added column not from same source")
	}

	return f.Core().AppendColumn(colx, replace)
}

func (f *DF) AppendDF(dfNew d.DF) (d.DF, error) {
	if _, ok := dfNew.(*DF); !ok {
		return nil, fmt.Errorf("must be *sql.DF to join")
	}

	n1 := f.ColumnNames()

	if len(n1) != len(dfNew.ColumnNames()) {
		return nil, fmt.Errorf("dataframes cannot be appended")
	}

	for c := range f.AllColumns() {
		var cNew d.Column
		if cNew = dfNew.Column(c.Name()); cNew == nil {
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

// By creates a new *DF with function fns calculated within the groups defined by groupBy.
//
//	groupBy - comma-separated list of fields to group on.  If groupBy is empty, then the output will have 1 row.
//	fns     - functions to calculate on the By groups.
func (f *DF) By(groupBy string, fns ...string) (d.DF, error) {
	dfOut := f.Copy().(*DF)

	if groupBy == "" {
		groupBy = f.Dialect().WithName()
		if e2 := d.Parse(dfOut, fmt.Sprintf("%s := 1", groupBy)); e2 != nil {
			return nil, e2
		}
	}

	flds := strings.Split(groupBy, ",")
	if e := dfOut.KeepColumns(flds...); e != nil {
		return nil, e
	}
	_ = d.DFsetSourceDF(f)(dfOut)

	dfOut.groupBy = groupBy

	for _, fn := range fns {
		if e1 := d.Parse(dfOut, fn); e1 != nil {
			return nil, e1
		}

	}

	if e := dfOut.SetParent(); e != nil {
		return nil, e
	}

	return dfOut, nil
}

// Categorical creates a categorical column
//
//	colName    - name of the source column
//	catMap     - optionally supply a category map of source value -> category level
//	fuzz       - if a source column value has counts < fuzz, then it is put in the 'other' category.
//	defaultVal - optional source column value for the 'other' category.
//	levels     - slice of source values to make categories from
func (f *DF) Categorical(colName string, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (d.Column, error) {
	var col d.Column
	if col = f.Column(colName); col == nil {
		return nil, fmt.Errorf("column %s not found", col)
	}

	nextInt := 0 // next category level
	// find nextInt and make sure map keys are of the correct type.
	for k, v := range catMap {
		if k != nil && d.WhatAmI(k) != col.DataType() {
			return nil, fmt.Errorf("map and column not same data types")
		}

		if v >= nextInt {
			nextInt = v + 1
		}
	}

	// toMap is the output map
	toMap := make(d.CategoryMap)
	maps.Copy(toMap, catMap)

	// add default value if it's not there
	if _, ok := toMap[defaultVal]; !ok {
		toMap[defaultVal] = -1

	}

	cn := col.Name()
	colSQL, _ := col.(*Col).SQL()
	var (
		tabl d.DF
		e4   error
	)
	if tabl, e4 = f.Table(cn); e4 != nil {
		return nil, e4
	}

	if e5 := tabl.Sort(true, cn); e5 != nil {
		return nil, e5
	}

	x := tabl.(*DF).MakeQuery()
	var (
		mDF *m.DF
		e1  error
	)
	if mDF, e1 = m.DBload(x, f.Dialect()); e1 != nil {
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
	for ind := range inCol.Len() {
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

		cond := fmt.Sprintf("%s = %s", colSQL, f.Dialect().ToString(val))
		whens = append(whens, cond)
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

	outCol, _ := NewCol(d.DTcategorical, f.Dialect(), sql1)
	_ = d.ColRawType(col.DataType())(outCol.Core())
	_ = d.ColCatMap(toMap)(outCol.Core())

	return outCol, nil
}

func (f *DF) Column(colName string) d.Column {
	if colName == "" {
		return nil
	}

	if c := f.Core().Column(colName); c != nil {
		return c
	}

	if f.SourceDF() != nil {
		return f.SourceDF().Column(colName)
	}

	return nil
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
	_ = d.DFsetFns(f.Fns())(dfNew)

	_ = dfNew.SetParent()

	return dfNew
}

func (f *DF) DropColumns(colNames ...string) error {
	return f.Core().DropColumns(colNames...)
}

func (f *DF) GroupBy() string {
	return f.groupBy
}

// Interp interpolates the columns (xIfield,yfield) at xsField points.
//
//	points   - input iterator (e.g. Column or DF) that yields the points to interpolate at
//	xSfield  - column name of x values in source DF
//	xIfield  - name of x values in iDF
//	yfield   - column name of y values in source DF
//	outField - column name of interpolated y's in return DF
//
// The output DF is restricted to interpolated points that lie within the data.  It has columns:
//
//	xIfield  - points at which to interpolate. This may be a subset of the input "points".
//	outField - interpolated values.
func (f *DF) Interp(points d.HasIter, xSfield, xIfield, yfield, outField string) (d.DF, error) {
	var (
		idf *DF
		e1  error
	)

	if idf, e1 = NewDF(f.Dialect(), points, d.DFsetFns(f.Fns())); e1 != nil {
		return nil, e1
	}

	// if points isn't a d.DF or d.Column, then idf will have the column name "col"
	_, isDF := points.(d.DF)
	_, isCol := points.(d.Column)
	if !isDF && !isCol {
		col := idf.Column("col")
		if col == nil {
			return nil, fmt.Errorf("interp unexpected error")
		}

		_ = col.Rename(xIfield)
	}

	if c := f.Column(xSfield); c == nil || c.DataType() != d.DTfloat {
		return nil, fmt.Errorf("invalid source X in Interp")
	}

	if c := f.Column(yfield); c == nil || c.DataType() != d.DTfloat {
		return nil, fmt.Errorf("invalid source Y in Interp")
	}

	if c := idf.Column(xIfield); c == nil || c.DataType() != d.DTfloat {
		return nil, fmt.Errorf("invalid interp X in Interp")
	}

	var (
		favg d.DF
		eavg error
	)
	fld := f.Dialect().WithName()
	if favg, eavg = f.By(xSfield, fld+":=mean("+yfield+")"); eavg != nil {
		return nil, eavg
	}

	if es := favg.Sort(true, xSfield); es != nil {
		return nil, es
	}

	if es := idf.Sort(true, xIfield); es != nil {
		return nil, es
	}

	sQry := favg.(*DF).MakeQuery()
	iQry := idf.MakeQuery()

	qry := f.Dialect().Interp(sQry, iQry, xSfield, xIfield, fld, outField)

	var (
		df *DF
		e  error
	)
	if df, e = DBload(qry, f.Dialect(), d.DFsetFns(f.Fns())); e != nil {
		return nil, e
	}

	return df, nil
}

// Join joins f and df on the columns of joinOn. This is an inner join.
//
//	df - data to join.
//	joinOn - comma-separated list of fields to join on.  These fields must have the same name in both data sets.
func (f *DF) Join(df d.HasIter, joinOn string) (d.DF, error) {
	var (
		dfRight *DF
		e       error
	)

	if dfRight, e = NewDF(f.Dialect(), df, d.DFsetFns(f.Fns())); e != nil {
		return nil, fmt.Errorf("invalid input to Join")
	}

	jCols := strings.Split(strings.ReplaceAll(joinOn, " ", ""), ",")
	if !f.HasColumns(jCols...) || !dfRight.HasColumns(jCols...) {
		return nil, fmt.Errorf("missing some join columns")
	}

	leftNames, rightNames := f.ColumnNames(), dfRight.ColumnNames()

	var rNames []string
	for ind := range len(rightNames) {
		rn := rightNames[ind]
		// don't keep join columns for right
		if d.Has(rn, jCols) {
			continue
		}

		// rename any field names in right that are also in left
		if d.Has(rn, leftNames) {
			col := dfRight.Column(rn)
			rn += "DUP"
			_ = col.Rename(rn)
		}

		rNames = append(rNames, rn)
	}

	qry := f.Dialect().Join(f.MakeQuery(), dfRight.MakeQuery(), leftNames, rNames, jCols)

	var (
		outDF *DF
		e1    error
	)
	if outDF, e1 = DBload(qry, f.Dialect(), d.DFsetFns(f.Fns())); e1 != nil {
		return nil, e1
	}

	return outDF, nil
}

func (f *DF) MakeQuery(colNames ...string) string {
	var fields []string

	if len(colNames) == 0 {
		colNames = f.ColumnNames()
	}

	for ind := range len(colNames) {
		var cx d.Column
		if cx = f.Column(colNames[ind]); cx == nil {
			panic(fmt.Errorf("missing name %s", colNames[ind]))
		}

		var field string
		field = f.Dialect().ToName(cx.Name())
		if fn, isName := cx.(*Col).SQL(); !isName {
			field = fmt.Sprintf("%s AS %s", fn, f.Dialect().ToName(cx.Name()))
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

// RowCount returns # of rows in f
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

// SetParent sets the parent to f for all the columns in f.
func (f *DF) SetParent() error {
	for c := range f.AllColumns() {
		if e := d.ColParent(f)(c); e != nil {
			return e
		}

		_ = d.ColDialect(f.Dialect())(c)
	}

	return nil
}

// Sort sorts f according to sortCols.
// ascending - true = sort ascending
// sortCols - comma-separated list of columns to sort on.
func (f *DF) Sort(ascending bool, sortCols string) error {
	keys := strings.Split(strings.ReplaceAll(sortCols, " ", ""), ",")
	for _, k := range keys {
		if c := f.Column(k); c == nil {
			return fmt.Errorf("missing column %s", k)
		}
	}

	if !ascending {
		for ind := range len(keys) {
			keys[ind] += " DESC"
		}
	}

	f.orderBy = strings.Join(keys, ",")

	return nil
}

// SourceQuery returns the query used to create f.
func (f *DF) SourceSQL() string {
	return f.sourceSQL
}

// String produces a summary of f.
func (f *DF) String() string {
	const padLen = 5
	var (
		sc  [][]string
		cat string
	)

	for col := range f.AllColumns() {
		if col.DataType() == d.DTcategorical {
			cat += col.String()
			continue
		}

		sc = append(sc, d.StringSlice("", strings.Split(col.String(), "\n")))
	}

	out := fmt.Sprintf("Rows: %d\n", f.RowCount())
	pad := strings.Repeat(" ", padLen)
	for ind := 0; ind < len(sc); ind = ind + 3 {
		var s string
		for k := 0; k < len(sc[ind]); k++ {
			s += sc[ind][k] + pad
			if ind+1 < len(sc) {
				s += sc[ind+1][k] + pad
			}
			if ind+2 < len(sc) {
				s += sc[ind+2][k]
			}

			s += "\n"
		}
		out += s
	}

	return out + cat
}

// Table produces a table based on cols. cols is a comma-separated list of fields.
// The metrics within each group calculated are:
//
//	n    - count of rows
//	rate - fraction of original row count.
func (f *DF) Table(cols string) (d.DF, error) {
	var (
		dfOut d.DF
		e     error
	)

	c := strings.Split(strings.ReplaceAll(cols, " ", ""), ",")
	fn1 := fmt.Sprintf("count:=count(%s)", c[0])
	fn2 := fmt.Sprintf("rate:=float(count)/float(count(global(%s)))", c[0])
	if dfOut, e = f.By(cols, fn1, fn2); e != nil {
		return nil, e
	}

	if e1 := dfOut.Sort(false, "count"); e1 != nil {
		return nil, e
	}

	return dfOut, nil
}

// Where subsets f to rows where condition is true.
func (f *DF) Where(condition string) (d.DF, error) {
	if e := d.Parse(f, "wherec:="+condition); e != nil {
		return nil, e
	}

	col := f.Column("wherec")

	dfNew := f.Copy().(*DF)

	if col.DataType() != d.DTint {
		return nil, fmt.Errorf("where column must be type DTint")
	}

	wSQL, _ := col.(*Col).SQL()
	if dfNew.where != "" {
		dfNew.where = fmt.Sprintf("(%s) AND (%s > 0)", dfNew.where, wSQL)
	} else {
		dfNew.where = fmt.Sprintf("%s > 0", wSQL)
	}

	_ = dfNew.DropColumns("wherec")

	return dfNew, nil
}

// ***************** Helpers *****************

func sameSource(s1, s2 any) bool {
	sql1, sql2 := "No", "Match"
	grp1, grp2 := "", ""
	var (
		c1, c2   *Col
		df1, df2 *DF
	)

	if cx, ok := s1.(*Col); ok {
		c1 = cx
	}
	if dfx, ok := s1.(*DF); ok {
		df1 = dfx
		sql1 = df1.SourceSQL()
		grp1 = df1.groupBy
	}
	if cx, ok := s2.(*Col); ok {
		c2 = cx
	}
	if dfx, ok := s2.(*DF); ok {
		df2 = dfx
		sql2 = df2.SourceSQL()
		grp2 = df2.groupBy
	}

	if df1 != nil && c2 != nil && c2.Parent() == nil {
		_ = d.ColParent(df1)(c2)
	}

	if df2 != nil && c1 != nil && c1.Parent() == nil {
		_ = d.ColParent(df2)(c1)
	}
	if c1 != nil {
		sql1 = c1.Parent().(*DF).SourceSQL()
		grp1 = c1.Parent().(*DF).groupBy
	}

	if c2 != nil {
		sql2 = c2.Parent().(*DF).SourceSQL()
		grp2 = c2.Parent().(*DF).groupBy
	}

	return sql1 == sql2 && grp1 == grp2
}
