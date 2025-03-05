package sql

import (
	"database/sql"
	"fmt"
	"io"
	"maps"
	"strings"

	d "github.com/invertedv/df"

	m "github.com/invertedv/df/mem"
)

func StandardFunctions(dlct *d.Dialect) d.Fns {
	fns := d.Fns{applyCat,
		global,
		sortDF, table, toCat, where, by}
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

	rows *sql.Rows
	row  []any
}

// ***************** DF - Create *****************
func NewDFcol(funcs d.Fns, cols []*Col, opts ...d.DFopt) (*DF, error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDFcol")
	}

	for ind := 1; ind < len(cols); ind++ {
		if !sameSource(cols[ind-1], cols[ind]) {
			return nil, fmt.Errorf("incompatible columns in NewDFcol %s %s", cols[ind-1].Name(), cols[ind].Name())
		}
	}

	df := &DF{
		sourceSQL: cols[0].Parent().MakeQuery(),
		orderBy:   "",
		where:     "",
		DFcore:    nil,
	}

	dlct := cols[0].Dialect()
	if funcs == nil {
		funcs = StandardFunctions(dlct)
	}

	var (
		tmp *d.DFcore
		e   error
	)

	var colsx []d.Column
	for ind := 0; ind < len(cols); ind++ {
		colsx = append(colsx, cols[ind])
	}
	if tmp, e = d.NewDF(funcs, colsx); e != nil {
		return nil, e
	}

	df.DFcore = tmp

	_ = d.DFdialect(dlct)(df)

	for _, opt := range opts {
		if ex := opt(df); ex != nil {
			return nil, ex
		}
	}

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

func DBload(query string, dlct *d.Dialect, fns ...d.Fn) (*DF, error) {
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
	if fns == nil {
		fns = StandardFunctions(dlct)
	}
	if tmp, e = d.NewDF(fns, cols); e != nil {
		return nil, e
	}
	df.DFcore = tmp

	_ = d.DFdialect(dlct)(df)

	if ex := df.SetParent(); ex != nil {
		return nil, ex
	}

	return df, nil
}

// ***************** DF - Methods *****************
func (f *DF) Join(df d.DF, joinOn string) (d.DF, error) {
	jCols := strings.Split(strings.ReplaceAll(joinOn, " ", ""), ",")

	if !f.HasColumns(jCols...) || !df.HasColumns(jCols...) {
		return nil, fmt.Errorf("missing some join columns")
	}

	dfR := df.Copy()
	leftNames, rightNames := f.ColumnNames(), df.ColumnNames()

	var rNames []string
	for ind := 0; ind < len(rightNames); ind++ {
		rn := rightNames[ind]
		// don't keep join columns for right
		if d.Has(rn, jCols) {
			continue
		}

		// rename any field names in right that are also in left
		if d.Has(rn, leftNames) {
			col := dfR.Column(rn)
			rn += "DUP"
			_ = col.Rename(rn)
		}

		rNames = append(rNames, rn)
	}

	qry := f.Dialect().Join(f.MakeQuery(), dfR.(*DF).MakeQuery(), leftNames, rNames, jCols)

	var (
		outDF *DF
		e     error
	)
	if outDF, e = DBload(qry, f.Dialect(), f.Fns()...); e != nil {
		return nil, e
	}

	return outDF, nil
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

func (f *DF) AppendColumn(col d.Column, replace bool) error {
	// toCol allows us to append constants
	colx := toCol(f, col)

	if !sameSource(f, colx) {
		return fmt.Errorf("added column not from same source")
	}

	return f.Core().AppendColumn(colx, replace)
}

func (f *DF) AppendDF(dfNew d.DF) (d.DF, error) {
	n1 := f.ColumnNames()

	if len(n1) != len(dfNew.ColumnNames()) {
		return nil, fmt.Errorf("dataframes cannot be appended")
	}

	for c := f.First(); c != nil; c = f.Next() {
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

func (f *DF) By(groupBy string, fns ...string) (d.DF, error) {
	if groupBy == "" {
		return nil, fmt.Errorf("must have groupBy in DF.By")
	}

	flds := strings.Split(groupBy, ",")
	dfOut := f.Copy().(*DF)

	var e error
	if dfOut.DFcore, e = f.KeepColumns(flds...); e != nil {
		return nil, e
	}
	_ = d.DFsetSourceDF(f)(dfOut)

	dfOut.groupBy = groupBy

	for ind, fn := range fns {
		var (
			out *d.Parsed
			e1  error
		)
		if out, e1 = d.Parse(dfOut, fn); e1 != nil {
			return nil, e1
		}

		if out != nil {
			if e2 := d.ColName(fmt.Sprintf("c%d", ind))(out.Column()); e2 != nil {
				return nil, e2
			}

			if e2 := dfOut.AppendColumn(out.Column(), false); e2 != nil {
				return nil, e2
			}
		}
	}

	if e := dfOut.SetParent(); e != nil {
		return nil, e
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

	// f.row elements are pointers to interface, remove the "pointer" part
	newRow := make([]any, len(f.row))
	for ind, x := range f.row {
		var z any = *x.(*any)
		newRow[ind] = z
	}

	return newRow, nil
}

func (f *DF) MakeQuery(colNames ...string) string {
	var fields []string

	if colNames == nil {
		colNames = f.ColumnNames()
	}

	for ind := 0; ind < len(colNames); ind++ {
		var cx d.Column
		if cx = f.Column(colNames[ind]); cx == nil {
			panic(fmt.Errorf("missing name %s", colNames[ind]))
		}

		var field string
		field = f.Dialect().ToName(cx.Name())
		//		if fn := cx.(*Col).SQL(); fn != cx.Name() {
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

func (f *DF) Table(cols ...string) (d.DF, error) {
	var (
		dfOut d.DF
		e     error
	)

	fn1 := fmt.Sprintf("count:=count(%s)", cols[0])
	fn2 := fmt.Sprintf("rate:=float(count)/float(count(global(%s)))", cols[0])
	if dfOut, e = f.By(strings.Join(cols, ","), fn1, fn2); e != nil {
		return nil, e
	}

	if e1 := dfOut.Sort(false, "count"); e1 != nil {
		return nil, e
	}

	return dfOut, nil
}

func (f *DF) Where(col d.Column) (d.DF, error) {
	panicer(col)
	if col == nil {
		return nil, fmt.Errorf("where column is nil")
	}

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

func panicer(cols ...d.Column) {
	for _, c := range cols {
		if _, ok := c.(*Col); !ok {
			panic("non sql.*Col argument")
		}
	}
}
