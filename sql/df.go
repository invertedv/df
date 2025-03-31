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

func StandardFunctions(dlct *d.Dialect) d.Fns {
	fns := d.Fns{applyCat, global, toCat}
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

func NewDF(dlct *d.Dialect, input d.HasIter, opts ...d.DFopt) (*DF, error) {
	switch inp := input.(type) {
	case *DF:
		return inp, nil
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

// TODO: unused...need?
func NewDFcolXXX(cols []*Col, opts ...d.DFopt) (*DF, error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDFcol")
	}

	for ind := 1; ind < len(cols); ind++ {
		if !sameSource(cols[ind-1], cols[ind]) {
			return nil, fmt.Errorf("incompatible columns in NewDFcol %s %s", cols[ind-1].Name(), cols[ind].Name())
		}
	}

	df := &DF{
		sourceSQL: cols[0].Parent().(*DF).MakeQuery(),
		orderBy:   "",
		where:     "",
		DFcore:    nil,
	}

	dlct := cols[0].Dialect()

	var (
		tmp *d.DFcore
		e   error
	)

	var colsx []d.Column
	for ind := range len(cols) {
		colsx = append(colsx, cols[ind])
	}
	if tmp, e = d.NewDFcore(colsx); e != nil {
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

func NewDFseq(dlct *d.Dialect, n int, opts ...d.DFopt) (*DF, error) {
	seqSQL := fmt.Sprintf("SELECT %s AS seq", dlct.Seq(n))

	var (
		cc *d.ColCore
		e  error
	)
	if cc, e = d.NewColCore(d.ColDataType(d.DTint), d.ColName("seq")); e != nil {
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

func (f *DF) Join(df d.DF, joinOn string) (d.DF, error) {
	if _, ok := df.(*DF); !ok {
		return nil, fmt.Errorf("must be *sql.DF to join")
	}

	jCols := strings.Split(strings.ReplaceAll(joinOn, " ", ""), ",")

	if !f.HasColumns(jCols...) || !df.HasColumns(jCols...) {
		return nil, fmt.Errorf("missing some join columns")
	}

	dfR := df.Copy()
	leftNames, rightNames := f.ColumnNames(), df.ColumnNames()

	var rNames []string
	for ind := range len(rightNames) {
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
	if outDF, e = DBload(qry, f.Dialect(), d.DFsetFns(f.Fns())); e != nil {
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

func (f *DF) By(groupBy string, fns ...string) (d.DF, error) {
	if groupBy == "" {
		return nil, fmt.Errorf("must have groupBy in DF.By")
	}

	flds := strings.Split(groupBy, ",")
	dfOut := f.Copy().(*DF)

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

	outCol, _ := NewColSQL(d.DTcategorical, f.Dialect(), sql1)
	_ = d.ColRawType(col.DataType())(outCol.Core())
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
	for c := range f.AllColumns() {
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
		for ind := range len(keys) {
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
