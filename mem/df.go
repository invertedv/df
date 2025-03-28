package df

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"iter"
	"maps"
	"sort"
	"strings"
	"time"

	d "github.com/invertedv/df"
)

type DF struct {
	sourceQuery string
	orderBy     []*Col
	ascending   bool
	row         int

	*d.DFcore

	//	groupBy groups
}

type groups map[uint64]*groupVal

type groupVal struct {
	groupDF *DF // slice of original DF corresponding to this group

	row []any // values of the grouping fields
}

func StandardFunctions() d.Fns {
	fns := d.Fns{toCat, applyCat, global}
	fns = append(fns, vectorFunctions()...)

	return fns
}

func NewDF(input any, opts ...d.DFopt) (*DF, error) {
	var df *DF
	switch inp := input.(type) {
	case *DF:
		df = inp
		for _, opt := range opts {
			if e := opt(df); e != nil {
				return nil, e
			}
		}
	case d.Column:
		data := inp.Data()
		var (
			col    *Col
			ec, ed error
		)
		if col, ec = NewCol(data, d.ColName(inp.Name())); ec != nil {
			return nil, ec
		}

		if df, ed = NewDFcol([]*Col{col}, opts...); ed != nil {
			return nil, ed
		}
	case *d.Vector:
		var (
			col    *Col
			ec, ed error
		)
		if col, ec = NewCol(inp, d.ColName("col")); ec != nil {
			return nil, ec
		}

		if df, ed = NewDFcol([]*Col{col}, opts...); ed != nil {
			return nil, ed
		}
	case d.HasMQDlct:
		if inp, ok := input.(d.HasMQDlct); ok {
			var e error
			if df, e = DBload(inp.MakeQuery(), inp.Dialect(), opts...); e != nil {
				return nil, e
			}
		}
	default:
		return nil, fmt.Errorf("can't make *mem.DF from input in NewDf")
	}

	return df, nil
}

func NewDFcol(cols []*Col, opts ...d.DFopt) (*DF, error) {
	rowCount := cols[0].Len()
	var cc []d.Column
	for ind := range len(cols) {
		if rc := cols[ind].Len(); rc > 1 && rc != rowCount {
			return nil, fmt.Errorf("all MemCols must have same length")
		}

		cc = append(cc, cols[ind])
	}

	var (
		df *d.DFcore
		e  error
	)

	if df, e = d.NewDFcore(cc); e != nil {
		return nil, e
	}

	outDF := &DF{DFcore: df, row: -1}

	for _, opt := range opts {
		if ex := opt(outDF); ex != nil {
			return nil, ex
		}
	}

	if ex := outDF.SetParent(); ex != nil {
		return nil, ex
	}

	if outDF.Fns() == nil {
		_ = d.DFsetFns(StandardFunctions())(outDF)
	}

	return outDF, nil
}

func NewDFseq(n int, opts ...d.DFopt) (*DF, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be positive in NewDFseq")
	}

	data := make([]int, n)
	for ind := range n {
		data[ind] = ind
	}

	col, _ := NewCol(data, d.ColName("seq"))

	df, _ := NewDFcol([]*Col{col}, opts...)

	return df, nil
}

func DBload(qry string, dlct *d.Dialect, opts ...d.DFopt) (*DF, error) {
	var (
		columnNames []string
		columnTypes []d.DataTypes
		e           error
	)

	var memData []*d.Vector
	if memData, columnNames, columnTypes, e = dlct.Load(qry); e != nil {
		return nil, e
	}

	var memDF *DF
	for ind := range len(columnTypes) {
		var col *Col

		if col, e = NewCol(memData[ind], d.ColName(columnNames[ind])); e != nil {
			return nil, e
		}

		if ind == 0 {
			if memDF, e = NewDFcol([]*Col{col}, opts...); e != nil {
				return nil, e
			}

			continue
		}

		if ex := memDF.AppendColumn(col, false); ex != nil {
			return nil, ex
		}
	}

	memDF.sourceQuery = qry
	memDF.row = -1

	return memDF, nil
}

func FileLoad(f *d.Files, opts ...d.DFopt) (*DF, error) {
	var (
		memData []*d.Vector
		e       error
	)
	if memData, e = f.Load(); e != nil {
		return nil, e
	}

	var memDF *DF
	for ind := range len(f.FieldNames()) {
		var col *Col

		if col, e = NewCol(memData[ind], d.ColName(f.FieldNames()[ind])); e != nil {
			return nil, e
		}

		if ind == 0 {
			if memDF, e = NewDFcol([]*Col{col}, opts...); e != nil {
				return nil, e
			}

			continue
		}

		if ex := memDF.AppendColumn(col, false); ex != nil {
			return nil, ex
		}
	}

	memDF.row = -1

	return memDF, nil
}

// ***************** Methods *****************

// AppendColumn masks the DFcore version so that we can handle appending scalars
func (f *DF) AppendColumn(col d.Column, replace bool) error {
	if e := checkType(col); e != nil {
		return e
	}

	if f.RowCount() != col.Len() && col.Len() > 1 {
		return fmt.Errorf("unequal lengths in AppendColumn")
	}

	var (
		colx *Col
		val  any
	)
	if cx1, ok := col.(*Col); ok {
		colx = cx1
		val = colx.Data().Element(0)
	}

	// Is this a scalar or one-row Column?
	if cx2, ok := col.(*d.Scalar); ok || (col.Len() == 1 && f.RowCount() > 1) {
		if ok {
			val = cx2.Data().Element(0)
			// a double single quote means keep a single quote in the string
			if s, ok := val.(string); ok {
				val = strings.ReplaceAll(s, "''", "'")
			}
		}

		v := d.MakeVector(col.DataType(), f.RowCount())
		for ind := range v.Len() {
			v.SetAny(val, ind)
		}

		colx, _ = NewCol(v, d.ColName(col.Name()))
	}

	if colx == nil {
		return fmt.Errorf("bad column to append")
	}

	if ex := f.DFcore.AppendColumn(colx, replace); ex != nil {
		return ex
	}

	// need to wait til end to assign in case DFcore.AppendColumn needed to drop the column
	if e := d.ColParent(f)(colx); e != nil {
		return e
	}

	return nil
}

func (f *DF) AppendDF(df d.DF) (d.DF, error) {
	if _, ok := df.(*DF); !ok {
		return nil, fmt.Errorf("must be *DF to append to *DF")
	}

	if len(f.ColumnNames()) != len(df.ColumnNames()) {
		return nil, fmt.Errorf("cannot append dataframes - differing columns")
	}

	outDF := f.Copy()
	for _, cn := range f.ColumnNames() {
		cSource := outDF.Column(cn)
		var cAppend d.Column
		if cAppend = df.Column(cn); cAppend == nil {
			return nil, fmt.Errorf("missing column %s", cn)
		}

		if e := cSource.Data().AppendVector(cAppend.Data()); e != nil {
			return nil, e
		}
	}

	return outDF, nil
}

func (f *DF) By(groupBy string, fns ...string) (d.DF, error) {
	if groupBy == "" {
		return nil, fmt.Errorf("must have groupBy in DF.By")
	}

	if fns == nil {
		return nil, fmt.Errorf("must have at least on function in By")
	}

	flds := strings.Split(groupBy, ",")
	var gCol []*Col

	var outVecs []*d.Vector
	for ind := range len(flds) {
		var col d.Column
		cName := strings.ReplaceAll(flds[ind], " ", "")
		if col = f.Column(cName); col == nil {
			return nil, fmt.Errorf("missing column %s in By", cName)
		}

		gCol = append(gCol, col.(*Col))
		outVecs = append(outVecs, d.MakeVector(col.DataType(), 0))
	}

	var (
		grp groups
		e   error
	)
	if grp, e = buildGroups(f, gCol); e != nil {
		return nil, e
	}

	var left []string
	for ind := range len(fns) {
		lr := strings.Split(fns[ind], ":=")
		left = append(left, strings.ReplaceAll(lr[0], " ", ""))
	}

	for _, v := range grp {
		for ind := range len(fns) {
			// create group columns on first pass
			if ind == 0 {
				for ind1 := range len(gCol) {
					if e5 := outVecs[ind1].Append(v.row[ind1]); e5 != nil {
						return nil, e5
					}
				}
			}

			if e1 := d.Parse(v.groupDF, fns[ind]); e1 != nil {
				return nil, e1
			}

			col := v.groupDF.Column(left[ind])

			if len(outVecs) < len(gCol)+ind+1 {
				outVecs = append(outVecs, d.MakeVector(col.DataType(), 0))
			}

			if e2 := outVecs[ind+len(gCol)].Append(col.Data().Element(0)); e2 != nil {
				return nil, e2
			}
		}
	}

	var cols []*Col
	names := append(flds, left...)
	for ind := range len(outVecs) {
		var (
			col *Col
			e3  error
		)

		if col, e3 = NewCol(outVecs[ind], d.ColName(names[ind])); e3 != nil {
			return nil, e3
		}

		cols = append(cols, col)
	}

	var (
		outDF *DF
		e4    error
	)
	if outDF, e4 = NewDFcol(cols, d.DFsetFns(f.Fns())); e4 != nil {
		return nil, e4
	}

	//	outDF.groupBy = grp
	_ = d.DFsetSourceDF(f)(outDF)

	return outDF, nil
}

func (f *DF) Categorical(colName string, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (d.Column, error) {
	var col d.Column
	if col = f.Column(colName); col == nil {
		return nil, fmt.Errorf("column %s not found", colName)
	}

	if col.DataType() == d.DTfloat {
		return nil, fmt.Errorf("cannot make float to categorical")
	}

	var (
		tab d.DF
		e2  error
	)
	if tab, e2 = f.Table(colName); e2 != nil {
		return nil, e2
	}

	if e3 := tab.Sort(true, colName); e3 != nil {
		return nil, e3
	}

	// check incoming map is of the correct types
	nextInt := 0
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

	if _, ok := toMap[defaultVal]; !ok {
		toMap[defaultVal] = -1
	}

	// cnts will count the frequencies of each level of toMap
	cnts := make(d.CategoryMap)

	lvls := tab.Column(colName)
	cs := tab.Column("count")
	for ind := range tab.RowCount() {
		lvl := lvls.(*Col).Element(ind)
		cnt, _ := cs.(*Col).ElementInt(ind)
		if levels != nil && !d.Has(lvl, levels) {
			lvl = defaultVal
		}

		if *cnt < fuzz {
			//lvl = defaultVal
			toMap[lvl] = -1
		}

		if _, ok := toMap[lvl]; !ok {
			toMap[lvl] = nextInt
			nextInt++
		}
		cnts[lvl] += *cnt
	}

	vec := d.MakeVector(d.DTint, 0)

	for ind := range col.Len() {
		inVal := col.(*Col).Element(ind)

		var (
			ok     bool
			mapVal int
		)
		// if inVal isn't in the map, map it to the default level
		if mapVal, ok = toMap[inVal]; !ok {
			mapVal = toMap[defaultVal]
		}

		if e := vec.Append(mapVal); e != nil {
			return nil, e
		}

	}

	var (
		outCol *Col
		e      error
	)

	if outCol, e = NewCol(vec); e != nil {
		return nil, e
	}

	_ = d.ColDataType(d.DTcategorical)(outCol.ColCore)
	_ = d.ColCatMap(toMap)(outCol.ColCore)

	return outCol, nil
}

func (f *DF) Copy() d.DF {
	dfC := f.DFcore.Copy()

	mNew := &DF{
		sourceQuery: "",
		orderBy:     nil,
		ascending:   false,
		DFcore:      dfC,
	}

	_ = mNew.SetParent()

	return mNew
}

func (f *DF) AllRows() iter.Seq2[int, []any] {
	return func(yield func(int, []any) bool) {
		for ind := 0; ind < f.RowCount(); ind++ {
			var row []any
			for c := range f.AllColumns() {
				row = append(row, c.(*Col).Element(ind))
			}

			if !yield(ind, row) {
				return
			}
		}
	}
}

// allow for repeats in x?
func findIndx(x []float64, xLoc float64, indStart int) int {
	if indStart >= len(x) {
		return len(x)
	}

	indStart = max(0, indStart)
	if xLoc < x[indStart] {
		return -1
	}

	if xLoc > x[len(x)-1] {
		return len(x)
	}

	for ind := indStart; ind < len(x)-1; ind++ {
		if xLoc >= x[ind] && xLoc <= x[ind+1] {
			return ind
		}
	}

	return len(x) - 1
}

func (f *DF) Interp(iDF d.DF, xSfield, xIfield, yfield, outField string) (d.DF, error) {
	var (
		idf *DF
		ok  bool
	)
	if idf, ok = iDF.(*DF); !ok {
		return nil, fmt.Errorf("iDF argument to iterp is not *mem.DF")
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
		e    error
	)
	fld := d.RandomLetters(4)
	if favg, e = f.By(xSfield, fld+":=mean("+yfield+")"); e != nil {
		return nil, e
	}

	if e := favg.Sort(true, xSfield); e != nil {
		return nil, e
	}

	if e := idf.Sort(true, xIfield); e != nil {
		return nil, e
	}

	xI := idf.Column(xIfield).Data().AsAny().([]float64)
	xS := favg.Column(xSfield).Data().AsAny().([]float64)
	yS := favg.Column(fld).Data().AsAny().([]float64)
	yOut := make([]float64, len(xI))
	iOut := make([]int, len(xI))

	indSource := 0
	for ind := range len(xI) {
		indSource = findIndx(xS, xI[ind], indSource)
		// out of range
		if indSource < 0 || indSource == len(xS) {
			continue
		}

		// max element
		if ind == len(xI)-1 {
			yOut[ind] = xS[indSource]
			iOut[ind] = 1
			continue
		}

		var width float64
		if width = (xS[indSource+1] - xS[indSource]); width == 0 {
			continue
		}

		w := (xS[indSource+1] - xI[ind]) / width
		yOut[ind] = w*yS[indSource] + (1-w)*yS[indSource+1]
		iOut[ind] = 1
	}

	var (
		colX, colI *Col
		eX, eI     error
	)

	if colX, eX = NewCol(yOut, d.ColName(outField), d.ColParent(idf)); eX != nil {
		return nil, eX
	}

	iCN := outField + "IndOK"
	if colI, eI = NewCol(iOut, d.ColName(iCN), d.ColParent(idf)); eI != nil {
		return nil, eI
	}

	if e := idf.AppendColumn(colX, false); e != nil {
		return nil, e
	}

	if e := idf.AppendColumn(colI, false); e != nil {
		return nil, e
	}

	var (
		dfOut d.DF
		eOut  error
	)
	if dfOut, eOut = idf.Where(iCN + "==1"); eOut != nil {
		return nil, eOut
	}

	if ed := dfOut.DropColumns(iCN); ed != nil {
		return nil, ed
	}

	return dfOut, nil
}

func (f *DF) Join(df d.DF, joinOn string) (d.DF, error) {
	var (
		fRight *DF
		ok     bool
	)
	if fRight, ok = df.(*DF); !ok {
		return nil, fmt.Errorf("must be mem.*DF to join")
	}

	jCols := strings.Split(strings.ReplaceAll(joinOn, " ", ""), ",")
	if !f.HasColumns(jCols...) || !df.HasColumns(jCols...) {
		return nil, fmt.Errorf("missing some join columns")
	}

	if e := f.Sort(true, jCols...); e != nil {
		return nil, e
	}

	if e := df.Sort(true, jCols...); e != nil {
		return nil, e
	}

	leftNames := f.ColumnNames()
	rightNames := df.ColumnNames()
	outCols := doCols(nil, f, nil, nil)
	outCols = doCols(outCols, df, jCols, leftNames)

	// location of the join fields in both dataframes
	var colsLeft, colsRight []int
	for ind := range len(jCols) {
		colsLeft = append(colsLeft, d.Position(jCols[ind], leftNames))
		colsRight = append(colsRight, d.Position(jCols[ind], rightNames))
	}

	indLeft, indRight := 0, 0
	// pull first rows from both
	leftRow := f.Row(indLeft)
	rightRow := fRight.Row(indRight)
	indLeft++
	indRight++

	// subset the rows to the values we're joining on
	leftJoin := subset(leftRow, colsLeft)
	rightJoin := subset(rightRow, colsRight)

	// rh is the row number of the first row of right that matches the current row of left
	rh := -1

	for len(leftRow) > 0 && len(rightRow) > 0 {
		if rowCompare(leftJoin, rightJoin, "eq") {
			// append
			if rh == -1 {
				rh = indRight - 1
			}

			if e := appendRow(outCols, leftRow, rightRow, colsRight); e != nil {
				return nil, e
			}

			// get next row from right side
			if rightRow = fRight.Row(indRight); rightRow == nil {
				continue
			}
			indRight++

			rightJoin = subset(rightRow, colsRight)
			continue
		}

		// if left is less than right, increment left
		if rowCompare(leftJoin, rightJoin, "lt") {
			leftJoinHold := leftJoin
			if leftRow = f.Row(indLeft); leftRow == nil {
				continue
			}

			indLeft++
			leftJoin = subset(leftRow, colsLeft)

			// if the next row of left is identical on the join fields, then back up to start of matching df rows on right
			if rh >= 0 && rowCompare(leftJoin, leftJoinHold, "eq") {
				indRight = rh
				rightRow = fRight.Row(indRight)
				rightJoin = subset(rightRow, colsRight)
			}

			rh = -1
			continue
		}

		// if left is greater than right, increment right
		if rowCompare(leftJoin, rightJoin, "gt") {
			if rightRow = fRight.Row(indRight); rightRow == nil {
				continue
			}

			rightJoin = subset(rightRow, colsRight)
			indRight++
		}
	}

	outDF, e1 := NewDFcol(outCols, d.DFsetFns(f.Fns()))

	return outDF, e1
}

// Len is required for sort
func (f *DF) Len() int {
	return f.RowCount()
}

func (f *DF) Less(i, j int) bool {
	for ind := range len(f.orderBy) {
		less := f.orderBy[ind].Less(i, j)
		greater := f.orderBy[ind].Less(j, i)
		equal := !less && !greater

		// if equal, keep checking
		if equal {
			continue
		}

		if f.ascending {
			return less
		}

		return greater
	}

	// all equal, return false
	return false
}

// TODO: consider adding to interface?
func (f *DF) Row(rowNum int) []any {
	if rowNum < 0 || rowNum >= f.RowCount() {
		return nil
	}

	var row []any
	for c := range f.AllColumns() {
		row = append(row, c.(*Col).Element(rowNum))
	}

	return row
}

func (f *DF) RowCount() int {
	return f.Column(f.ColumnNames()[0]).Len()
}

func (f *DF) SetParent() error {
	for c := range f.AllColumns() {
		if e := d.ColParent(f)(c); e != nil {
			return e
		}
	}

	return nil
}

func (f *DF) Sort(ascending bool, cols ...string) error {
	var byCols []*Col

	for ind := range len(cols) {
		var x d.Column
		if x = f.Column(cols[ind]); x == nil {
			return fmt.Errorf("column %s not found", cols[ind])
		}

		byCols = append(byCols, x.(*Col))
	}

	f.orderBy = byCols
	f.ascending = ascending
	sort.Sort(f)

	return nil
}

func (f *DF) SourceQuery() string {
	return f.sourceQuery
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

func (f *DF) Swap(i, j int) {
	for h := range f.AllColumns() {
		h.(*Col).Swap(i, j)
	}
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

	indicator := f.Column("wherec")

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("argument to Where must be int")
	}

	dfNew := f.Copy()
	i1 := indicator.(*Col)
	for col := range dfNew.AllColumns() {
		cx := col.(*Col)
		cx.Vector = cx.Where(i1.Vector)
		if cx.Len() == 0 {
			return nil, fmt.Errorf("no data after applying where")
		}
	}

	_ = dfNew.DropColumns("wherec")

	return dfNew, nil
}

// ***************** Helpers *****************

func checkType(cols ...d.Column) error {
	for _, c := range cols {
		if _, ok := c.(*Col); !ok {
			if _, oks := c.(*d.Scalar); !oks {
				return fmt.Errorf("column is wrong type: need mem/Col")
			}
		}
	}

	return nil
}

// ****************************************************************************

// buildGroups creates a groups map by grouping df along the columns gbCol
func buildGroups(df *DF, gbCol []*Col) (groups, error) {
	type entry struct {
		cols []*d.Vector
		row  []any
	}

	cn := df.ColumnNames()
	ct, _ := df.ColumnTypes()

	var inVecs []*d.Vector
	for ind := range len(cn) {
		inVecs = append(inVecs, df.Column(cn[ind]).Data())
	}

	type oneD map[any]int64

	// the levels of each column in the table are stored in mps which maps the native value to int64
	// the byte representation of the int64 are concatenated and fed to the hash function
	var mps []oneD

	// nextIndx is the next index value to use for each column
	nextIndx := make([]int64, len(gbCol))
	for range len(gbCol) {
		mps = append(mps, make(oneD))
	}

	// tabMap is the map represenation of the table. The key is the hash value.
	tabMap := make(map[uint64]*entry)

	// buf is the 8 byte representation of the index number for a level of a column
	buf := new(bytes.Buffer)
	// h will be the hash of the bytes of the index numbers for each level of the table columns
	h := fnv.New64()

	// scan the rows to build the table
	for rowNum := range gbCol[0].Len() {
		// str is the byte array that is hashed, its length is 8 times the # of columns
		var str []byte

		// rowVal holds the values of the columns for that row of the table
		var rowVal []any
		// build hash value from the values of the grouping columns
		for c := range len(gbCol) {
			val := gbCol[c].Element(rowNum)
			rowVal = append(rowVal, val)
			var (
				cx int64
				ok bool
			)

			if cx, ok = mps[c][val]; !ok {
				mps[c][val] = nextIndx[c]
				cx = nextIndx[c]
				nextIndx[c]++
			}

			// write the binary value of the index (cx) to the buffer
			if e := binary.Write(buf, binary.LittleEndian, cx); e != nil {
				panic(e)
			}

			// append the binary value to the byte array
			str = append(str, buf.Bytes()...)
			buf.Reset()
		}

		// write the byte array to the fnv hash
		_, _ = h.Write(str)

		// retrieve the hash value:
		entryx := h.Sum64()
		// need a new entry?
		if _, ok := tabMap[entryx]; !ok {
			var vecs []*d.Vector
			for ind := range len(cn) {
				vecs = append(vecs, d.MakeVector(ct[ind], 0))
			}

			tabMap[entryx] = &entry{
				cols: vecs,
				row:  rowVal,
			}
		}

		// put the data in the entry
		v := tabMap[entryx]
		for ind := range len(cn) {
			if e := v.cols[ind].Append(inVecs[ind].Element(rowNum)); e != nil {
				return nil, e
			}
		}

		h.Reset()
	}

	grp := make(groups)
	for k, v := range tabMap {
		var cols []*Col
		for ind := range len(cn) {
			var (
				col *Col
				e1  error
			)
			if col, e1 = NewCol(v.cols[ind], d.ColName(cn[ind])); e1 != nil {
				return nil, e1
			}

			cols = append(cols, col)
		}

		var (
			dfg *DF
			e2  error
		)
		if dfg, e2 = NewDFcol(cols); e2 != nil {
			return nil, e2
		}

		_ = d.DFsetSourceDF(df)(dfg)

		grp[k] = &groupVal{
			groupDF: dfg,
			row:     v.row,
		}
	}

	return grp, nil
}

// rowCompare compares the elements of rowLeft to rowRight.  It returns true if the test passes.
// comp = "eq", "lt" or "gt"
func rowCompare(rowLeft, rowRight []any, comp string) bool {
	var (
		compFns []any
		value   int
	)
	switch comp {
	case "eq":
		compFns = []any{eqFn[float64], eqFn[int], eqFn[string], eqFn[time.Time]}
		value = 0
	case "gt":
		compFns = []any{ltFn[float64], ltFn[int], ltFn[string], ltFn[time.Time]}
		value = 1
	case "lt":
		compFns = []any{gtFn[float64], gtFn[int], gtFn[string], gtFn[time.Time]}
		value = 1
	default:
		panic(fmt.Errorf("unsupported comparison in rowCompare"))
	}

	for ind := range len(rowLeft) {
		switch left := rowLeft[ind].(type) {
		case float64:
			fn := compFns[0].(func(float64, float64) int)
			if fn(left, rowRight[ind].(float64)) == value {
				return false
			}
		case int:
			fn := compFns[1].(func(int, int) int)
			if fn(left, rowRight[ind].(int)) == value {
				return false
			}
		case string:
			fn := compFns[2].(func(string, string) int)
			if fn(left, rowRight[ind].(string)) == value {
				return false
			}
		case time.Time:
			fn := compFns[3].(func(time.Time, time.Time) int)
			if fn(left, rowRight[ind].(time.Time)) == value {
				return false
			}
		}
	}

	return true

}

// subset returns elements of row whose index is in cols, conceptually row[cols]
func subset(row []any, cols []int) []any {
	var out []any
	for ind := range len(cols) {
		out = append(out, row[cols[ind]])
	}

	return out
}

// appendRow appends a row to cols.  The values are the union of left and right.  The columns of right whose
// indices in rightExclude are exluded.
func appendRow(cols []*Col, left, right []any, rightExclude []int) error {
	for ind := range len(left) {
		if e := cols[ind].Data().Append(left[ind]); e != nil {
			return e
		}
	}

	colInd := len(left)
	for ind := range len(right) {
		if d.Has(ind, rightExclude) {
			continue
		}

		if e := cols[colInd].Data().Append(right[ind]); e != nil {
			return e
		}

		colInd++
	}

	return nil
}

// doCols appends columns to outCols.  It appends empty columns with the names/types of df.
// columns with names in exclude are not appended.
// columns with names in dups have "DUP" appended to their name
func doCols(outCols []*Col, df d.DF, exclude, dups []string) []*Col {
	names := df.ColumnNames()
	for ind := range len(names) {
		src := df.Column(names[ind])
		cn := names[ind]
		if exclude != nil && d.Has(cn, exclude) {
			continue
		}
		if dups != nil && d.Has(cn, dups) {
			cn += "DUP"
		}

		data := d.MakeVector(src.DataType(), 0)
		var (
			col *Col
			e0  error
		)
		if col, e0 = NewCol(data, d.ColName(cn)); e0 != nil {
			panic(e0)
		}

		outCols = append(outCols, col)
	}

	return outCols
}
