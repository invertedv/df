package df

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"sort"

	d "github.com/invertedv/df"
)

type DF struct {
	sourceQuery string
	by          []*Col
	ascending   bool
	row         int

	*d.DFcore
}

func StandardFunctions() d.Fns {
	// DF returns
	fns := d.Fns{sortDF, table, where}

	// vector returns
	fns = append(fns, comparisons()...)
	fns = append(fns, mathOps()...)
	fns = append(fns, logicalOps()...)
	fns = append(fns, mathFuncs()...)
	fns = append(fns, otherVectors()...)
	fns = append(fns, castOps()...)
	fns = append(fns, toCat, applyCat)

	// scalar returns
	fns = append(fns, summaries()...)

	return fns
}

// ***************** DF - Create *****************

func NewDFcol(funcs d.Fns, cols ...*Col) (*DF, error) {
	if funcs == nil {
		funcs = StandardFunctions()
	}

	rowCount := cols[0].Len()
	var cc []d.Column
	for ind := 0; ind < len(cols); ind++ {
		if rc := cols[ind].Len(); rc > 1 && rc != rowCount {
			return nil, fmt.Errorf("all MemCols must have same length")
		}

		cc = append(cc, cols[ind])
	}

	var (
		df *d.DFcore
		e  error
	)

	if df, e = d.NewDF(funcs, cc...); e != nil {
		return nil, e
	}

	outDF := &DF{DFcore: df, row: -1}

	return outDF, nil
}

func NewDFseq(funcs d.Fns, n int) *DF {
	if n <= 0 {
		panic(fmt.Errorf("n must be positive in NewDFseq"))
	}

	if funcs == nil {
		funcs = StandardFunctions()
	}

	data := make([]int, n)
	for ind := 0; ind < n; ind++ {
		data[ind] = ind
	}

	col, _ := NewCol("seq", data)

	df, _ := NewDFcol(funcs, col)

	return df
}

func DBLoad(qry string, dlct *d.Dialect) (*DF, error) {
	var (
		columnNames []string
		columnTypes []d.DataTypes
		e           error
	)

	if columnNames, columnTypes, _, e = dlct.Types(qry); e != nil {
		return nil, e
	}

	var memData []*d.Vector
	if memData, e = dlct.Load(qry); e != nil {
		return nil, e
	}

	var memDF *DF
	for ind := 0; ind < len(columnTypes); ind++ {
		var col *Col

		if col, e = NewCol(columnNames[ind], memData[ind]); e != nil {
			return nil, e
		}

		if ind == 0 {
			if memDF, e = NewDFcol(StandardFunctions(), col); e != nil {
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

func FileLoad(f *d.Files) (*DF, error) {
	var (
		memData []*d.Vector
		e       error
	)
	if memData, e = f.Load(); e != nil {
		return nil, e
	}

	var memDF *DF
	for ind := 0; ind < len(f.FieldNames()); ind++ {
		var col *Col

		if col, e = NewCol(f.FieldNames()[ind], memData[ind]); e != nil {
			return nil, e
		}

		if ind == 0 {
			if memDF, e = NewDFcol(StandardFunctions(), col); e != nil {
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

// ***************** DF - Methods *****************

// AppendColumn masks the DFcore version so that we can handle appending scalars
func (f *DF) AppendColumn(col d.Column, replace bool) error {
	panicer(col)
	if f.RowCount() != col.Len() && col.Len() > 1 {
		return fmt.Errorf("unequal lengths in AppendColumn")
	}

	colx := col.(*Col)
	if colx.Len() == 1 {
		colx.Vector = d.NewVector(colx.Element(0), f.RowCount())
	}

	if ex := f.DFcore.AppendColumn(colx, replace); ex != nil {
		return ex
	}

	return nil
}

func (f *DF) AppendDF(df d.DF) (d.DF, error) {
	if _, ok := df.(*DF); !ok {
		return nil, fmt.Errorf("must be *DF to append to *DF")
	}

	var (
		dfCore *d.DFcore
		e      error
	)

	if dfCore, e = f.AppendDFcore(df); e != nil {
		return nil, e
	}

	ndf := &DF{
		sourceQuery: "",
		by:          nil,
		DFcore:      dfCore,
	}

	return ndf, nil
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
	if tab, e2 = f.Table(true, colName); e2 != nil {
		return nil, e2
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
	for ind := 0; ind < tab.RowCount(); ind++ {
		lvl := lvls.(*Col).Element(ind)
		cnt := cs.(*Col).ElementInt(ind)
		if levels != nil && !d.In(lvl, levels) {
			lvl = defaultVal
		}

		if cnt < fuzz {
			lvl = defaultVal
		}

		if _, ok := toMap[lvl]; !ok {
			toMap[lvl] = nextInt
			nextInt++
		}
		cnts[lvl] += cnt
	}

	vec := d.MakeVector(d.DTint, 0)

	for ind := 0; ind < col.Len(); ind++ {
		inVal := col.(*Col).Element(ind)

		var (
			ok     bool
			mapVal int
		)
		// if inVal isn't in the map, map it to the default level
		if mapVal, ok = toMap[inVal]; !ok {
			mapVal = toMap[defaultVal]
		}

		vec.Append(mapVal)

	}

	var (
		outCol *Col
		e      error
	)

	if outCol, e = NewCol("", vec); e != nil {
		return nil, e
	}

	d.ColDataType(d.DTcategorical)(outCol.ColCore)
	d.ColCatMap(toMap)(outCol.ColCore)
	d.ColCatCounts(cnts)(outCol.ColCore)

	return outCol, nil
}

func (f *DF) Copy() d.DF {
	dfC := f.DFcore.Copy()

	mNew := &DF{
		sourceQuery: "",
		by:          nil,
		ascending:   false,
		DFcore:      dfC,
	}

	return mNew
}

func (f *DF) Iter(reset bool) (row []any, err error) {
	if reset {
		f.row = 0
	}

	if f.row+1 > f.RowCount() {
		return nil, io.EOF
	}

	for c := f.First(); c != nil; c = f.Next() {
		row = append(row, c.(*Col).Element(f.row))
	}

	f.row++

	return row, nil
}

// Len is required for sort
func (f *DF) Len() int {
	return f.RowCount()
}

func (f *DF) Less(i, j int) bool {
	for ind := 0; ind < len(f.by); ind++ {
		var less bool
		if f.ascending {
			less = f.by[ind].Less(i, j)

		} else {
			less = f.by[ind].Less(j, i)
		}

		// if greater, it's false
		if !less {
			return false
		}

		// if < (rather than <=) it's true
		if f.by[ind].Less(i, j) && !f.by[ind].Less(j, i) {
			return true
		}

		// equal -- keep checking
	}

	return true
}

func (f *DF) MakeQuery(colNames ...string) string {
	return ""
}

func (f *DF) RowCount() int {
	return f.First().Len()
}

func (f *DF) Sort(ascending bool, cols ...string) error {
	var by []*Col

	for ind := 0; ind < len(cols); ind++ {
		var x d.Column
		if x = f.Column(cols[ind]); x == nil {
			return fmt.Errorf("column %s not found", cols[ind])
		}

		by = append(by, x.(*Col))
	}

	f.by = by
	f.ascending = ascending
	sort.Sort(f)

	return nil
}

func (f *DF) SourceQuery() string {
	return f.sourceQuery
}

func (f *DF) String() string {
	var sx string
	for c := f.First(); c != nil; c = f.Next() {
		sx += c.String() + "\n"
	}

	return sx
}

func (f *DF) Swap(i, j int) {
	for h := f.First(); h != nil; h = f.Next() {
		h.(*Col).Swap(i, j)
		/*		data := h.(*Col).data
				switch h.DataType() {
				case d.DTfloat:
					data.([]float64)[i], data.([]float64)[j] = data.([]float64)[j], data.([]float64)[i]
				case d.DTint:
					data.([]int)[i], data.([]int)[j] = data.([]int)[j], data.([]int)[i]
				case d.DTstring:
					data.([]string)[i], data.([]string)[j] = data.([]string)[j], data.([]string)[i]
				case d.DTdate:
					data.([]time.Time)[i], data.([]time.Time)[j] = data.([]time.Time)[j], data.([]time.Time)[i]
				default:
					panic(fmt.Errorf("unsupported data type in Swap"))
				}*/
	}
}

func (f *DF) Table(sortByRows bool, cols ...string) (d.DF, error) {
	var mCols, outCols []*Col
	for ind := 0; ind < len(cols); ind++ {
		var c d.Column
		if c = f.Column(cols[ind]); c == nil {
			return nil, fmt.Errorf("column %s not found", cols[ind])
		}

		if c.DataType() == d.DTfloat {
			return nil, fmt.Errorf("cannot make table with type float")
		}

		mCols = append(mCols, c.(*Col))
	}

	outCols = makeTable(mCols...)

	var (
		outDF d.DF
		e     error
	)

	if outDF, e = NewDFcol(f.Fns(), outCols...); e != nil {
		return nil, e
	}

	sortBy := []string{"count"}
	ascending := false
	if sortByRows {
		sortBy = cols
		ascending = true
	}

	if ex := outDF.Sort(ascending, sortBy...); ex != nil {
		return nil, ex
	}

	// add rate to the table
	expr := "float(count) / float(sum(count))"
	var (
		rate d.Column
		ret  *d.Parsed
		ex   error
	)

	if ret, ex = d.ParseExpr(expr, outDF); ex != nil || ret.Which() != "Column" {
		return nil, ex
	}

	rate = ret.Value().(d.Column)
	_ = rate.Rename("rate")

	if ex1 := outDF.AppendColumn(rate, false); ex1 != nil {
		return nil, ex1
	}

	return outDF, nil
}

func (f *DF) Where(indicator d.Column) (d.DF, error) {
	panicer(indicator)
	if indicator.Len() != f.RowCount() {
		return nil, fmt.Errorf("indicator column wrong length. Got %d needed %d", indicator.Len(), f.RowCount())
	}

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("argument to Where must be int")
	}

	dfNew := f.Copy()
	i1 := indicator.(*Col)

	for col := dfNew.First(); col != nil; col = dfNew.Next() {
		cx := col.(*Col)
		cx.Vector = cx.Where(i1.Vector)
		if cx.Len() == 0 {
			return nil, fmt.Errorf("no data after applying where")
		}
	}

	return dfNew, nil
}

// ***************** Helpers *****************

func makeTable(cols ...*Col) []*Col {
	type oneD map[any]int64
	type entry struct {
		count int
		row   []any
	}

	// the levels of each column in the table are stored in mps which maps the native value to int64
	// the byte representation of the int64 are concatenated and fed to the hash function
	var mps []oneD

	// nextIndx is the next index value to use for each column
	nextIndx := make([]int64, len(cols))
	for ind := 0; ind < len(cols); ind++ {
		mps = append(mps, make(oneD))
	}

	// tabMap is the map represenation of the table. The key is the hash value.
	tabMap := make(map[uint64]*entry)

	// buf is the 8 byte representation of the index number for a level of a column
	buf := new(bytes.Buffer)
	// h will be the hash of the bytes of the index numbers for each level of the table columns
	h := fnv.New64()

	// scan the rows to build the table
	for rowNum := 0; rowNum < cols[0].Len(); rowNum++ {
		// str is the byte array that is hashed, its length is 8 times the # of columns
		var str []byte

		// rowVal holds the values of the columns for that row of the table
		var rowVal []any
		for c := 0; c < len(cols); c++ {
			val := cols[c].Element(rowNum)
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

			if e := binary.Write(buf, binary.LittleEndian, cx); e != nil {
				panic(e)
			}

			str = append(str, buf.Bytes()...)
			buf.Reset()
		}

		_, _ = h.Write(str)
		// increment the counter if that row is already mapped, o.w. add a new row
		if v, ok := tabMap[h.Sum64()]; ok {
			v.count++
		} else {
			tabMap[h.Sum64()] = &entry{
				count: 1,
				row:   rowVal,
			}
		}

		h.Reset()
	}

	var outVecs []*d.Vector
	for c := 0; c < len(cols); c++ {
		outVecs = append(outVecs, d.MakeVector(cols[c].DataType(), 0))
	}

	// counts
	outVecs = append(outVecs, d.MakeVector(d.DTint, 0))

	for _, v := range tabMap {
		for c := 0; c < len(cols); c++ {
			outVecs[c].Append(v.row[c])
		}

		outVecs[len(outVecs)-1].Append(v.count)
	}

	var (
		outCols []*Col
	)

	for c := 0; c <= len(cols); c++ {
		var (
			col *Col
			e   error
		)
		name := "count"
		if c < len(cols) {
			name = cols[c].Name()
		}

		if col, e = NewCol(name, outVecs[c]); e != nil {
			panic(e)
		}

		outCols = append(outCols, col)
	}

	return outCols
}

func panicer(cols ...d.Column) {
	for _, c := range cols {
		if _, ok := c.(*Col); !ok {
			panic("non-*Col argument")
		}
	}
}
