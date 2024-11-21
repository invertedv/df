package df

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"sort"
	"time"

	d "github.com/invertedv/df"

	"gonum.org/v1/gonum/stat"
)

type DF struct {
	sourceQuery string
	by          []*Col
	ascending   bool
	row         int

	*d.DFcore
}

type Col struct {
	data any

	*d.ColCore
}

// ***************** DF - Create *****************

func NewDFcol(runDF d.RunFn, funcs d.Fns, context *d.Context, cols ...*Col) (*DF, error) {
	if runDF == nil {
		runDF = RunDFfn
	}

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

	if df, e = d.NewDF(runDF, funcs, cc...); e != nil {
		return nil, e
	}

	df.SetContext(d.NewContext(nil, nil, nil, nil))
	if context != nil {
		df.SetContext(context)
	}

	outDF := &DF{DFcore: df, row: -1}
	outDF.Context().SetSelf(outDF)

	return outDF, nil
}

func NewDFseq(runDF d.RunFn, funcs d.Fns, context *d.Context, n int) *DF {
	if n <= 0 {
		panic(fmt.Errorf("n must be positive in NewDFseq"))
	}

	if runDF == nil {
		runDF = RunDFfn
	}

	if funcs == nil {
		funcs = StandardFunctions()
	}

	data := make([]int, n)
	for ind := 0; ind < n; ind++ {
		data[ind] = ind
	}

	col, _ := NewCol("seq", data)

	df, _ := NewDFcol(runDF, funcs, context, col)
	df.Context().SetSelf(df)

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

	var memData []any
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
			if memDF, e = NewDFcol(RunDFfn, StandardFunctions(), nil, col); e != nil {
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

	memDF.SetContext(d.NewContext(nil, memDF, nil)) // HERE

	return memDF, nil
}

func FileLoad(f *d.Files) (*DF, error) {
	var (
		memData []any
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
			if memDF, e = NewDFcol(RunDFfn, StandardFunctions(), nil, col); e != nil {
				return nil, e
			}
			continue
		}

		if ex := memDF.AppendColumn(col, false); ex != nil {
			return nil, ex
		}
	}

	memDF.row = -1

	memDF.SetContext(d.NewContext(nil, memDF, nil)) // HERE

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
		var e error
		dt := col.DataType()

		xs := d.MakeSlice(col.DataType(), 0, nil)
		val := colx.Element(0)
		for ind := 0; ind < f.RowCount(); ind++ {
			xs = d.AppendSlice(xs, val, dt)
		}

		if colx, e = NewCol(col.Name(), xs); e != nil {
			return e
		}
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
		cnt := cs.(*Col).Element(ind).(int)
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

	data := d.MakeSlice(d.DTint, 0, nil)
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

		data = d.AppendSlice(data, mapVal, d.DTint)
	}

	var (
		outCol *Col
		e      error
	)

	if outCol, e = NewCol("", data); e != nil {
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

	ctx := d.NewContext(f.Context().Dialect(), mNew)
	mNew.SetContext(ctx)

	return mNew
}

func (f *DF) Iter(reset bool) (row []any, err error) {
	if reset {
		f.row = 0
	}

	if f.row+1 > f.RowCount() {
		return nil, io.EOF
	}

	for c := f.Next(true); c != nil; c = f.Next(false) {
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
			less = f.by[ind].Greater(i, j)
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
	return f.Next(true).Len()
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
	for c := f.Next(true); c != nil; c = f.Next(false) {
		sx += c.String() + "\n"
	}

	return sx
}

func (f *DF) Swap(i, j int) {
	for h := f.Next(true); h != nil; h = f.Next(false) {
		data := h.(*Col).data
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
		}
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

	ctx := d.NewContext(f.Context().Dialect(), nil, nil)
	if outDF, e = NewDFcol(f.Runner(), f.Fns(), ctx, outCols...); e != nil {
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

	if ret, ex = d.ParseExpr(expr, outDF.(*DF).DFcore); ex != nil || ret.Which() != "Column" {
		return nil, ex
	}

	rate = ret.Value().(d.Column)
	rate.Rename("rate")

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

	var n int
	for col := dfNew.Next(true); col != nil; col = dfNew.Next(false) {
		cx := col.(*Col)
		n = 0
		newData := d.MakeSlice(cx.DataType(), 0, nil)

		for ind := 0; ind < cx.Len(); ind++ {
			if indicator.Data().([]int)[ind] > 0 {
				n++
				newData = d.AppendSlice(newData, cx.Element(ind), cx.DataType())
			}
		}

		if n == 0 {
			return nil, fmt.Errorf("no data after applying where")
		}

		cx.data = newData
	}

	return dfNew, nil
}

// ***************** Col - Create *****************

func NewCol(name string, data any) (*Col, error) {
	var dt d.DataTypes
	if dt = d.WhatAmI(data); dt == d.DTunknown {
		return nil, fmt.Errorf("unsupported data type in NewCol")
	}

	var t any
	switch dx := data.(type) {
	case float64:
		t = []float64{dx}
	case int:
		t = []int{dx}
	case time.Time:
		t = []time.Time{dx}
	case string:
		t = []string{dx}
	default:
		t = data
	}

	c := &Col{
		data:    t,
		ColCore: d.NewColCore(dt, d.ColName(name)),
	}

	return c, nil
}

// ***************** Col - Methods *****************

func (c *Col) AppendRows(col2 d.Column) (d.Column, error) {
	panicer(col2)
	return AppendRows(c, col2, c.Name())
}

//func (c *Col) CategoryMap() d.CategoryMap {
//	return c.catMap
//}

// TODO: populate
func (c *Col) Context() *d.Context {
	return nil
}

func (c *Col) Copy() d.Column {
	var copiedData any
	n := c.Len()
	switch c.DataType() {
	case d.DTfloat:
		copiedData = make([]float64, n)
		copy(copiedData.([]float64), c.data.([]float64))
	case d.DTint:
		copiedData = make([]int, n)
		copy(copiedData.([]int), c.data.([]int))
	case d.DTstring:
		copiedData = make([]string, n)
		copy(copiedData.([]string), c.data.([]string))
	case d.DTdate:
		copiedData = make([]time.Time, n)
		copy(copiedData.([]time.Time), c.data.([]time.Time))
	default:
		panic(fmt.Errorf("unsupported data type in Copy"))
	}

	col := &Col{
		data:    copiedData,
		ColCore: d.NewColCore(c.DataType(), d.ColName(c.Name()), d.ColCatMap(c.CategoryMap())),
	}

	return col
}

func (c *Col) Data() any {
	return c.data
}

func (c *Col) Element(row int) any {
	if c.Len() == 1 {
		row = 0
	}

	switch c.DataType() {
	case d.DTfloat:
		return c.Data().([]float64)[row]
	case d.DTint, d.DTcategorical:
		return c.Data().([]int)[row]
	case d.DTstring:
		return c.Data().([]string)[row]
	case d.DTdate:
		return c.Data().([]time.Time)[row]
	default:
		panic(fmt.Errorf("unsupported data type in Element"))
	}
}

func (c *Col) Greater(i, j int) bool {
	switch c.DataType() {
	case d.DTfloat:
		return c.data.([]float64)[i] >= c.data.([]float64)[j]
	case d.DTint:
		return c.data.([]int)[i] >= c.data.([]int)[j]
	case d.DTstring:
		return c.data.([]string)[i] >= c.data.([]string)[j]
	case d.DTdate:
		return !c.data.([]time.Time)[i].Before(c.data.([]time.Time)[j])
	default:
		panic(fmt.Errorf("unsupported data type in Less"))
	}
}

func (c *Col) Len() int {
	switch c.DataType() {
	case d.DTfloat:
		return len(c.Data().([]float64))
	case d.DTint, d.DTcategorical:
		return len(c.Data().([]int))
	case d.DTstring:
		return len(c.Data().([]string))
	case d.DTdate:
		return len(c.Data().([]time.Time))
	default:
		return -1
	}
}

func (c *Col) Less(i, j int) bool {
	switch c.DataType() {
	case d.DTfloat:
		return c.data.([]float64)[i] <= c.data.([]float64)[j]
	case d.DTint:
		return c.data.([]int)[i] <= c.data.([]int)[j]
	case d.DTstring:
		return c.data.([]string)[i] <= c.data.([]string)[j]
	case d.DTdate:
		return !c.data.([]time.Time)[i].After(c.data.([]time.Time)[j])
	default:
		panic(fmt.Errorf("unsupported data type in Less"))
	}
}

func (c *Col) Rename(newName string) {
	if !d.ValidName(c.Name()) {
		panic(fmt.Errorf("illegal name: %s", c.Name()))
	}

	d.ColName(newName)(c.ColCore)
	//	c.name = newName
}

func (c *Col) Replace(indicator, replacement d.Column) (d.Column, error) {
	panicer(indicator, replacement)
	if c.DataType() != replacement.DataType() {
		return nil, fmt.Errorf("incompatible columns in Replace")
	}

	n := d.MaxInt(c.Len(), indicator.Len(), replacement.Len())
	if (c.Len() > 1 && c.Len() != n) || (indicator.Len() > 1 && indicator.Len() != n) ||
		(replacement.Len() > 1 && replacement.Len() != n) {
		return nil, fmt.Errorf("columns not same length in Replacef")
	}

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("indicator not type DTint in Replace")
	}

	data := d.MakeSlice(c.DataType(), 0, nil)

	for ind := 0; ind < n; ind++ {
		x := c.Element(ind)
		if indicator.(*Col).Element(ind).(int) > 0 {
			x = replacement.(*Col).Element(ind)
		}

		data = d.AppendSlice(data, x, c.DataType())
	}
	var (
		outCol *Col
		e      error
	)
	if outCol, e = NewCol("", data); e != nil {
		return nil, e
	}

	return outCol, nil
}

func (c *Col) SetContext(ctx *d.Context) {

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
		tab, _ := NewDFcol(nil, nil, nil, makeTable(c)...)
		_ = tab.Sort(false, "count")
		l := tab.Column(c.Name())
		c := tab.Column("count")

		header := []string{l.Name(), c.Name()}

		return t + d.PrettyPrint(header, l.Data(), c.Data())
	}

	x := make([]float64, c.Len())
	copy(x, c.Data().([]float64))
	sort.Float64s(x)
	minx := x[0]
	maxx := x[len(x)-1]
	q25 := stat.Quantile(0.25, 4, x, nil)
	q50 := stat.Quantile(0.5, 4, x, nil)
	q75 := stat.Quantile(0.75, 4, x, nil)
	xbar := stat.Mean(x, nil)
	n := float64(c.Len())
	cats := []string{"min", "lq", "median", "mean", "uq", "max", "n"}
	vals := []float64{minx, q25, q50, xbar, q75, maxx, n}
	header := []string{"metric", "value"}

	return t + d.PrettyPrint(header, cats, vals)
}

func (c *Col) SetDependencies(dep []string) {
	d.ColSetDependencies(dep)(c.ColCore)

}

// ***************** Helpers *****************

func AppendRows(col1, col2 d.Column, name string) (*Col, error) {
	if col1.DataType() != col2.DataType() {
		return nil, fmt.Errorf("append columns must have same type, got %s and %s for %s and %s",
			col1.DataType(), col2.DataType(), col1.Name(), col2.Name())
	}

	var data any
	switch col1.DataType() {
	case d.DTfloat:
		data = append(col1.Data().([]float64), col2.Data().([]float64)...)
	case d.DTint:
		data = append(col1.Data().([]int), col2.Data().([]int)...)
	case d.DTstring:
		data = append(col1.Data().([]string), col2.Data().([]string)...)
	case d.DTdate:
		data = append(col1.Data().([]time.Time), col2.Data().([]time.Time)...)
	default:
		return nil, fmt.Errorf("unsupported data type in AppendRows")
	}

	var (
		col *Col
		e   error
	)
	if col, e = NewCol(name, data); e != nil {
		return nil, e
	}

	return col, nil
}

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
	for row := 0; row < cols[0].Len(); row++ {
		// str is the byte array that is hashed, its length is 8 times the # of columns
		var str []byte

		// rowVal holds the values of the columns for that row of the table
		var rowVal []any
		for c := 0; c < len(cols); c++ {
			val := cols[c].Element(row)
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

	// build the table in d.DF format
	var outData []any
	for c := 0; c < len(cols); c++ {
		outData = append(outData, d.MakeSlice(cols[c].DataType(), 0, nil))
	}

	outData = append(outData, d.MakeSlice(d.DTint, 0, nil))

	for _, v := range tabMap {
		for c := 0; c < len(v.row); c++ {
			outData[c] = d.AppendSlice(outData[c], v.row[c], cols[c].DataType())
		}

		outData[len(v.row)] = d.AppendSlice(outData[len(v.row)], v.count, d.DTint)
	}

	// make into columns
	var outCols []*Col
	var (
		mCol *Col
		e    error
	)
	for c := 0; c < len(cols); c++ {
		if mCol, e = NewCol(cols[c].Name(), outData[c]); e != nil {
			panic(e)
		}

		outCols = append(outCols, mCol)
	}

	if mCol, e = NewCol("count", outData[len(cols)]); e != nil {
		panic(e)
	}

	outCols = append(outCols, mCol)

	return outCols
}

// getNames returns the names of the input Columns starting with startInd element
func getNames(startInd int, cols ...any) ([]string, error) {
	var colNames []string
	for ind := startInd; ind < len(cols); ind++ {
		var cn string
		if cn = cols[ind].(*Col).Name(); cn == "" {
			return nil, fmt.Errorf("column with no name in table")
		}

		colNames = append(colNames, cn)
	}

	return colNames, nil
}

func panicer(cols ...d.Column) {
	for _, c := range cols {
		if _, ok := c.(*Col); !ok {
			panic("non-*Col argument")
		}
	}
}
