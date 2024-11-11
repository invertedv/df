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

	u "github.com/invertedv/utilities"

	d "github.com/invertedv/df"
)

type MemDF struct {
	sourceQuery string
	by          []*MemCol
	ascending   bool
	row         int

	*d.DFcore
}

type MemCol struct {
	name  string
	dType d.DataTypes
	data  any

	catMap    d.CategoryMap
	catCounts d.CategoryMap
	rawType   d.DataTypes
}

// ***************** MemDF - Create *****************

func NewDFcol(runDF d.RunFn, funcs d.Fns, context *d.Context, cols ...*MemCol) (*MemDF, error) {
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

	outDF := &MemDF{DFcore: df, row: -1}
	outDF.Context().SetSelf(outDF)

	return outDF, nil
}

func DBLoad(qry string, dlct *d.Dialect) (*MemDF, error) {
	var (
		columnNames []string
		columnTypes []d.DataTypes
		e           error
	)

	if columnNames, columnTypes, e = dlct.Types(qry); e != nil {
		return nil, e
	}

	var memData []any
	if memData, e = dlct.Load(qry); e != nil {
		return nil, e
	}

	var memDF *MemDF
	for ind := 0; ind < len(columnTypes); ind++ {
		var col *MemCol

		if col, e = NewMemCol(columnNames[ind], memData[ind]); e != nil {
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

func FileLoad(f *d.Files) (*MemDF, error) {
	var (
		memData []any
		e       error
	)
	if memData, e = f.Load(); e != nil {
		return nil, e
	}

	var memDF *MemDF
	for ind := 0; ind < len(f.FieldNames()); ind++ {
		var col *MemCol

		if col, e = NewMemCol(f.FieldNames()[ind], memData[ind]); e != nil {
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

// ***************** MemDF - Methods *****************

// AppendColumn masks the DFcore version so that we can handle appending scalars
func (m *MemDF) AppendColumn(col d.Column, replace bool) error {
	panicer(col)
	if m.RowCount() != col.Len() && col.Len() > 1 {
		return fmt.Errorf("unequal lengths in AppendColumn")
	}

	colx := col.(*MemCol)
	if colx.Len() == 1 {
		var e error
		dt := col.DataType()
		xs := d.MakeSlice(col.DataType(), 0, nil)
		val := colx.Element(0)
		for ind := 0; ind < m.RowCount(); ind++ {
			xs = d.AppendSlice(xs, val, dt)
		}

		if colx, e = NewMemCol(col.Name(""), xs); e != nil {
			return e
		}
	}

	if ex := m.DFcore.AppendColumn(colx, replace); ex != nil {
		return ex
	}

	return nil
}

func (m *MemDF) AppendDF(df d.DF) (d.DF, error) {
	if _, ok := df.(*MemDF); !ok {
		return nil, fmt.Errorf("must be *MemDF to append to *MemDF")
	}

	var (
		dfCore *d.DFcore
		e      error
	)

	if dfCore, e = m.AppendDFcore(df); e != nil {
		return nil, e
	}

	ndf := &MemDF{
		sourceQuery: "",
		by:          nil,
		DFcore:      dfCore,
	}

	return ndf, nil
}

func (m *MemDF) DBsave(tableName string, overwrite bool) error {
	if m.Context().Dialect() == nil {
		return fmt.Errorf("no dialect")
	}

	return m.Context().Dialect().Save(tableName, "", overwrite, m)
}

func (m *MemDF) Categorical(colName string, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (d.Column, error) {
	var (
		col d.Column
		e0  error
	)
	if col, e0 = m.Column(colName); e0 != nil {
		return nil, e0
	}

	if col.DataType() == d.DTfloat {
		return nil, fmt.Errorf("cannot make float to categorical")
	}

	var (
		tab d.DF
		e2  error
	)
	if tab, e2 = m.Table(true, colName); e2 != nil {
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

	lvls, _ := tab.Column(colName)
	cs, _ := tab.Column("count")
	for ind := 0; ind < tab.RowCount(); ind++ {
		lvl := lvls.(*MemCol).Element(ind)
		cnt := cs.(*MemCol).Element(ind).(int)
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
		inVal := col.(*MemCol).Element(ind)

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
		outCol *MemCol
		e      error
	)

	if outCol, e = NewMemCol("", data); e != nil {
		return nil, e
	}

	outCol.dType = d.DTcategorical

	outCol.catMap = toMap
	outCol.catCounts = cnts

	return outCol, nil
}

func (m *MemDF) Copy() d.DF {
	dfC := m.DFcore.Copy()

	mNew := &MemDF{
		sourceQuery: "",
		by:          nil,
		ascending:   false,
		DFcore:      dfC,
	}

	mNew.Context().SetSelf(mNew)

	return mNew
}

func (m *MemDF) Iter(reset bool) (row []any, err error) {
	if reset {
		m.row = 0
	}

	if m.row+1 > m.RowCount() {
		return nil, io.EOF
	}

	for c := m.Next(true); c != nil; c = m.Next(false) {
		row = append(row, c.(*MemCol).Element(m.row))
	}

	m.row++

	return row, nil
}

// Len is required for sort
func (m *MemDF) Len() int {
	return m.RowCount()
}

func (m *MemDF) Less(i, j int) bool {
	for ind := 0; ind < len(m.by); ind++ {
		var less bool
		if m.ascending {
			less = m.by[ind].Less(i, j)

		} else {
			less = m.by[ind].Greater(i, j)
		}

		// if greater, it's false
		if !less {
			return false
		}

		// if < (rather than <=) it's true
		if m.by[ind].Less(i, j) && !m.by[ind].Less(j, i) {
			return true
		}

		// equal -- keep checking
	}

	return true
}

func (m *MemDF) MakeQuery() string {
	return ""
}

// TODO: delete
func (m *MemDF) RowX(rowNum int) []any {
	if rowNum >= m.RowCount() {
		return nil
	}

	var r []any
	for cx := m.Next(true); cx != nil; cx = m.Next(false) {
		var v any
		i := u.MinInt(rowNum, cx.Len()-1)
		switch cx.DataType() {
		case d.DTfloat:
			v = cx.Data().([]float64)[i]
		case d.DTint, d.DTcategorical:
			v = cx.Data().([]int)[i]
		case d.DTdate:
			v = cx.Data().([]time.Time)[i]
		case d.DTstring:
			v = cx.Data().([]string)[i]
		default:
			panic(fmt.Errorf("unknown data type in Row"))
		}
		r = append(r, v)
	}

	return r
}

func (m *MemDF) RowCount() int {
	return m.Next(true).Len()
}

func (m *MemDF) Sort(ascending bool, cols ...string) error {
	var by []*MemCol

	for ind := 0; ind < len(cols); ind++ {
		var (
			x d.Column
			e error
		)

		if x, e = m.Column(cols[ind]); e != nil {
			return e
		}

		by = append(by, x.(*MemCol))
	}

	m.by = by
	m.ascending = ascending
	sort.Sort(m)

	return nil
}

func (m *MemDF) SourceQuery() string {
	return m.sourceQuery
}

func (m *MemDF) Swap(i, j int) {
	for h := m.Next(true); h != nil; h = m.Next(false) {
		data := h.(*MemCol).data
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

func (m *MemDF) Table(sortByRows bool, cols ...string) (d.DF, error) {
	var mCols, outCols []*MemCol
	for ind := 0; ind < len(cols); ind++ {
		var (
			c d.Column
			e error
		)

		if c, e = m.Column(cols[ind]); e != nil {
			return nil, e
		}

		if c.DataType() == d.DTfloat {
			return nil, fmt.Errorf("cannot make table with type float")
		}

		mCols = append(mCols, c.(*MemCol))
	}

	outCols = makeTable(mCols...)

	var (
		outDF d.DF
		e     error
	)

	ctx := d.NewContext(m.Context().Dialect(), nil, nil)
	if outDF, e = NewDFcol(m.Runner(), m.Fns(), ctx, outCols...); e != nil {
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

	if ret, ex = d.ParseExpr(expr, outDF.(*MemDF).DFcore); ex != nil || ret.Which() != "Column" {
		return nil, ex
	}

	rate = ret.Value().(d.Column)
	rate.Name("rate")

	if ex1 := outDF.AppendColumn(rate, false); ex1 != nil {
		return nil, ex1
	}

	return outDF, nil
}

func (m *MemDF) Where(indicator d.Column) (d.DF, error) {
	panicer(indicator)
	if indicator.Len() != m.RowCount() {
		return nil, fmt.Errorf("indicator column wrong length. Got %d needed %d", indicator.Len(), m.RowCount())
	}

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("argument to Where must be int")
	}

	dfNew := m.Copy()

	var n int
	for col := dfNew.Next(true); col != nil; col = dfNew.Next(false) {
		cx := col.(*MemCol)
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

// ***************** MemCol - Create *****************

func NewMemCol(name string, data any) (*MemCol, error) {
	var dt d.DataTypes
	if dt = d.WhatAmI(data); dt == d.DTunknown {
		return nil, fmt.Errorf("unsupported data type in NewMemCol")
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

	c := &MemCol{
		name:   name,
		dType:  dt,
		data:   t,
		catMap: nil,
	}

	return c, nil
}

// ***************** MemCol - Methods *****************

func (m *MemCol) AppendRows(col2 d.Column) (d.Column, error) {
	panicer(col2)
	return AppendRows(m, col2, m.Name(""))
}

func (m *MemCol) CategoryMap() d.CategoryMap {
	return m.catMap
}

func (m *MemCol) Copy() d.Column {
	var copiedData any
	n := m.Len()
	switch m.dType {
	case d.DTfloat:
		copiedData = make([]float64, n)
		copy(copiedData.([]float64), m.data.([]float64))
	case d.DTint:
		copiedData = make([]int, n)
		copy(copiedData.([]int), m.data.([]int))
	case d.DTstring:
		copiedData = make([]string, n)
		copy(copiedData.([]string), m.data.([]string))
	case d.DTdate:
		copiedData = make([]time.Time, n)
		copy(copiedData.([]time.Time), m.data.([]time.Time))
	default:
		panic(fmt.Errorf("unsupported data type in Copy"))
	}

	col := &MemCol{
		name:   m.name,
		dType:  m.dType,
		data:   copiedData,
		catMap: m.catMap,
	}

	return col
}

func (m *MemCol) Data() any {
	return m.data
}

func (m *MemCol) DataType() d.DataTypes {
	return m.dType
}

func (m *MemCol) Element(row int) any {
	if m.Len() == 1 {
		row = 0
	}

	switch m.dType {
	case d.DTfloat:
		return m.Data().([]float64)[row]
	case d.DTint, d.DTcategorical:
		return m.Data().([]int)[row]
	case d.DTstring:
		return m.Data().([]string)[row]
	case d.DTdate:
		return m.Data().([]time.Time)[row]
	default:
		panic(fmt.Errorf("unsupported data type in Element"))
	}
}

func (m *MemCol) Greater(i, j int) bool {
	switch m.dType {
	case d.DTfloat:
		return m.data.([]float64)[i] >= m.data.([]float64)[j]
	case d.DTint:
		return m.data.([]int)[i] >= m.data.([]int)[j]
	case d.DTstring:
		return m.data.([]string)[i] >= m.data.([]string)[j]
	case d.DTdate:
		return !m.data.([]time.Time)[i].Before(m.data.([]time.Time)[j])
	default:
		panic(fmt.Errorf("unsupported data type in Less"))
	}
}

func (m *MemCol) Len() int {
	switch m.dType {
	case d.DTfloat:
		return len(m.Data().([]float64))
	case d.DTint, d.DTcategorical:
		return len(m.Data().([]int))
	case d.DTstring:
		return len(m.Data().([]string))
	case d.DTdate:
		return len(m.Data().([]time.Time))
	default:
		return -1
	}
}

func (m *MemCol) Less(i, j int) bool {
	switch m.dType {
	case d.DTfloat:
		return m.data.([]float64)[i] <= m.data.([]float64)[j]
	case d.DTint:
		return m.data.([]int)[i] <= m.data.([]int)[j]
	case d.DTstring:
		return m.data.([]string)[i] <= m.data.([]string)[j]
	case d.DTdate:
		return !m.data.([]time.Time)[i].After(m.data.([]time.Time)[j])
	default:
		panic(fmt.Errorf("unsupported data type in Less"))
	}
}

func (m *MemCol) Name(renameTo string) string {
	if renameTo != "" {
		m.name = renameTo
	}

	return m.name
}

func (m *MemCol) RawType() d.DataTypes {
	return m.rawType
}

func (m *MemCol) Replace(indicator, replacement d.Column) (d.Column, error) {
	panicer(indicator, replacement)
	if m.DataType() != replacement.DataType() {
		return nil, fmt.Errorf("incompatible columns in Replace")
	}

	n := u.MaxInt(m.Len(), indicator.Len(), replacement.Len())
	if (m.Len() > 1 && m.Len() != n) || (indicator.Len() > 1 && indicator.Len() != n) ||
		(replacement.Len() > 1 && replacement.Len() != n) {
		return nil, fmt.Errorf("columns not same length in Replacef")
	}

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("indicator not type DTint in Replace")
	}

	data := d.MakeSlice(m.DataType(), 0, nil)

	for ind := 0; ind < n; ind++ {
		x := m.Element(ind)
		if indicator.(*MemCol).Element(ind).(int) > 0 {
			x = replacement.(*MemCol).Element(ind)
		}

		data = d.AppendSlice(data, x, m.DataType())
	}
	var (
		outCol *MemCol
		e      error
	)
	if outCol, e = NewMemCol("", data); e != nil {
		return nil, e
	}

	return outCol, nil
}

// ***************** Helpers *****************

func AppendRows(col1, col2 d.Column, name string) (*MemCol, error) {
	if col1.DataType() != col2.DataType() {
		return nil, fmt.Errorf("append columns must have same type, got %s and %s for %s and %s",
			col1.DataType(), col2.DataType(), col1.Name(""), col2.Name(""))
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
		col *MemCol
		e   error
	)
	if col, e = NewMemCol(name, data); e != nil {
		return nil, e
	}

	return col, nil
}

func makeTable(cols ...*MemCol) []*MemCol {
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
	var outCols []*MemCol
	var (
		mCol *MemCol
		e    error
	)
	for c := 0; c < len(cols); c++ {
		if mCol, e = NewMemCol(cols[c].Name(""), outData[c]); e != nil {
			panic(e)
		}

		outCols = append(outCols, mCol)
	}

	if mCol, e = NewMemCol("count", outData[len(cols)]); e != nil {
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
		if cn = cols[ind].(*MemCol).Name(""); cn == "" {
			return nil, fmt.Errorf("column with no name in table")
		}

		colNames = append(colNames, cn)
	}

	return colNames, nil
}

func panicer(cols ...d.Column) {
	for _, c := range cols {
		if _, ok := c.(*MemCol); !ok {
			panic("non-*MemCol argument")
		}
	}
}
