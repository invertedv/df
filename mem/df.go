package df

import (
	_ "embed"
	"fmt"
	"sort"
	"time"

	u "github.com/invertedv/utilities"

	d "github.com/invertedv/df"
)

type MemDF struct {
	sourceQuery string
	by          []*MemCol

	*d.DFcore
}

type MemCol struct {
	name  string
	dType d.DataTypes
	data  any

	catMap    d.CategoryMap
	catCounts d.CategoryMap
}

// ///////// MemDF

func NewMemDF(runRow, runDF d.RunFn, funcs d.Fns, cols ...*MemCol) (*MemDF, error) {
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

	if df, e = d.NewDF(runRow, runDF, funcs, cc...); e != nil {
		return nil, e
	}

	df.SetContext(d.NewContext(nil, nil, &rowCount, nil))

	outDF := &MemDF{DFcore: df}

	return outDF, nil
}

func DBLoad(qry string, dialect *d.Dialect) (*MemDF, error) {
	var (
		columnNames []string
		columnTypes []d.DataTypes
		e           error
	)

	if columnNames, columnTypes, e = dialect.Types(qry); e != nil {
		return nil, e
	}

	var memData []any
	if memData, e = dialect.Read(qry); e != nil {
		return nil, e
	}

	var memDF *MemDF
	for ind := 0; ind < len(columnTypes); ind++ {
		var col *MemCol

		if col, e = NewMemCol(columnNames[ind], memData[ind]); e != nil {
			return nil, e
		}

		if ind == 0 {
			if memDF, e = NewMemDF(RunRowFn, RunDFfn, StandardFunctions(), col); e != nil {
				return nil, e
			}
			continue
		}

		if e = memDF.AppendColumn(col, false); e != nil {
			return nil, e
		}
	}

	memDF.sourceQuery = qry

	rc := memDF.RowCount()
	memDF.SetContext(d.NewContext(dialect, d.NewFiles(), &rc, nil))

	return memDF, nil
}

func (df *MemDF) SourceQuery() string {
	return df.sourceQuery
}

func (df *MemDF) DBsave(tableName string, overwrite bool, cols ...string) error {
	return nil
}

// AppendColumn masks the DFcore version so that we can handle appending scalars
func (df *MemDF) AppendColumn(col d.Column, replace bool) error {
	colx := col.(*MemCol)
	if colx.Len() == 1 {
		var e error
		dt := col.DataType()
		xs := d.MakeSlice(col.DataType(), 0, nil)
		val := colx.Element(0)
		for ind := 0; ind < df.RowCount(); ind++ {
			xs = d.AppendSlice(xs, val, dt)
		}

		if colx, e = NewMemCol(col.Name(""), xs); e != nil {
			return e
		}
	}

	if ex := df.DFcore.AppendColumn(colx, replace); ex != nil {
		return ex
	}

	return nil
}

func (df *MemDF) Less(i, j int) bool {
	for ind := 0; ind < len(df.by); ind++ {
		less := df.by[ind].Less(i, j)

		// if greater, it's false
		if !less {
			return false
		}

		// if < (rather than <=) it's true
		if df.by[ind].Less(i, j) && !df.by[ind].Less(j, i) {
			return true
		}

		// equal -- keep checking
	}

	return true
}

func (df *MemDF) Swap(i, j int) {
	for h := df.Next(true); h != nil; h = df.Next(false) {
		//		if h.Len() == 1 {
		//			continue
		//		}

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

func (df *MemDF) Sort(cols ...string) error {
	var by []*MemCol

	for ind := 0; ind < len(cols); ind++ {
		var (
			x d.Column
			e error
		)

		if x, e = df.Column(cols[ind]); e != nil {
			return e
		}

		by = append(by, x.(*MemCol))
	}

	df.by = by
	sort.Sort(df)

	return nil
}

func (df *MemDF) RowCount() int {
	return df.Next(true).Len()
}

// Len() is required for sort
func (df *MemDF) Len() int {
	return df.RowCount()
}

func (df *MemDF) Row(rowNum int) []any {
	if rowNum >= df.RowCount() {
		return nil
	}

	var r []any
	for cx := df.Next(true); cx != nil; cx = df.Next(false) {
		var v any
		i := u.MinInt(rowNum, cx.Len()-1)
		switch cx.DataType() {
		case d.DTfloat:
			v = cx.Data().([]float64)[i]
		case d.DTint:
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

func (df *MemDF) FileSave(fileName string) error {
	if e := df.Files().Create(fileName); e != nil {
		return e
	}
	defer func() { _ = df.Files().Close() }()

	df.Files().FieldNames = df.ColumnNames()

	if e := df.Files().WriteHeader(); e != nil {
		return e
	}

	for ind := 0; ind < df.RowCount(); ind++ {
		var row []any
		if row = df.Row(ind); row == nil {
			return fmt.Errorf("unexpected end of MemDF")
		}
		if e := df.Files().WriteLine(row); e != nil {
			return e
		}
	}

	return nil
}

// MakeColumn creates a column of length 1 with data equal to value
func (m *MemDF) MakeColumn(value any) (d.Column, error) {
	var dt d.DataTypes
	if dt = d.WhatAmI(value); dt == d.DTunknown {
		return nil, fmt.Errorf("unsupported data type")
	}

	data := d.MakeSlice(dt, 0, nil)
	data = d.AppendSlice(data, value, dt)

	cx, e := NewMemCol("", data)
	return cx, e
}

func (m *MemDF) Where(indicator d.Column) error {
	if indicator.Len() != m.RowCount() {
		return fmt.Errorf("indicator column wrong length. Got %d needed %d", indicator.Len(), m.RowCount())
	}

	var n int
	for col := m.Next(true); col != nil; col = m.Next(false) {
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
			return fmt.Errorf("no data after applying where")
		}

		cx.data = newData
	}

	m.Context.UpdateLen(n)

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

func (m *MemDF) Tablex(colNames ...string) (d.DF, error) {
	// HERE HERE
	return nil, nil
}

///////////// MemCol

func NewMemCol(name string, data any) (*MemCol, error) {
	var dt d.DataTypes
	if dt = d.WhatAmI(data); dt == d.DTunknown {
		return nil, fmt.Errorf("unsupported data type in NewMemCol")
	}

	c := &MemCol{
		name:   name,
		dType:  dt,
		data:   data,
		catMap: nil,
	}

	return c, nil
}

func (m *MemCol) DataType() d.DataTypes {
	return m.dType
}

func (m *MemCol) Len() int {
	switch m.dType {
	case d.DTfloat:
		return len(m.Data().([]float64))
	case d.DTint:
		return len(m.Data().([]int))
	case d.DTstring:
		return len(m.Data().([]string))
	case d.DTdate:
		return len(m.Data().([]time.Time))
	default:
		return -1
	}
}

func (m *MemCol) Data() any {
	return m.data
}

func (m *MemCol) Name(renameTo string) string {
	if renameTo != "" {
		m.name = renameTo
	}

	return m.name
}

func (m *MemCol) Element(row int) any {
	if m.Len() == 1 {
		row = 0
	}

	switch m.dType {
	case d.DTfloat:
		return m.Data().([]float64)[row]
	case d.DTint:
		return m.Data().([]int)[row]
	case d.DTstring:
		return m.Data().([]string)[row]
	case d.DTdate:
		return m.Data().([]time.Time)[row]
	default:
		panic(fmt.Errorf("unsupported data type in Element"))
	}
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

func (m *MemCol) AppendRows(col2 d.Column) (d.Column, error) {
	return AppendRows(m, col2, m.Name(""))
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

func AppendRows(col1, col2 d.Column, name string) (*MemCol, error) {
	if col1.DataType() != col2.DataType() {
		return nil, fmt.Errorf("append columns must have same type, got %s and %s for %s and %s", col1.DataType(), col2.DataType(), col1.Name(""), col2.Name(""))
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
