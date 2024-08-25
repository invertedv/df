package df

import (
	_ "embed"
	"fmt"
	"sort"
	"time"

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

	catMap d.CategoryMap
}

// ///////// MemDF
// TODO: incorporate context with length.  Allow lengths of n or 1
func NewMemDF(run d.RunFunc, funcs d.Functions, cols ...*MemCol) (*MemDF, error) {
	rowCount := cols[0].Len()
	var cc []d.Column
	for ind := 0; ind < len(cols); ind++ {
		if cols[ind].Len() != rowCount {
			return nil, fmt.Errorf("all MemCols must have same length")
		}

		cc = append(cc, cols[ind])
	}

	var (
		df *d.DFcore
		e  error
	)

	if df, e = d.NewDF(run, funcs, cc...); e != nil {
		return nil, e
	}

	df.SetContext(d.NewContext(nil, rowCount, nil))

	outDF := &MemDF{DFcore: df}

	return outDF, nil
}

// TODO: look at RowCount and fix for 1 or n lengths
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
			if memDF, e = NewMemDF(Run, StandardFunctions(), col); e != nil {
				return nil, e
			}
			continue
		}

		if e = memDF.AppendColumn(col); e != nil {
			return nil, e
		}
	}

	memDF.sourceQuery = qry
	memDF.Dialect = dialect
	memDF.SetContext(d.NewContext(dialect, memDF.RowCount(), nil))

	return memDF, nil
}

func (df *MemDF) SourceQuery() string {
	return df.sourceQuery
}

func (df *MemDF) DBsave(tableName string, overwrite bool, cols ...string) error {
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
		if h.Len() == 1 {
			continue
		}

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
	n := 0
	for c := df.Next(true); c != nil; c = df.Next(false) {
		if m := c.Len(); m > n {
			n = m
		}
	}

	return n
}

// Len() is required for sort
func (df *MemDF) Len() int {
	return df.RowCount()
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
