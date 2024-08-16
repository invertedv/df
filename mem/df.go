package df

import (
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"sort"
	"time"

	d "github.com/invertedv/df"
	s "github.com/invertedv/df/sql"
)

type MemDF struct {
	sourceFileName string
	destFileName   string
	destFile       *os.File
	sourceQuery    string
	by             []*MemCol

	rowCount int

	*d.DFcore
}

type MemCol struct {
	name  string
	dType d.DataTypes
	data  any

	catMap d.CategoryMap
}

/////////// MemDF

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

	outDF := &MemDF{DFcore: df, rowCount: cols[0].Len()}

	return outDF, nil
}

func LoadSQL(qry string, db *sql.DB) (*MemDF, error) {
	var (
		df *s.SQLdf
		e  error
	)
	if df, e = s.NewSQLdf(qry, db); e != nil {
		return nil, e
	}

	columnNames := df.ColumnNames()
	columnTypes := df.ColumnTypes()

	var (
		rows *sql.Rows
		err  error
	)
	if rows, err = db.Query(qry); err != nil {
		return nil, err
	}

	r := make([]any, df.ColumnCount())
	for ind := range r {
		var x any
		r[ind] = &x
	}

	var memData []any
	for ind := 0; ind < len(columnTypes); ind++ {
		memData = append(memData, d.MakeSlice(columnTypes[ind], df.RowCount()))
	}

	xind := 0
	for rows.Next() {
		var rx []any
		for ind := 0; ind < len(columnTypes); ind++ {
			rx = append(rx, d.Address(memData[ind], df.ColumnTypes()[ind], xind))
		}

		xind++
		if e := rows.Scan(rx...); e != nil {
			return nil, e
		}

	}

	var memDF *MemDF
	for ind := 0; ind < len(columnTypes); ind++ {
		var (
			col *MemCol
			e   error
		)

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

	return memDF, nil
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
	return df.rowCount
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
	}

	return -1
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
	}

	return nil
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
	}

	panic("error in Less")
}
