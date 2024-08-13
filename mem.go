package df

import (
	_ "embed"
	"os"
	"sort"
	"time"
)

type categoryMap map[any]uint32

type MemDF struct {
	sourceFileName string
	destFileName   string
	destFile       *os.File
	rows           int
	by             []*MemCol

	*DF
}

type MemCol struct {
	name  string
	dType DataTypes
	data  any

	catMap categoryMap
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
	for h := df.head; h != nil; h = h.next {
		data := h.col.(*MemCol).data
		switch h.col.DataType() {
		case DTfloat:
			data.([]float64)[i], data.([]float64)[j] = data.([]float64)[j], data.([]float64)[i]
		case DTint:
			data.([]int)[i], data.([]int)[j] = data.([]int)[j], data.([]int)[i]
		case DTstring:
			data.([]string)[i], data.([]string)[j] = data.([]string)[j], data.([]string)[i]
		case DTdate:
			data.([]time.Time)[i], data.([]time.Time)[j] = data.([]time.Time)[j], data.([]time.Time)[i]
		}
	}
}

func (df *MemDF) Sort(cols ...string) error {
	var by []*MemCol
	for ind := 0; ind < len(cols); ind++ {
		var (
			x Column
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

func (df *MemDF) Len() int {
	return df.head.col.Len()
}

func (m *MemCol) DataType() DataTypes {
	return m.dType
}

func (m *MemCol) Len() int {
	switch m.dType {
	case DTfloat:
		return len(m.Data().([]float64))
	case DTint:
		return len(m.Data().([]int))
	case DTstring:
		return len(m.Data().([]string))
	case DTdate:
		return len(m.Data().([]time.Time))
	}

	return 0
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
	case DTfloat:
		return m.Data().([]float64)[row]
	case DTint:
		return m.Data().([]int)[row]
	case DTstring:
		return m.Data().([]string)[row]
	case DTdate:
		return m.Data().([]time.Time)[row]
	}

	return nil
}

func (m *MemCol) Copy() Column {
	var copiedData any
	n := m.Len()
	switch m.dType {
	case DTfloat:
		copiedData = make([]float64, n)
		copy(copiedData.([]float64), m.data.([]float64))
	case DTint:
		copiedData = make([]int, n)
		copy(copiedData.([]int), m.data.([]int))
	case DTstring:
		copiedData = make([]string, n)
		copy(copiedData.([]string), m.data.([]string))
	case DTdate:
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
	case DTfloat:
		return m.data.([]float64)[i] <= m.data.([]float64)[j]
	case DTint:
		return m.data.([]int)[i] <= m.data.([]int)[j]
	case DTstring:
		return m.data.([]string)[i] <= m.data.([]string)[j]
	case DTdate:
		return !m.data.([]time.Time)[i].After(m.data.([]time.Time)[j])
	}

	panic("error in Less")
}

func MemLoad(from string) ([]Column, error) {
	x := []float64{1, 2, 3}
	y := []float64{4, 5, 6}

	xCol := &MemCol{
		name: "x",
		//		n:      len(x),
		dType:  0,
		data:   x,
		catMap: nil,
	}

	yCol := &MemCol{
		//		n:    len(y),
		name: "y",
		data: y,
	}

	return []Column{xCol, yCol}, nil
}

func MemSave(to string, cols []Column) error {

	return nil
}
