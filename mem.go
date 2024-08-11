package df

import (
	_ "embed"
	"os"
	"time"
)

type categoryMap map[any]uint32

type MemDF struct {
	sourceFileName string
	destFileName   string
	destFile       *os.File
	rows           int

	*DF
}

type MemCol struct {
	name  string
	dType DataTypes
	data  any

	catMap categoryMap
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
