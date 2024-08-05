package df

import (
	_ "embed"
	"fmt"
	"os"
	"time"
)

type MemFunc struct {
	name     string
	inputs   []DataTypes
	output   DataTypes
	function AnyFunction
}

func Col2MemCol(cols ...Column) (mem []*MemCol, err error) {
	var vals []*MemCol
	for j := 0; j < len(cols); j++ {
		val, ok := cols[j].(*MemCol)
		if !ok {
			return nil, fmt.Errorf("not *MemCol type")
		}

		vals = append(vals, val)
	}

	return vals, nil
}

func (fn *MemFunc) Run(cols ...Column) (outCol Column, err error) {
	if len(cols) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(cols), fn.name, len(fn.inputs))
	}

	xOut := makeSlice(fn.output)
	var vals []*MemCol
	if vals, err = Col2MemCol(cols...); err != nil {
		return nil, err
	}

	for ind := 0; ind < vals[0].Len(); ind++ {
		var xs []any
		for j := 0; j < len(vals); j++ {
			xadd, e := toDataType(vals[j].Element(ind), fn.inputs[j], false)
			if e != nil {
				return nil, e
			}
			xs = append(xs, xadd)
		}

		x, e := fn.function(xs...)
		if e != nil {
			return nil, e
		}

		xOut = appendSlice(xOut, x, fn.output)
	}

	outCol = &MemCol{
		name: "",
		//		n:      cols[0].N(),
		dType:  fn.output,
		data:   xOut,
		catMap: nil,
	}

	return outCol, nil
}

type categoryMap map[any]uint32

type MemFuncMap map[string]*MemFunc

type AnyFunction func(...any) (any, error)

type MemCol struct {
	name  string
	dType DataTypes
	data  any

	catMap categoryMap
}

type MemDF struct {
	sourceFileName string
	destFileName   string
	destFile       *os.File
	rows           int

	*DFlist
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

func (m *MemCol) Cast(dt DataTypes) (out any, err error) {
	return SliceToDataType(m, dt, true)
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
