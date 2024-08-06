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

func (fn *MemFunc) Run(inputs ...any) (outCol Column, err error) {
	if len(inputs) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.name, len(fn.inputs))
	}

	var (
		vals   []*MemCol
		params []any
	)

	for ind := 0; ind < len(inputs); ind++ {
		var (
			col *MemCol
			ok  bool
		)

		if col, ok = inputs[ind].(*MemCol); ok {
			vals = append(vals, col)
		} else {
			params = append(params, inputs[ind])
		}
	}

	var (
		xOut    any
		outType DataTypes
	)
	for ind := 0; ind < vals[0].Len(); ind++ {
		var xs []any

		for j := 0; j < len(params); j++ {
			xadd, e := toDataType(params[j], fn.inputs[j], true)
			if e != nil {
				return nil, e
			}
			xs = append(xs, xadd)
		}

		for j := 0; j < len(vals); j++ {
			xadd, e := toDataType(vals[j].Element(ind), fn.inputs[j+len(params)], false)
			if e != nil {
				return nil, e
			}
			xs = append(xs, xadd)
		}

		x, e := fn.function(xs...)
		if e != nil {
			return nil, e
		}

		if ind == 0 {
			outType = whatAmI(x)
			if fn.output != DTany && fn.output != outType {
				panic("function return not required type")
			}
		}

		if whatAmI(x) != outType {
			panic("inconsistent function return types")
		}

		// or have Run return a type?
		if ind == 0 {
			xOut = makeSlice(outType)
		}

		xOut = appendSlice(xOut, x, outType)
	}

	outCol = &MemCol{
		name:   "",
		dType:  outType,
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
