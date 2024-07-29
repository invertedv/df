package df

import (
	_ "embed"
	"fmt"
	"math"
	"time"
)

//go:embed funcs/funcs.txt
var functions string

type Function struct {
	name     string
	inputs   []DataTypes
	output   DataTypes
	function AnyFunction
}

func (fn Function) Check(cols ...Column) error {

	return nil
}

func (fn Function) Run(cols ...Column) (outCol Column, err error) {
	if len(cols) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(cols), fn.name, len(fn.inputs))
	}

	xOut := makeSlice(fn.output)

	for ind := 0; ind < cols[0].N(); ind++ {
		var xs []any
		for j := 0; j < len(cols); j++ {
			xadd, e := toDataType(cols[j].Element(ind), fn.inputs[j], true)
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

	outCol = &Memory{
		name:   "",
		n:      cols[0].N(),
		dType:  fn.output,
		data:   xOut,
		catMap: nil,
	}

	return outCol, nil
}

type categoryMap map[any]uint32

type FunctionMap map[string]Function

type AnyFunction func(...any) (any, error)

type Memory struct {
	name  string
	n     int
	dType DataTypes
	data  any

	catMap categoryMap
}

func (mem *Memory) DataType() DataTypes {
	return mem.dType
}

func (mem *Memory) N() int {
	return mem.n
}

func (mem *Memory) Data() any {
	return mem.data
}

func (mem *Memory) Name(newName string) string {
	if newName != "" {
		mem.name = newName
	}

	return mem.name
}

func (mem *Memory) To(dt DataTypes) (out any, err error) {
	return SliceToDataType(mem, dt, false)
}

func (mem *Memory) Element(row int) any {
	switch mem.dType {
	case DTfloat:
		return mem.Data().([]float64)[row]
	case DTint:
		return mem.Data().([]int)[row]
	case DTstring:
		return mem.Data().([]string)[row]
	case DTdate:
		return mem.Data().([]time.Time)[row]
	}

	return nil
}

func addFloat(inputs ...any) (any, error) {
	return inputs[0].(float64) + inputs[1].(float64), nil
}

func addInt(inputs ...any) (any, error) {
	return inputs[0].(int) + inputs[1].(int), nil
}

func exp(xs ...any) (any, error) {
	return math.Exp(xs[0].(float64)), nil
}

func MemLoad(from string) ([]Column, error) {
	x := []float64{1, 2, 3}
	y := []float64{4, 5, 6}

	xCol := &Memory{
		name:   "x",
		n:      len(x),
		dType:  0,
		data:   x,
		catMap: nil,
	}

	yCol := &Memory{
		n:    len(y),
		name: "y",
		data: y,
	}

	return []Column{xCol, yCol}, nil
}

func MemSave(to string, cols []Column) error {

	return nil
}

func LoadFunctions() FunctionMap {
	fn := make(FunctionMap)
	fn["addFloat"] = Function{
		name:     "addFloat",
		inputs:   []DataTypes{DTfloat, DTfloat},
		output:   DTfloat,
		function: addFloat,
	}

	fn["exp"] = Function{
		name:     "exp",
		inputs:   []DataTypes{DTfloat},
		output:   DTfloat,
		function: exp,
	}

	return fn
}

var Functions = LoadFunctions()
