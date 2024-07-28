package df

import (
	_ "embed"
	"fmt"
	"math"
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

func (fn Function) Run(xs ...any) (any, error) {

	return fn.function(xs)
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

func (mem *Memory) Name() string {
	return mem.name
}

func (mem *Memory) To(dt DataTypes) (out any, err error) {
	switch dt {
	case DTdouble:
		return SliceToDouble(mem.Data())
	case DTinteger:
		return SliceToInt(mem.Data())
	case DTdate:
		return SliceToDate(mem.Data())
	case DTchar:
		return SliceToString(mem.Data())
	}

	return nil, fmt.Errorf("cannot convert column %s from %v to %v", mem.Name(), mem.DataType(), dt)
}

func (mem *Memory) Element(row int) any {
	switch mem.dType {
	case DTdouble:
		return mem.data.([]float64)[row]
	case DTinteger:
		return mem.data.([]int)[row]
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
		inputs:   []DataTypes{DTdouble, DTdouble},
		output:   DTdouble,
		function: addFloat,
	}

	fn["exp"] = Function{
		name:     "exp",
		inputs:   []DataTypes{DTdouble},
		output:   DTdouble,
		function: exp,
	}

	return fn
}

var Functions = LoadFunctions()

func makeSlice(dt DataTypes) any {
	var xout any
	switch dt {
	case DTdouble:
		xout = make([]float64, 0)
	case DTinteger:
		xout = make([]int, 0)
	}

	return xout
}

func appendSlice(x, xadd any, dt DataTypes) any {
	switch dt {
	case DTdouble:
		x = append(x.([]float64), xadd.(float64))
	case DTinteger:
		x = append(x.([]int), xadd.(int))

	}

	return x
}

func MemOp(resultName, op string, cols ...Column) (out Column, err error) {
	fn := Functions[op].function

	xout := makeSlice(Functions[op].output)

	for ind := 0; ind < cols[0].N(); ind++ {
		var xs []any
		for j := 0; j < len(cols); j++ {
			xs = append(xs, cols[j].Element(ind))
		}

		x, e := fn(xs...)
		if e != nil {
			return nil, e
		}

		xout = appendSlice(xout, x, Functions[op].output)
	}

	out = &Memory{
		name:   resultName,
		n:      cols[0].N(),
		dType:  DTdouble,
		data:   xout,
		catMap: nil,
	}

	return out, nil
}

/*
func add2F1F(x, y float64) float64 {
	return x + y
}

func ToSlices(types []DataTypes, cols []Column) (data []any, err error) {
	if len(types) != len(cols) {
		return nil, fmt.Errorf("# of inputs incorrect")
	}

	for ind := 0; ind < len(types); ind++ {
		d, e := cols[ind].To(types[ind])
		if e != nil {
			return nil, e
		}
		data = append(data, d)
	}

	return data, nil
}

func LoadFuncsX() (fns []any, fnNames, fnTypes []string) {
	fns, fnNames, fnTypes = append(fns, math.Exp), append(fnNames, "exp"), append(fnTypes, "FF")
	fns, fnNames, fnTypes = append(fns, math.Abs), append(fnNames, "abs"), append(fnTypes, "FF")
	fns, fnNames, fnTypes = append(fns, addFloat), append(fnNames, "addFloat"), append(fnTypes, "FFF")
	fns, fnNames, fnTypes = append(fns, addInt), append(fnNames, "addInt"), append(fnTypes, "III")
	fmt.Println(functions)
	return fns, fnNames, fnTypes
}

func LoadFuncIO(funcTypes []string) (fnInputs [][]DataTypes, fnOutput []DataTypes) {

	return fnInputs, fnOutput
}

var (
	//FuncNames = []string{"exp", "abs", "addFloat", "addInt"}
	//	FuncTypes  = []string{"1FF", "1FF", "2FF", "2II"}
	FuncInputs                  = [][]DataTypes{{DTdouble}, {DTdouble}, {DTdouble, DTdouble}, {DTinteger, DTinteger}}
	FuncOutput                  = []DataTypes{DTdouble, DTdouble, DTdouble, DTinteger}
	Funcs, FuncNames, FuncTypes = LoadFuncsX() // Move to an Init or something...
)

func Oper(resultName, op string, cols ...Column) (Column, error) {
	indx := utilities.Position(op, "", FuncNames...)
	if indx < 0 {
		return nil, fmt.Errorf("no such op: %s", op)
	}

	var outData any
	switch FuncOutput[indx] {
	case DTdouble:
		outData = make([]float64, cols[0].N())
	case DTinteger:
		outData = make([]int, cols[0].N())
	}

	for ind := 0; ind < int(cols[0].N()); ind++ {
		xs, e := ToSlices(FuncInputs[indx], cols)
		if e != nil {
			return nil, e
		}

		switch FuncTypes[indx] {
		case "1FF":
			outData.([]float64)[ind] = Funcs[indx].(func(float64) float64)(xs[0].([]float64)[ind])
		case "2FF":
			outData.([]float64)[ind] = Funcs[indx].(func(float64, float64) float64)(xs[0].([]float64)[ind], xs[1].([]float64)[ind])
		case "2II":
			outData.([]int)[ind] = Funcs[indx].(func(int, int) int)(xs[0].([]int)[ind], xs[1].([]int)[ind])
		}
	}

	col := &Memory{
		name:   resultName,
		n:      cols[0].N(),
		dType:  FuncOutput[indx],
		data:   outData,
		catMap: nil,
	}

	return col, nil
}

var (
	Funcs1F1F = []func(x float64) float64{math.Abs, math.Exp}
	Names1F1F = []string{"abs", "exp"}
	Funcs2F1F = []func(x, y float64) float64{add2F1F}
	Names2F1F = []string{"add"}

	FuncNamesOld = []string{"abs", "exp", "add"}
	FuncTypesOld = []string{"1F1F", "1F1F", "2F1F"}
)

func Func2F1F(funcName string) (func(x, y float64) float64, error) {
	var ind int
	if ind = utilities.Position(funcName, "", Names2F1F...); ind < 0 {
		return nil, fmt.Errorf("function %s not found", funcName)
	}

	return Funcs2F1F[ind], nil
}

func Func1F1F(funcName string) (func(x float64) float64, error) {
	var ind int
	if ind = utilities.Position(funcName, "", Names1F1F...); ind < 0 {
		return nil, fmt.Errorf("function %s not found", funcName)
	}

	return Funcs1F1F[ind], nil
}

func MemAdd(resultName string, cols ...Column) (out Column, err error) {
	var (
		x   []float64
		tmp any
	)

	if tmp, err = cols[0].To(DTdouble); err != nil {
		return nil, err
	}

	x = tmp.([]float64)

	for ind := 1; ind < len(cols); ind++ {
		var y []float64
		if tmp, err = cols[ind].To(DTdouble); err != nil {
			return nil, err
		}

		y = tmp.([]float64)
		for j, yVal := range y {
			x[j] += yVal
		}
	}

	out = &Memory{
		name:   resultName,
		n:      len(x),
		dType:  DTdouble,
		data:   x,
		catMap: nil,
	}

	return out, nil
}

func MemAddX(resultName string, cols ...Column) (out Column, err error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns to Add")
	}

	var (
		//		xDouble []float64
		//		xInt    []int
		outData any
		dt      DataTypes
	)

	if _, ok := cols[0].(*Memory); !ok {
		return nil, fmt.Errorf("not *Memory in MemAdd")
	}

	n := cols[0].N()
	dt = cols[0].DataType()

	xOut := makeAny(n, dt)
	var e error
	if xOut, e = copyAny(xOut, cols[0].Data(), dt); e != nil {
		return nil, fmt.Errorf("oops")
	}

	for ind := 1; ind < len(cols); ind++ {
		xx, ex := convertAny(cols[1].Data(), dt)
		if ex != nil {
			return nil, fmt.Errorf("oh oh")
		}
		if xOut, e = addAny(xOut, xx, dt); e != nil {
			return nil, e
		}
	}

	out = &Memory{
		name:   resultName,
		n:      n,
		dType:  dt,
		data:   outData,
		catMap: nil,
	}

	return out, nil
}

func makeAny(n int, dt DataTypes) any {
	switch dt {
	case DTdouble:
		return make([]float64, n)
	default:
		return nil
	}
}

func copyAny(x, y any, dt DataTypes) (out any, err error) {
	switch dt {
	case DTdouble:
		xx := x.([]float64)
		copy(xx, y.([]float64))
		return xx, nil
	default:
		return nil, fmt.Errorf("oh oh")
	}
}

func convertAny(x any, dt DataTypes) (out any, err error) {
	switch dt {
	case DTdouble:
		xx, ok := x.([]float64)
		if !ok {
			// try from int & chart
			return nil, fmt.Errorf("oh oh")
		}
		return xx, nil
	default:
		return nil, fmt.Errorf("oh oh")
	}
}

func addAny(x, y any, dt DataTypes) (out any, err error) {
	switch dt {
	case DTdouble:
		xx, yy := x.([]float64), y.([]float64)
		for ind := 0; ind < len(xx); ind++ {
			xx[ind] += yy[ind]
		}
		return xx, nil
	default:
		return nil, fmt.Errorf("oh oh")
	}
}


*/
