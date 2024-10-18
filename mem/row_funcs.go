package df

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"sort"

	d "github.com/invertedv/df"
)

func RunDFfn(fn d.Fn, context *d.Context, inputs []any) (any, error) {
	info := fn(true, nil)
	if !info.Varying && len(inputs) != len(info.Inputs[0]) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Varying && len(inputs) < len(info.Inputs[0]) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	var (
		inps []any
		cols []*MemCol
	)
	for j := 0; j < len(inputs); j++ {
		var (
			ok  bool
			col *MemCol
		)
		if col, ok = inputs[j].(*MemCol); !ok {
			var e error
			if col, e = NewMemCol("", inputs[j]); e != nil {
				return nil, e
			}
		}

		inps = append(inps, col)
		cols = append(cols, col)
	}

	if ok, _ := okParams(cols, info.Inputs, info.Output); !ok {
		return nil, fmt.Errorf("bad parameters to %s", info.Name)
	}

	var fnR *d.FnReturn
	if fnR = fn(false, context, inps...); fnR.Err != nil {
		return nil, fnR.Err
	}

	//TODO: check return type

	return fnR.Value, nil
}

func StandardFunctions() d.Fns {
	return d.Fns{abs, add, and, applyCat, divide, eq,
		exp, ge, gt, ifs, le, lt, log, mean, multiply,
		ne, not, or, sortDF, subtract, sum, table, toCat,
		toDate, toFloat, toInt, toString, fuzzCat,
		where,
	}
}

// ***************** Functions that return a data frame *****************

func where(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "where", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTdf}}
	}

	var (
		outDF d.DF
		e     error
	)
	outDF, e = context.Self().Where(inputs[0].(d.Column))

	return &d.FnReturn{Value: outDF, Err: e}
}

func sortDF(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sort", Inputs: [][]d.DataTypes{{d.DTstring}},
			Output: []d.DataTypes{d.DTdf}, Varying: true}
	}

	ascending := true
	if inputs[0].(*MemCol).Element(0).(string) == "desc" {
		ascending = false
	}

	var (
		colNames []string
		e        error
	)

	if colNames, e = getNames(1, inputs...); e != nil {
		return &d.FnReturn{Err: e}
	}

	if ex := context.Self().Sort(ascending, colNames...); ex != nil {
		return &d.FnReturn{Err: ex}
	}

	return &d.FnReturn{Value: context.Self()}
}

// TODO: what if I try to do a table on a float?
func table(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "table", Inputs: [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}},
			Output: []d.DataTypes{d.DTdf, d.DTdf, d.DTdf}, Varying: true}
	}

	var (
		colNames []string
		e        error
	)

	if colNames, e = getNames(0, inputs...); e != nil {
		return &d.FnReturn{Err: e}
	}

	var (
		outDF d.DF
		ex    error
	)
	if outDF, ex = context.Self().Table(false, colNames...); ex != nil {
		return &d.FnReturn{Err: ex}
	}

	ret := &d.FnReturn{Value: outDF}

	return ret
}

// ***************** Functions that return a Column *****************

// ***************** Functions that take 1 float and return a float *****************

// realFn applies a func(float)float to the input column
func realFn(fn func(x float64) float64, name string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat}}, Output: []d.DataTypes{d.DTfloat}}
	}

	col, n := parameters(inputs...)
	data := d.MakeSlice(d.DTfloat, n, nil)
	for ind := 0; ind < n; ind++ {
		data.([]float64)[ind] = fn(col[0].Element(ind).(float64))
	}

	return returnCol(data)
}

func exp(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return realFn(math.Exp, "exp", info, context, inputs...)
}

func log(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return realFn(math.Log, "log", info, context, inputs...)
}

// ***************** type conversions *****************

func toFloat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTint}, {d.DTfloat}, {d.DTstring}}
	out := []d.DataTypes{d.DTfloat, d.DTfloat, d.DTfloat}

	return cast("float", in, out, info, context, inputs...)
}

func toInt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTint}, {d.DTfloat}, {d.DTstring}}
	out := []d.DataTypes{d.DTint, d.DTint, d.DTint}

	return cast("int", in, out, info, context, inputs...)
}

func toDate(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTdate}, {d.DTstring}, {d.DTint}}
	out := []d.DataTypes{d.DTdate, d.DTdate, d.DTdate}

	return cast("date", in, out, info, context, inputs...)
}

func toString(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTdate}, {d.DTstring}}
	out := []d.DataTypes{d.DTstring, d.DTstring, d.DTstring, d.DTstring}

	return cast("string", in, out, info, context, inputs...)
}

func cast(name string, in [][]d.DataTypes, out []d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: in, Output: out}
	}

	col := inputs[0].(*MemCol)
	data := d.MakeSlice(out[0], 0, nil)
	for ind := 0; ind < col.Len(); ind++ {
		var (
			x any
			e error
		)
		if x, e = d.ToDataType(col.Element(ind), out[0], true); e != nil {
			return &d.FnReturn{Err: e}
		}

		data = d.AppendSlice(data, x, out[0])
	}

	return returnCol(data)
}

// ***************** Functions that return a scalar *****************

// ***************** Functions that take a single column and return a scalar *****************
func single(name string, fn func(any) any, in [][]d.DataTypes,
	out []d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: in, Output: out}
	}
	col := inputs[0].(*MemCol)

	return returnCol(fn(col))
}

func mean(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	out := []d.DataTypes{d.DTfloat, d.DTfloat}
	return single("mean", meanC, in, out, info, context, inputs...)
}

func meanC(xx any) any {
	x := sumC(xx)
	l := float64(xx.(*MemCol).Len())

	switch d.WhatAmI(x) {
	case d.DTfloat:
		return x.(float64) / l
	case d.DTint:
		return float64(x.(int)) / l
	default:
		panic("cannot find mean")
	}
}

func sum(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	out := []d.DataTypes{d.DTfloat, d.DTint}
	return single("sum", sumC, in, out, info, context, inputs...)
}

func sumC(xx any) any {
	switch x := xx.(*MemCol).Data().(type) {
	case []float64:
		s := 0.0
		for _, xv := range x {
			s += xv
		}

		return s
	case []int:
		s := 0
		for _, xv := range x {
			s += xv
		}

		return s
	default:
		panic("cannot find sum")

	}

	return nil
}

// ***************** Basic arithmetic functions *****************

func arithmetic(op string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: op, Inputs: [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTfloat}, {d.DTstring, d.DTint}},
			Output: []d.DataTypes{d.DTfloat, d.DTint, d.DTfloat, d.DTint}}
	}

	cols, n := parameters(inputs...)

	type floatFn func(a, b float64) float64
	type intFn func(a, b int) int

	floats := []floatFn{
		func(a, b float64) float64 { return a + b },
		func(a, b float64) float64 { return a - b },
		func(a, b float64) float64 { return a * b },
		func(a, b float64) float64 { return a / b },
	}

	ints := []intFn{
		func(a, b int) int { return a + b },
		func(a, b int) int { return a - b },
		func(a, b int) int { return a * b },
		func(a, b int) int { return a / b },
	}

	var dataOut any
	if cols[1].DataType() == d.DTfloat {
		data := make([]float64, n)
		var fn func(a, b float64) float64
		switch op {
		case "add":
			fn = floats[0]
		case "subtract":
			fn = floats[1]
		case "multiply":
			fn = floats[2]
		case "divide":
			fn = floats[3]
		}

		for ind := 0; ind < n; ind++ {
			var x float64
			switch xx := cols[0].Element(ind).(type) {
			case float64:
				x = xx
			default:
				x = 0
			}
			data[ind] = fn(x, cols[1].Element(ind).(float64))
		}

		dataOut = data
	}

	if cols[1].DataType() == d.DTint {
		data := make([]int, n)
		var fn func(a, b int) int
		switch op {
		case "add":
			fn = ints[0]
		case "subtract":
			fn = ints[1]
		case "multiply":
			fn = ints[2]
		case "divide":
			fn = ints[3]
		}

		for ind := 0; ind < n; ind++ {
			var x int
			switch xx := cols[0].Element(ind).(type) {
			case int:
				x = xx
			default:
				x = 0

			}
			data[ind] = fn(x, cols[1].Element(ind).(int))
		}

		dataOut = data
	}

	return returnCol(dataOut)

}

func add(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("add", info, context, inputs...)
}

func subtract(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("subtract", info, context, inputs...)
}

func multiply(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("multiply", info, context, inputs...)
}

func divide(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("divide", info, context, inputs...)
}

// ***************** Other functions *****************

func abs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "abs", Inputs: [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, Output: []d.DataTypes{d.DTfloat, d.DTint}}
	}

	col := inputs[0].(*MemCol)
	data := d.MakeSlice(col.DataType(), col.Len(), nil)
	for ind := 0; ind < col.Len(); ind++ {
		switch col.DataType() {
		case d.DTfloat:
			data.([]float64)[ind] = math.Abs(col.Element(ind).(float64))
		case d.DTint:
			x := col.Element(ind).(int)
			if x < 0 {
				x = -x
			}

			data.([]int)[ind] = x
		}
	}

	return returnCol(data)
}

////////////// logical functions

func logic(a, b int, condition string) int {
	var val int
	switch condition {
	case "and":
		val = 0
		if a > 0 && b > 0 {
			val = 1
		}
	case "or":
		val = 0
		if a > 0 || b > 0 {
			val = 1
		}
	case "not":
		val = 1
		if b >= 1 {
			val = 0
		}
	}

	return val
}

func logical(op string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: op, Inputs: [][]d.DataTypes{{d.DTint, d.DTint}}, Output: []d.DataTypes{d.DTint}}
	}

	cols, n := parameters(inputs...)
	data := d.MakeSlice(d.DTint, n, nil)
	for ind := 0; ind < n; ind++ {
		data.([]int)[ind] = logic(cols[0].Element(ind).(int), cols[1].Element(ind).(int), op)
	}

	return returnCol(data)
}

func and(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return logical("and", info, context, inputs...)
}

func or(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return logical("or", info, context, inputs...)
}

func not(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "not", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTint}}
	}

	cols, n := parameters(inputs...)
	data := d.MakeSlice(d.DTint, n, nil)
	for ind := 0; ind < n; ind++ {
		data.([]int)[ind] = logic(0, cols[0].Element(ind).(int), "not")
	}

	return returnCol(data)
}

func ifs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "if", Inputs: [][]d.DataTypes{{d.DTint, d.DTfloat, d.DTfloat},
			{d.DTint, d.DTint, d.DTint}, {d.DTint, d.DTdate, d.DTdate}, {d.DTint, d.DTstring, d.DTstring}},
			Output: []d.DataTypes{d.DTfloat, d.DTint, d.DTdate, d.DTstring}}
	}

	cols, n := parameters(inputs...)
	dt := cols[1].DataType()
	data := d.MakeSlice(dt, 0, nil)

	for ind := 0; ind < n; ind++ {
		val := cols[2].Element(ind)
		if cols[0].Element(ind).(int) > 0 {
			val = cols[1].Element(ind)
		}

		data = d.AppendSlice(data, val, dt)
	}

	return returnCol(data)
}

// ***************** Functions that compare two columns element-wise *****************

func compare(op, name string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint},
			{d.DTdate, d.DTdate}, {d.DTstring, d.DTstring}}, Output: []d.DataTypes{d.DTfloat, d.DTint,
			d.DTdate, d.DTstring}}
	}

	cols, n := parameters(inputs...)
	data := d.MakeSlice(d.DTint, n, nil)

	for ind := 0; ind < n; ind++ {
		truth := 0

		var (
			val bool
			e   error
		)
		if val, e = d.Comparator(cols[0].Element(ind), cols[1].Element(ind), op); e != nil {
			return &d.FnReturn{Err: e}
		}

		if val {
			truth = 1
		}

		data.([]int)[ind] = truth
	}

	return returnCol(data)
}

func eq(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("==", "eq", info, context, inputs...)
}

func ge(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare(">=", "ge", info, context, inputs...)
}

func gt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare(">", "gt", info, context, inputs...)
}

func le(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("<=", "le", info, context, inputs...)
}

func lt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("<", "lt", info, context, inputs...)
}

func ne(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("!=", "ne", info, context, inputs...)
}

// ***************** Categorical Functions *****************

func toCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cat", Inputs: [][]d.DataTypes{{d.DTstring}, {d.DTint}, {d.DTdate}},
			Output:  []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical},
			Varying: true}
	}

	col := inputs[0].(*MemCol)
	dt := col.DataType()
	if !(dt == d.DTint || dt == d.DTstring || dt == d.DTdate) {
		return &d.FnReturn{Err: fmt.Errorf("cannot make %s into categorical", dt)}
	}

	var levels []any
	for ind := 1; ind < len(inputs); ind++ {
		c := inputs[ind].(*MemCol)
		if c.DataType() != col.DataType() {
			return &d.FnReturn{Err: fmt.Errorf("types of cat are not the same as the column")}
		}

		levels = append(levels, c.Element(0))
	}

	var (
		outCol *MemCol
		e      error
	)

	if outCol, e = toCategorical(col, nil, 1, nil, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	outCol.rawType = dt
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// applyCat
// - vector to apply cats to
// - vector with cats
// - default if new category
// TODO: should the default be an existing category?
func applyCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := inputs[0].(*MemCol)
	oldData := inputs[1].(*MemCol)
	newVal := inputs[2].(*MemCol)

	if newData.DataType() != oldData.rawType {
		return &d.FnReturn{Err: fmt.Errorf("new column must be same type as original data in applyCat")}
	}

	var (
		defaultValue any
		e            error
	)

	if newVal.DataType() != newData.DataType() {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value to correct type in applyCat")}
	}

	defaultValue = newVal.Element(0)

	var levels []any
	for k := range oldData.catMap {
		levels = append(levels, k)
	}

	var outCol *MemCol
	if outCol, e = toCategorical(newData, oldData.catMap, 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	outCol.rawType = newData.DataType()
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

func fuzzCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "fuzzCat", Inputs: [][]d.DataTypes{{d.DTcategorical, d.DTint, d.DTint}, {d.DTcategorical, d.DTint, d.DTstring},
			{d.DTcategorical, d.DTint, d.DTdate}}, Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	col := inputs[0].(*MemCol)
	fuzz := inputs[1].(*MemCol).Element(0).(int)
	defCol := inputs[2].(*MemCol)

	if defCol.DataType() != col.RawType() {
		return &d.FnReturn{Err: fmt.Errorf("default not same type as underlying in fuzzCat")}
	}

	defaultVal := defCol.Element(0)

	outCol := fuzzCategorical(col, fuzz, defaultVal)

	return &d.FnReturn{Value: outCol}
}

// TODO: does this sync with sql version?
// toCategorical
// - levels: list of values to keep, any others get set to default
func toCategorical(col *MemCol, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (*MemCol, error) {
	if col.DataType() == d.DTfloat {
		return nil, fmt.Errorf("cannot make float to categorical")
	}

	nextInt := 0
	for k, v := range catMap {
		if k != nil && d.WhatAmI(k) != col.DataType() {
			return nil, fmt.Errorf("map and column not same data types")
		}

		if v >= nextInt {
			nextInt = v + 1
		}
	}

	toMap := make(d.CategoryMap)
	maps.Copy(toMap, catMap)

	if _, ok := toMap[defaultVal]; !ok {
		toMap[defaultVal] = -1
	}

	cnts := make(d.CategoryMap)

	data := d.MakeSlice(d.DTint, 0, nil)
	for ind := 0; ind < col.Len(); ind++ {
		inVal := col.Element(ind)
		if levels != nil && !d.In(inVal, levels) {
			inVal = defaultVal
		}

		cnts[inVal]++

		if mapVal, ok := toMap[inVal]; ok {
			data = d.AppendSlice(data, mapVal, d.DTint)
			continue
		}

		toMap[inVal] = nextInt
		data = d.AppendSlice(data, nextInt, d.DTint)
		nextInt++
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

	if fuzz > 1 {
		outCol = fuzzCategorical(outCol, fuzz, defaultVal)
	}

	return outCol, nil
}

func fuzzCategorical(col *MemCol, fuzzValue int, defaultVal any) *MemCol {
	catMap := make(d.CategoryMap)
	catCounts := make(d.CategoryMap)
	data := make([]int, col.Len())
	copy(data, col.Data().([]int))
	consVals := []int{-1} // map values to keep

	for k, v := range col.catMap {
		if col.catCounts[k] >= fuzzValue {
			catMap[k] = v
			catCounts[k] = col.catCounts[k]
			consVals = append(consVals, v)
			continue
		}

		catCounts[defaultVal]++
	}

	sort.Ints(consVals)

	for ind, x := range col.Data().([]int) {
		checkVal := x
		if indx := sort.SearchInts(consVals, checkVal); indx == len(consVals) || consVals[indx] != checkVal {
			data[ind] = -1
		}
	}

	var (
		outCol *MemCol
		e      error
	)
	if outCol, e = NewMemCol("", data); e != nil {
		panic(e) // should not happen
	}

	outCol.catMap, outCol.catCounts, outCol.dType = catMap, catCounts, d.DTcategorical

	return outCol
}

// ***************** Helpers *****************

func parameters(inputs ...any) (cols []*MemCol, n int) {
	n = 1
	for j := 0; j < len(inputs); j++ {
		cx := inputs[j].(*MemCol)
		cols = append(cols, cx)

		if nn := cx.Len(); nn > n {
			n = nn
		}
	}

	return cols, n
}

func returnCol(data any) *d.FnReturn {
	var (
		outCol *MemCol
		e      error
	)

	if outCol, e = NewMemCol("", data); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outCol}
}

// TODO: change function inputs to Column from any
// TODO: check same length?
func okParams(cols []*MemCol, inputs [][]d.DataTypes, outputs []d.DataTypes) (ok bool, outType d.DataTypes) {
	for j := 0; j < len(inputs); j++ {
		ok = true
		for k := 0; k < len(inputs[j]); k++ {
			if cols[k].DataType() != inputs[j][k] {
				ok = false
				break
			}
		}

		if ok {
			return true, outputs[j]
		}
	}

	return false, d.DTunknown
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
