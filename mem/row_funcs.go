package df

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"sort"
	"time"

	d "github.com/invertedv/df"
)

func RunDFfn(fn d.Fn, context *d.Context, inputs []any) (outCol any, err error) {
	info := fn(true, nil)
	if !info.Varying && len(inputs) != len(info.Inputs) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Varying && len(inputs) < len(info.Inputs) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	for j := 0; j < len(inputs); j++ {
		var (
			ok  bool
			col *MemCol
		)

		// fix this up...don't need mod. Use something other than c
		col, ok = inputs[j].(*MemCol)
		if !ok {
			return nil, fmt.Errorf("input to function %s is not a Column", info.Name)
		}

		if j < len(info.Inputs) && info.Inputs[j] != d.DTany && info.Inputs[j] != col.DataType() {
			return nil, fmt.Errorf("incorrect data type to function %s", info.Name)
		}

	}

	var fnR *d.FnReturn
	if fnR = fn(false, context, inputs...); fnR.Err != nil {
		return nil, fnR.Err
	}

	return fnR.Value.(d.Column), nil
}

func RunRowFn(fn d.Fn, context *d.Context, inputs []any) (outCol any, err error) {
	info := fn(true, nil)
	if !info.Varying && len(inputs) != len(info.Inputs) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Varying && len(inputs) < len(info.Inputs) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	var (
		xOut    any
		outType d.DataTypes
	)

	// TODO: think about moving the type check outside of this loop
	n := context.Self().RowCount()
	for ind := 0; ind < n; ind++ {
		var xs []any

		for j := 0; j < len(inputs); j++ {
			var (
				xadd any
				e    error
			)

			// fix this up...don't need mod. Use something other than c
			if cx, ok := inputs[j].(*MemCol); ok {
				if xadd, e = d.ToDataType(cx.Element(ind), info.Inputs[j], false); e != nil {
					return nil, e
				}

				xs = append(xs, xadd)
				continue
			}

			if !info.Varying || (info.Varying && j < len(info.Inputs)) {
				if xadd, e = d.ToDataType(inputs[j], info.Inputs[j], true); e != nil {
					return nil, e
				}
			}

			xs = append(xs, xadd)
		}

		var fnr *d.FnReturn
		if fnr = fn(false, nil, xs...); fnr.Err != nil {
			return nil, fnr.Err
		}

		if ind == 0 {
			outType = fnr.Output
			if info.Output != d.DTany && info.Output != outType {
				return nil, fmt.Errorf("inconsistent function return types: got %s need %s", info.Output, outType)
			}
		}

		var dt d.DataTypes
		if dt = d.WhatAmI(fnr.Value); dt != outType {
			return nil, fmt.Errorf("inconsistent function return types: got %s need %s", dt, outType)
		}

		if dt == d.DTnone {
			continue
		}

		if ind == 0 {
			xOut = d.MakeSlice(outType, 0, nil)
		}

		xOut = d.AppendSlice(xOut, fnr.Value, outType)
	}

	outCol = &MemCol{
		name:   "",
		dType:  outType,
		data:   xOut,
		catMap: nil,
	}

	return outCol, nil
}

func StandardFunctions() d.Fns {
	return d.Fns{
		abs, add, and, applyCat, cast, divide,
		eq, exp, fuzzCat, ge, gt, ifs, le, log, lt,
		multiply, ne, not, or, subtract, sum, toCat,
		toDate, toFloat, toInt, toString}
}

/////////// Standard Fns

func abs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	switch x := inputs[0].(type) {
	case float64:
		return &d.FnReturn{Value: math.Abs(x), Output: d.DTfloat, Err: nil}
	case int:
		if x < 0 {
			return &d.FnReturn{Value: -x, Output: d.DTint, Err: nil}
		}
		return &d.FnReturn{Value: x, Output: d.DTint, Err: nil}
	default:
		return &d.FnReturn{Value: nil, Output: d.DTunknown, Err: fmt.Errorf("abs requires float or int")}
	}
}

func add(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "add", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) + inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) + float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) + inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) + inputs[1].(int), Output: d.DTint, Err: nil}
	case dt0 == d.DTstring:
		if s, e := d.ToString(inputs[1], true); e == nil {
			return &d.FnReturn{Value: inputs[0].(string) + s.(string), Output: d.DTstring, Err: nil}
		}
	case dt1 == d.DTstring:
		if s, e := d.ToString(inputs[0], true); e == nil {
			return &d.FnReturn{Value: s.(string) + inputs[1].(string), Output: d.DTstring, Err: nil}
		}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot add %s and %s", dt0, dt1)}
}

func and(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "and", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}

	val := 0
	if inputs[0].(int) > 0 && inputs[1].(int) > 0 {
		val = 1
	}

	return &d.FnReturn{Value: val, Output: d.DTint}
}

func or(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "or", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}

	val := 0
	if inputs[0].(int) > 0 || inputs[1].(int) > 0 {
		val = 1
	}

	return &d.FnReturn{Value: val, Output: d.DTint}
}

func cast(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	var dt d.DataTypes
	if dt = d.DTFromString(inputs[0].(string)); dt == d.DTunknown {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert to %s", inputs[0].(string))}
	}

	x, e := d.ToDataType(inputs[1], dt, true)
	return &d.FnReturn{Value: x, Output: dt, Err: e}
}

func divide(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "divide", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) / inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) / float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) / inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) / inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot divide %s and %s", dt0, dt1)}
}

func eq(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "eq", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("==", inputs[0], inputs[1])
}

func ge(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "ge", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare(">=", inputs[0], inputs[1])
}

func gt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "gt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare(">", inputs[0], inputs[1])
}

func exp(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	return &d.FnReturn{Value: math.Exp(inputs[0].(float64)), Output: d.DTfloat, Err: nil}
}

func ifs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "if", Inputs: []d.DataTypes{d.DTint, d.DTany, d.DTany}, Output: d.DTany}
	}

	if inputs[0].(int) > 0 {
		return &d.FnReturn{Value: inputs[1], Output: d.WhatAmI(inputs[1])}
	}

	return &d.FnReturn{Value: inputs[2], Output: d.WhatAmI(inputs[2])}
}

func le(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "le", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("<=", inputs[0], inputs[1])
}

func log(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "log", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	x := inputs[0].(float64)
	if x <= 0 {
		return &d.FnReturn{Err: fmt.Errorf("log of non-positive number")}
	}

	return &d.FnReturn{Value: math.Log(x), Output: d.DTfloat, Err: nil}
}

func lt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "lt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("<", inputs[0], inputs[1])
}

func multiply(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "multiply", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) * inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) * float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) * inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) * inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot multiply %s and %s", dt0, dt1)}
}

func ne(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "ne", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("!=", inputs[0], inputs[1])
}

func not(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "not", Inputs: []d.DataTypes{d.DTany, d.DTint}, Output: d.DTint}
	}

	val := 1
	if inputs[1].(int) > 0 {
		val = 0
	}

	return &d.FnReturn{Value: val, Output: d.DTint}
}

func subtract(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "subtract", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) - inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) - float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) - inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) - inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot subtract %s and %s", dt0, dt1)}
}

func makeMCvalue(val any, dt d.DataTypes) *MemCol {
	data := d.MakeSlice(dt, 1, nil)
	switch dt {
	case d.DTfloat:
		data.([]float64)[0] = val.(float64)
	case d.DTint:
		data.([]int)[0] = val.(int)
	case d.DTstring:
		data.([]string)[0] = val.(string)
	case d.DTdate:
		data.([]time.Time)[0] = val.(time.Time)
	}

	var (
		outCol *MemCol
		e      error
	)

	if outCol, e = NewMemCol("", data); e != nil {
		panic("probelm in makeMCvalue")
	}

	return outCol
}

func sum(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sum", Inputs: []d.DataTypes{d.DTany}, Output: d.DTany, DFlevel: true}
	}

	col := inputs[0].(*MemCol)
	dt := col.DataType()
	data := col.Data()
	if !dt.IsNumeric() {
		return &d.FnReturn{Err: fmt.Errorf("input to sum must be numeric, got %v", dt)}
	}

	sf := d.InitAny(dt)

	for ind := 0; ind < col.Len(); ind++ {
		switch col.DataType() {
		case d.DTfloat:
			x := sf.(float64)
			x += data.([]float64)[ind]
			sf = x
		case d.DTint:
			x := sf.(int)
			x += data.([]int)[ind]
			sf = x
		default:
			return &d.FnReturn{Err: fmt.Errorf("invalid type in sum")}
		}
	}

	outCol := makeMCvalue(sf, dt)

	return &d.FnReturn{Value: outCol, Output: dt, Err: nil}
}

// toCat creates a categorical MemCol.
func toCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cat", Inputs: []d.DataTypes{d.DTany}, Output: d.DTcategorical,
			DFlevel: true, Varying: true}
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
			return &d.FnReturn{Err: fmt.Errorf("levels of cat are not the same as the column")}
		}

		levels = append(levels, c.Element(0))
	}

	var (
		outCol *MemCol
		e      error
	)

	if outCol, e = ToCategorical(col, nil, 1, nil, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	outCol.rawType = dt
	outFn := &d.FnReturn{Value: outCol, Output: d.DTcategorical}

	return outFn
}

func fuzzCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "fuzzCat", Inputs: []d.DataTypes{d.DTcategorical, d.DTint, d.DTany}, Output: d.DTcategorical,
			DFlevel: true}
	}

	col := inputs[0].(*MemCol)
	fuzz := inputs[1].(*MemCol).Element(0).(int)
	defCol := inputs[2].(*MemCol)

	if defCol.DataType() != col.RawType() {
		return &d.FnReturn{Err: fmt.Errorf("default not same type as underlying in fuzzCat")}
	}

	defaultVal := defCol.Element(0)

	outCol := fuzzCategorical(col, fuzz, defaultVal)

	return &d.FnReturn{Value: outCol, Output: d.DTcategorical}
}

// applyCat
// - vector to apply cats to
// - vector with cats
// - default if new category
func applyCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: []d.DataTypes{d.DTany, d.DTcategorical, d.DTany},
			Output: d.DTcategorical, DFlevel: true}
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
	_ = defaultValue
	if outCol, e = ToCategorical(newData, oldData.catMap, 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	outCol.rawType = newData.DataType()
	outFn := &d.FnReturn{Value: outCol, Output: d.DTcategorical}

	return outFn
}

func toDate(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "date", Output: d.DTdate, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTdate", inputs[0])
}

func toFloat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "float", Output: d.DTfloat, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTfloat", inputs[0])
}

func toInt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "int", Output: d.DTint, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTint", inputs[0])
}

func toString(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "string", Output: d.DTstring, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTstring", inputs[0])
}

///////// helpers

func compare(condition string, left, right any) *d.FnReturn {
	var truth bool
	ret := &d.FnReturn{Value: int(0), Output: d.DTint}
	if truth, ret.Err = d.Comparator(left, right, condition); truth {
		ret.Value = int(1)
	}

	return ret
}

func ToCategorical(col *MemCol, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (*MemCol, error) {
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
	toMap[defaultVal] = -1
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
	//	catMap[defaultVal] = -1
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
