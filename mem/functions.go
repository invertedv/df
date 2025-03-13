package df

// Learning: converting output from any to <type> takes a long time

import (
	_ "embed"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"

	d "github.com/invertedv/df"
)

type frameTypes interface {
	float64 | int | string | time.Time
}

var (
	//go:embed data/functions.txt
	functions string
)

func printFn[T frameTypes](a []T) {
	fmt.Println(a[0])
}

// each function here must have an entry in functions.txt
func rawFuncs() []any {
	fns := []any{rowNumberFn,
		isInfFn, isNaNfn,
		floatFn[float64], floatFn[int], floatFn[string],
		intFn[float64], intFn[int], intFn[string],
		stringFn[float64], stringFn[int], stringFn[string], stringFn[time.Time],
		dateFn[int], dateFn[string], dateFn[time.Time],
		negFn[float64], negFn[int],
		addFn[float64], addFn[int],
		subtractFn[float64], subtractFn[int],
		multiplyFn[float64], multiplyFn[int],
		divideFn[float64], divideFn[int],
		absFn[float64], absFn[int],
		andFn, orFn, notFn,
		gtFn[float64], gtFn[int], gtFn[string], gtFn[time.Time],
		ltFn[float64], ltFn[int], ltFn[string], ltFn[time.Time],
		geFn[float64], geFn[int], geFn[string], geFn[time.Time],
		leFn[float64], leFn[int], leFn[string], leFn[time.Time],
		eqFn[float64], eqFn[int], eqFn[string], eqFn[time.Time],
		neFn[float64], neFn[int], neFn[string], neFn[time.Time],
		maxFn[float64], maxFn[int], maxFn[string], maxFn[time.Time],
		minFn[float64], minFn[int], minFn[string], minFn[time.Time],
		ifFn[float64], ifFn[int], ifFn[string], ifFn[time.Time],
		elemFn[float64], elemFn[int], elemFn[string], elemFn[time.Time],
		math.Exp, math.Log,
		quantileFn[float64], quantileFn[int],
		lqFn[float64], lqFn[int],
		medianFn[float64], medianFn[int],
		uqFn[float64], uqFn[int],
		meanFn[float64], meanFn[int],
		sumFn[float64], sumFn[int],
		countFn[float64], countFn[int], countFn[string], countFn[time.Time],
		printFn[float64], printFn[int], printFn[string], printFn[time.Time],
	}

	return fns
}

func vectorFunctions() d.Fns {
	specs := d.LoadFunctions(functions)
	fns := rawFuncs()

	for _, spec := range specs {
		for _, fn := range fns {
			fc := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
			fnname := fc.Name()
			if fnname != spec.FnDetail {
				continue
			}

			spec.Fns = append(spec.Fns, fn)
		}
	}

	var outFns d.Fns
	for _, spec := range specs {
		fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
			if info {
				return &d.FnReturn{Name: spec.Name, Inputs: spec.Inputs, Output: spec.Outputs, IsScalar: spec.IsScalar}
			}

			fnUse := spec.Fns[0]

			var ind int

			n := df.RowCount()
			if spec.Inputs != nil {
				n = loopDim(inputs...)
				ind = signature(spec.Inputs, inputs)
				if ind < 0 {
					panic("no signature")
				}
				fnUse = fnToUse(spec.Fns, spec.Inputs[ind], spec.Outputs[ind])
			}

			// scalar returns take the whole vector as inputs
			if spec.IsScalar {
				n = 1
			}

			var oas any

			outVec := d.MakeVector(spec.Outputs[ind], n)
			oas = outVec.AsAny()

			if e := level0(oas, fnUse, inputs); e != nil {
				return &d.FnReturn{Err: e}
			}

			return returnCol(outVec)
		}

		outFns = append(outFns, fn)
	}

	return outFns
}

func GetKind(fn reflect.Type) d.DataTypes {
	switch fn.Kind() {
	case reflect.Pointer:
		return d.DTunknown
	case reflect.Float64:
		return d.DTfloat
	case reflect.Int:
		return d.DTint
	case reflect.String:
		return d.DTstring
	case reflect.Struct:
		if fn == reflect.TypeOf(time.Time{}) {
			return d.DTdate
		}

		return d.DTunknown
	case reflect.Slice:
		return GetKind(fn.Elem())
	default:
		return d.DTunknown
	}
}

// fnToUse chooses the element of fns (slice of functions) that matches the pattern of inputs in targetIns and
// output of targOut.
func fnToUse(fns []any, targetIns []d.DataTypes, targOut d.DataTypes) any {
	for _, fn := range fns {
		rfn := reflect.TypeOf(fn)
		ok := true
		for ind := range rfn.NumIn() {
			if GetKind(rfn.In(ind)) != targetIns[ind] {
				ok = false
				break
			}
		}

		if ok && rfn.NumOut() > 0 && GetKind(rfn.Out(0)) == targOut {
			return fn
		}
	}

	return nil
}

// **************** Vector Functions ****************

func elemFn[T frameTypes](x []T, ind []int) (T, error) {
	if ind[0] < 0 || ind[0] > len(x) {
		return x[0], fmt.Errorf("index out of range")
	}

	return x[ind[0]], nil
}

func quantileFn[T float64 | int](x []T, p []float64) float64 {
	var y any = x
	if xFlt, ok := y.([]float64); ok {
		if sort.Float64sAreSorted(xFlt) {
			return stat.Quantile(p[0], stat.LinInterp, xFlt, nil)
		}

		vSort := make([]float64, len(x))
		copy(vSort, xFlt)
		sort.Float64s(vSort)
		return stat.Quantile(p[0], stat.LinInterp, vSort, nil)
	}

	xFlt := make([]float64, len(x))
	for ind, xx := range x {
		xFlt[ind] = float64(xx)
	}

	return quantileFn(xFlt, p)
}

func lqFn[T float64 | int](a []T) float64 {
	return quantileFn(a, []float64{0.25}) // had quantileFn[T]
}

func medianFn[T float64 | int](a []T) float64 {
	return quantileFn(a, []float64{0.5}) // had quantileFn[T]
}

func uqFn[T float64 | int](a []T) float64 {
	return quantileFn(a, []float64{0.75}) // had quantileFn[T]
}

func maxFn[T frameTypes](a []T) T {
	maxVal := a[0]
	for _, val := range a {
		if greater(val, maxVal) {
			maxVal = val
		}
	}

	return maxVal
}

func minFn[T frameTypes](a []T) T {
	minVal := a[0]
	for _, val := range a {
		if greater(minVal, val) {
			minVal = val
		}
	}

	return minVal
}

func negFn[T float64 | int](a T) T {
	return -a
}

func addFn[T float64 | int](a, b T) T {
	return a + b
}

func subtractFn[T float64 | int](a, b T) T { return a - b }

func multiplyFn[T float64 | int](a, b T) T { return a * b }

func divideFn[T float64 | int](a, b T) (T, error) {
	if b != 0 {
		return a / b, nil
	}

	return 0, fmt.Errorf("divide by 0")
}

func andFn(a, b int) int {
	if a > 0 && b > 0 {
		return 1
	}

	return 0
}

func orFn(a, b int) int {
	if a > 0 || b > 0 {
		return 1
	}

	return 0
}

func notFn(a int) int {
	return 1 - a
}

func rowNumberFn(ind int) int {
	return ind
}

func absFn[T float64 | int](a T) T {
	if a >= 0 {
		return a
	}

	return -a
}

func bToI(a bool) int {
	if a {
		return 1
	}

	return 0
}

func greater(a, b any) bool {
	switch v := a.(type) {
	case float64:
		return v > b.(float64)
	case int:
		return v > b.(int)
	case string:
		return v > b.(string)
	case time.Time:
		return v.After(b.(time.Time))
	}

	return false
}

func gtFn[T frameTypes](a, b T) int { return bToI(greater(a, b)) }

func ltFn[T frameTypes](a, b T) int { return bToI(greater(b, a)) }

func geFn[T frameTypes](a, b T) int { return bToI(!greater(b, a)) }

func leFn[T frameTypes](a, b T) int { return bToI(!greater(a, b)) }

func eqFn[T frameTypes](a, b T) int { return bToI(a == b) }

func neFn[T frameTypes](a, b T) int { return bToI(a != b) }

func ifFn[T frameTypes](a int, b, c T) T {
	if a == 1 {
		return b
	}

	return c
}

func isInfFn(x float64) int {
	if math.IsInf(x, 0) || math.IsInf(x, 1) {
		return 1
	}

	return 0
}

func isNaNfn(x float64) int {
	if math.IsNaN(x) {
		return 1
	}

	return 0
}

func floatFn[T float64 | int | string](x T) (float64, error) {
	var xx any = x
	switch v := xx.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	}

	return 0, fmt.Errorf("cannot convert to float")
}

func intFn[T float64 | int | string](x T) (int, error) {
	var xx any = x
	switch v := xx.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case string:
		var (
			xo int64
			e  error
		)
		if xo, e = strconv.ParseInt(v, 10, 64); e != nil {
			return 0, e
		}

		return int(xo), nil
	}

	return 0, fmt.Errorf("cannot convert to int")
}

func stringFn[T frameTypes](x T) (string, error) {
	var xx any = x
	switch v := xx.(type) {
	case float64:
		return fmt.Sprintf("%v", v), nil
	case int:
		return fmt.Sprintf("%d", v), nil
	case string:
		return v, nil
	case time.Time:
		return v.Format("2006-01-02"), nil
	}

	return "", fmt.Errorf("cannot convert to string")
}

func dateFn[T int | string | time.Time](x T) (time.Time, error) {
	var xx any = x
	switch v := xx.(type) {
	case int:
		vs := fmt.Sprintf("%d", v)
		return dateFn(vs)
	case string:
		for _, fmtx := range d.DateFormats {
			if dt, e := time.Parse(fmtx, strings.ReplaceAll(v, "'", "")); e == nil {
				return dt, nil
			}
		}

		return time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), fmt.Errorf("date conversion failed: %s", v)
	case time.Time:
		return v, nil
	}

	return time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
		fmt.Errorf("cannot convert to date")
}

func sumFn[T float64 | int](x []T) (T, error) {
	var total T = 0
	for _, xVal := range x {
		total += xVal
	}

	return total, nil
}

func meanFn[T float64 | int](x []T) (float64, error) {
	var xx any = x
	switch v := xx.(type) {
	case []float64:
		return stat.Mean(v, nil), nil
	case []int:
		s, _ := sumFn(x)
		return float64(s) / float64(len(v)), nil
	}

	return 0, fmt.Errorf("error in mean")
}

func countFn[T frameTypes](x []T) int {
	return len(x)
}

func global(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		inTypes := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTdate}, {d.DTcategorical}}
		outTypes := []d.DataTypes{d.DTfloat, d.DTint, d.DTstring, d.DTdate, d.DTcategorical}
		return &d.FnReturn{Name: "global", Inputs: inTypes, Output: outTypes, IsScalar: false}
	}

	// if there is a SourceDF, get the data from there.
	if df.SourceDF() != nil {
		var col d.Column
		name := inputs[0].(d.Column).Name()
		if col = df.SourceDF().Column(name); col == nil {
			return &d.FnReturn{Err: fmt.Errorf("no such column in sourceDF: %s", name)}
		}

		return &d.FnReturn{Value: col}
	}

	return &d.FnReturn{Value: inputs[0]}
}

// ***************** Categorical Operations *****************

func toCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cat", Inputs: [][]d.DataTypes{{d.DTstring}, {d.DTint}, {d.DTdate}},
			Output:  []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical},
			Varying: true}
	}

	col := inputs[0].(*Col)
	dt := col.DataType()
	if !(dt == d.DTint || dt == d.DTstring || dt == d.DTdate) {
		return &d.FnReturn{Err: fmt.Errorf("cannot make %s into categorical", dt)}
	}

	fuzz := 1
	if len(inputs) > 1 {
		c := toCol(inputs[1])
		if c.DataType() != d.DTint {
			return &d.FnReturn{Err: fmt.Errorf("fuzz parameter to Cat must be type int")}
		}
		f, _ := c.ElementInt(0)
		fuzz = *f
	}

	var (
		outCol d.Column
		e      error
	)

	if outCol, e = df.Categorical(col.Name(), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(dt)(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// applyCat
// - vector to apply cats to
// - vector with cats
// - default if new category
func applyCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := toCol(inputs[0])
	oldData := toCol(inputs[1])
	newVal := toCol(inputs[2])

	if newData.DataType() != oldData.RawType() {
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
	if _, ok := oldData.CategoryMap()[defaultValue]; !ok {
		return &d.FnReturn{Err: fmt.Errorf("default value in applyCat not an existing category level")}
	}

	var levels []any
	for k := range oldData.CategoryMap() {
		levels = append(levels, k)
	}

	var outCol d.Column
	if outCol, e = df.Categorical(newData.Name(), oldData.CategoryMap(), 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(newData.DataType())(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// **************** run a function ****************
func increment(len1 int) int {
	if len1 == 1 {
		return 0
	}

	return 1
}

func dofn0[T frameTypes | any](out []T, fn any) error {
	switch fnx := fn.(type) {
	case func(int) T:
		for ind := range len(out) {
			out[ind] = fnx(ind)
		}
	case func() T:
		out[0] = fnx()
	default:
		return fmt.Errorf("unsupported function signature in  dofn0")
	}

	return nil
}

func dofn1[T frameTypes, S frameTypes | any](a []T, out []S, fn any) error {
	indx, incr := 0, increment(len(a))
	n := max(len(a), len(out))
	switch fnx := fn.(type) {
	case func(T) S:
		for ind := range n {
			out[ind] = fnx(a[indx])
			indx += incr
		}
	case func(T) (S, error):
		var e error
		for ind := range n {
			if out[ind], e = fnx(a[indx]); e != nil {
				return e
			}

			indx += incr
		}
	case func([]T) S:
		out[0] = fnx(a)
	case func([]T) (S, error):
		var e error
		out[0], e = fnx(a)
		return e
	case func([]T):
		fnx(a)
	default:
		return fmt.Errorf("unsupported function signature in  dofn1")
	}

	return nil
}

func dofn2[T, S frameTypes, U frameTypes | any](a []T, b []S, out []U, fn any) error {
	indx1, incr1, indx2, incr2 := 0, increment(len(a)), 0, increment(len(b))
	n := max(len(a), len(b), len(out))
	switch fnx := fn.(type) {
	case func(a T, b S) U:
		for ind := range n {
			out[ind] = fnx(a[indx1], b[indx2])
			indx1 += incr1
			indx2 += incr2
		}
	case func(a T, b S) (U, error):
		var e error
		for ind := range n {
			if out[ind], e = fnx(a[indx1], b[indx2]); e != nil {
				return e
			}
			indx1 += incr1
			indx2 += incr2
		}
	case func(a []T, b []S) U:
		out[0] = fnx(a, b)
	case func(a []T, b []S) (U, error):
		var e error
		if out[0], e = fnx(a, b); e != nil {
			return e
		}
	case func(a []T, b []S):
		fnx(a, b)
	default:
		return fmt.Errorf("unsupported function signature in dofn2")
	}

	return nil
}

func dofn3[T, S, U frameTypes, V frameTypes | any](a []T, b []S, c []U, out []V, fn any) error {
	indx1, incr1, indx2, incr2, indx3, incr3 := 0, increment(len(a)), 0, increment(len(b)), 0, increment(len(c))
	n := max(len(a), len(b), len(c), len(out))
	switch fnx := fn.(type) {
	case func(a T, b S, C U) V:
		for ind := range n {
			out[ind] = fnx(a[indx1], b[indx2], c[indx3])
			indx1 += incr1
			indx2 += incr2
			indx3 += incr3
		}
	case func(a T, b S, c U) (V, error):
		var e error
		for ind := range n {
			if out[ind], e = fnx(a[indx1], b[indx2], c[indx3]); e != nil {
				return e
			}
			indx1 += incr1
			indx2 += incr2
			indx3 += incr3
		}
	case func(a []T, b []S):
		fnx(a, b)
	}

	return nil
}

func splitCol(cols []d.Column) (d.Column, []d.Column) {
	if cols == nil {
		return nil, nil
	}

	col0 := cols[0]

	if _, ok := col0.(*d.Scalar); ok {
		col0 = toCol(col0)
	}

	if len(cols) == 1 {
		return col0, nil
	}

	return col0, cols[1:]
}

func level0(out, fn any, cols []d.Column) error {
	if cols == nil {
		switch outx := out.(type) {
		case []float64:
			return dofn0(outx, fn)
		case []int:
			return dofn0(outx, fn)
		case []string:
			return dofn0(outx, fn)
		case []time.Time:
			return dofn0(outx, fn)
		case nil:
			return dofn0[any](nil, fn)
		}
	}

	col0, colsRemain := splitCol(cols)

	switch v := col0.(type) {
	case *Col:
		switch v.DataType() {
		case d.DTfloat:
			return level1(v.AsAny().([]float64), out, fn, colsRemain)
		case d.DTint:
			return level1(v.AsAny().([]int), out, fn, colsRemain)
		case d.DTstring:
			return level1(v.AsAny().([]string), out, fn, colsRemain)
		case d.DTdate:
			return level1(v.AsAny().([]time.Time), out, fn, colsRemain)
		}
	}

	return nil
}

func level1[T frameTypes](a []T, out, fn any, cols []d.Column) error {
	if cols == nil {
		switch outx := out.(type) {
		case []float64:
			return dofn1(a, outx, fn)
		case []int:
			return dofn1(a, outx, fn)
		case []string:
			return dofn1(a, outx, fn)
		case []time.Time:
			return dofn1(a, outx, fn)
		case nil:
			return dofn1[T, any](a, nil, fn)
		}
	}

	col0, colsRemain := splitCol(cols)

	switch v := col0.(type) {
	case *Col:
		switch v.DataType() {
		case d.DTfloat:
			return level2(a, v.AsAny().([]float64), out, fn, colsRemain)
		case d.DTint:
			return level2(a, v.AsAny().([]int), out, fn, colsRemain)
		case d.DTstring:
			return level2(a, v.AsAny().([]string), out, fn, colsRemain)
		case d.DTdate:
			return level2(a, v.AsAny().([]time.Time), out, fn, colsRemain)
		}
	}

	return nil
}

func level2[T, S frameTypes](a []T, b []S, out, fn any, cols []d.Column) error {
	if cols == nil {
		switch outx := out.(type) {
		case []float64:
			return dofn2(a, b, outx, fn)
		case []int:
			return dofn2(a, b, outx, fn)
		case []string:
			return dofn2(a, b, outx, fn)
		case []time.Time:
			return dofn2(a, b, outx, fn)
		case nil:
			return dofn2[T, S, any](a, b, nil, fn)
		}
	}

	col0, colsRemain := splitCol(cols)

	switch v := col0.(type) {
	case *Col:
		switch v.DataType() {
		case d.DTfloat:
			return level3(a, b, v.AsAny().([]float64), out, fn, colsRemain)
		case d.DTint:
			return level3(a, b, v.AsAny().([]int), out, fn, colsRemain)
		case d.DTstring:
			return level3(a, b, v.AsAny().([]string), out, fn, colsRemain)
		case d.DTdate:
			return level3(a, b, v.AsAny().([]time.Time), out, fn, colsRemain)
		}
	}

	return nil
}

func level3[T, S, U frameTypes](a []T, b []S, c []U, out, fn any, cols []d.Column) error {
	if cols == nil {
		switch outx := out.(type) {
		case []float64:
			return dofn3(a, b, c, outx, fn)
		case []int:
			return dofn3(a, b, c, outx, fn)
		case []string:
			return dofn3(a, b, c, outx, fn)
		case []time.Time:
			return dofn3(a, b, c, outx, fn)
		case nil:
			return dofn3[T, S, U, any](a, b, nil, nil, fn)
		}
	}

	return fmt.Errorf("4 argument functions aren't implemented")

}
