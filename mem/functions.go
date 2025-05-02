package mem

// Learning: converting output from any to <type> takes a long time

import (
	_ "embed"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"

	d "github.com/invertedv/df"
)

// Data types supported in frames
type frameTypes interface {
	float64 | int | string | time.Time
}

// function definitions
var (
	//go:embed data/functions.txt
	functions string
)

// rawFuncs returns a slice of functions to operate on vectors. Each function here must have an entry in functions.txt.
func rawFuncs() []any {
	// there is no ordering required here
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
		sqrtFn[float64], sqrtFn[int],
		signFn[float64], signFn[int],
		modFn,
		powFn[float64, float64], powFn[float64, int], powFn[int, float64], powFn[int, int],
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
		math.Exp, math.Log, math.Round,
		math.Sin, math.Cos, math.Tan,
		math.Asin, math.Acos, math.Atan, math.Atan2,
		quantileFn[float64], quantileFn[int],
		lqFn[float64], lqFn[int],
		medianFn[float64], medianFn[int],
		uqFn[float64], uqFn[int],
		meanFn[float64], meanFn[int],
		varFn[float64], varFn[int],
		stdFn[float64], stdFn[int],
		sumFn[float64], sumFn[int],
		countFn[float64], countFn[int], countFn[string], countFn[time.Time],
		substrFn, pi, concatFn, ageMonthsFn, ageYearsFn,
		toLastDayFn, addMonthsFn, yearFn, monthFn, dayFn, dayOfWeekFn, makeDateFn[int], makeDateFn[string],
		replaceFn, positionFn, strings.ToUpper, strings.ToLower,
		randUnifFn[float64], randUnifFn[int], randNormFn[float64], randNormFn[int], randBinFn[float64], randBinFn[int],
		randBern[float64], randBern[int], randExp[float64], randExp[int],
		probNormFn,
	}

	return fns
}

// varying creates a d.Fn with a varying number of inputs from *.FnSpec. For the most part,
// this is used to create summary functions across columns (e.g. colSum, colMean).  It restricts the inputs to
// all having the same type.
func varying(spec *d.FnSpec) d.Fn {
	fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: spec.Name, Inputs: nil,
				Output:  spec.Outputs,
				Varying: true}
		}

		var (
			cols []*Col
		)

		for ind := range len(inputs) {
			col := toCol(inputs[ind])
			cols = append(cols, col)

			if cols[0].DataType() != col.DataType() {
				return &d.FnReturn{Err: fmt.Errorf("all entries to %s function must be same type", spec.Name)}
			}
		}

		// pull the correct function to run
		var (
			ind   int
			fnUse any
		)
		if spec.Inputs != nil {
			ind = signature(spec.Inputs, inputs)
			if ind < 0 {
				panic(fmt.Errorf("no signature"))
			}

			fnUse = fnToUse(spec.Fns, spec.Inputs[ind], spec.Outputs[ind])
		}

		// run down the rows to populate the output
		row := d.MakeVector(cols[0].DataType(), len(cols))
		inCol, _ := NewCol(row)
		outData := d.MakeVector(spec.Outputs[ind], df.RowCount())
		for r := range df.RowCount() {
			for c := range len(cols) {
				row.SetAny(cols[c].Data().Element(r), c)
			}

			out := d.MakeVector(spec.Outputs[ind], 1)

			if e := level0(out.AsAny(), fnUse, []d.Column{inCol}); e != nil {
				return &d.FnReturn{Err: e}
			}

			outData.SetAny(out.Element(0), r)
		}

		outCol, _ := NewCol(outData)

		return &d.FnReturn{Value: outCol}
	}

	return fn
}

// buildFn creates a d.Fn from *.FnSpec.
func buildFn(spec *d.FnSpec) d.Fn {
	fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: spec.Name, Inputs: spec.Inputs, Output: spec.Outputs, IsScalar: spec.IsScalar}
		}

		fnUse := spec.Fns[0]

		var ind int

		n := df.RowCount()
		// if there are inputs to the function, then we need to pick the correct one to run.
		if spec.Inputs != nil {
			ind = signature(spec.Inputs, inputs)
			if ind < 0 {
				panic(fmt.Errorf("no signature"))
			}

			fnUse = fnToUse(spec.Fns, spec.Inputs[ind], spec.Outputs[ind])
		}

		// scalar returns take the whole vector as inputs
		if spec.IsScalar {
			n = 1
		}

		// make output
		outVec := d.MakeVector(spec.Outputs[ind], n)
		oas := outVec.AsAny()

		// level0 starts the process to run the function, returning oas loaded
		if e := level0(oas, fnUse, inputs); e != nil {
			return &d.FnReturn{Err: e}
		}

		return returnCol(outVec)
	}

	return fn
}

// vectorFunctions returns a slice of functions that will be availble to Parse.
func vectorFunctions() d.Fns {
	specs := d.LoadFunctions(functions)
	fns := rawFuncs()

	for _, spec := range specs {
		// run through the raw functions and see
		for _, fn := range fns {
			// get name of fn
			fc := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
			// if the names don't match, move on
			if fc.Name() != spec.FnDetail {
				continue
			}

			spec.Fns = append(spec.Fns, fn)
		}
	}

	// with the functions loaded into .FnDetail, we can now build the slice of functions, d.Fns, for Parse.
	var outFns d.Fns
	for _, spec := range specs {
		if !spec.Varying {
			outFns = append(outFns, buildFn(spec))
			continue
		}

		outFns = append(outFns, varying(spec))
	}

	return outFns
}

// fnToUse chooses the element of fns (slice of functions) that matches the pattern of inputs in targetIns and
// output of targOut.
func fnToUse(fns []any, targetIns []d.DataTypes, targOut d.DataTypes) any {
	for _, fn := range fns {
		rfn := reflect.TypeOf(fn)
		ok := true
		for ind := range rfn.NumIn() {
			if d.GetKind(rfn.In(ind)) != targetIns[ind] {
				ok = false
				break
			}
		}

		if ok && rfn.NumOut() > 0 && d.GetKind(rfn.Out(0)) == targOut {
			return fn
		}
	}

	return nil
}

// **************** run a function ****************

// increment determines how to increment the counter given the length of a slice
func increment(len1 int) int {
	if len1 == 1 {
		return 0
	}

	return 1
}

// dofn0 runs a function that takes no arguments
func dofn0[T frameTypes | any](out []T, fn any) error {
	switch fnx := fn.(type) {
	// functions that, according to the definition in the .txt file, take no arguments may actually take
	// the row number as an argument.
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

// dofn1 runs a function that takes a single argument, which may be a value or a slice.
// Functions may or may not also return an error.
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

// dofn2 runs a function that takes two arguments, which may be a value or a slice.
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

// dofn3 runs a function that takes three arguments, which may be a value or a slice. This is the greatest number of arguments allowed.
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

// splitCol peels of the first entry, returning that and the remainder. If the first entry is a *d.Scalar,
// it is converted to a *Col.
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

// level0 is the entry point for determining what kind of function fn is and then running it.
// This seems tailor-made for recursion, but don't see a good way to do the casting of the function.
func level0(out, fn any, cols []d.Column) error {
	// no arguments to fn
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

	// go to level1, which handles functions with at least 1 argument.
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

// level1 handles functions with 1 or more arguments.
func level1[T frameTypes](a []T, out, fn any, cols []d.Column) error {
	// 1 argument
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

	// go to level2, which handles functions with at least 2 arguments.
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

// level2 handles functions with 2 or more arguments.
func level2[T, S frameTypes](a []T, b []S, out, fn any, cols []d.Column) error {
	// 2 arguments
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

	// go to level3, which handles functions with 3 arguments (that's the max)
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

// level3 handles functions with 3 arguments.
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

// **************** Functions ****************

func elemFn[T frameTypes](x []T, ind []int) (T, error) {
	if ind[0] < 0 || ind[0] > len(x) {
		return x[0], fmt.Errorf("index out of range")
	}

	return x[ind[0]], nil
}

func substrFn(x string, start, length int) string {
	start = min(max(start, 0), len(x)-1)
	length = min(length, len(x)-start)

	return x[start : start+length]
}

func quantileFn[T float64 | int](p []float64, x []T) float64 {
	var y any = x
	if xFlt, ok := y.([]float64); ok {
		if sort.Float64sAreSorted(xFlt) {
			return stat.Quantile(p[0], stat.Empirical, xFlt, nil) //stat.LinInterp
		}

		vSort := make([]float64, len(x))
		copy(vSort, xFlt)
		sort.Float64s(vSort)
		return stat.Quantile(p[0], stat.Empirical, vSort, nil)
	}

	xFlt := make([]float64, len(x))
	for ind, xx := range x {
		xFlt[ind] = float64(xx)
	}

	return quantileFn(p, xFlt)
}

func lqFn[T float64 | int](a []T) float64 {
	return quantileFn([]float64{0.25}, a) // had quantileFn[T]
}

func medianFn[T float64 | int](a []T) float64 {
	return quantileFn([]float64{0.5}, a) // had quantileFn[T]
}

func uqFn[T float64 | int](a []T) float64 {
	return quantileFn([]float64{0.75}, a) // had quantileFn[T]
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

func sqrtFn[T float64 | int](a T) (float64, error) {
	r := float64(a)
	if r < 0 {
		return 0, fmt.Errorf("sqrt of negative number")
	}

	return math.Sqrt(r), nil
}

func signFn[T float64 | int](a T) int {
	switch {
	case a == 0:
		return 0
	case a < 0:
		return -1
	default:
		return 1
	}
}

func modFn(a, b int) int {
	return a % b
}

func powFn[S, T float64 | int](a S, b T) float64 {
	return math.Pow(float64(a), float64(b))
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

func varFn[T float64 | int](x []T) float64 {
	n := len(x)
	if n == 1 {
		return 0
	}

	var xx any = x
	switch v := xx.(type) {
	case []float64:
		return stat.Variance(v, nil)
	case []int:
		mn, _ := meanFn(x)
		t := 0.0
		for _, xr := range v {
			res := float64(xr) - mn
			t += res * res
		}

		return t / float64(n-1)
	}

	return 0
}

func stdFn[T float64 | int](x []T) float64 {
	return math.Sqrt(varFn(x))
}

func countFn[T frameTypes](x []T) int {
	return len(x)
}

func pi() float64 {
	return math.Pi
}

func concatFn(str []string) string {
	out := ""
	for ind := range len(str) {
		out += str[ind]
	}

	return out
}

func ageYearsFn(dt1, dt2 time.Time) int {
	return ageMonthsFn(dt1, dt2) / 12
}

func ageMonthsFn(dt1, dt2 time.Time) int {
	y1, m1, d1 := dt1.Year(), int(dt1.Month()), dt1.Day()
	y2, m2, d2 := dt2.Year(), int(dt2.Month()), dt2.Day()
	if dt1.After(dt2) {
		y1, y2 = y2, y1
		m1, m2 = m2, m1
		d1, d2 = d2, d1
	}

	// dt1 is earlier
	mDiff := 12*(y2-y1) + m2 - m1
	if d1 > d2 {
		mDiff--
	}

	if dt1.After(dt2) {
		return -mDiff
	}

	return mDiff
}

func toLastDayFn(dt time.Time) time.Time {
	yr, mon, _ := dt.Date()
	mon++

	if mon > 12 {
		mon -= 12
		yr++
	}

	dt2 := time.Date(yr, mon, 1, 0, 0, 0, 0, time.UTC)
	dt3 := dt2.AddDate(0, 0, -1)

	return dt3
}

func addMonthsFn(dt time.Time, moToAdd int) time.Time {
	yr, mo, day := dt.Date()
	yr += moToAdd / 12
	mon := int(mo) + (moToAdd % 12)
	if mon > 12 {
		mon -= 12
		yr++
	}

	dtOut := time.Date(yr, time.Month(mon), day, 0, 0, 0, 0, time.UTC)

	if mon == int(dtOut.Month()) {
		return dtOut
	}

	return toLastDayFn(time.Date(yr, time.Month(mon), 1, 0, 0, 0, 0, time.UTC))
}

func yearFn(dt time.Time) int {
	return dt.Year()
}

func monthFn(dt time.Time) int {
	return int(dt.Month())
}

func dayFn(dt time.Time) int {
	return dt.Day()
}

func dayOfWeekFn(dt time.Time) string {
	return dt.Weekday().String()
}

func toInt[T int | string](x T) (int, error) {
	var v any = x
	switch v1 := v.(type) {
	case int:
		return v1, nil
	case string:
		v2, e := strconv.ParseInt(v1, 10, 32)
		return int(v2), e
	}

	return -1, fmt.Errorf("cannot make int")
}

func makeDateFn[T int | string](year, month, day T) (time.Time, error) {
	y, ey := toInt(year)
	m, em := toInt(month)
	d, ed := toInt(day)

	if ey != nil || em != nil || ed != nil {
		return time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), fmt.Errorf("invalid inputs to makeDate")
	}

	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), nil
}

func replaceFn(src, in, repl string) string {
	return strings.ReplaceAll(src, in, repl)
}

func positionFn(haystack, needle string) int {
	return strings.Index(haystack, needle)
}

func randUnifFn[T float64 | int](unused T) float64 {
	return rand.Float64()
}

func randNormFn[T float64 | int](unused T) float64 {
	return rand.NormFloat64()
}

func randBinFn[T float64 | int](n int, p float64, unused T) int {
	b := 0
	for range n {
		x := rand.Float64()
		if x <= p {
			b++
		}
	}

	return b
}

func randBern[T float64 | int](p float64, unused T) int {
	if rand.Float64() < p {
		return 1
	}

	return 0
}

func randExp[T float64 | int](lambda float64, unused T) float64 {
	return rand.ExpFloat64() / lambda
}

func probNormFn(x float64) float64 {
	return distuv.Normal{Mu: 0, Sigma: 1}.CDF(x)
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
		name := inputs[0].Name()
		if col = df.SourceDF().Column(name); col == nil {
			return &d.FnReturn{Err: fmt.Errorf("no such column in sourceDF: %s", name)}
		}

		return &d.FnReturn{Value: col}
	}

	return &d.FnReturn{Value: inputs[0]}
}

// ***************** Categorical Operations *****************

// toCat creates a categorical column -- for use in Parse. This is not a full implementation of the
// Categorical method.
//
// Inputs are:
//  1. Column to operate on
//  2. fuzz value (optional)
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

// applyCat is for use in Parse.
// - vector to apply cats to
// - existing categorical column to use as the source.
// - default if a new level is encountered.
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
