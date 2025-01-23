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

func plotFn(x, y []float64) (*d.Plot, error) {
	plt, _ := d.NewPlot()
	if e := plt.PlotXY(x, y, "", "black"); e != nil {
		return nil, e
	}

	return plt, nil
}

func printFn[T frameTypes](a []T) {
	fmt.Println(a[0])
}

//func plotTitle(title []string)

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
		plotFn,
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
				return &d.FnReturn{Name: spec.Name, Inputs: spec.Inputs, Output: spec.Outputs, RT: spec.RT}
			}

			fnUse := spec.Fns[0]

			var (
				col []*Col
				ind int
			)
			n := df.RowCount()
			if spec.Inputs != nil {
				col, n = parameters(inputs...)
				ind = signature(spec.Inputs, col...)
				if ind < 0 {
					panic("no signature")
				}
				fnUse = fnToUse(spec.Fns, spec.Inputs[ind], spec.RT, spec.Outputs[ind])
			}

			// scalar and plot returns take the whole vector as inputs
			if spec.RT == d.RTscalar || spec.RT == d.RTnone || spec.RT == d.RTplot {
				n = 1
			}

			var (
				oas any
				e   error
			)

			lenx := 0
			if spec.Inputs != nil {
				lenx = len(spec.Inputs[ind])
			}

			switch lenx {
			case 0:
				oas, _ = wrap0(fnUse, spec.Outputs[ind], n)
			case 1:
				oas, e = case1(spec.Inputs[ind][0], fnUse, n, spec.Outputs[ind], col[0])
			case 2:
				oas, e = case2(fmt.Sprintf("%s%s", spec.Inputs[ind][0], spec.Inputs[ind][1]), fnUse, n, spec.Outputs[ind], col[0], col[1])
			case 3:
				oas, e = case3(fmt.Sprintf("%s%s%s", spec.Inputs[ind][0], spec.Inputs[ind][1], spec.Inputs[ind][2]), fnUse, n,
					spec.Outputs[ind], col[0], col[1], col[2])
			}

			if e != nil {
				return &d.FnReturn{Err: e}
			}

			return returnCol(oas)
		}

		outFns = append(outFns, fn)
	}

	return outFns
}

func GetKind(fn reflect.Type) d.DataTypes {
	switch fn.Kind() {
	case reflect.Float64:
		return d.DTfloat
	case reflect.Int:
		return d.DTint
	case reflect.String:
		return d.DTstring
	case reflect.Struct:
		return d.DTdate
	case reflect.Slice:
		return GetKind(fn.Elem())
	default:
		return d.DTunknown
	}
}

func fnToUse(fns []any, targetIns []d.DataTypes, retType d.ReturnTypes, targOut d.DataTypes) any {
	for _, fn := range fns {
		rfn := reflect.TypeOf(fn)
		ok := true
		for ind := 0; ind < rfn.NumIn(); ind++ {
			if GetKind(rfn.In(ind)) != targetIns[ind] {
				ok = false
				break
			}
		}

		if ok && retType == d.RTplot && rfn.Out(0).Kind() == reflect.Pointer {
			return fn
		}

		if ok && rfn.NumOut() == 0 && targOut == d.DTnil {
			return fn
		}

		if ok && rfn.NumOut() > 0 && GetKind(rfn.Out(0)) == targOut {
			return fn
		}
	}

	return nil
}

func case1(ins d.DataTypes, fnUse any, n int, output d.DataTypes, in *Col) (*d.Vector, error) {
	var (
		oas *d.Vector
		e   error
	)
	switch ins {
	case d.DTfloat:
		oas, e = wrap1[float64](fnUse, n, output, in)
	case d.DTint:
		oas, e = wrap1[int](fnUse, n, output, in)
	case d.DTstring:
		oas, e = wrap1[string](fnUse, n, output, in)
	case d.DTdate:
		oas, e = wrap1[time.Time](fnUse, n, output, in)
	}

	return oas, e
}

func case2(ins string, fnUse any, n int, output d.DataTypes, in1, in2 *Col) (any, error) {
	var (
		oas any
		e   error
	)
	switch ins {
	case "DTfloatDTfloat":
		oas, e = wrap2[float64, float64](fnUse, n, output, in1, in2)
	case "DTintDTint":
		oas, e = wrap2[int, int](fnUse, n, output, in1, in2)
	case "DTstringDTstring":
		oas, e = wrap2[string, string](fnUse, n, output, in1, in2)
	case "DTdateDTdate":
		oas, e = wrap2[time.Time, time.Time](fnUse, n, output, in1, in2)

	case "DTfloatDTint":
		oas, e = wrap2[float64, int](fnUse, n, output, in1, in2)
	case "DTfloatDTstring":
		oas, e = wrap2[float64, string](fnUse, n, output, in1, in2)
	case "DTfloatDTdate":
		oas, e = wrap2[float64, time.Time](fnUse, n, output, in1, in2)

	case "DTintDTfloat":
		oas, e = wrap2[int, float64](fnUse, n, output, in1, in2)
	case "DTintDTstring":
		oas, e = wrap2[int, string](fnUse, n, output, in1, in2)
	case "DTintDTdate":
		oas, e = wrap2[int, time.Time](fnUse, n, output, in1, in2)

	case "DTstringDTfloat":
		oas, e = wrap2[string, float64](fnUse, n, output, in1, in2)
	case "DTstringDTint":
		oas, e = wrap2[string, int](fnUse, n, output, in1, in2)
	case "DTstringDTdate":
		oas, e = wrap2[string, time.Time](fnUse, n, output, in1, in2)

	case "DTdateDTfloat":
		oas, e = wrap2[time.Time, float64](fnUse, n, output, in1, in2)
	case "DTdateDTint":
		oas, e = wrap2[time.Time, int](fnUse, n, output, in1, in2)
	case "DTdateDTstring":
		oas, e = wrap2[time.Time, string](fnUse, n, output, in1, in2)
	}

	return oas, e
}

func case3(ins string, fnUse any, n int, output d.DataTypes, in1, in2, in3 *Col) (*d.Vector, error) {
	var (
		oas *d.Vector
		e   error
	)
	switch ins {
	case "DTintDTfloatDTfloat":
		oas, e = wrap3[int, float64, float64](fnUse, n, output, in1, in2, in3)
	case "DTintDTintDTint":
		oas, e = wrap3[int, int, int](fnUse, n, output, in1, in2, in3)
	case "DTintDTstringDTstring":
		oas, e = wrap3[int, string, string](fnUse, n, output, in1, in2, in3)
	case "DTintDTdateDTdate":
		oas, e = wrap3[int, time.Time, time.Time](fnUse, n, output, in1, in2, in3)
	}

	return oas, e
}

func wrap0(fn any, outType d.DataTypes, n int) (*d.Vector, error) {
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch fnx := fn.(type) {
		case func(i int) float64:
			v.SetAny(fnx(ind), ind)
		case func(i int) int:
			v.SetAny(fnx(ind), ind)
		case func(i int) string:
			v.SetAny(fnx(ind), ind)
		case func(i int) time.Time:
			v.SetAny(fnx(ind), ind)
		case func(i int) (float64, error):
			x, e := fnx(ind)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(i int) (int, error):
			x, e := fn.(func(int) (int, error))(ind)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(i int) (string, error):
			x, e := fn.(func(int) (int, error))(ind)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(i int) (time.Time, error):
			x, e := fn.(func(int) (int, error))(ind)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		default:
			return nil, fmt.Errorf("wrap0 failed")
		}
	}

	return v, nil
}

func wrap1[T frameTypes](fn any, n int, outType d.DataTypes, col *Col) (*d.Vector, error) {
	inData := col.Data().AsAny().([]T)
	inc, ind := 1, 0
	if n == 1 {
		inc = 0
	}

	var v *d.Vector
	if outType != d.DTnil {
		v = d.MakeVector(outType, n)
	}

	for indx := 0; indx < n; indx++ {
		switch fnx := fn.(type) {
		case func(x []T):
			fnx(inData)
		case func(x T) float64:
			v.SetAny(fnx(inData[ind]), indx)
		case func(x T) int:
			v.SetAny(fnx(inData[ind]), indx)
		case func(x T) string:
			v.SetAny(fnx(inData[ind]), indx)
		case func(x T) time.Time:
			v.SetAny(fnx(inData[ind]), indx)
		case func(x []T) float64:
			v.SetAny(fnx(inData), indx)
		case func(x []T) int:
			v.SetAny(fnx(inData), indx)
		case func(x []T) string:
			v.SetAny(fnx(inData), indx)
		case func(x []T) time.Time:
			v.SetAny(fnx(inData), indx)
		case func(x T) (float64, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T) (int, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T) (string, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T) (time.Time, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x []T) (float64, error):
			x, e := fnx(inData)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
		case func(x []T) (int, error):
			x, e := fnx(inData)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
		case func(x []T) (string, error):
			x, e := fnx(inData)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
		case func(x []T) (time.Time, error):
			x, e := fnx(inData)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
		default:
			return nil, fmt.Errorf("wrap1 failed")
		}

		ind += inc
	}

	return v, nil
}

func wrap2[T, S frameTypes](fn any, n int, outType d.DataTypes, col1, col2 *Col) (any, error) {
	inData1 := col1.Data().AsAny().([]T)
	inData2 := col2.Data().AsAny().([]S)

	inc1, inc2, ind1, ind2 := 1, 1, 0, 0
	if len(inData1) == 1 {
		inc1 = 0
	}
	if len(inData2) == 1 {
		inc2 = 0
	}

	var v *d.Vector
	if outType != d.DTnil {
		v = d.MakeVector(outType, n)
	}
	for indx := 0; indx < n; indx++ {
		switch fnx := fn.(type) {
		case func(x T, y S):
			fnx(inData1[ind1], inData2[ind2])
		case func(x []T, y []S):
			fnx(inData1, inData2)
		case func(x T, y S) float64:
			v.SetAny(fnx(inData1[ind1], inData2[ind2]), indx)
		case func(x T, y S) int:
			v.SetAny(fnx(inData1[ind1], inData2[ind2]), indx)
		case func(x T, y S) string:
			v.SetAny(fnx(inData1[ind1], inData2[ind2]), indx)
		case func(x T, y S) time.Time:
			v.SetAny(fnx(inData1[ind1], inData2[ind2]), indx)
		case func(x []T, y []S) float64:
			v.SetAny(fnx(inData1, inData2), indx)
		case func(x T, y S) (float64, error):
			x, e := fnx(inData1[ind1], inData2[ind2])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T, y S) (int, error):
			x, e := fnx(inData1[ind1], inData2[ind2])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T, y S) (string, error):
			x, e := fnx(inData1[ind1], inData2[ind2])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T, y S) (time.Time, error):
			x, e := fnx(inData1[ind1], inData2[ind2])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x []T, y []S) (float64, error):
			x, e := fnx(inData1, inData2)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
			return v, nil
		case func(x []T, y []S) (int, error):
			x, e := fnx(inData1, inData2)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
			return v, nil
		case func(x []T, y []S) (string, error):
			x, e := fnx(inData1, inData2)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
			return v, nil
		case func(x []T, y []S) (time.Time, error):
			x, e := fnx(inData1, inData2)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, 0)
			return v, nil
		case func(x []T, y []S) (*d.Plot, error):
			return fnx(inData1, inData2)
		default:
			return nil, fmt.Errorf("wrap2 failed")
		}

		ind1 += inc1
		ind2 += inc2
	}

	return v, nil
}

func wrap3[T, S, R frameTypes](fn any, n int, outType d.DataTypes, col1, col2, col3 *Col) (*d.Vector, error) {
	inData1 := col1.Data().AsAny().([]T)
	inData2 := col2.Data().AsAny().([]S)
	inData3 := col3.Data().AsAny().([]R)
	v := d.MakeVector(outType, n)

	inc1, inc2, inc3, ind1, ind2, ind3 := 1, 1, 1, 0, 0, 0
	if len(inData1) == 1 {
		inc1 = 0
	}
	if len(inData2) == 1 {
		inc2 = 0
	}
	if len(inData3) == 1 {
		inc3 = 0
	}
	for indx := 0; indx < n; indx++ {
		switch fnx := fn.(type) {
		case func(x T, y S, z R) float64:
			v.SetAny(fnx(inData1[ind1], inData2[ind2], inData3[ind3]), indx)
		case func(x T, y S, z R) int:
			v.SetAny(fnx(inData1[ind1], inData2[ind2], inData3[ind3]), indx)
		case func(x T, y S, z R) string:
			v.SetAny(fnx(inData1[ind1], inData2[ind2], inData3[ind3]), indx)
		case func(x T, y S, z R) time.Time:
			v.SetAny(fnx(inData1[ind1], inData2[ind2], inData3[ind3]), indx)
		case func(x T, y S, z R) (float64, error):
			x, e := fnx(inData1[ind1], inData2[ind2], inData3[ind3])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T, y S, z R) (int, error):
			x, e := fnx(inData1[ind1], inData2[ind2], inData3[ind3])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T, y S, z R) (string, error):
			x, e := fnx(inData1[ind1], inData2[ind2], inData3[ind3])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		case func(x T, y S, z R) (time.Time, error):
			x, e := fnx(inData1[ind1], inData2[ind2], inData3[ind3])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, indx)
		default:
			return nil, fmt.Errorf("wrap3 failed")
		}

		ind1 += inc1
		ind2 += inc2
		ind3 += inc3
	}

	return v, nil
}

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
	return quantileFn[T](a, []float64{0.25})
}

func medianFn[T float64 | int](a []T) float64 {
	return quantileFn[T](a, []float64{0.5})
}

func uqFn[T float64 | int](a []T) float64 {
	return quantileFn[T](a, []float64{0.75})
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
// TODO: should the default be an existing category?
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

func newVector(data any, dt d.DataTypes) *d.Vector {
	var (
		x *d.Vector
		e error
	)
	if x, e = d.NewVector(data, dt); e != nil {
		panic(e)
	}

	return x
}
