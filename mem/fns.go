package df

import (
	_ "embed"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"

	d "github.com/invertedv/df"
)

// TODO: remove github... from functions.txt
// TODO: move function string specs parser up a level??

type FrameTypes interface {
	float64 | int | string | time.Time
}

var (
	//go:embed data/functions.txt
	functions string
)

func elemer[T FrameTypes](x []T, ind []int) (T, error) {
	if ind[0] < 0 || ind[0] > len(x) {
		return x[0], fmt.Errorf("index out of range")
	}

	return x[ind[0]], nil
}

// Learning: converting output from any to <type> takes a long time

func adder[T float64 | int](a, b T) (T, error) { return a + b, nil }

func suber[T float64 | int](a, b T) (T, error) { return a - b, nil }

func multer[T float64 | int](a, b T) (T, error) { return a * b, nil }

func diver[T float64 | int](a, b T) (T, error) {
	if b != 0 {
		return a / b, nil
	}

	return 0, fmt.Errorf("divide by 0")
}

func ander(a, b int) (int, error) {
	if a > 0 && b > 0 {
		return 1, nil
	}

	return 0, nil
}

func orer(a, b int) (int, error) {
	if a > 0 || b > 0 {
		return 1, nil
	}

	return 0, nil
}

func noter(a int) (int, error) {
	return 1 - a, nil
}

func rner(ind int) (int, error) {
	return ind, nil
}

func abser[T float64 | int](a T) (T, error) {
	if a >= 0 {
		return a, nil
	}

	return -a, nil
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

func gter[T FrameTypes](a, b T) int {
	return bToI(greater(a, b))
}

func lter[T FrameTypes](a, b T) (int, error) {
	return bToI(greater(b, a)), nil
}

func geer[T FrameTypes](a, b T) (int, error) {
	return bToI(!greater(b, a)), nil
}

func leer[T FrameTypes](a, b T) (int, error) {
	return bToI(!greater(a, b)), nil
}

func eqer[T FrameTypes](a, b T) (int, error) {
	return bToI(a == b), nil
}

func neer[T FrameTypes](a, b T) (int, error) {
	return bToI(a != b), nil
}

func ifer[T FrameTypes](a int, b, c T) (T, error) {
	if a == 1 {
		return b, nil
	}

	return c, nil
}

func isInfer(x float64) (int, error) {
	if math.IsInf(x, 0) || math.IsInf(x, 1) {
		return 1, nil
	}

	return 0, nil
}

func isNaNer(x float64) (int, error) {
	if math.IsNaN(x) {
		return 1, nil
	}

	return 0, nil
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

func fnToUse(fns []any, targetIns []d.DataTypes, targOut d.DataTypes) any {
	for _, fn := range fns {
		rfn := reflect.TypeOf(fn)
		ok := true
		for ind := 0; ind < rfn.NumIn(); ind++ {
			if GetKind(rfn.In(ind)) != targetIns[ind] {
				ok = false
				break
			}
		}

		if ok && GetKind(rfn.Out(0)) == targOut {
			return fn
		}
	}

	return nil
}

func floater[T float64 | int | string](x T) (float64, error) {
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

func inTer[T float64 | int | string](x T) (int, error) {
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

func stringer[T FrameTypes](x T) (string, error) {
	var xx any = x
	switch v := xx.(type) {
	case float64:
		return fmt.Sprintf("%v", v), nil
	case int:
		return fmt.Sprintf("%d", v), nil
	case string:
		return v, nil
	case time.Time:
		return v.Format("20060102"), nil
	}

	return "", fmt.Errorf("cannot convert to string")
}

func dater[T int | string | time.Time](x T) (time.Time, error) {
	var xx any = x
	switch v := xx.(type) {
	case int:
		vs := fmt.Sprintf("%d", v)
		return dater(vs)
	case string:
		for _, fmtx := range d.DateFormats {
			return time.Parse(fmtx, strings.ReplaceAll(v, "'", ""))
		}

		return time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case time.Time:
		return v, nil
	}

	return time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
		fmt.Errorf("cannot convert to date")
}

func wrap0(fn any, outType d.DataTypes, n int) (*d.Vector, error) {
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch outType {
		case d.DTfloat:
			x, e := fn.(func(int) (float64, error))(ind)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case d.DTint:
			x, e := fn.(func(int) (int, error))(ind)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case d.DTstring:
			x, e := fn.(func(int) (string, error))(ind)
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case d.DTdate:
			x, e := fn.(func(int) (time.Time, error))(ind)
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

func sumer[T float64 | int](x []T) (T, error) {
	var total T = 0
	for _, xVal := range x {
		total += xVal
	}

	return total, nil
}

func meaner[T float64 | int](x []T) (float64, error) {
	var xx any = x
	switch v := xx.(type) {
	case []float64:
		return stat.Mean(v, nil), nil
	case []int:
		s, _ := sumer(x)
		return float64(s) / float64(len(v)), nil
	}

	return 0, fmt.Errorf("error in mean")
}

func wrap1[T FrameTypes](fn any, n int, outType d.DataTypes, col *Col) (*d.Vector, error) {
	inData := col.Data().AsAny().([]T)
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch fnx := fn.(type) {
		case func(x T) (float64, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T) (int, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T) (string, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T) (time.Time, error):
			x, e := fnx(inData[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
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
	}

	return v, nil
}

func wrap2[T, S FrameTypes](fn any, n int, outType d.DataTypes, col1, col2 *Col) (*d.Vector, error) {
	inData1 := col1.Data().AsAny().([]T)
	inData2 := col2.Data().AsAny().([]S)
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch fnx := fn.(type) {
		case func(x T, y S) (float64, error):
			x, e := fnx(inData1[ind], inData2[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T, y S) (int, error):
			x, e := fnx(inData1[ind], inData2[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T, y S) (string, error):
			x, e := fnx(inData1[ind], inData2[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T, y S) (time.Time, error):
			x, e := fnx(inData1[ind], inData2[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
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
		default:
			return nil, fmt.Errorf("failed")
		}
	}

	return v, nil
}

func wrap3[T, S, R FrameTypes](fn any, n int, outType d.DataTypes, col1, col2, col3 *Col) (*d.Vector, error) {
	inData1 := col1.Data().AsAny().([]T)
	inData2 := col2.Data().AsAny().([]S)
	inData3 := col3.Data().AsAny().([]R)
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch fnx := fn.(type) {
		case func(x T, y S, z R) (float64, error):
			x, e := fnx(inData1[ind], inData2[ind], inData3[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T, y S, z R) (int, error):
			x, e := fnx(inData1[ind], inData2[ind], inData3[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T, y S, z R) (string, error):
			x, e := fnx(inData1[ind], inData2[ind], inData3[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		case func(x T, y S, z R) (time.Time, error):
			x, e := fnx(inData1[ind], inData2[ind], inData3[ind])
			if e != nil {
				return nil, e
			}
			v.SetAny(x, ind)
		default:
			return nil, fmt.Errorf("failed")
		}
	}

	return v, nil
}

func buildFunctionsSC() d.Fns {
	specs := d.LoadFunctions(functions)
	fns := []any{rner,
		isInfer, isNaNer,
		floater[float64], floater[int], floater[string],
		inTer[float64], inTer[int], inTer[string],
		stringer[float64], stringer[int], stringer[string], stringer[time.Time],
		dater[int], dater[string], dater[time.Time],
		adder[float64], adder[int],
		suber[float64], suber[int],
		multer[float64], multer[int],
		diver[float64], diver[int],
		abser[float64], abser[int],
		ander, orer, noter,
		math.Exp, math.Log,
		gter[float64], gter[int], gter[string], gter[time.Time],
		lter[float64], lter[int], lter[string], lter[time.Time],
		geer[float64], geer[int], geer[string], geer[time.Time],
		leer[float64], leer[int], leer[string], leer[time.Time],
		eqer[float64], eqer[int], eqer[string], eqer[time.Time],
		neer[float64], neer[int], neer[string], neer[time.Time],
		ifer[float64], ifer[int], ifer[string], ifer[time.Time],
		elemer[float64], elemer[int], elemer[string], elemer[time.Time],
		meaner[float64], meaner[int],
		sumer[float64], sumer[int],
	}

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
				fnUse = fnToUse(spec.Fns, spec.Inputs[ind], spec.Outputs[ind])
			}

			if spec.RT == d.RTscalar {
				n = 1
			}

			var (
				oas *d.Vector
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
				switch spec.Inputs[ind][0] {
				case d.DTfloat:
					oas, e = wrap1[float64](fnUse, n, spec.Outputs[ind], col[0])
				case d.DTint:
					oas, e = wrap1[int](fnUse, n, spec.Outputs[ind], col[0])
				case d.DTstring:
					oas, e = wrap1[string](fnUse, n, spec.Outputs[ind], col[0])
				case d.DTdate:
					oas, e = wrap1[time.Time](fnUse, n, spec.Outputs[ind], col[0])
				}
			case 2:
				switch fmt.Sprintf("%s%s", spec.Inputs[ind][0], spec.Inputs[ind][1]) {
				case "DTfloatDTfloat":
					oas, e = wrap2[float64, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTintDTint":
					oas, e = wrap2[int, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTstringDTstring":
					oas, e = wrap2[string, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTdateDTdate":
					oas, e = wrap2[time.Time, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTfloatDTint":
					oas, e = wrap2[float64, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTfloatDTstring":
					oas, e = wrap2[float64, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTfloatDTdate":
					oas, e = wrap2[float64, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTintDTfloat":
					oas, e = wrap2[int, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTintDTstring":
					oas, e = wrap2[int, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTintDTdate":
					oas, e = wrap2[int, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTstringDTfloat":
					oas, e = wrap2[string, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTstringDTint":
					oas, e = wrap2[string, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTstringDTdate":
					oas, e = wrap2[string, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTdateDTfloat":
					oas, e = wrap2[time.Time, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTdateDTint":
					oas, e = wrap2[time.Time, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTdateDTstring":
					oas, e = wrap2[time.Time, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				}
			case 3:
				switch fmt.Sprintf("%s%s%s", spec.Inputs[ind][0], spec.Inputs[ind][1], spec.Inputs[ind][2]) {
				case "DTintDTfloatDTfloat":
					oas, e = wrap3[int, float64, float64](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				case "DTintDTintDTint":
					oas, e = wrap3[int, int, int](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				case "DTintDTstringDTstring":
					oas, e = wrap3[int, string, string](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				case "DTintDTdateDTdate":
					oas, e = wrap3[int, time.Time, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				}
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

// ************ read specs file ************
