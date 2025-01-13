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

	d "github.com/invertedv/df"
)

type FrameTypes interface {
	float64 | int | string | time.Time
}

type fnSpec struct {
	Name    string
	SQL     string
	Inputs  [][]d.DataTypes
	Outputs []d.DataTypes
	Fns     []any
}

type fMap map[string]*fnSpec

var (
	//go:embed data/functions.txt
	functions string
)

// Learning: converting output from any to <type> takes a long time

// TODO: add error return?

func adder[T float64 | int](a, b T) T {
	return a + b
}

func suber[T float64 | int](a, b T) T { return a - b }

func multer[T float64 | int](a, b T) T { return a * b }

func diver[T float64 | int](a, b T) T {
	if b != 0 {
		return a / b
	}

	return 0
}

func ander(a, b int) int {
	if a > 0 && b > 0 {
		return 1
	}

	return 0
}

func orer(a, b int) int {
	if a > 0 || b > 0 {
		return 1
	}

	return 0
}

func noter(a int) int {
	return 1 - a
}

func rner(ind int) int {
	return ind
}

func abser[T float64 | int](a T) T {
	if a >= 0 {
		return a
	}

	return -a
}

func toInt(a bool) int {
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
	return toInt(greater(a, b))
}

func lter[T FrameTypes](a, b T) int {
	return toInt(greater(b, a))
}

func geer[T FrameTypes](a, b T) int {
	return toInt(!greater(b, a))
}

func leer[T FrameTypes](a, b T) int {
	return toInt(!greater(a, b))
}

func eqer[T FrameTypes](a, b T) int {
	return toInt(a == b)
}

func neer[T FrameTypes](a, b T) int {
	return toInt(a != b)
}

func ifer[T FrameTypes](a int, b, c T) T {
	if a == 1 {
		return b
	}

	return c
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
			if ok && GetKind(rfn.Out(0)) == targOut {
				return fn
			}
		}
	}

	return nil
}

func floater[T float64 | int | string](x T) float64 {
	var xx any = x
	switch v := xx.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case string:
		xo, e := strconv.ParseFloat(v, 64)
		if e == nil {
			return xo
		}

		return 0
	}

	return 0
}

func inTer[T float64 | int | string](x T) int {
	var xx any = x
	switch v := xx.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		xo, e := strconv.ParseInt(v, 10, 64)
		if e == nil {
			return int(xo)
		}

		return 0
	}

	return 0
}

func stringer[T FrameTypes](x T) string {
	var xx any = x
	switch v := xx.(type) {
	case float64:
		return fmt.Sprintf("%v", v)
	case int:
		return fmt.Sprintf("%d", v)
	case string:
		return v
	case time.Time:
		return v.Format("20060102")
	}

	return ""
}

func dater[T int | string | time.Time](x T) time.Time {
	var xx any = x
	switch v := xx.(type) {
	case int:
		vs := fmt.Sprintf("%d", v)
		return dater(vs)
	case string:
		for _, fmtx := range d.DateFormats {
			dt, e := time.Parse(fmtx, strings.ReplaceAll(v, "'", ""))
			if e == nil {
				return dt
			}
		}
		return time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	case time.Time:
		return v
	}

	return time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
}

func wrap0(fn any, outType d.DataTypes, n int) (*d.Vector, error) {
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch outType {
		case d.DTfloat:
			v.SetAny(fn.(func(int) float64)(ind), ind)
		case d.DTint:
			v.SetAny(fn.(func(int) int)(ind), ind)
		case d.DTstring:
			v.SetAny(fn.(func(int) string)(ind), ind)
		case d.DTdate:
			v.SetAny(fn.(func(int) time.Time)(ind), ind)
		default:
			return nil, fmt.Errorf("failed")
		}
	}

	return v, nil
}

func wrap1[T FrameTypes](fn any, n int, outType d.DataTypes, col *Col) (*d.Vector, error) {
	inData := col.Data().AsAny().([]T)
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch outType {
		case d.DTfloat:
			v.SetAny(fn.(func(x T) float64)(inData[ind]), ind)
		case d.DTint:
			v.SetAny(fn.(func(x T) int)(inData[ind]), ind)
		case d.DTstring:
			v.SetAny(fn.(func(x T) string)(inData[ind]), ind)
		case d.DTdate:
			v.SetAny(fn.(func(x T) time.Time)(inData[ind]), ind)
		default:
			return nil, fmt.Errorf("failed")
		}
	}

	return v, nil
}

func wrap2[T, S FrameTypes](fn any, n int, outType d.DataTypes, col1, col2 *Col) (*d.Vector, error) {
	inData1 := col1.Data().AsAny().([]T)
	inData2 := col2.Data().AsAny().([]S)
	v := d.MakeVector(outType, n)
	for ind := 0; ind < n; ind++ {
		switch outType {
		case d.DTfloat:
			v.SetAny(fn.(func(x T, y S) float64)(inData1[ind], inData2[ind]), ind)
		case d.DTint:
			v.SetAny(fn.(func(x T, y S) int)(inData1[ind], inData2[ind]), ind)
		case d.DTstring:
			v.SetAny(fn.(func(x T, y S) string)(inData1[ind], inData2[ind]), ind)
		case d.DTdate:
			v.SetAny(fn.(func(x T, y S) time.Time)(inData1[ind], inData2[ind]), ind)
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
		switch outType {
		case d.DTfloat:
			v.SetAny(fn.(func(x T, y S, z R) float64)(inData1[ind], inData2[ind], inData3[ind]), ind)
		case d.DTint:
			v.SetAny(fn.(func(x T, y S, z R) int)(inData1[ind], inData2[ind], inData3[ind]), ind)
		case d.DTstring:
			v.SetAny(fn.(func(x T, y S, z R) string)(inData1[ind], inData2[ind], inData3[ind]), ind)
		case d.DTdate:
			v.SetAny(fn.(func(x T, y S, z R) time.Time)(inData1[ind], inData2[ind], inData3[ind]), ind)
		default:
			return nil, fmt.Errorf("failed")
		}
	}

	return v, nil
}

func b() d.Fns {
	specs := loadFunctions(functions)
	fns := []any{rner,
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
	}

	for _, spec := range specs {
		for _, fn := range fns {
			fc := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
			fnname := fc.Name()
			if fnname != spec.SQL {
				continue
			}

			spec.Fns = append(spec.Fns, fn)
		}
	}

	var outFns d.Fns
	for _, spec := range specs {
		fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
			if info {
				return &d.FnReturn{Name: spec.Name, Inputs: spec.Inputs, Output: spec.Outputs}
			}

			fnUse := spec.Fns[0]
			n := df.RowCount()
			var (
				col []*Col
				ind int
			)

			if spec.Inputs != nil {
				col, n = parameters(inputs...)
				ind = signature(spec.Inputs, col...)
				if ind < 0 {
					panic("no signature")
				}
				fnUse = fnToUse(spec.Fns, spec.Inputs[ind], spec.Outputs[ind])
			}

			var oas *d.Vector
			//switch reflect.TypeOf(fnUse).NumIn() {
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
					oas, _ = wrap1[float64](fnUse, n, spec.Outputs[ind], col[0])
				case d.DTint:
					oas, _ = wrap1[int](fnUse, n, spec.Outputs[ind], col[0])
				case d.DTstring:
					oas, _ = wrap1[string](fnUse, n, spec.Outputs[ind], col[0])
				case d.DTdate:
					oas, _ = wrap1[time.Time](fnUse, n, spec.Outputs[ind], col[0])
				}
			case 2:
				switch fmt.Sprintf("%s%s", spec.Inputs[ind][0], spec.Inputs[ind][1]) {
				case "DTfloatDTfloat":
					oas, _ = wrap2[float64, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTintDTint":
					oas, _ = wrap2[int, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTstringDTstring":
					oas, _ = wrap2[string, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTdateDTdate":
					oas, _ = wrap2[time.Time, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTfloatDTint":
					oas, _ = wrap2[float64, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTfloatDTstring":
					oas, _ = wrap2[float64, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTfloatDTdate":
					oas, _ = wrap2[float64, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTintDTfloat":
					oas, _ = wrap2[int, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTintDTstring":
					oas, _ = wrap2[int, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTintDTdate":
					oas, _ = wrap2[int, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTstringDTfloat":
					oas, _ = wrap2[string, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTstringDTint":
					oas, _ = wrap2[string, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTstringDTdate":
					oas, _ = wrap2[string, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1])

				case "DTdateDTfloat":
					oas, _ = wrap2[time.Time, float64](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTdateDTint":
					oas, _ = wrap2[time.Time, int](fnUse, n, spec.Outputs[ind], col[0], col[1])
				case "DTdateDTstring":
					oas, _ = wrap2[time.Time, string](fnUse, n, spec.Outputs[ind], col[0], col[1])
				}
			case 3:
				switch fmt.Sprintf("%s%s%s", spec.Inputs[ind][0], spec.Inputs[ind][1], spec.Inputs[ind][2]) {
				case "DTintDTfloatDTfloat":
					oas, _ = wrap3[int, float64, float64](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				case "DTintDTintDTint":
					oas, _ = wrap3[int, int, int](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				case "DTintDTstringDTstring":
					oas, _ = wrap3[int, string, string](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				case "DTintDTdateDTdate":
					oas, _ = wrap3[int, time.Time, time.Time](fnUse, n, spec.Outputs[ind], col[0], col[1], col[2])
				}
			}

			return returnCol(oas)
		}

		outFns = append(outFns, fn)
	}

	return outFns
}

func loadFunctions(fns string) fMap {
	m := make(fMap)
	specs := strings.Split(fns, "\n")
	for _, spec := range specs {
		details := strings.Split(spec, ":")
		if len(details) != 4 {
			continue
		}

		s := &fnSpec{
			Name:    details[0],
			SQL:     details[1],
			Inputs:  parseInputs(details[2]),
			Outputs: parseOutputs(details[3]),
		}

		m[s.Name] = s
	}

	return m
}

func parseInputs(inp string) [][]d.DataTypes {
	var outDT [][]d.DataTypes
	dts := strings.Split(inp, "{")
	for ind := 1; ind < len(dts); ind++ {
		s := strings.ReplaceAll(dts[ind], "},", "")
		s = strings.ReplaceAll(s, "}", "")
		if s != "" {
			outDT = append(outDT, parseOutputs(s))
		}
	}

	return outDT
}

func parseOutputs(outp string) []d.DataTypes {
	var outDT []d.DataTypes

	outs := strings.Split(outp, ",")
	for ind := 0; ind < len(outs); ind++ {
		outDT = append(outDT, d.DTFromString("DT"+outs[ind]))
	}

	return outDT
}
