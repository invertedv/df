package df

import (
	_ "embed"
	"fmt"
	"math"
	"reflect"
	"runtime"
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

// Leanring: converting output from any to <type> takes a long time

func adder[T float64 | int](a, b T) T {
	return a + b
}

func ander(a, b int) int {
	if a > 0 && b > 0 {
		return 1
	}

	return 0
}

func abser[T float64 | int](a T) T {
	if a >= 0 {
		return a
	}

	return -a
}

func GetOutType(fn any) d.DataTypes {
	switch reflect.TypeOf(fn).Out(0).Kind() {
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

// add input sigs
func fnToUse(fns []any, targOut d.DataTypes) any {
	for _, fn := range fns {
		if GetOutType(fn) == targOut {
			return fn
		}
	}

	return nil
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

func wrap1a[T FrameTypes](example T, fn any) {
	fmt.Println(example)
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

func b() d.Fns {
	specs := loadFunctions(functions)
	fns := []any{adder[float64], adder[int], abser[float64], abser[int], ander, math.Exp}

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
				fnUse = fnToUse(spec.Fns, spec.Outputs[ind])
			}

			fnsx := []any{wrap1a[int], wrap1a[float64]}
			for _, fn := range fnsx {
				a := reflect.TypeOf(fn).In(0).Kind()

				if a == reflect.Int {

				}
				_ = a
			}
			var oas *d.Vector
			switch reflect.TypeOf(fnUse).NumIn() {
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
			}

			return returnCol(oas)
		}

		outFns = append(outFns, fn)
	}

	fmt.Println("done")

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
		outDT = append(outDT, parseOutputs(s))
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
