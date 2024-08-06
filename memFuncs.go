package df

import (
	_ "embed"
	"fmt"
	"math"
	"strings"

	"github.com/invertedv/utilities"
)

var (
	//go:embed funcs/memFuncs.txt
	memFuncs  string
	Functions = MemLoadFunctions()
)

func MemLoadFunctions() MemFuncMap {
	fns := make(MemFuncMap)
	fDetail := strings.Split(memFuncs, "\n")
	for _, f := range fDetail {
		if f == "" {
			continue
		}

		detail := strings.Split(f, ",")
		if len(detail) < 3 {
			continue
		}

		var (
			output   DataTypes
			inputs   []DataTypes
			thisFunc AnyFunction
		)

		name := detail[0]
		if thisFunc = function(name); thisFunc == nil {
			panic(fmt.Sprintf("unknown mem function: %s", name))
		}

		if output = DTFromString(detail[len(detail)-1]); output == DTunknown {
			panic(fmt.Sprintf("unknown DataTypes %s", detail[len(detail)-1]))
		}

		for ind := 1; ind < len(detail)-1; ind++ {
			var val DataTypes
			if val = DTFromString(detail[ind]); val == DTunknown {
				panic(fmt.Sprintf("unknown DataTypes %s", detail[ind]))
			}

			inputs = append(inputs, val)
		}

		fns[name] = &MemFunc{
			name:     name,
			inputs:   inputs,
			output:   output,
			function: thisFunc,
		}
	}

	return fns
}

func function(funcName string) AnyFunction {
	names := []string{
		"addFloat", "addInt", "exp", "abs", "cast", "add",
	}
	fns := []AnyFunction{
		addFloat, addInt, exp, abs, cast, add,
	}

	pos := utilities.Position(funcName, "", names...)
	if pos < 0 {
		return nil
	}

	return fns[pos]
}

func cast(inputs ...any) (any, error) {
	dt := DTFromString(inputs[0].(string))

	return toDataType(inputs[1], dt, true)
}

func add(inputs ...any) (any, error) {
	dt0 := whatAmI(inputs[0])
	dt1 := whatAmI(inputs[1])

	switch {
	case dt0 == DTfloat && dt1 == DTfloat:
		return inputs[0].(float64) + inputs[1].(float64), nil
	case dt0 == DTfloat && dt1 == DTint:
		return inputs[0].(float64) + float64(inputs[1].(int)), nil
	case dt0 == DTint && dt1 == DTfloat:
		return float64(inputs[0].(int)) + inputs[1].(float64), nil
	case dt0 == DTint && dt1 == DTint:
		return inputs[0].(int) + inputs[1].(int), nil
	case dt0 == DTstring:
		if s, e := toString(inputs[1], true); e == nil {
			return inputs[0].(string) + s.(string), nil
		}
	case dt1 == DTstring:
		if s, e := toString(inputs[0], true); e == nil {
			return s.(string) + inputs[1].(string), nil
		}
	}

	return nil, fmt.Errorf("cannot add %s and %s", dt0, dt1)
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

func abs(xs ...any) (any, error) { return math.Abs(xs[0].(float64)), nil }
