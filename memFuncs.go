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
		"addFloat", "addInt", "exp", "abs", "cast",
	}
	fns := []AnyFunction{
		addFloat, addInt, exp, abs, cast,
	}

	pos := utilities.Position(funcName, "", names...)
	if pos < 0 {
		return nil
	}

	return fns[pos]
}

func cast(inputs ...any) (any, error) {
	var dt DataTypes
	//	if dt = DTFromString(inputs[0].(string)); dt == DTunknown {
	//		return nil, fmt.Errorf("unknown datatype in cast")
	//	}
	dt = DTstring

	return toDataType(inputs[0], dt, true)
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
