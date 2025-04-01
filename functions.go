package df

import (
	"fmt"
	"strings"
)

type Fn func(info bool, df DF, inputs ...Column) *FnReturn

type Fns []Fn

func (fs Fns) Get(fnName string) Fn {
	for _, f := range fs {
		if f(true, nil).Name == fnName {
			return f
		}
	}

	return nil
}

type FnReturn struct {
	Value Column

	Name   string
	Output []DataTypes
	Inputs [][]DataTypes

	Varying bool

	IsScalar bool

	Err error
}

func RunDFfn(fn Fn, df DF, inputs []Column) (Column, error) {
	info := fn(true, nil)
	if !info.Varying && info.Inputs != nil && len(inputs) != len(info.Inputs[0]) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Varying && info.Inputs != nil && len(inputs) < len(info.Inputs[0]) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	var fnR *FnReturn
	if fnR = fn(false, df, inputs...); fnR.Err != nil {
		return nil, fnR.Err
	}

	return fnR.Value, nil
}

// *********

type FnSpec struct {
	Name     string
	FnDetail string
	Inputs   [][]DataTypes
	Outputs  []DataTypes
	IsScalar bool
	Varying  bool
	Fns      []any
}

type Fmap map[string]*FnSpec

func LoadFunctions(fns string) Fmap {
	m := make(Fmap)

	for spec := range strings.SplitSeq(fns, "\n") {
		details := strings.Split(spec, ":")
		if len(details) != 6 {
			continue
		}

		s := &FnSpec{
			Name:     details[0],
			FnDetail: details[1],
			Inputs:   parseInputs(details[2]),
			Outputs:  parseOutputs(details[3]),
			IsScalar: details[4][0] == 'S',
			Varying:  details[5][0] == 'Y',
		}

		m[s.Name] = s
	}

	return m
}

func parseInputs(inp string) [][]DataTypes {
	var outDT [][]DataTypes
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

func parseOutputs(outp string) []DataTypes {
	var outDT []DataTypes

	outs := strings.Split(outp, ",")
	for ind := range len(outs) {
		outDT = append(outDT, DTFromString("DT"+outs[ind]))
	}

	return outDT
}
