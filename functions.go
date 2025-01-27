package df

import (
	"fmt"
	"strings"
)

type Fn func(info bool, df DF, inputs ...any) *FnReturn

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
	Value any

	Name   string
	Output []DataTypes
	Inputs [][]DataTypes

	Varying bool

	RT ReturnTypes // TODO: add to dialect file

	Err error
}

type ReturnTypes rune

const (
	RTscalar    ReturnTypes = 'S'
	RTcolumn    ReturnTypes = 'C'
	RTdataFrame ReturnTypes = 'D'
	RTplot      ReturnTypes = 'P'
	RTnone                  = 'N'
)

//go:generate stringer -type=ReturnTypes

func RunDFfn(fn Fn, df DF, inputs []any) (any, error) {
	info := fn(true, nil)
	if !info.Varying && info.Inputs != nil && len(inputs) != len(info.Inputs[0]) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Inputs != nil && info.Varying && len(inputs) < len(info.Inputs[0]) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	if ok, _ := okParams(inputs, info.Inputs, info.Output); !ok {
		return nil, fmt.Errorf("bad parameters to %s", info.Name)
	}

	var fnR *FnReturn
	if fnR = fn(false, df, inputs...); fnR.Err != nil {
		return nil, fnR.Err
	}

	return fnR.Value, nil
}

func okParams(cols []any, inputs [][]DataTypes, outputs []DataTypes) (ok bool, outType DataTypes) {
	if inputs == nil {
		return true, outputs[0]
	}

	for j := 0; j < len(inputs); j++ {
		ok = true
		for k := 0; k < len(inputs[j]); k++ {
			if _, isPlot := cols[k].(*Plot); isPlot && inputs[j][k] != DTplot {
				ok = false
				break
			}

			if _, isCol := cols[k].(Column); isCol && inputs[j][k] != DTany && cols[k].(Column).DataType() != inputs[j][k] {
				ok = false
				break
			}
		}

		if ok {
			return true, outputs[j]
		}
	}

	return false, DTunknown
}

// *********

type FnSpec struct {
	Name     string
	FnDetail string
	Inputs   [][]DataTypes
	Outputs  []DataTypes
	RT       ReturnTypes
	Fns      []any
}

type Fmap map[string]*FnSpec

func LoadFunctions(fns string) Fmap {
	m := make(Fmap)
	specs := strings.Split(fns, "\n")
	for _, spec := range specs {
		details := strings.Split(spec, ":")
		if len(details) != 5 {
			continue
		}

		rt := rune(details[4][0])

		s := &FnSpec{
			Name:     details[0],
			FnDetail: details[1],
			Inputs:   parseInputs1(details[2]),
			Outputs:  parseOutputs1(details[3]),
			RT:       ReturnTypes(rt),
		}

		m[s.Name] = s
	}

	return m
}

func parseInputs1(inp string) [][]DataTypes {
	var outDT [][]DataTypes
	dts := strings.Split(inp, "{")
	for ind := 1; ind < len(dts); ind++ {
		s := strings.ReplaceAll(dts[ind], "},", "")
		s = strings.ReplaceAll(s, "}", "")
		if s != "" {
			outDT = append(outDT, parseOutputs1(s))
		}
	}

	return outDT
}

func parseOutputs1(outp string) []DataTypes {
	var outDT []DataTypes

	outs := strings.Split(outp, ",")
	for ind := 0; ind < len(outs); ind++ {
		outDT = append(outDT, DTFromString("DT"+outs[ind]))
	}

	return outDT
}
