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

// FnSpec specifies a function that the parser will have access to.
type FnSpec struct {
	// Name is the name of the function that the parser will recognize in user statements.
	Name string

	// FnDetail gives the specifics of the function.
	// For df/sql, this is the SQL that is run.
	// For df/mem, this is the name of the Go function to call.
	FnDetail string

	// Inputs is a slice that lists all valid combinations of inputs.
	Inputs [][]DataTypes

	// Outputs is a slice that lists the outputs corresponding to each element of Inputs.
	Outputs []DataTypes

	// IsScalar is true if the function reduces a column to a scalar (e.g. mean, sum)
	IsScalar bool

	// Varying is true if the number of inputs can vary.
	Varying bool

	// This is a slice of Go functions to call, corresponding to the elements of inputs/outputs.
	// Not used for df/sql.
	Fns []any
}

// Fmap maps the function name to its spec
type Fmap map[string]*FnSpec

// LoadFunctions loads functions from a string which is an embedded file.
// LoadFunctions expects functions to be separated by "\n"
// Within each line there are 6 fields separated by colons. The fields are:
//
//	function name
//	function spec
//	inputs
//	outputs
//	return type (C = column, S = scalar)
//	varying inputs (Y = yes).
//
// Inputs are sets of types with in braces separated by commas.
//
//	{int,int},{float,float}
//
// specifies the function takes two parameters which can be either {int,int} or {float,float}.
//
// Corresponding to each set of inputs is an output type.  In the above example, if the function always
// returns a float, the output would be:
//
//	float,float.
//
// Legal types are float, int, string and date.  Categorical inputs are ints.
//
// If there is no input parameter, leave the field empty as in:
//
//	::
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
