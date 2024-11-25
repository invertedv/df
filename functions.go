package df

import "fmt"

// *********** Function types ***********

type Fn func(info bool, context *Context, inputs ...any) *FnReturn

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

	Err error
}

func RunDFfn(fn Fn, context *Context, inputs []any) (any, error) {
	info := fn(true, nil)
	if !info.Varying && info.Inputs != nil && len(inputs) != len(info.Inputs[0]) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Inputs != nil && info.Varying && len(inputs) < len(info.Inputs[0]) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	var inps []any

	for j := 0; j < len(inputs); j++ {
		var (
			ok  bool
			col Column
		)
		if col, ok = inputs[j].(Column); !ok {
			return nil, fmt.Errorf("inputs include a non-column")
		}

		ColContext(context)(col.Core())
		inps = append(inps, col)
	}

	if ok, _ := okParams(inps, info.Inputs, info.Output); !ok {
		return nil, fmt.Errorf("bad parameters to %s", info.Name)
	}

	var fnR *FnReturn
	if fnR = fn(false, context, inps...); fnR.Err != nil {
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
			if inputs[j][k] != DTany && cols[k].(Column).DataType() != inputs[j][k] {
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
