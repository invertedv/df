package df

import "fmt"

func RunDFfn(fn Fn, context *Context, inputs []any) (any, error) {
	info := fn(true, nil)
	if !info.Varying && info.Inputs != nil && len(inputs) != len(info.Inputs[0]) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Inputs != nil && info.Varying && len(inputs) < len(info.Inputs[0]) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	var (
		inps []any
		cols []Column
	)
	for j := 0; j < len(inputs); j++ {
		var (
			ok  bool
			col Column
		)
		if col, ok = inputs[j].(Column); !ok {
			col = NewScalar(inputs[j], ColContext(context))
		}

		inps = append(inps, col)
		cols = append(cols, col)
	}

	// TODO: change this to take inps as arg
	if ok, _ := okParams(cols, info.Inputs, info.Output); !ok {
		return nil, fmt.Errorf("bad parameters to %s", info.Name)
	}

	var fnR *FnReturn
	if fnR = fn(false, context, inps...); fnR.Err != nil {
		return nil, fnR.Err
	}

	return fnR.Value, nil
}

func okParams(cols []Column, inputs [][]DataTypes, outputs []DataTypes) (ok bool, outType DataTypes) {
	if inputs == nil {
		return true, outputs[0]
	}

	for j := 0; j < len(inputs); j++ {
		ok = true
		for k := 0; k < len(inputs[j]); k++ {
			if inputs[j][k] != DTany && cols[k].DataType() != inputs[j][k] {
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

type Scalar struct {
	value any
	*ColCore
}

func (s *Scalar) AppendRows(col Column) (Column, error) {
	panic("no can do")
}

func (s *Scalar) CategoryMap() CategoryMap {
	return nil
}

func (s *Scalar) Copy() Column {
	return nil
}

func (s *Scalar) Core() *ColCore {
	return s.ColCore
}

func (s *Scalar) Context() *Context {
	return s.ctx
}

func (s *Scalar) Data() any {
	return s.value
}

func (s *Scalar) DataType() DataTypes {
	return s.dt
}

func (s *Scalar) Dependencies() []string {
	return nil
}

func (s *Scalar) Len() int {
	return 1
}

func (s *Scalar) Name() string {
	return s.name
}

func (s *Scalar) Replace(ind, repl Column) (Column, error) {
	return nil, fmt.Errorf("no replace in scalar")
}

func (s *Scalar) Rename(newName string) {
	//TODO: add valid check
	s.name = newName
}

func (s *Scalar) SetContext(ctx *Context) {
	s.ctx = ctx
}

func (s *Scalar) SetDependencies(d []string) {

}

func (s *Scalar) String() string {
	return fmt.Sprintf("%v", s.value)
}

func NewScalar(val any, opts ...COpt) *Scalar {
	dt := WhatAmI(val)
	if dt == DTunknown {
		panic("unknown data type")
	}

	cc := NewColCore(dt, opts...)
	return &Scalar{
		value:   val,
		ColCore: cc,
	}
}
