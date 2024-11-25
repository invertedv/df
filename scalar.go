package df

import "fmt"

type Scalar struct {
	value any
	*ColCore
}

func (s *Scalar) AppendRows(col Column) (Column, error) {
	return nil, fmt.Errorf("cannot append to scalar")
}

func (s *Scalar) CategoryMap() CategoryMap {
	return nil
}

func (s *Scalar) Copy() Column {
	return NewScalar(s.Data(), ColContext(s.Context()))
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
	return s.Dependencies()
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
