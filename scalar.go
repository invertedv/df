package df

import "fmt"

type Scalar struct {
	value any
	*ColCore
}

func (s *Scalar) AppendRows(col Column) (Column, error) {
	return nil, fmt.Errorf("cannot append to scalar")
}

func (s *Scalar) Copy() Column {
	return &Scalar{
		value:   s.value,
		ColCore: s.Core().Copy(),
	}
}

func (s *Scalar) Core() *ColCore {
	return s.ColCore
}

func (s *Scalar) Data() any {
	return s.value
}

func (s *Scalar) Len() int {
	return 1
}

func (s *Scalar) Replace(ind, repl Column) (Column, error) {
	return nil, fmt.Errorf("no replace in scalar")
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
