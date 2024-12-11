package df

import "fmt"

type Scalar struct {
	*Atomic
	*ColCore
}

func (s *Scalar) AppendRows(col Column) (Column, error) {
	return nil, fmt.Errorf("cannot append to scalar")
}

func (s *Scalar) Copy() Column {
	return &Scalar{
		Atomic:  s.Atomic.Copy(),
		ColCore: s.Core().Copy(),
	}
}

func (s *Scalar) Core() *ColCore {
	return s.ColCore
}

func (s *Scalar) Data() any {
	return s.Atomic.AsAny()
}

func (s *Scalar) Len() int {
	return 1
}

func (s *Scalar) Replace(ind, repl Column) (Column, error) {
	return nil, fmt.Errorf("no replace in scalar")
}

func (s *Scalar) String() string {
	return *s.Atomic.AsString()
}

func NewScalar(val any, opts ...COpt) *Scalar {
	var dt DataTypes
	if dt = WhatAmI(val); dt == DTunknown {
		panic("unsupported data type")
	}

	cc := NewColCore(dt, opts...)
	return &Scalar{
		Atomic:  NewAtomic(val, dt),
		ColCore: cc,
	}
}
