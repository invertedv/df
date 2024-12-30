package df

import "fmt"

type Scalar struct {
	atomic any
	*ColCore
}

func (s *Scalar) AppendRows(col Column) (Column, error) {
	return nil, fmt.Errorf("cannot append to scalar")
}

func (s *Scalar) Copy() Column {
	return &Scalar{
		atomic:  s.atomic,
		ColCore: s.Core().Copy(),
	}
}

func (s *Scalar) Core() *ColCore {
	return s.ColCore
}

func (s *Scalar) Data() *Vector {
	// should not fail
	v, _ := NewVector(s.atomic, WhatAmI(s.atomic))
	return v
}

func (s *Scalar) Len() int {
	return 1
}

func (s *Scalar) Replace(ind, repl Column) (Column, error) {
	return nil, fmt.Errorf("no replace in scalar")
}

func (s *Scalar) String() string {
	return fmt.Sprintf("%v", s.atomic)
}

func NewScalar(val any, opts ...COpt) *Scalar {
	var dt DataTypes
	if dt = WhatAmI(val); dt == DTunknown {
		panic("unsupported data type")
	}

	cc := NewColCore(dt, opts...)
	return &Scalar{
		atomic:  val,
		ColCore: cc,
	}
}
