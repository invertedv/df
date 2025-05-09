package df

import (
	"fmt"
	"iter"
)

// Scalar implements Column for scalars.
type Scalar struct {
	atomic any
	*ColCore
}

func (s *Scalar) AllRows() iter.Seq2[int, []any] {
	return s.Data().AllRows()
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

func (s *Scalar) Rename(newName string) error {
	return ColName(newName)(s)
}

func (s *Scalar) Replace(ind, repl Column) (Column, error) {
	return nil, fmt.Errorf("no replace in scalar")
}

func (s *Scalar) String() string {
	return fmt.Sprintf("%v", s.atomic)
}

func NewScalar(val any, opts ...ColOpt) (*Scalar, error) {
	var dt DataTypes
	if dt = WhatAmI(val); dt == DTunknown {
		return nil, fmt.Errorf("unsupported data type")
	}

	var (
		cc *ColCore
		e  error
	)
	opts = append(opts, ColDataType(dt))

	if cc, e = NewColCore(opts...); e != nil {
		return nil, e
	}

	return &Scalar{
		atomic:  val,
		ColCore: cc,
	}, nil
}
