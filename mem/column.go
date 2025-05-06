package mem

import (
	"fmt"
	"iter"

	d "github.com/invertedv/df"
)

// Col implements Column for in-memory data.
type Col struct {
	*d.Vector

	*d.ColCore
}

// NewCol creates a new mem.Column from data.
func NewCol(data any, opts ...d.ColOpt) (*Col, error) {
	var col *Col
	if v, ok := data.(*d.Vector); ok {
		cx, _ := d.NewColCore(d.ColDataType(v.VectorType()))
		col = &Col{
			Vector:  v,
			ColCore: cx,
		}
	} else {
		var dt d.DataTypes
		if dt = d.WhatAmI(data); dt == d.DTunknown {
			return nil, fmt.Errorf("unsupported data type")
		}

		var (
			v *d.Vector
			e error
		)
		if v, e = d.NewVector(data, dt); e != nil {
			return nil, e
		}

		cy, _ := d.NewColCore(d.ColDataType(dt))
		col = &Col{
			Vector:  v,
			ColCore: cy,
		}
	}

	for _, opt := range opts {
		if e := opt(col); e != nil {
			return nil, e
		}
	}

	return col, nil
}

func (c *Col) AllRows() iter.Seq2[int, []any] {
	return c.Data().AllRows()
}

func (c *Col) Copy() d.Column {
	col := &Col{
		Vector:  c.Data().Copy(),
		ColCore: c.Core().Copy(),
	}

	return col
}

func (c *Col) Data() *d.Vector {
	return c.Vector
}

func (c *Col) String() string {
	if c.Name() == "" {
		_ = d.ColName("unnamed")(c)
	}

	t := fmt.Sprintf("column: %s\ntype: %s\nlength %d\n", c.Name(), c.DataType(), c.Len())

	if cm := c.CategoryMap(); cm != nil {
		return t + cm.String()
	}

	return t + c.Data().String()
}
