package df

import (
	"fmt"
	"sort"

	d "github.com/invertedv/df"
	"gonum.org/v1/gonum/stat"
)

type Col struct {
	*d.Vector

	*d.ColCore
}

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

// ***************** Methods *****************

func (c *Col) Copy() d.Column {
	col := &Col{
		Vector:  c.Data().Copy(),
		ColCore: c.Core().Copy(),
	}

	return col
}

func (c *Col) String() string {
	if c.Name() == "" {
		_ = d.ColName("unnamed")(c)
	}

	t := fmt.Sprintf("column: %s\ntype: %s\n", c.Name(), c.DataType())

	if c.CategoryMap() != nil {
		var keys []string
		var vals []int
		for k, v := range c.CategoryMap() {
			if k == nil {
				k = "Other"
			}

			var x any
			x = fmt.Sprintf("%v", x)

			keys = append(keys, x.(string))
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + d.PrettyPrint(header, keys, vals) + "\n"
	}

	if c.DataType() != d.DTfloat {
		var (
			tab d.DF
			e   error
		)
		if tab, e = c.Parent().Table(c.Name()); e != nil {
			panic(e)
		}
		_ = tab.Sort(true, c.Name())
		l := tab.Column(c.Name())
		cx := tab.Column("count")

		header := []string{l.Name(), c.Name()}

		cxi, _ := cx.Data().AsInt()
		str, _ := l.Data().AsString()
		return t + d.PrettyPrint(header, str, cxi)
	}

	x := make([]float64, c.Len())
	f, _ := c.AsFloat()
	copy(x, f)
	sort.Float64s(x)
	minx := x[0]
	maxx := x[len(x)-1]
	q25 := stat.Quantile(0.25, 1, x, nil)
	q50 := stat.Quantile(0.5, 1, x, nil)
	q75 := stat.Quantile(0.75, 1, x, nil)
	xbar := stat.Mean(x, nil)
	n := float64(c.Len())
	cats := []string{"min", "lq", "median", "mean", "uq", "max", "n"}
	vals := []float64{minx, q25, q50, xbar, q75, maxx, n}
	header := []string{"metric", "value"}

	return t + d.PrettyPrint(header, cats, vals)
}
