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

// ***************** Col - Create *****************

func NewCol(data any, dt d.DataTypes, opts ...d.ColOpt) (*Col, error) {
	var col *Col
	if v, ok := data.(*d.Vector); ok {
		cx, _ := d.NewColCore(v.VectorType())
		col = &Col{
			Vector:  v,
			ColCore: cx,
		}
	}

	if col == nil {
		var (
			v *d.Vector
			e error
		)
		if v, e = d.NewVector(data, dt); e != nil {
			return nil, e
		}

		cy, _ := d.NewColCore(dt)
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

// ***************** Col - Methods *****************

func (c *Col) AppendRows(col2 d.Column) (d.Column, error) {
	if e := checkType(col2); e != nil {
		return nil, e
	}

	return appendRows(c, col2) // NOTE: , c.Name())
}

func (c *Col) Copy() d.Column {
	col := &Col{
		Vector:  c.Data().Copy(),
		ColCore: c.Core().Copy(),
	}

	return col
}

// TODO: get rid of this ... was using d.ToString(x)
func toString(x any) (any, bool) {
	return fmt.Sprintf("%v", x), true
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

			var (
				x  any
				ok bool
			)
			if x, ok = toString(k); !ok {
				panic(fmt.Errorf("cannot convert to string in Col.String()"))
			}

			keys = append(keys, x.(string))
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + prettyPrint(header, keys, vals) + "\n"
	}

	if c.DataType() != d.DTfloat {
		tab, _ := NewDFcol(nil, makeTable(c))
		_ = tab.Sort(false, "count")
		l := tab.Column(c.Name())
		cx := tab.Column("count")

		header := []string{l.Name(), c.Name()}

		cxi, _ := cx.Data().AsInt()
		str, _ := l.Data().AsString()
		return t + prettyPrint(header, str, cxi)
	}

	x := make([]float64, c.Len())
	f, _ := c.AsFloat()
	copy(x, f)
	sort.Float64s(x)
	minx := x[0]
	maxx := x[len(x)-1]
	q25 := stat.Quantile(0.25, 4, x, nil)
	q50 := stat.Quantile(0.5, 4, x, nil)
	q75 := stat.Quantile(0.75, 4, x, nil)
	xbar := stat.Mean(x, nil)
	n := float64(c.Len())
	cats := []string{"min", "lq", "median", "mean", "uq", "max", "n"}
	vals := []float64{minx, q25, q50, xbar, q75, maxx, n}
	header := []string{"metric", "value"}

	return t + prettyPrint(header, cats, vals)
}

// ***************** Helpers *****************

func appendRows(col1, col2 d.Column) (*Col, error) {
	if col1.DataType() != col2.DataType() {
		return nil, fmt.Errorf("append columns must have same type, got %s and %s for %s and %s",
			col1.DataType(), col2.DataType(), col1.Name(), col2.Name())
	}

	v := col1.(*Col).Data().Copy()
	_ = v.AppendVector(col2.(*Col).Data())

	col := &Col{
		Vector:  v,
		ColCore: col1.(*Col).Core().Copy(),
	}

	return col, nil
}
