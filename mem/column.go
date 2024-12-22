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

func NewCol(data any, dt d.DataTypes, opts ...d.COpt) (*Col, error) {
	var col *Col
	if v, ok := data.(*d.Vector); ok {
		col = &Col{
			Vector:  v,
			ColCore: d.NewColCore(v.VectorType()),
		}
	}

	if col == nil {
		col = &Col{
			Vector:  d.NewVector(data, dt),
			ColCore: d.NewColCore(dt),
		}
	}

	for _, opt := range opts {
		opt(col)
	}

	return col, nil
}

// ***************** Col - Methods *****************

func (c *Col) AppendRows(col2 d.Column) (d.Column, error) {
	panicer(col2)
	return appendRows(c, col2) // NOTE: , c.Name())
}

func (c *Col) Copy() d.Column {
	col := &Col{
		Vector:  c.Vector.Copy(),
		ColCore: c.ColCore.Copy(),
	}

	return col
}

func (c *Col) Replace(indicator, replacement d.Column) (d.Column, error) {
	panicer(indicator, replacement)
	panic("not implemented")
	/*
		if c.DataType() != replacement.DataType() {
			return nil, fmt.Errorf("incompatible columns in Replace")
		}

		n := d.MaxInt(c.Len(), indicator.Len(), replacement.Len())
		if (c.Len() > 1 && c.Len() != n) || (indicator.Len() > 1 && indicator.Len() != n) ||
			(replacement.Len() > 1 && replacement.Len() != n) {
			return nil, fmt.Errorf("columns not same length in Replacef")
		}

		if indicator.DataType() != d.DTint {
			return nil, fmt.Errorf("indicator not type DTint in Replace")
		}

		data := d.MakeSlice(c.DataType(), 0, nil)

		for ind := 0; ind < n; ind++ {
			x := c.Element(ind)
			if indicator.(*Col).Element(ind).(int) > 0 {
				x = replacement.(*Col).Element(ind)
			}

			data = d.AppendSlice(data, x, c.DataType())
		}
		var (
			outCol *Col
			e      error
		)
		if outCol, e = NewCol("", data); e != nil {
			return nil, e
		}

		return outCol, nil
	*/
}

func (c *Col) String() string {
	if c.Name() == "" {
		panic("column has no name")
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
			if x, ok = d.ToString(k); !ok {
				panic(fmt.Errorf("cannot convert to string in Col.String()"))
			}

			keys = append(keys, x.(string))
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + d.PrettyPrint(header, keys, vals) + "\n"
	}

	if c.DataType() != d.DTfloat {
		tab, _ := NewDFcol(nil, makeTable(c))
		_ = tab.Sort(false, "count")
		l := tab.Column(c.Name())
		cx := tab.Column("count")

		header := []string{l.Name(), c.Name()}

		return t + d.PrettyPrint(header, l.Data().AsString(), cx.Data().AsInt())
	}

	x := make([]float64, c.Len())
	copy(x, c.AsFloat())
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

	return t + d.PrettyPrint(header, cats, vals)
}

// ***************** Helpers *****************

func appendRows(col1, col2 d.Column) (*Col, error) {
	if col1.DataType() != col2.DataType() {
		return nil, fmt.Errorf("append columns must have same type, got %s and %s for %s and %s",
			col1.DataType(), col2.DataType(), col1.Name(), col2.Name())
	}

	v := col1.(*Col).Vector.Copy()
	v.AppendVector(col2.(*Col).Vector)

	col := &Col{
		Vector:  v,
		ColCore: col1.(*Col).ColCore.Copy(),
	}

	return col, nil
}
