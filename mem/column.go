package df

import (
	"fmt"
	"sort"
	"time"

	d "github.com/invertedv/df"
	"gonum.org/v1/gonum/stat"
)

// ***************** Col - Create *****************

func NewCol(name string, data any) (*Col, error) {
	if e := d.ValidName(name); e != nil {
		return nil, e
	}

	var dt d.DataTypes
	if dt = d.WhatAmI(data); dt == d.DTunknown {
		return nil, fmt.Errorf("unsupported data type in NewCol")
	}

	var t any
	switch dx := data.(type) {
	case float64:
		t = []float64{dx}
	case int:
		t = []int{dx}
	case time.Time:
		t = []time.Time{dx}
	case string:
		t = []string{dx}
	default:
		t = data
	}

	c := &Col{
		data:    t,
		ColCore: d.NewColCore(dt, d.ColName(name)),
	}

	return c, nil
}

// ***************** Col - Methods *****************

func (c *Col) AppendRows(col2 d.Column) (d.Column, error) {
	panicer(col2)
	return AppendRows(c, col2, c.Name())
}

func (c *Col) Copy() d.Column {
	var copiedData any
	n := c.Len()
	switch c.DataType() {
	case d.DTfloat:
		copiedData = make([]float64, n)
		copy(copiedData.([]float64), c.data.([]float64))
	case d.DTint:
		copiedData = make([]int, n)
		copy(copiedData.([]int), c.data.([]int))
	case d.DTstring:
		copiedData = make([]string, n)
		copy(copiedData.([]string), c.data.([]string))
	case d.DTdate:
		copiedData = make([]time.Time, n)
		copy(copiedData.([]time.Time), c.data.([]time.Time))
	default:
		panic(fmt.Errorf("unsupported data type in Copy"))
	}

	col := &Col{
		data:    copiedData,
		ColCore: c.Core().Copy(),
	}

	return col
}

func (c *Col) Core() *d.ColCore {
	return c.ColCore
}

func (c *Col) Data() any {
	return c.data
}

func (c *Col) Element(row int) any {
	if c.Len() == 1 {
		row = 0
	}

	switch c.DataType() {
	case d.DTfloat:
		return c.Data().([]float64)[row]
	case d.DTint, d.DTcategorical:
		return c.Data().([]int)[row]
	case d.DTstring:
		return c.Data().([]string)[row]
	case d.DTdate:
		return c.Data().([]time.Time)[row]
	default:
		panic(fmt.Errorf("unsupported data type in Element"))
	}
}

func (c *Col) Greater(i, j int) bool {
	switch c.DataType() {
	case d.DTfloat:
		return c.data.([]float64)[i] >= c.data.([]float64)[j]
	case d.DTint:
		return c.data.([]int)[i] >= c.data.([]int)[j]
	case d.DTstring:
		return c.data.([]string)[i] >= c.data.([]string)[j]
	case d.DTdate:
		return !c.data.([]time.Time)[i].Before(c.data.([]time.Time)[j])
	default:
		panic(fmt.Errorf("unsupported data type in Less"))
	}
}

func (c *Col) Len() int {
	switch c.DataType() {
	case d.DTfloat:
		return len(c.Data().([]float64))
	case d.DTint, d.DTcategorical:
		return len(c.Data().([]int))
	case d.DTstring:
		return len(c.Data().([]string))
	case d.DTdate:
		return len(c.Data().([]time.Time))
	default:
		return -1
	}
}

func (c *Col) Less(i, j int) bool {
	switch c.DataType() {
	case d.DTfloat:
		return c.data.([]float64)[i] <= c.data.([]float64)[j]
	case d.DTint:
		return c.data.([]int)[i] <= c.data.([]int)[j]
	case d.DTstring:
		return c.data.([]string)[i] <= c.data.([]string)[j]
	case d.DTdate:
		return !c.data.([]time.Time)[i].After(c.data.([]time.Time)[j])
	default:
		panic(fmt.Errorf("unsupported data type in Less"))
	}
}

func (c *Col) Replace(indicator, replacement d.Column) (d.Column, error) {
	panicer(indicator, replacement)
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
			x, _ := d.ToString(k, true)

			keys = append(keys, x.(string))
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + d.PrettyPrint(header, keys, vals) + "\n"
	}

	if c.DataType() != d.DTfloat {
		tab, _ := NewDFcol(nil, nil, makeTable(c)...)
		_ = tab.Sort(false, "count")
		l := tab.Column(c.Name())
		c := tab.Column("count")

		header := []string{l.Name(), c.Name()}

		return t + d.PrettyPrint(header, l.Data(), c.Data())
	}

	x := make([]float64, c.Len())
	copy(x, c.Data().([]float64))
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

func AppendRows(col1, col2 d.Column, name string) (*Col, error) {
	if col1.DataType() != col2.DataType() {
		return nil, fmt.Errorf("append columns must have same type, got %s and %s for %s and %s",
			col1.DataType(), col2.DataType(), col1.Name(), col2.Name())
	}

	var data any
	switch col1.DataType() {
	case d.DTfloat:
		data = append(col1.Data().([]float64), col2.Data().([]float64)...)
	case d.DTint:
		data = append(col1.Data().([]int), col2.Data().([]int)...)
	case d.DTstring:
		data = append(col1.Data().([]string), col2.Data().([]string)...)
	case d.DTdate:
		data = append(col1.Data().([]time.Time), col2.Data().([]time.Time)...)
	default:
		return nil, fmt.Errorf("unsupported data type in AppendRows")
	}

	var (
		col *Col
		e   error
	)
	if col, e = NewCol(name, data); e != nil {
		return nil, e
	}

	return col, nil
}
