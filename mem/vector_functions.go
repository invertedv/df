package df

import (
	"fmt"
	d "github.com/invertedv/df"
)

func vector(name string, inp [][]d.DataTypes, outp []d.DataTypes, fnx ...any) d.Fn {
	fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
		}

		fnUse := fnx[0]
		n := df.RowCount()
		var col []*Col
		if inp != nil {
			col, n = parameters(inputs...)
			ind := signature(inp, col...)
			if ind < 0 {
				panic("no signature")
			}

			fnUse = fnx[ind]
		}

		inData := getVecs(n, col...)
		var (
			data *d.Vector
			err  error
		)
		if data, err = fnUse.(func(x ...any) (*d.Vector, error))(inData...); err != nil {
			return &d.FnReturn{Err: err}
		}

		return returnCol(data)
	}

	return fn
}

func getVecs(n int, cols ...*Col) []any {
	v := []any{n}
	for ind := 0; ind < len(cols); ind++ {
		v = append(v, cols[ind].Data().AsAny())
	}

	return v
}

// ***************** Categorical Operations *****************

func toCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cat", Inputs: [][]d.DataTypes{{d.DTstring}, {d.DTint}, {d.DTdate}},
			Output:  []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical},
			Varying: true}
	}

	col := inputs[0].(*Col)
	dt := col.DataType()
	if !(dt == d.DTint || dt == d.DTstring || dt == d.DTdate) {
		return &d.FnReturn{Err: fmt.Errorf("cannot make %s into categorical", dt)}
	}

	fuzz := 1
	if len(inputs) > 1 {
		c := toCol(inputs[1])
		if c.DataType() != d.DTint {
			return &d.FnReturn{Err: fmt.Errorf("fuzz parameter to Cat must be type int")}
		}
		f, _ := c.ElementInt(0)
		fuzz = *f
	}

	var (
		outCol d.Column
		e      error
	)

	if outCol, e = df.Categorical(col.Name(), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(dt)(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// applyCat
// - vector to apply cats to
// - vector with cats
// - default if new category
// TODO: should the default be an existing category?
func applyCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := toCol(inputs[0])
	oldData := toCol(inputs[1])
	newVal := toCol(inputs[2])

	if newData.DataType() != oldData.RawType() {
		return &d.FnReturn{Err: fmt.Errorf("new column must be same type as original data in applyCat")}
	}

	var (
		defaultValue any
		e            error
	)

	if newVal.DataType() != newData.DataType() {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value to correct type in applyCat")}
	}

	defaultValue = newVal.Element(0)

	var levels []any
	for k := range oldData.CategoryMap() {
		levels = append(levels, k)
	}

	var outCol d.Column
	if outCol, e = df.Categorical(newData.Name(), oldData.CategoryMap(), 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(newData.DataType())(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

func newVector(data any, dt d.DataTypes) *d.Vector {
	var (
		x *d.Vector
		e error
	)
	if x, e = d.NewVector(data, dt); e != nil {
		panic(e)
	}

	return x
}
