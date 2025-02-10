package sql

import (
	"fmt"

	d "github.com/invertedv/df"
)

func fnDefs(dlct *d.Dialect) d.Fns {
	var fns d.Fns
	for _, v := range dlct.Functions() {
		fns = append(fns,
			buildFn(v.Name, v.FnDetail, v.Inputs, v.Outputs, v.RT))
	}

	return fns
}

func buildFn(name, sql string, inp [][]d.DataTypes, outp []d.DataTypes, rt d.ReturnTypes) d.Fn {
	fn := func(info bool, df d.DF, inputs ...any) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp, RT: rt}
		}

		sqls := getSQL(df, inputs...)
		dts := getDataTypes(df, inputs...)

		var sa []any
		for j := 0; j < len(sqls); j++ {
			sa = append(sa, sqls[j])
		}

		sqlOut := fmt.Sprintf(sql, sa...)

		// if this returns a scalar, but there is no GROUP BY, then make a column of the global value
		// if you want to return the global value within a GROUP BY, put it within the function "global"
		if rt == d.RTscalar && df.(*DF).groupBy == "" {
			sqlOut = df.Dialect().Global(df.(*DF).SourceSQL(), sqlOut)
		}

		outType := outp[0]
		// output type
		for ind := 0; ind < len(inp); ind++ {
			ok := true
			for j := 0; j < len(dts); j++ {
				if dts[j] != inp[ind][j] {
					ok = false
					break
				}
			}

			if ok {
				outType = outp[ind]
				break
			}
		}

		outCol, _ := NewColSQL(outType, df.Dialect(), sqlOut, d.ColReturnType(rt))

		_ = d.ColParent(df)(outCol)
		_ = d.ColDialect(df.Dialect())(outCol)

		return &d.FnReturn{Value: outCol}
	}

	return fn
}

func global(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "global", Inputs: [][]d.DataTypes{{d.DTany}}, Output: []d.DataTypes{d.DTany}, RT: d.RTscalar}
	}

	sqls := getSQL(df, inputs...)
	qry := df.Dialect().Global(df.(*DF).SourceSQL(), sqls[0])
	outCol, _ := NewColSQL(d.DTint, df.Dialect(), qry, d.ColReturnType(d.RTscalar))

	return &d.FnReturn{Value: outCol}
}

// getNames returns the names of the input Columns starting with startInd element
func getNames(startInd int, cols ...any) ([]string, error) {
	var colNames []string
	for ind := startInd; ind < len(cols); ind++ {
		var cn string
		if cn = cols[ind].(*Col).Name(); cn == "" {
			return nil, fmt.Errorf("column with no name in table")
		}

		colNames = append(colNames, cn)
	}

	return colNames, nil
}

// ***************** categorical Operations *****************

func toCat(info bool, df d.DF, inputs ...any) *d.FnReturn {
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
		f := toCol(df, inputs[1]).SQL()

		var (
			fa any
			ok bool
		)
		if fa, ok = d.ToDataType(f, d.DTint); !ok {
			return &d.FnReturn{Err: fmt.Errorf("cannot interpret fuzz as integer in cat")}
		}

		fuzz = fa.(int)
		if fuzz < 1 {
			return &d.FnReturn{Err: fmt.Errorf("fuzz value must be positive")}
		}
	}

	var (
		outCol d.Column
		e      error
	)

	if outCol, e = df.(*DF).Categorical(col.Name(), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outCol}
}

func applyCat(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := inputs[0].(*Col)
	oldData := inputs[1].(*Col)
	newVal := inputs[2].(*Col)

	if newData.DataType() != oldData.RawType() {
		return &d.FnReturn{Err: fmt.Errorf("new column must be same type as original data in applyCat")}
	}

	if newVal.DataType() != newData.DataType() {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value to correct type in applyCat")}
	}

	var (
		defaultValue any
		ok           bool
	)
	if defaultValue, ok = d.ToDataType(newVal.SQL(), newVal.DataType()); !ok {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value")}
	}

	var levels []any
	for k := range oldData.CategoryMap() {
		levels = append(levels, k)
	}

	var (
		outCol d.Column
		e      error
	)
	if outCol, e = df.(*DF).Categorical(newData.Name(), oldData.CategoryMap(), 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(newData.DataType())(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// ***************** Helpers *****************

func toCol(df d.DF, x any) *Col {
	if c, ok := x.(*Col); ok {
		return c
	}

	if s, ok := x.(*d.Scalar); ok {
		var c *Col
		xx, _ := s.Data().ElementString(0)
		fld := *xx
		if s.DataType() == d.DTstring {
			fld = df.Dialect().ToString(fld)
		}

		c, _ = NewColSQL(s.DataType(), nil, fld, d.ColName(s.Name()))

		return c
	}

	panic("can't make column")
}

func getSQL(df d.DF, inputs ...any) []string {
	var sOut []string
	for ind := 0; ind < len(inputs); ind++ {
		sOut = append(sOut, toCol(df, inputs[ind]).SQL())
	}

	return sOut
}

func getDataTypes(df d.DF, inputs ...any) []d.DataTypes {
	var sOut []d.DataTypes
	for ind := 0; ind < len(inputs); ind++ {
		sOut = append(sOut, toCol(df, inputs[ind]).DataType())
	}

	return sOut
}

func fnGen(name, sql string, inp [][]d.DataTypes, outp []d.DataTypes, info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
	}

	sqls := getSQL(df, inputs...)
	dts := getDataTypes(df, inputs...)

	var sa []any
	for j := 0; j < len(sqls); j++ {
		sa = append(sa, sqls[j])
	}

	sqlOut := fmt.Sprintf(sql, sa...)

	outType := outp[0]
	// output type
	for ind := 0; ind < len(inp); ind++ {
		ok := true
		for j := 0; j < len(dts); j++ {
			if dts[j] != inp[ind][j] {
				ok = false
				break
			}
		}

		if ok {
			outType = outp[ind]
			break
		}
	}

	outCol, _ := NewColSQL(outType, df.Dialect(), sqlOut)

	_ = d.ColParent(df)(outCol)
	_ = d.ColDialect(df.Dialect())(outCol)

	fmt.Println(name)
	return &d.FnReturn{Value: outCol}
}
