package sql

import (
	"fmt"
	d "github.com/invertedv/df"
)

// ***************** Functions that return a scalar *****************

func sum(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inp := [][]d.DataTypes{{d.DTint}, {d.DTfloat}}
	outp := []d.DataTypes{d.DTint, d.DTfloat}
	return fnGen("sum", "sum(%s)", inp, outp, info, df, inputs...)
}

func mean(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inp := [][]d.DataTypes{{d.DTint}, {d.DTfloat}}
	outp := []d.DataTypes{d.DTfloat, d.DTfloat}
	return fnGen("mean", "avg(%s)", inp, outp, info, df, inputs...)
}

func dot(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inp := [][]d.DataTypes{{d.DTfloat, d.DTfloat}}
	outp := []d.DataTypes{d.DTfloat}
	return fnGen("dot", "sum(%s*%s)", inp, outp, info, df, inputs...)
}

func summary2(dlct *d.Dialect) d.Fns {
	/*specs := []string{
		"sum:sum(%s)",
		"mean:avg(%s)",
	}

	var fns d.Fns

	inps := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	outp := []d.DataTypes{d.DTfloat, d.DTfloat}
	*/
	//	for _, spec := range specs {
	//		nsql := strings.Split(spec, ":")
	//		fn := buildFn(nsql[0], nsql[1], inps, outp)
	//		fns = append(fns, fn)
	//	}

	var fns d.Fns
	for _, v := range dlct.Functions() {
		fns = append(fns,
			buildFn(v.Name, v.SQL, v.Inputs, v.Outputs))
	}

	return fns
}

func buildFn(name, sql string, inp [][]d.DataTypes, outp []d.DataTypes) d.Fn {
	fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
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

		// TODO: think about best place to do this
		_ = d.ColParent(df)(outCol)
		_ = d.ColDialect(df.Dialect())(outCol)

		return &d.FnReturn{Value: outCol}
	}

	return fn
}
