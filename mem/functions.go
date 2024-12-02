package df

import (
	"fmt"
	"math"
	"time"

	"gonum.org/v1/gonum/stat"

	d "github.com/invertedv/df"
)

// NewDFseq - arrayJoin(range(1,n)) or cnt(1,n)

func StandardFunctions() d.Fns {
	fns := d.Fns{applyCat, dot,
		mean,
		sortDF, sum, table, toCat,
		toDate, toFloat, toInt, toString,
		where,
	}
	fns = append(fns, comparisons()...)
	fns = append(fns, mathOps()...)
	fns = append(fns, logicalOps()...)
	fns = append(fns, mathFuncs()...)
	fns = append(fns, OtherVectors()...)

	return fns
}

// ***************** Vector-Valued Functions that return take a single int/float *****************

func mathFuncs() d.Fns {
	inType1 := [][]d.DataTypes{{d.DTfloat}}
	inType2 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	outType1 := []d.DataTypes{d.DTfloat}
	outType2 := []d.DataTypes{d.DTfloat, d.DTint}

	absInt := func(x ...any) int {
		xx := x[0].(int)
		if xx >= 0 {
			return xx
		}

		return -xx
	}

	out := d.Fns{
		vector("exp", inType1, outType1, func(x ...any) float64 { return math.Exp(x[0].(float64)) }),
		vector("log", inType1, outType1, func(x ...any) float64 { return math.Log(x[0].(float64)) }),
		vector("sqrt", inType1, outType1, func(x ...any) float64 { return math.Sqrt(x[0].(float64)) }),
		vector("abs", inType2, outType2, func(x ...any) float64 { return math.Abs(x[0].(float64)) }, absInt)}

	return out
}

func logicalOps() d.Fns {
	inType2 := [][]d.DataTypes{{d.DTint, d.DTint}}
	inType1 := [][]d.DataTypes{{d.DTint}}
	outType := []d.DataTypes{d.DTint}
	out := d.Fns{
		vector("and", inType2, outType, func(x ...any) int { return bint(x[0].(int) > 0 && x[1].(int) > 0) }),
		vector("or", inType2, outType, func(x ...any) int { return bint(x[0].(int) > 0 || x[1].(int) > 0) }),
		vector("not", inType1, outType, func(x ...any) int { return 1 - bint(x[0].(int) > 0) })}

	return out
}

func comparisons() d.Fns {
	inType := [][]d.DataTypes{
		{d.DTfloat, d.DTfloat},
		{d.DTint, d.DTint},
		{d.DTstring, d.DTstring},
		{d.DTdate, d.DTdate},
	}

	outType := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	out := d.Fns{
		vector("gt", inType, outType,
			func(x ...any) int { return bint(x[0].(float64) > x[1].(float64)) },
			func(x ...any) int { return bint(x[0].(int) > x[1].(int)) },
			func(x ...any) int { return bint(x[0].(string) > x[1].(string)) },
			func(x ...any) int { return bint(x[0].(time.Time).Sub(x[1].(time.Time)).Minutes() > 0) }),
		vector("lt", inType, outType,
			func(x ...any) int { return bint(x[0].(float64) < x[1].(float64)) },
			func(x ...any) int { return bint(x[0].(int) < x[1].(int)) },
			func(x ...any) int { return bint(x[0].(string) < x[1].(string)) },
			func(x ...any) int { return bint(x[0].(time.Time).Sub(x[1].(time.Time)).Minutes() < 0) }),
		vector("ge", inType, outType,
			func(x ...any) int { return bint(x[0].(float64) >= x[1].(float64)) },
			func(x ...any) int { return bint(x[0].(int) >= x[1].(int)) },
			func(x ...any) int { return bint(x[0].(string) >= x[1].(string)) },
			func(x ...any) int { return bint(x[0].(time.Time).Sub(x[1].(time.Time)).Minutes() >= 0) }),
		vector("le", inType, outType,
			func(x ...any) int { return bint(x[0].(float64) <= x[1].(float64)) },
			func(x ...any) int { return bint(x[0].(int) <= x[1].(int)) },
			func(x ...any) int { return bint(x[0].(string) <= x[1].(string)) },
			func(x ...any) int { return bint(x[0].(time.Time).Sub(x[1].(time.Time)).Minutes() <= 0) }),
		vector("eq", inType, outType,
			func(x ...any) int { return bint(x[0].(float64) == x[1].(float64)) },
			func(x ...any) int { return bint(x[0].(int) == x[1].(int)) },
			func(x ...any) int { return bint(x[0].(string) == x[1].(string)) },
			func(x ...any) int { return bint(x[0].(time.Time).Sub(x[1].(time.Time)).Minutes() == 0) }),
		vector("ne", inType, outType,
			func(x ...any) int { return bint(x[0].(float64) != x[1].(float64)) },
			func(x ...any) int { return bint(x[0].(int) != x[1].(int)) },
			func(x ...any) int { return bint(x[0].(string) != x[1].(string)) },
			func(x ...any) int { return bint(x[0].(time.Time).Sub(x[1].(time.Time)).Minutes() != 0) })}

	return out
}

func mathOps() d.Fns {
	inType2 := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}}
	inType1 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	outType := []d.DataTypes{d.DTfloat, d.DTint}

	addFloat := func(x ...any) float64 {
		return x[0].(float64) + x[1].(float64)
	}
	addInt := func(x ...any) int {
		return x[0].(int) + x[1].(int)
	}
	subFloat := func(x ...any) float64 { return x[0].(float64) - x[1].(float64) }
	subInt := func(x ...any) int { return x[0].(int) - x[1].(int) }
	multFloat := func(x ...any) float64 { return x[0].(float64) * x[1].(float64) }
	multInt := func(x ...any) int { return x[0].(int) * x[1].(int) }
	divFloat := func(x ...any) float64 { return x[0].(float64) / x[1].(float64) }
	divInt := func(x ...any) int { return x[0].(int) / x[1].(int) }
	negFloat := func(x ...any) float64 { return -x[0].(float64) }
	negInt := func(x ...any) int { return -x[0].(int) }

	out := d.Fns{
		vector("add", inType2, outType, addFloat, addInt),
		vector("divide", inType2, outType, divFloat, divInt),
		vector("multiply", inType2, outType, multFloat, multInt),
		vector("subtract", inType2, outType, subFloat, subInt),
		vector("neg", inType1, outType, negFloat, negInt)}

	return out
}

func OtherVectors() d.Fns {
	outType := []d.DataTypes{d.DTint}

	out := d.Fns{
		vector("rowNumber", nil, outType, func(x ...any) int { return x[0].(int) }),
		ifOp()}

	return out
}

// ifOp implements the if statement
func ifOp() d.Fn {
	inType := [][]d.DataTypes{
		{d.DTint, d.DTfloat, d.DTfloat},
		{d.DTint, d.DTint, d.DTint},
		{d.DTint, d.DTstring, d.DTstring},
		{d.DTint, d.DTdate, d.DTdate},
	}
	outType := []d.DataTypes{d.DTfloat, d.DTint, d.DTstring, d.DTdate}

	iFloat64 := func(x ...any) float64 {
		if x[0].(int) > 0 {
			return x[1].(float64)
		}
		return x[2].(float64)
	}
	iInt := func(x ...any) int {
		if x[0].(int) > 0 {
			return x[1].(int)
		}
		return x[2].(int)
	}
	iString := func(x ...any) string {
		if x[0].(int) > 0 {
			return x[1].(string)
		}
		return x[2].(string)
	}
	iDate := func(x ...any) time.Time {
		if x[0].(int) > 0 {
			return x[1].(time.Time)
		}
		return x[2].(time.Time)
	}

	return vector("if", inType, outType, iFloat64, iInt, iString, iDate)
}

// ***************** Functions that return a data frame *****************

func where(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "where", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTdf}}
	}

	var (
		outDF d.DF
		e     error
	)
	outDF, e = context.Self().Where(inputs[0].(d.Column))

	return &d.FnReturn{Value: outDF, Err: e}
}

func sortDF(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sort", Inputs: [][]d.DataTypes{{d.DTstring}},
			Output: []d.DataTypes{d.DTdf}, Varying: true}
	}

	ascending := true
	if toCol(inputs[0]).Element(0).(string) == "desc" {
		ascending = false
	}

	var (
		colNames []string
		e        error
	)

	if colNames, e = getNames(1, inputs...); e != nil {
		return &d.FnReturn{Err: e}
	}

	if ex := context.Self().Sort(ascending, colNames...); ex != nil {
		return &d.FnReturn{Err: ex}
	}

	return &d.FnReturn{Value: context.Self()}
}

func table(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "table", Inputs: [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}},
			Output: []d.DataTypes{d.DTdf, d.DTdf, d.DTdf}, Varying: true}
	}

	var (
		colNames []string
		e        error
	)

	if colNames, e = getNames(0, inputs...); e != nil {
		return &d.FnReturn{Err: e}
	}

	var (
		outDF d.DF
		ex    error
	)
	if outDF, ex = context.Self().Table(false, colNames...); ex != nil {
		return &d.FnReturn{Err: ex}
	}

	ret := &d.FnReturn{Value: outDF}

	return ret
}

// ***************** Functions that return a Column *****************

func row(ind int, cols ...*Col) []any {
	if cols == nil {
		return []any{ind}
	}

	outRow := make([]any, len(cols))
	for j := 0; j < len(cols); j++ {
		outRow[j] = cols[j].Element(ind)
	}

	return outRow
}

func signature(target [][]d.DataTypes, cols ...*Col) int {
	for j := 0; j < len(target); j++ {
		ind := j
		for k := 0; k < len(target[j]); k++ {
			if target[j][k] != cols[k].DataType() {
				ind = -1
				break
			}
		}

		if ind >= 0 {
			return ind
		}
	}

	return -1
}

// ***************** Functions that take 1 float and return a float *****************
func vector(name string, inp [][]d.DataTypes, outp []d.DataTypes, fnx ...any) d.Fn {
	fn := func(info bool, context *d.Context, inputs ...any) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
		}

		fnUse := fnx[0]
		n := context.Self().RowCount()
		dtOut := outp[0]
		var col []*Col
		if inp != nil {
			col, n = parameters(inputs...)
			ind := signature(inp, col...)
			if ind < 0 {
				panic("no signature")
			}
			fnUse = fnx[ind]
			dtOut = outp[ind]
		}

		data := d.MakeSlice(dtOut, n, nil)
		switch {
		case dtOut == d.DTfloat:
			fny := fnUse.(func(...any) float64)
			for ind := 0; ind < n; ind++ {
				data.([]float64)[ind] = fny(row(ind, col...)...)
			}
		case dtOut == d.DTint:
			fny := fnUse.(func(...any) int)
			for ind := 0; ind < n; ind++ {
				data.([]int)[ind] = fny(row(ind, col...)...)
			}
		case dtOut == d.DTstring:
			fny := fnUse.(func(...any) string)
			for ind := 0; ind < n; ind++ {
				data.([]string)[ind] = fny(row(ind, col...)...)
			}
		case dtOut == d.DTdate:
			fny := fnUse.(func(...any) time.Time)
			for ind := 0; ind < n; ind++ {
				data.([]time.Time)[ind] = fny(row(ind, col...)...)
			}
		}

		return returnCol(data)
	}

	return fn
}

// ***************** type conversions *****************

func toFloat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTint}, {d.DTfloat}, {d.DTstring}}
	out := []d.DataTypes{d.DTfloat, d.DTfloat, d.DTfloat}

	return cast("float", in, out, info, context, inputs...)
}

func toInt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTint}, {d.DTfloat}, {d.DTstring}, {d.DTcategorical}}
	out := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}

	return cast("int", in, out, info, context, inputs...)
}

func toDate(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTdate}, {d.DTstring}, {d.DTint}}
	out := []d.DataTypes{d.DTdate, d.DTdate, d.DTdate}

	return cast("date", in, out, info, context, inputs...)
}

func toString(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTdate}, {d.DTstring}}
	out := []d.DataTypes{d.DTstring, d.DTstring, d.DTstring, d.DTstring}

	return cast("string", in, out, info, context, inputs...)
}

func cast(name string, in [][]d.DataTypes, out []d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: in, Output: out}
	}

	col := toCol(inputs[0])
	data := d.MakeSlice(out[0], 0, nil)
	for ind := 0; ind < col.Len(); ind++ {
		var (
			x any
			e error
		)
		if x, e = d.ToDataType(col.Element(ind), out[0], true); e != nil {
			return &d.FnReturn{Err: e}
		}

		data = d.AppendSlice(data, x, out[0])
	}

	return returnCol(data)
}

type fun func(x ...any) any

// ***************** Functions that return a scalar *****************

// ***************** Functions that take a single column and return a scalar *****************
func apply(name string, fn fun, in [][]d.DataTypes,
	out []d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: in, Output: out}
	}
	var xs []any

	for ind := 0; ind < len(inputs); ind++ {
		xs = append(xs, inputs[ind].(*Col).Data())
	}

	return returnCol(fn(xs...))
}

func dot(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTfloat, d.DTfloat}}
	out := []d.DataTypes{d.DTfloat}
	fn := func(x ...any) any {
		return dotP(x[0].([]float64), x[1].([]float64))
	}

	return apply("dot", fn, in, out, info, context, inputs...)
}

func mean(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	out := []d.DataTypes{d.DTfloat, d.DTfloat}
	mn := func(x ...any) any {
		if xx, ok := x[0].([]float64); ok {
			return stat.Mean(xx, nil)
		}

		return MeanInt(x[0].([]int))
	}

	return apply("mean", mn, in, out, info, context, inputs...)
}

func sum(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	in := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	out := []d.DataTypes{d.DTfloat, d.DTint}
	s := func(x ...any) any {
		return sumC(x[0])
	}

	return apply("sum", s, in, out, info, context, inputs...)
}

// ***************** Categorical Operations *****************

func toCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
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
		fuzz = c.Element(0).(int)
	}

	var (
		outCol d.Column
		e      error
	)

	if outCol, e = context.Self().(*DF).Categorical(col.Name(), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	//	outCol.(*Col).rawType = dt
	d.ColRawType(dt)(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// applyCat
// - vector to apply cats to
// - vector with cats
// - default if new category
// TODO: should the default be an existing category?
func applyCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
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
	if outCol, e = context.Self().(*DF).Categorical(newData.Name(), oldData.CategoryMap(), 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	//	outCol.(*Col).RawType() = newData.DataType()
	d.ColRawType(newData.DataType())(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// ***************** Helpers *****************

func toCol(x any) *Col {
	if c, ok := x.(*Col); ok {
		return c
	}

	if s, ok := x.(*d.Scalar); ok {
		var (
			c *Col
			e error
		)
		if c, e = NewCol(s.Name(), s.Data()); e != nil {
			panic(e)
		}

		return c
	}

	panic("can't make column")
}

func parameters(inputs ...any) (cols []*Col, n int) {
	n = 1
	for j := 0; j < len(inputs); j++ {
		cx := toCol(inputs[j])
		cols = append(cols, cx)

		if nn := cx.Len(); nn > n {
			n = nn
		}
	}

	return cols, n
}

func returnCol(data any) *d.FnReturn {
	var (
		outCol *Col
		e      error
	)

	if outCol, e = NewCol("", data); e != nil {
		return &d.FnReturn{Err: e}
	}

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

func sumFloat(xx []float64) float64 {
	s := 0.0
	for _, x := range xx {
		s += x
	}

	return s
}

func sumInt(xx []int) int {
	s := 0
	for _, x := range xx {
		s += x
	}

	return s
}

func MeanInt(xx []int) float64 {
	return float64(sumInt(xx)) / float64(len(xx))
}

func sumC(xx any) any {
	switch x := xx.(type) {
	case []float64:
		return sumFloat(x)
	case []int:
		return sumInt(x)
	default:
		panic("cannot find sum")

	}

	return nil
}

func dotP(x, y []float64) float64 {
	p := 0.0
	for ind := 0; ind < len(x); ind++ {
		p += x[ind] * y[ind]
	}

	return p
}

func bint(x bool) int {
	if x {
		return 1
	}
	return 0
}
