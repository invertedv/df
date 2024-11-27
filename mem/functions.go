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
	return d.Fns{and, applyCat, divide, dot, eq,
		vector("add", [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}}, []d.DataTypes{d.DTfloat, d.DTint},
			addFloat, addInt),
		vector("exp", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, func(x ...any) float64 { return math.Exp(x[0].(float64)) }),
		vector("log", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, func(x ...any) float64 { return math.Log(x[0].(float64)) }),
		vector("sqrt", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, func(x ...any) float64 { return math.Sqrt(x[0].(float64)) }),
		vector("abs", [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, []d.DataTypes{d.DTfloat, d.DTint},
			func(x ...any) float64 { return math.Abs(x[0].(float64)) }, absInt),
		ge, gt, ifs, le, lt, mean, multiply,
		ne, neg, not, or, rowNumber, sortDF, subtract, sum, table, toCat,
		toDate, toFloat, toInt, toString,
		where,
	}
}

// ***************** Vector-Valued Functions that return take a single int/float *****************

func absInt(x ...any) int {
	xx := x[0].(int)
	if xx >= 0 {
		return xx
	}

	return -xx
}

func addFloat(x ...any) float64 {
	return x[0].(float64) + x[1].(float64)
}

func addInt(x ...any) int {
	return x[0].(int) + x[1].(int)
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
	outRow := make([]any, len(cols))
	for j := 0; j < len(cols); j++ {
		outRow[j] = cols[j].Element(ind)
	}

	return outRow
}

// ***************** Functions that take 1 float and return a float *****************
func vector(name string, inp [][]d.DataTypes, outp []d.DataTypes, fnx ...any) d.Fn {
	fn := func(info bool, context *d.Context, inputs ...any) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
		}

		col, n := parameters(inputs...)
		dtIn := col[0].DataType()
		dtOut := d.DTunknown
		var fnUse any
		for ind, d := range inp {
			if d[0] == dtIn {
				dtOut = outp[ind]
				fnUse = fnx[ind]
				break
			}
		}

		data := d.MakeSlice(col[0].DataType(), n, nil)
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

// ***************** Functions that take no parameters *****************

func rowNumber(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "rowNumber", Output: []d.DataTypes{d.DTint}}
	}

	n := context.Self().RowCount()
	data := make([]int, n)
	for ind := 0; ind < n; ind++ {
		data[ind] = ind
	}

	return returnCol(data)
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

// ***************** Basic arithmetic functions *****************

func arithmetic(op string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: op, Inputs: [][]d.DataTypes{{d.DTfloat, d.DTfloat},
			{d.DTint, d.DTint}, {d.DTstring, d.DTfloat}, {d.DTstring, d.DTint}},
			Output: []d.DataTypes{d.DTfloat, d.DTint, d.DTfloat, d.DTint}}
	}

	cols, n := parameters(inputs...)

	type floatFn func(a, b float64) float64
	type intFn func(a, b int) int

	floats := []floatFn{
		func(a, b float64) float64 { return a + b },
		func(a, b float64) float64 { return a - b },
		func(a, b float64) float64 { return a * b },
		func(a, b float64) float64 { return a / b },
	}

	ints := []intFn{
		func(a, b int) int { return a + b },
		func(a, b int) int { return a - b },
		func(a, b int) int { return a * b },
		func(a, b int) int { return a / b },
	}

	var dataOut any
	if cols[1].DataType() == d.DTfloat {
		data := make([]float64, n)
		var fn func(a, b float64) float64
		switch op {
		case "add":
			fn = floats[0]
		case "subtract":
			fn = floats[1]
		case "multiply":
			fn = floats[2]
		case "divide":
			fn = floats[3]
		}

		for ind := 0; ind < n; ind++ {
			x := cols[0].Element(ind).(float64)
			data[ind] = fn(x, cols[1].Element(ind).(float64))
		}

		dataOut = data
	}

	if cols[1].DataType() == d.DTint {
		data := make([]int, n)
		var fn func(a, b int) int
		switch op {
		case "add":
			fn = ints[0]
		case "subtract":
			fn = ints[1]
		case "multiply":
			fn = ints[2]
		case "divide":
			fn = ints[3]
		}

		for ind := 0; ind < n; ind++ {
			x := cols[0].Element(ind).(int)
			data[ind] = fn(x, cols[1].Element(ind).(int))
		}

		dataOut = data
	}

	return returnCol(dataOut)

}

func add(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("add", info, context, inputs...)
}

func subtract(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("subtract", info, context, inputs...)
}

func multiply(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("multiply", info, context, inputs...)
}

func divide(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("divide", info, context, inputs...)
}

// ***************** Other functions *****************

func abs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "abs", Inputs: [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, Output: []d.DataTypes{d.DTfloat, d.DTint}}
	}

	col := toCol(inputs[0])
	data := d.MakeSlice(col.DataType(), col.Len(), nil)
	for ind := 0; ind < col.Len(); ind++ {
		switch col.DataType() {
		case d.DTfloat:
			data.([]float64)[ind] = math.Abs(col.Element(ind).(float64))
		case d.DTint:
			x := col.Element(ind).(int)
			if x < 0 {
				x = -x
			}

			data.([]int)[ind] = x
		default:
			panic(fmt.Errorf("unexpected error in abs"))
		}
	}

	return returnCol(data)
}

func neg(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "neg", Inputs: [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, Output: []d.DataTypes{d.DTfloat, d.DTint}}
	}

	col := toCol(inputs[0])
	data := d.MakeSlice(col.DataType(), col.Len(), nil)
	for ind := 0; ind < col.Len(); ind++ {
		switch col.DataType() {
		case d.DTfloat:
			data.([]float64)[ind] = -col.Element(ind).(float64)
		case d.DTint:
			data.([]int)[ind] = -col.Element(ind).(int)

		}
	}

	return returnCol(data)
}

////////////// logical functions

func logic(a, b int, condition string) int {
	var val int
	switch condition {
	case "and":
		val = 0
		if a > 0 && b > 0 {
			val = 1
		}
	case "or":
		val = 0
		if a > 0 || b > 0 {
			val = 1
		}
	case "not":
		val = 1
		if b >= 1 {
			val = 0
		}
	}

	return val
}

func logical(op string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: op, Inputs: [][]d.DataTypes{{d.DTint, d.DTint}}, Output: []d.DataTypes{d.DTint}}
	}

	cols, n := parameters(inputs...)
	data := d.MakeSlice(d.DTint, n, nil)
	for ind := 0; ind < n; ind++ {
		data.([]int)[ind] = logic(cols[0].Element(ind).(int), cols[1].Element(ind).(int), op)
	}

	return returnCol(data)
}

func and(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return logical("and", info, context, inputs...)
}

func or(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return logical("or", info, context, inputs...)
}

func not(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "not", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTint}}
	}

	cols, n := parameters(inputs...)
	data := d.MakeSlice(d.DTint, n, nil)
	for ind := 0; ind < n; ind++ {
		data.([]int)[ind] = logic(0, cols[0].Element(ind).(int), "not")
	}

	return returnCol(data)
}

func ifs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "if", Inputs: [][]d.DataTypes{{d.DTint, d.DTfloat, d.DTfloat},
			{d.DTint, d.DTint, d.DTint}, {d.DTint, d.DTdate, d.DTdate}, {d.DTint, d.DTstring, d.DTstring}},
			Output: []d.DataTypes{d.DTfloat, d.DTint, d.DTdate, d.DTstring}}
	}

	cols, _ := parameters(inputs...)

	var (
		outCol d.Column
		e      error
	)
	if outCol, e = cols[2].Replace(cols[0], cols[1]); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outCol}
}

// ***************** Functions that compare two columns element-wise *****************

func compare(op, name string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint},
			{d.DTdate, d.DTdate}, {d.DTstring, d.DTstring}}, Output: []d.DataTypes{d.DTfloat, d.DTint,
			d.DTdate, d.DTstring}}
	}

	cols, n := parameters(inputs...)
	data := d.MakeSlice(d.DTint, n, nil)

	for ind := 0; ind < n; ind++ {
		truth := 0

		var (
			val bool
			e   error
		)
		if val, e = d.Comparator(cols[0].Element(ind), cols[1].Element(ind), op); e != nil {
			return &d.FnReturn{Err: e}
		}

		if val {
			truth = 1
		}

		data.([]int)[ind] = truth
	}

	return returnCol(data)
}

func eq(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("==", "eq", info, context, inputs...)
}

func ge(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare(">=", "ge", info, context, inputs...)
}

func gt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare(">", "gt", info, context, inputs...)
}

func le(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("<=", "le", info, context, inputs...)
}

func lt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("<", "lt", info, context, inputs...)
}

func ne(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return compare("!=", "ne", info, context, inputs...)
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
