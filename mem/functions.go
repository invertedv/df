package df

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"

	d "github.com/invertedv/df"
)

// NewDFseq - arrayJoin(range(1,n)) or cnt(1,n)

func StandardFunctions() d.Fns {
	fns := d.Fns{applyCat,
		sortDF, table, toCat,
		where,
	}
	fns = append(fns, comparisons()...)
	fns = append(fns, mathOps()...)
	fns = append(fns, logicalOps()...)
	fns = append(fns, mathFuncs()...)
	fns = append(fns, otherVectors()...)
	fns = append(fns, castOps()...)
	fns = append(fns, summaries()...)

	return fns
}

// ***************** Vector-Valued Functions that return take a single int/float *****************

func castOps() d.Fns {
	const (
		bitSizeF = 64
		bitSizeI = 64
		base     = 10
		dtFmt    = "2006-01-02"
	)

	inType1 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}}
	inType2 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTdate}}
	inType3 := [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}}
	fltOutType := []d.DataTypes{d.DTfloat, d.DTfloat, d.DTfloat}
	intOutType := []d.DataTypes{d.DTint, d.DTint, d.DTint}
	strOutType := []d.DataTypes{d.DTstring, d.DTstring, d.DTstring, d.DTstring}
	dtOutType := []d.DataTypes{d.DTdate, d.DTdate, d.DTdate}

	asDate := func(x string) time.Time {
		formats := []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006", "Jan 2 2006", "January 2 2006", "2006-01-02"}
		for _, fmtx := range formats {
			dt, e := time.Parse(fmtx, strings.ReplaceAll(x, "'", ""))
			if e == nil {
				return dt
			}
		}

		panic("cannot convert to date")
	}

	out := d.Fns{
		vector("float", inType1, fltOutType,
			func(x ...any) float64 { return x[0].(float64) },
			func(x ...any) float64 { return float64(x[0].(int)) },
			func(x ...any) float64 {
				if xf, e := strconv.ParseFloat(x[0].(string), bitSizeF); e == nil {
					return xf
				}
				panic("cannot convert string to float")
			}),
		vector("int", inType1, intOutType,
			func(x ...any) int { return int(x[0].(float64)) },
			func(x ...any) int { return x[0].(int) },
			func(x ...any) int {
				if xi, e := strconv.ParseInt(x[0].(string), base, bitSizeI); e == nil {
					return int(xi)
				}
				panic("cannot convert string to int")
			}),
		vector("string", inType2, strOutType,
			func(x ...any) string { v := x[0].(float64); return fmt.Sprintf(d.SelectFormat([]float64{v}), v) },
			func(x ...any) string { return fmt.Sprintf("%d", x[0].(int)) },
			func(x ...any) string { return x[0].(string) },
			func(x ...any) string { return x[0].(time.Time).Format(dtFmt) }),
		vector("date", inType3, dtOutType,
			func(x ...any) time.Time { return asDate(fmt.Sprintf("%d", x[0].(int))) },
			func(x ...any) time.Time { return asDate(x[0].(string)) },
			func(x ...any) time.Time { return x[0].(time.Time) })}

	return out
}

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
		vector("abs", inType2, outType2, func(x ...any) float64 { return math.Abs(x[0].(float64)) }, absInt),
	}

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

func otherVectors() d.Fns {
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

// ***************** handler for functions that return a vector *****************
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

// ***************** Functions that take a single column and return a scalar *****************

func scalar(name string, inp [][]d.DataTypes, outp []d.DataTypes, fnx ...any) d.Fn {
	fn := func(info bool, context *d.Context, inputs ...any) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
		}

		fnUse := fnx[0]
		dtOut := outp[0]
		var col []*Col
		if inp != nil {
			col, _ = parameters(inputs...) // this should return inData + vector of row # if no parameters
			ind := signature(inp, col...)
			if ind < 0 {
				panic("no signature")
			}

			fnUse = fnx[ind]
			dtOut = outp[ind]
		}

		data := d.MakeSlice(dtOut, 1, nil)

		var inData []any
		for ind := 0; ind < len(col); ind++ {
			inData = append(inData, col[ind].Data())
		}

		switch {
		case dtOut == d.DTfloat:
			fny := fnUse.(func(...any) float64)
			data.([]float64)[0] = fny(inData...)

		case dtOut == d.DTint:
			fny := fnUse.(func(...any) int)
			data.([]int)[0] = fny(inData...)
		case dtOut == d.DTstring:
			fny := fnUse.(func(...any) string)
			data.([]string)[0] = fny(inData...)
		case dtOut == d.DTdate:
			fny := fnUse.(func(...any) time.Time)
			data.([]time.Time)[0] = fny(inData...)
		}

		return returnCol(data)
	}

	return fn
}

func summaries() d.Fns {
	inType1 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	inType2 := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTfloat, d.DTint}}
	inType3 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTdate}}
	inType4 := [][]d.DataTypes{{d.DTfloat, d.DTfloat}}

	outType1 := []d.DataTypes{d.DTfloat, d.DTint}
	outType2 := []d.DataTypes{d.DTfloat, d.DTfloat}
	outType3 := []d.DataTypes{d.DTfloat, d.DTint, d.DTstring, d.DTdate}
	outType4 := []d.DataTypes{d.DTfloat}

	out := d.Fns{
		scalar("sum", inType1, outType1,
			func(x ...any) float64 {
				s := 0.0
				for _, xx := range x[0].([]float64) {
					s += xx
				}
				return s
			},
			func(x ...any) int {
				s := 0
				for _, xx := range x[0].([]int) {
					s += xx
				}
				return s
			}),
		scalar("mean", inType1, outType2, meanFlt, meanInt),
		scalar("var", inType1, outType2, varFlt, varInt),
		scalar("sdev", inType1, outType2,
			func(x ...any) float64 { return math.Sqrt(varFlt(x...)) },
			func(x ...any) float64 { return math.Sqrt(varInt(x...)) }),
		scalar("median", inType1, outType1,
			func(x ...any) float64 { return qFlt(0.5, x[0].([]float64)) },
			func(x ...any) int { return qInt(0.5, x[0].([]int)) }),
		scalar("quantile", inType2, outType1,
			func(x ...any) float64 { return qFlt(x[0].([]float64)[0], x[1].([]float64)) },
			func(x ...any) int { return qInt(x[0].([]float64)[0], x[1].([]int)) }),
		scalar("min", inType3, outType3,
			func(x ...any) float64 { return minMaxFlt(x[0].([]float64))[0] },
			func(x ...any) int { return minMaxInt(x[0].([]int))[0] },
			func(x ...any) string { return minMaxStr(x[0].([]string))[0] },
			func(x ...any) time.Time { return minMaxDt(x[0].([]time.Time))[0] },
		),
		scalar("max", inType3, outType3,
			func(x ...any) float64 { return minMaxFlt(x[0].([]float64))[1] },
			func(x ...any) int { return minMaxInt(x[0].([]int))[1] },
			func(x ...any) string { return minMaxStr(x[0].([]string))[1] },
			func(x ...any) time.Time { return minMaxDt(x[0].([]time.Time))[1] },
		),
		scalar("dot", inType4, outType4, func(x ...any) float64 { return dotP(x[0].([]float64), x[1].([]float64)) }),
	}

	return out
}

// mean

func meanFlt(x ...any) float64 {
	s, v := 0.0, x[0].([]float64)
	for _, xx := range v {
		s += xx
	}
	return s / float64(len(v))
}
func meanInt(x ...any) float64 {
	s, v := 0.0, x[0].([]int)
	for _, xx := range v {
		s += float64(xx)
	}
	return s / float64(len(v))
}

// variance

func varFlt(x ...any) float64 {
	mn := meanFlt(x[0])
	varx, v := 0.0, x[0].([]float64)
	for _, xx := range v {
		varx += (xx - mn) * (xx - mn)
	}

	return varx / float64(len(v)-1)
}
func varInt(x ...any) float64 {
	mn := meanInt(x[0])
	varx, v := 0.0, x[0].([]int)
	for _, xx := range v {
		xxf := float64(xx)
		varx += (xxf - mn) * (xxf - mn)
	}

	return varx / float64(len(v)-1)
}

// quantiles

func qFlt(p float64, x []float64) float64 {
	if sort.Float64sAreSorted(x) {
		return stat.Quantile(p, stat.LinInterp, x, nil)
	}

	vSort := make([]float64, len(x))
	copy(vSort, x)
	sort.Float64s(vSort)
	return stat.Quantile(p, stat.LinInterp, vSort, nil)
}
func qInt(p float64, x []int) int {
	vFlt := make([]float64, len(x))
	for ind, xx := range x {
		vFlt[ind] = float64(xx)
	}

	return int(qFlt(p, vFlt))
}

// min/max

func minMaxFlt(x []float64) []float64 {
	minx, maxx := x[0], x[0]
	for _, xx := range x {
		if xx > maxx {
			maxx = xx
		}
		if xx < minx {
			minx = xx
		}
	}

	return []float64{minx, maxx}
}
func minMaxInt(x []int) []int {
	minx, maxx := x[0], x[0]
	for _, xx := range x {
		if xx > maxx {
			maxx = xx
		}
		if xx < minx {
			minx = xx
		}
	}

	return []int{minx, maxx}
}
func minMaxStr(x []string) []string {
	minx, maxx := x[0], x[0]
	for _, xx := range x {
		if xx > maxx {
			maxx = xx
		}
		if xx < minx {
			minx = xx
		}
	}

	return []string{minx, maxx}
}
func minMaxDt(x []time.Time) []time.Time {
	minx, maxx := x[0], x[0]
	for _, xx := range x {
		if maxx.Sub(xx).Minutes() < 0 {
			maxx = xx
		}
		if minx.Sub(xx).Minutes() > 0 {
			minx = xx
		}
	}

	return []time.Time{minx, maxx}
}

// dot product
func dotP(x, y []float64) float64 {
	p := 0.0
	for ind := 0; ind < len(x); ind++ {
		p += x[ind] * y[ind]
	}

	return p
}

type fun func(x ...any) any

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

func bint(x bool) int {
	if x {
		return 1
	}
	return 0
}
