package df

import (
	d "github.com/invertedv/df"
	"gonum.org/v1/gonum/stat"
	"math"
	"sort"
	"time"
)

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

		data := d.MakeVector(dtOut, 1)

		var inData []any
		for ind := 0; ind < len(col); ind++ {
			inData = append(inData, col[ind].Data())
		}

		switch {
		case dtOut == d.DTfloat:
			fny := fnUse.(func(...any) float64)
			data.SetFloat(fny(inData...), 0)
		case dtOut == d.DTint:
			fny := fnUse.(func(...any) int)
			data.SetInt(fny(inData...), 0)
		case dtOut == d.DTstring:
			fny := fnUse.(func(...any) string)
			data.SetString(fny(inData...), 0)
		case dtOut == d.DTdate:
			fny := fnUse.(func(...any) time.Time)
			data.SetDate(fny(inData...), 0)
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
