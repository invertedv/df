package df

import (
	"math"
	"sort"
	"time"

	d "github.com/invertedv/df"
	"gonum.org/v1/gonum/stat"
)

func scalar(name string, inp [][]d.DataTypes, outp []d.DataTypes, fnx ...any) d.Fn {
	fn := func(info bool, context *d.Context, inputs ...d.Column) *d.FnReturn {
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

		var inData []*d.Vector
		for ind := 0; ind < len(col); ind++ {
			inData = append(inData, col[ind].Vector) // .Data()
		}

		switch {
		case dtOut == d.DTfloat:
			fny := fnUse.(func(...*d.Vector) float64)
			data.SetFloat(fny(inData...), 0)
		case dtOut == d.DTint:
			fny := fnUse.(func(...*d.Vector) int)
			data.SetInt(fny(inData...), 0)
		case dtOut == d.DTstring:
			fny := fnUse.(func(...*d.Vector) string)
			data.SetString(fny(inData...), 0)
		case dtOut == d.DTdate:
			fny := fnUse.(func(...*d.Vector) time.Time)
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
			func(x ...*d.Vector) float64 {
				s := 0.0
				for _, xx := range x[0].AsFloat() {
					s += xx
				}
				return s
			},
			func(x ...*d.Vector) int {
				s := 0
				for _, xx := range x[0].AsInt() {
					s += xx
				}
				return s
			}),
		scalar("mean", inType1, outType2, meanFlt, meanInt),
		scalar("var", inType1, outType2, varFlt, varInt),
		scalar("sdev", inType1, outType2,
			func(x ...*d.Vector) float64 { return math.Sqrt(varFlt(x...)) },
			func(x ...*d.Vector) float64 { return math.Sqrt(varInt(x...)) }),
		scalar("median", inType1, outType1,
			func(x ...*d.Vector) float64 { return qFlt(0.5, x[0].AsFloat()) },
			func(x ...*d.Vector) int { return qInt(0.5, x[0].AsInt()) }),
		scalar("quantile", inType2, outType1,
			func(x ...*d.Vector) float64 { return qFlt(x[0].AsFloat()[0], x[1].AsFloat()) },
			func(x ...*d.Vector) int { return qInt(x[0].AsFloat()[0], x[1].AsInt()) }),
		scalar("min", inType3, outType3,
			func(x ...*d.Vector) float64 { return minMaxFlt(x[0].AsFloat())[0] },
			func(x ...*d.Vector) int { return minMaxInt(x[0].AsInt())[0] },
			func(x ...*d.Vector) string { return minMaxStr(x[0].AsString())[0] },
			func(x ...*d.Vector) time.Time { return minMaxDt(x[0].AsDate())[0] },
		),
		scalar("max", inType3, outType3,
			func(x ...*d.Vector) float64 { return minMaxFlt(x[0].AsFloat())[1] },
			func(x ...*d.Vector) int { return minMaxInt(x[0].AsInt())[1] },
			func(x ...*d.Vector) string { return minMaxStr(x[0].AsString())[1] },
			func(x ...*d.Vector) time.Time { return minMaxDt(x[0].AsDate())[1] },
		),
		scalar("dot", inType4, outType4, func(x ...*d.Vector) float64 { return dotP(x[0].AsFloat(), x[1].AsFloat()) }),
	}

	return out
}

func meanFlt(x ...*d.Vector) float64 {
	s, v := 0.0, x[0].AsFloat()
	for _, xx := range v {
		s += xx
	}
	return s / float64(len(v))
}

func meanInt(x ...*d.Vector) float64 {
	s, v := 0.0, x[0].AsInt()
	for _, xx := range v {
		s += float64(xx)
	}
	return s / float64(len(v))
}

// variance

func varFlt(x ...*d.Vector) float64 {
	mn := meanFlt(x[0])
	varx, v := 0.0, x[0].AsFloat()
	for _, xx := range v {
		varx += (xx - mn) * (xx - mn)
	}

	return varx / float64(len(v)-1)
}

func varInt(x ...*d.Vector) float64 {
	mn := meanInt(x[0])
	varx, v := 0.0, x[0].AsInt()
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
