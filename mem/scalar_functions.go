package df

import (
	"math"
	"sort"
	"time"

	d "github.com/invertedv/df"
	"gonum.org/v1/gonum/stat"
)

func summaries() d.Fns {
	inType1 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	inType2 := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTfloat, d.DTint}}
	inType3 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTdate}}
	inType4 := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}}

	outType1 := []d.DataTypes{d.DTfloat, d.DTint}
	outType2 := []d.DataTypes{d.DTfloat, d.DTfloat}
	outType3 := []d.DataTypes{d.DTfloat, d.DTint, d.DTstring, d.DTdate}

	out := d.Fns{
		vector("dot", inType4, outType2,
			func(x ...any) (*d.Vector, error) {
				return newVector(dotP(x[1].([]float64), x[2].([]float64)), outType2[0]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(dotP(x[1].([]int), x[2].([]int)), outType2[1]), nil
			},
		),
		vector("sum", inType1, outType1,
			func(x ...any) (*d.Vector, error) { return newVector(sum(x[1].([]float64)), outType1[0]), nil },
			func(x ...any) (*d.Vector, error) { return newVector(sum(x[1].([]int)), outType1[1]), nil },
		),
		vector("mean", inType1, outType2,
			func(x ...any) (*d.Vector, error) { return newVector(mean(x[1].([]float64)), outType2[0]), nil },
			func(x ...any) (*d.Vector, error) { return newVector(mean(x[1].([]int)), outType2[1]), nil },
		),
		vector("var", inType1, outType2,
			func(x ...any) (*d.Vector, error) { return newVector(variance(x[1].([]float64)), outType2[0]), nil },
			func(x ...any) (*d.Vector, error) { return newVector(variance(x[1].([]int)), outType2[1]), nil },
		),
		vector("sdev", inType1, outType2,
			func(x ...any) (*d.Vector, error) {
				return newVector(math.Sqrt(variance(x[1].([]float64))), outType2[0]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(math.Sqrt(variance(x[1].([]int))), outType2[1]), nil
			},
		),
		vector("quantile", inType2, outType1,
			func(x ...any) (*d.Vector, error) {
				return newVector(quantile(x[1].([]float64)[0], x[2].([]float64)), outType1[0]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(qInt(x[1].([]float64)[0], x[2].([]int)), outType1[1]), nil
			},
		),
		vector("median", inType1, outType1,
			func(x ...any) (*d.Vector, error) { return newVector(quantile(0.5, x[1].([]float64)), outType1[0]), nil },
			func(x ...any) (*d.Vector, error) { return newVector(qInt(0.5, x[1].([]int)), outType1[1]), nil },
		),
		vector("min", inType3, outType3,
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(true, x[1].([]float64), func(a, b float64) bool { return a < b }), outType3[0]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(true, x[1].([]int), func(a, b int) bool { return a < b }), outType3[1]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(true, x[1].([]string), func(a, b string) bool { return a < b }), outType3[2]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(true, x[1].([]time.Time), func(a, b time.Time) bool { return a.Before(b) }), outType3[3]), nil
			},
		),
		vector("max", inType3, outType3,
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(false, x[1].([]float64), func(a, b float64) bool { return a < b }), outType3[0]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(false, x[1].([]int), func(a, b int) bool { return a < b }), outType3[1]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(false, x[1].([]string), func(a, b string) bool { return a < b }), outType3[2]), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(minMax(false, x[1].([]time.Time), func(a, b time.Time) bool { return a.Before(b) }), outType3[3]), nil
			},
		),
	}

	return out
}

func sum[T float64 | int](x []T) T {
	var s T = 0
	for _, xv := range x {
		s += xv
	}

	return s
}

func mean[T float64 | int](x []T) float64 {
	return float64(sum(x)) / float64(len(x))
}

func variance[T float64 | int](x []T) float64 {
	mn := mean(x)
	varx := 0.0
	for _, xv := range x {
		res := float64(xv) - mn
		varx += res * res
	}

	return varx / float64(len(x)-1)
}

// quantiles

func quantile(p float64, x []float64) float64 {
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

	return int(quantile(p, vFlt))
}

func minMax[T float64 | int | string | time.Time](wantMin bool, x []T, less func(a, b T) bool) T {
	minx, maxx := x[0], x[0]
	for _, xv := range x {
		if less(xv, minx) {
			minx = xv
		}
		if less(maxx, xv) {
			maxx = xv
		}
	}

	if wantMin {
		return minx
	}

	return maxx
}

// dot product
func dotP[T float64 | int](x, y []T) float64 {
	var p T = 0
	for ind := 0; ind < len(x); ind++ {
		p += x[ind] * y[ind]
	}

	return float64(p)
}
