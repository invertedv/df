package df

import (
	"fmt"
	"strings"

	d "github.com/invertedv/df"
)

// where implements Where to subset a dataframe
func where(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "where", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTdf}}
	}

	var (
		outDF d.DF
		e     error
	)
	outDF, e = df.Where(inputs[0].(d.Column))

	return &d.FnReturn{Value: outDF, Err: e}
}

func sortDF(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sort", Inputs: [][]d.DataTypes{{d.DTstring}},
			Output: []d.DataTypes{d.DTnil}, Varying: true}
	}

	ascending := true
	s, _ := toCol(inputs[0]).ElementString(0)
	if *s == "desc" {
		ascending = false
	}

	var (
		colNames []string
		e        error
	)

	if colNames, e = getNames(1, inputs...); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Err: df.Sort(ascending, colNames...)}
}

func table(info bool, df d.DF, inputs ...any) *d.FnReturn {
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
	if outDF, ex = df.Table(colNames...); ex != nil {
		return &d.FnReturn{Err: ex}
	}

	ret := &d.FnReturn{Value: outDF}

	return ret
}

func by(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "by", Inputs: [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTdate}},
			Output: []d.DataTypes{d.DTdf, d.DTdf, d.DTdf}, Varying: true}
	}

	var (
		gb   []string
		eqns []string
	)
	onCols := true
	for ind := 0; ind < len(inputs); ind++ {
		if sc, ok := inputs[ind].(*d.Scalar); ok {
			onCols = false
			var (
				eqn *string
				e   error
			)
			if eqn, e = sc.Data().ElementString(0); e != nil {
				return &d.FnReturn{Err: e}
			}
			eqns = append(eqns, *eqn)
		} else {
			if !onCols {
				return &d.FnReturn{Err: fmt.Errorf("parameters out of order in By")}
			}
			gb = append(gb, inputs[ind].(*Col).Name())
		}
	}

	var (
		outDF d.DF
		ex    error
	)
	if outDF, ex = df.By(strings.Join(gb, ","), eqns...); ex != nil {
		return &d.FnReturn{Err: ex}
	}

	ret := &d.FnReturn{Value: outDF}

	return ret

}
