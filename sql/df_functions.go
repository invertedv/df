package sql

import (
	"fmt"
	"strings"

	d "github.com/invertedv/df"
)

// ***************** Functions that return a data frame *****************

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

func where(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "where", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTdf}, RT: d.RTdf}
	}

	var (
		outDF d.DF
		e     error
	)
	outDF, e = df.Where(toCol(df, inputs[0]))

	return &d.FnReturn{Value: outDF, Err: e}
}

func table(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "table", Inputs: [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}},
			Output: []d.DataTypes{d.DTdf, d.DTdf, d.DTdf}, RT: d.RTdf, Varying: true}
	}
	var (
		outDF d.DF
		e     error
	)

	var names []string
	for ind := 0; ind < len(inputs); ind++ {
		names = append(names, inputs[ind].(*Col).Name())
	}

	if outDF, e = df.(*DF).Table(names...); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outDF}
}

func sortDF(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sort", Inputs: [][]d.DataTypes{{d.DTstring}},
			Output: []d.DataTypes{d.DTnil}, RT: d.RTdf, Varying: true}
	}

	ascending := true
	// Any2String will strip out the single quotes
	if strings.ToLower(toStringX(toCol(df, inputs[0]).SQL())) == "desc" {
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
