package sql

import (
	d "github.com/invertedv/df"
	"strings"
)

// ***************** Functions that return a data frame *****************

func where(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "where", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTdf}}
	}

	var (
		outDF d.DF
		e     error
	)
	outDF, e = df.Where(inputs[0])

	return &d.FnReturn{Value: outDF, Err: e}
}

func table(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "table", Inputs: [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}},
			Output: []d.DataTypes{d.DTdf, d.DTdf, d.DTdf}, Varying: true}
	}
	var (
		outDF d.DF
		e     error
	)

	var names []string
	for ind := 0; ind < len(inputs); ind++ {
		names = append(names, inputs[ind].(*Col).Name())
	}

	if outDF, e = df.(*DF).Table(false, names...); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outDF}
}

func sortDF(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sort", Inputs: [][]d.DataTypes{{d.DTstring}},
			Output: []d.DataTypes{d.DTnil}, Varying: true}
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
