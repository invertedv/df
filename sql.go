package df

import (
	"database/sql"
	"fmt"
	"strings"
)

type SQLfunc struct {
	name     string
	inputs   []DataTypes
	output   DataTypes
	function string
}

func (fn *SQLfunc) Run(inputs ...any) (outCol Column, err error) {
	if len(inputs) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.name, len(fn.inputs))
	}

	var (
		vals   []*SQLcol
		params []any
	)

	fnx := fn.function

	for ind := 0; ind < len(inputs); ind++ {
		var (
			col *SQLcol
			ok  bool
		)

		if col, ok = inputs[ind].(*SQLcol); ok {
			vals = append(vals, col)
		} else {
			params = append(params, inputs[ind])
		}
	}

	for ind := 0; ind < len(params); ind++ {
		xadd, e := toDataType(params[ind], fn.inputs[ind], true)
		if e != nil {
			return nil, e
		}
		fnx = strings.Replace(fnx, fmt.Sprintf("P%d", ind), fmt.Sprintf("%d", xadd), 1)
	}

	for ind := 0; ind < len(vals); ind++ {
		if vals[ind].DataType() != fn.inputs[ind+len(params)] {
			return nil, fmt.Errorf("column %s is data type %d, need %d", vals[ind].Name(""), vals[ind].DataType(), fn.inputs[ind+len(params)])
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("X%d", ind), vals[ind].Name(""), 1)
	}

	outCol = &SQLcol{
		name:   "",
		n:      1,
		dType:  fn.output,
		sql:    fnx,
		catMap: nil,
	}

	return outCol, nil
}

type SQLfuncMap map[string]*SQLfunc

type SQLcol struct {
	name  string
	n     int
	dType DataTypes
	sql   string

	catMap categoryMap
}

type SQLdf struct {
	sourceSQL     string
	destTableName string
	db            *sql.DB

	*DFlist
}

func (s *SQLcol) DataType() DataTypes {
	return s.dType
}

func (s *SQLcol) Len() int {
	return s.n
}

func (s *SQLcol) Data() any {
	return s.sql
}

func (s *SQLcol) Name(renameTo string) string {
	if renameTo != "" {
		s.name = renameTo
	}

	return s.name
}

func (s *SQLcol) Cast(dt DataTypes) (any, error) {

	return nil, nil
}

var SQLfunctions SQLfuncMap = LoadSQLfunctions()

func LoadSQLfunctions() SQLfuncMap {
	fn := make(SQLfuncMap)

	fn["exp"] = &SQLfunc{
		name:     "exp",
		inputs:   []DataTypes{DTfloat},
		output:   DTfloat,
		function: "exp(X0)",
	}

	fn["addFloat"] = &SQLfunc{
		name:     "addFloat",
		inputs:   []DataTypes{DTfloat, DTfloat},
		output:   DTfloat,
		function: "X0 + X1",
	}

	return fn
}
