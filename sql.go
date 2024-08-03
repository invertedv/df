package df

import (
	"fmt"
	"strings"
)

type SQLfunc struct {
	name     string
	inputs   []DataTypes
	output   DataTypes
	function string
}

type SQLfunctionMap map[string]*SQLfunc

func (fn *SQLfunc) Run(cols ...Column) (outCol Column, err error) {
	if len(cols) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(cols), fn.name, len(fn.inputs))
	}

	fnx := fn.function
	for ind := 0; ind < len(cols); ind++ {
		fnx = strings.Replace(fnx, fmt.Sprintf("X%d", ind), cols[ind].Name(""), 1)
	}

	outCol = &SQLcol{
		name:   "",
		n:      1,
		dType:  fn.output,
		sql:    fn.function,
		catMap: nil,
	}

	return outCol, nil
}

type SQLcol struct {
	name  string
	n     int
	dType DataTypes
	sql   string

	catMap categoryMap
}

type SQLdf struct {
	sourceSQL string
	destSQL   string
	*DFlist
}

func (s *SQLcol) DataType() DataTypes {
	return s.dType
}

func (s *SQLcol) N() int {
	return s.n
}

func (s *SQLcol) Data() any {
	return s.sql
}

func (s *SQLcol) Name(newName string) string {
	if newName != "" {
		s.name = newName
	}

	return s.name
}

func (s *SQLcol) To(dt DataTypes) (any, error) {

	return nil, nil
}

func SQLAdd(cols ...*SQLcol) (out *SQLcol, err error) {
	out = &SQLcol{
		name:   "",
		n:      0,
		dType:  0,
		sql:    "",
		catMap: nil,
	}

	return out, nil
}

var SQLfunctions SQLfunctionMap = LoadSQLfunctions()

func LoadSQLfunctions() SQLfunctionMap {
	fn := make(SQLfunctionMap)

	fn["exp"] = &SQLfunc{
		name:     "exp",
		inputs:   []DataTypes{DTfloat},
		output:   DTfloat,
		function: "exp(x0)",
	}

	fn["addFloat"] = &SQLfunc{
		name:     "addFloat",
		inputs:   []DataTypes{DTfloat, DTfloat},
		output:   DTfloat,
		function: "x0 + x1",
	}

	return fn
}
