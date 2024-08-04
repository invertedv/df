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

type SQLfuncMap map[string]*SQLfunc

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
