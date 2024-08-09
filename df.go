package df

import (
	_ "embed"
	"fmt"
	"log"
	"strings"

	u "github.com/invertedv/utilities"
)

type DataTypes uint8

//go:embed funcs/funcDefs.txt
var funcDefs string

const (
	DTstring DataTypes = 0 + iota
	DTfloat
	DTint
	DTcategory
	DTdate
	DTdateTime
	DTtime
	DTslcString
	DTslcFloat
	DTslcInt
	DTany
	DTunknown
)

func DTFromString(nm string) DataTypes {
	const nms = "DTstring,DTfloat,DTint,DTcategory,DTdate,DTdateTime,DTtime,DTslcString,DTslcFloat,DTslcInt,DTany,DTunknown"

	pos := u.Position(nm, ",", nms)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}

//go:generate stringer -type=DataTypes

type Column interface {
	Name(reNameTo string) string
	DataType() DataTypes
	Len() int
	Data() any
}

type DF struct {
	head *columnList
}

func NewDF(cols ...Column) (df *DF, err error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDF")
	}

	var head, priorNode *columnList
	for ind := 0; ind < len(cols); ind++ {
		node := &columnList{
			col: cols[ind],

			prior: priorNode,
			next:  nil,
		}

		if priorNode != nil {
			priorNode.next = node
		}

		priorNode = node

		if ind == 0 {
			head = node
		}
	}

	return &DF{head: head}, nil
}

func (df *DF) RowCount() int {
	return df.head.col.Len()
}

func (df *DF) ColumnCount() int {
	cols := 0
	for c := df.head; c != nil; c = c.next {
		cols++
	}

	return cols
}

func (df *DF) ColumnNames() []string {
	var names []string

	for h := df.head; h != nil; h = h.next {
		names = append(names, h.col.Name(""))
	}

	return names
}

func (df *DF) Column(colName string) (col Column, err error) {
	for h := df.head; h != nil; h = h.next {
		if (h.col).Name("") == colName {
			return h.col, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

func (df *DF) Save(saver Saver, colNames ...string) error {
	var cols []Column
	for col := df.head; col != nil; col = col.next {
		if colNames == nil || u.Has(col.col.Name(""), "", colNames...) {
			cols = append(cols, col.col)
		}
	}

	return saver(cols...)
}

type Saver func(cols ...Column) error

func (df *DF) Apply(resultName string, op Runner, fn *Func, inputs ...string) error {
	if fn == nil {
		log.Printf("op to create %s not defined, operation skipped", resultName)
		return nil
	}

	var (
		vals   []Column
		params []any
	)

	doneParams := false
	for ind := 0; ind < len(inputs); ind++ {
		if c, e := df.Column(inputs[ind]); e == nil {
			doneParams = true
			vals = append(vals, c)
		} else {
			if doneParams {
				return fmt.Errorf("missing column? %s", inputs[ind])
			}

			params = append(params, inputs[ind])
		}
	}

	col, e := op(fn, params, vals)
	if e != nil {
		return e
	}

	col.Name(resultName)

	return df.Append(col)
}

type Runner func(fn *Func, params []any, inputs []Column) (Column, error)

func (df *DF) Append(col Column) error {
	if u.Has(col.Name(""), "", df.ColumnNames()...) {
		return fmt.Errorf("duplicate column name: %s", col.Name(""))
	}

	if col.Len() != df.RowCount() {
		return fmt.Errorf("length mismatch: dfList - %d, append col - %d", df.head.col.Len(), col.Len())
	}

	var tail *columnList
	for tail = df.head; tail.next != nil; tail = tail.next {
	}

	dfl := &columnList{
		col:   col,
		prior: tail,
		next:  nil,
	}

	tail.next = dfl

	return nil
}

func (df *DF) node(colName string) (node *columnList, err error) {
	for h := df.head; h != nil; h = h.next {
		if h.col.Name("") == colName {
			return h, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

func (df *DF) Drop(colName string) error {
	col, err := df.node(colName)
	if err != nil {
		return err
	}

	if col == df.head {
		if df.head.next == nil {
			df.head = nil
			return fmt.Errorf("no columns left")
		}

		df.head = df.head.next
		return nil
	}

	col.prior.next = col.next
	if col.next != nil {
		col.next.prior = col.prior
	}

	return nil
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

//func loadDF(loader Loader) (df *DF, err error) {
//
//	return nil, nil
//}

type FuncMap map[string]*Func

type Func struct {
	name     string
	inputs   []DataTypes
	output   DataTypes
	function AnyFunction
}

type AnyFunction func(...any) (any, DataTypes, error)

func LoadFunctions(wantMemFuncs bool) FuncMap {
	fns := make(FuncMap)
	names, inputs, outputs, fnsx := funcDetails(wantMemFuncs)
	for ind := 0; ind < len(names); ind++ {
		fns[names[ind]] = &Func{
			name:     names[ind],
			inputs:   inputs[ind],
			output:   outputs[ind],
			function: fnsx[ind],
		}
	}

	return fns
}

func funcDetails(wantMemFuncs bool) (names []string, inputs [][]DataTypes, outputs []DataTypes, fns []AnyFunction) {
	fDetail := strings.Split(funcDefs, "\n")
	for _, f := range fDetail {
		if f == "" {
			continue
		}

		detail := strings.Split(f, ",")
		if len(detail) < 3 {
			continue
		}

		var (
			outs     DataTypes
			inps     []DataTypes
			thisFunc AnyFunction
		)

		name := detail[0]
		if thisFunc = functions(name, wantMemFuncs); thisFunc == nil {
			panic(fmt.Sprintf("unknown function: %s", name))
		}

		if outs = DTFromString(detail[len(detail)-1]); outs == DTunknown {
			panic(fmt.Sprintf("unknown DataTypes %s", detail[len(detail)-1]))
		}

		for ind := 1; ind < len(detail)-1; ind++ {
			var val DataTypes
			if val = DTFromString(detail[ind]); val == DTunknown {
				panic(fmt.Sprintf("unknown DataTypes %s", detail[ind]))
			}

			inps = append(inps, val)
		}

		names = append(names, name)
		inputs = append(inputs, inps)
		outputs = append(outputs, outs)
		fns = append(fns, thisFunc)
	}

	return names, inputs, outputs, fns
}

func functions(funcName string, wantMemFunc bool) AnyFunction {
	names := []string{
		"exp", "abs", "cast", "add",
	}

	mem := []AnyFunction{
		memExp, memAbs, memCast, memAdd,
	}

	sql := []AnyFunction{
		sqlExp, sqlAbs, sqlCast, sqlAdd,
	}

	pos := u.Position(funcName, "", names...)
	if pos < 0 {
		return nil
	}
	if wantMemFunc {
		return mem[pos]
	}

	return sql[pos]
}
