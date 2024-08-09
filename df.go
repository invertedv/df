package df

import (
	_ "embed"
	"fmt"
	"log"
	"strings"

	"github.com/invertedv/utilities"
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

	pos := utilities.Position(nm, ",", nms)
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

type Saver func(cols ...Column) error

type DF struct {
	head *DFlist
}

func NewDF(cols ...Column) (df *DF, err error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDF")
	}

	var head, priorNode *DFlist
	for ind := 0; ind < len(cols); ind++ {
		node := &DFlist{
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

func (df *DF) ColumnCount() int {
	cols := 0
	for c := df.head; c != nil; c = c.Next() {
		cols++
	}

	return cols
}

func (df *DF) ColumnNames() []string {
	var names []string

	for h := df.head; h != nil; h = h.next {
		names = append(names, h.Col().Name(""))
	}

	return names
}

func (df *DF) Column(colName string) (col Column, err error) {
	var dfl *DFlist
	dfl, err = df.head.Node(colName)
	if err != nil {
		return nil, err
	}

	return dfl.Col(), err
}

func (df *DF) Save(saver Saver, colNames ...string) error {
	var cols []Column
	for col := df.head; col != nil; col = col.next {
		if colNames == nil || utilities.Has(col.col.Name(""), "", colNames...) {
			cols = append(cols, col.col)
		}
	}

	return saver(cols...)
}

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
	if utilities.Has(col.Name(""), "", df.ColumnNames()...) {
		return fmt.Errorf("duplicate column name: %s", col.Name(""))
	}

	if col.Len() != df.head.col.Len() {
		return fmt.Errorf("length mismatch: dfList - %d, append col - %d", df.head.col.Len(), col.Len())
	}

	tail := df.head.Tail()

	dfl := &DFlist{
		col:   col,
		prior: tail,
		next:  nil,
	}

	tail.next = dfl

	return nil
}

func (df *DF) Drop(colName string) error {
	col, err := df.head.Node(colName)
	if err != nil {
		return err
	}

	if col == df.head {
		df.head = col
		return nil
	}

	col.prior.next = col.next
	col.next.prior = col.prior

	return nil
}

type DFlist struct {
	col Column

	prior *DFlist
	next  *DFlist
}

func (df *DFlist) Col() Column {
	return df.col
}

func (df *DFlist) Next() *DFlist {
	return df.next
}

func (df *DFlist) Prior() *DFlist {
	return df.prior
}

func (df *DFlist) Head() *DFlist {
	var head *DFlist
	for head = df; head.prior != nil; head = head.prior {
	}

	return head
}

func (df *DFlist) Tail() *DFlist {
	var tail *DFlist
	for tail = df; tail.next != nil; tail = tail.next {
	}

	return tail
}

func (df *DFlist) Node(colName string) (dfl *DFlist, err error) {
	for h := df.Head(); h != nil; h = h.next {
		if (h.Col()).Name("") == colName {
			return h, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
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

	pos := utilities.Position(funcName, "", names...)
	if pos < 0 {
		return nil
	}
	if wantMemFunc {
		return mem[pos]
	}

	return sql[pos]
}
