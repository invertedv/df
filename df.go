package df

import (
	_ "embed"
	"fmt"
	"log"
	"strings"

	u "github.com/invertedv/utilities"
)

type DF interface {
	// generic from DFcore
	RowCount() int
	ColumnCount() int
	ColumnNames() []string
	Column(colName string) (col Column, err error)
	Apply(resultName, opName string, inputs ...string) error
	AppendColumn(col Column) error
	DropColumns(colNames ...string) error
	KeepColumns(keepColumns ...string) (*DFcore, error)
	Next(reset bool) Column

	// specific to underlying data source
	Sort(keys ...string) error
}

// DFcore is the data plus functions to operate on it
type DFcore struct {
	head    *columnList
	current *columnList

	funcs FuncMap
	run   RunFunc
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

type Column interface {
	Name(reNameTo string) string
	DataType() DataTypes
	Len() int
	Data() any
	Copy() Column
}

type DataTypes uint8

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

//go:generate stringer -type=DataTypes

type FuncMap map[string]*Func

type Func struct {
	Name     string
	Inputs   []DataTypes
	Output   DataTypes
	Function AnyFunction
}

type FuncReturn struct {
	Value any
	DT    DataTypes
	Name  string
	Err   error
}

type AnyFunction func(...any) *FuncReturn

func NewDF(run RunFunc, funcs FuncMap, cols ...Column) (df *DFcore, err error) {
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

	return &DFcore{head: head, funcs: funcs, run: run}, nil
}

type RunFunc func(fn *Func, params []any, inputs []Column) (Column, error)

///////////// DF methods

func (df *DFcore) Next(reset bool) Column {
	if reset || df.current == nil {
		df.current = df.head
		return df.current.col
	}

	if df.current.next == nil {
		df.current = nil
		return nil
	}

	df.current = df.current.next
	return df.current.col
}

func (df *DFcore) RowCount() int {
	return df.head.col.Len()
}

func (df *DFcore) ColumnCount() int {
	cols := 0
	for c := df.head; c != nil; c = c.next {
		cols++
	}

	return cols
}

func (df *DFcore) ColumnNames() []string {
	var names []string

	for h := df.head; h != nil; h = h.next {
		names = append(names, h.col.Name(""))
	}

	return names
}

func (df *DFcore) Column(colName string) (col Column, err error) {
	for h := df.head; h != nil; h = h.next {
		if (h.col).Name("") == colName {
			return h.col, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

func (df *DFcore) Save(saver Saver, colNames ...string) error {
	var cols []Column
	for col := df.head; col != nil; col = col.next {
		if colNames == nil || u.Has(col.col.Name(""), "", colNames...) {
			cols = append(cols, col.col)
		}
	}

	return saver(cols...)
}

type Saver func(cols ...Column) error

func (df *DFcore) Apply(resultName, opName string, inputs ...string) error {
	var (
		vals   []Column
		params []any
		fn     *Func
		ok     bool
	)

	if fn, ok = df.funcs[opName]; !ok {
		log.Printf("op to create %s not defined, operation skipped", resultName)
		return nil
		//		return fmt.Errorf("function %s not found", opName)
	}

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

	var (
		col Column
		e   error
	)

	if col, e = df.run(fn, params, vals); e != nil {
		return e
	}

	col.Name(resultName)

	return df.AppendColumn(col)
}

func (df *DFcore) AppendColumn(col Column) error {
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

func (df *DFcore) node(colName string) (node *columnList, err error) {
	for h := df.head; h != nil; h = h.next {
		if h.col.Name("") == colName {
			return h, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

func (df *DFcore) DropColumns(colNames ...string) error {
	for _, cName := range colNames {
		var (
			node *columnList
			e    error
		)

		if node, e = df.node(cName); e != nil {
			return fmt.Errorf("column %s not found", cName)
		}

		if node == df.head {
			if df.head.next == nil {
				df.head = nil
				return fmt.Errorf("no columns left")
			}

			df.head = df.head.next
			continue
		}

		node.prior.next = node.next
		if node.next != nil {
			node.next.prior = node.prior
		}
	}

	return nil
}

func (df *DFcore) KeepColumns(colNames ...string) (*DFcore, error) {
	var subHead, tail *columnList

	for ind := 0; ind < len(colNames); ind++ {
		var (
			col Column
			err error
		)

		if col, err = df.Column(colNames[ind]); err != nil {
			return nil, err
		}

		newNode := &columnList{
			col:   col,
			prior: nil,
			next:  nil,
		}

		if subHead == nil {
			subHead, tail = newNode, newNode
			continue
		}

		newNode.prior = tail
		tail.next = newNode
		tail = newNode
	}

	subsetDF := &DFcore{
		head:  subHead,
		funcs: df.funcs,
		run:   df.run,
	}

	return subsetDF, nil
}

///////////// FuncMap funcs

// TODO: can I combined funcDetails into this?
func LoadFunctions(functions FunctionList) FuncMap {
	fns := make(FuncMap)
	names, inputs, outputs, fnsx := funcDetails(functions)
	for ind := 0; ind < len(names); ind++ {
		fns[names[ind]] = &Func{
			Name:     names[ind],
			Inputs:   inputs[ind],
			Output:   outputs[ind],
			Function: fnsx[ind],
		}
	}

	return fns
}

//go:embed funcs/funcDefs.txt
var funcDefs string

func funcDetails(functions FunctionList) (names []string, inputs [][]DataTypes, outputs []DataTypes, fns []AnyFunction) {
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
		if thisFunc = functions.Get(name); thisFunc == nil {
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

type GetFunction func(funcName string) AnyFunction

type FunctionList []AnyFunction

func (f FunctionList) Get(fnName string) AnyFunction {
	var names []string

	for ind := 0; ind < len(f); ind++ {
		fnr := f[ind](nil)
		names = append(names, fnr.Name)
	}

	pos := u.Position(fnName, "", names...)
	if pos < 0 {
		return nil
	}
	return f[pos]

}

/////////// DataTypes

func DTFromString(nm string) DataTypes {
	const nms = "DTstring,DTfloat,DTint,DTcategory,DTdate,DTdateTime,DTtime,DTslcString,DTslcFloat,DTslcInt,DTany,DTunknown"

	pos := u.Position(nm, ",", nms)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}

////////////////

type CategoryMap map[any]uint32
