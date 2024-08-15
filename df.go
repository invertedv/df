package df

import (
	_ "embed"
	"fmt"
	u "github.com/invertedv/utilities"
	"log"
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

// DFcore is the data plus functions to operate on it -- it is the core structure of DF that is embedded
// in specific implementations
type DFcore struct {
	head    *columnList
	current *columnList

	funcs Functions
	run   RunFunc
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

// Column interface defines the methods the columns of DFcore that must be supported
type Column interface {
	Name(reNameTo string) string
	DataType() DataTypes
	Len() int
	Data() any
	Copy() Column
}

// DataTypes are the types of data that the package supports
type DataTypes uint8

// values of DataTypes
const (
	DTunknown DataTypes = 0 + iota
	DTstring
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
)

// max value of DataTypes type
const MaxDT = DTany

//go:generate stringer -type=DataTypes

type Functions []AnyFunction

type AnyFunction func(info bool, inputs ...any) *FuncReturn

type FuncReturn struct {
	Value any

	Name   string
	Output DataTypes
	Inputs []DataTypes

	Err error
}

func NewDF(run RunFunc, funcs Functions, cols ...Column) (df *DFcore, err error) {
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

type RunFunc func(fn AnyFunction, params []any, inputs []Column) (Column, error)

///////////// DFcor methods

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
		fn     AnyFunction
	)

	if fn = df.funcs.Get(opName); fn == nil {
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

/////////// DataTypes

func DTFromString(nm string) DataTypes {
	var nms []string
	for ind := DataTypes(0); ind <= MaxDT; ind++ {
		nms = append(nms, fmt.Sprintf("%v", ind))
	}

	pos := u.Position(nm, "", nms...)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}

//////// Functions Methods

func (fs Functions) Get(fnName string) AnyFunction {
	for _, f := range fs {
		if f(true, nil).Name == fnName {
			return f
		}
	}

	return nil
}

////////////////

type CategoryMap map[any]uint32
