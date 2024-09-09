package df

import (
	_ "embed"
	"fmt"
	"log"
	"strings"

	u "github.com/invertedv/utilities"
)

// TODO: add where to sql/df

type DF interface {
	// generic from DFcore
	ColumnCount() int
	ColumnNames() []string
	ColumnTypes(cols ...string) ([]DataTypes, error)
	Column(colName string) (col Column, err error)
	Apply(resultName, opName string, replace bool, inputs ...string) error
	AppendColumn(col Column, replace bool) error
	DropColumns(colNames ...string) error
	KeepColumns(keepColumns ...string) (*DFcore, error)
	Next(reset bool) Column
	CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error
	Fn(fn Ereturn) error
	Funcs() Functions
	DoOp(opName string, inputs ...any) (Column, error)

	// specific to underlying data source
	RowCount() int
	Sort(keys ...string) error
	DBsave(tableName string, overwrite bool, cols ...string) error
	FileSave(fileName string) error
	MakeColumn(value any) (Column, error)
	Where(indicator Column) error
}

type Ereturn func() error

// DFcore is the data plus functions to operate on it -- it is the core structure of DF that is embedded
// in specific implementations
type DFcore struct {
	head *columnList

	funcs Functions
	run   RunFunc

	current *columnList

	*Context
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
	DTnone
	DTany
)

//go:generate stringer -type=DataTypes

// max value of DataTypes type
const MaxDT = DTany

type Functions []AnyFunction

type AnyFunction func(info bool, context *Context, inputs ...any) *FuncReturn

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

type RunFunc func(fn AnyFunction, context *Context, inputs []any) (Column, error)

// /////////// DFcore methods
func (df *DFcore) Funcs() Functions {
	return df.funcs
}

func (df *DFcore) SetContext(c *Context) {
	df.Context = c
}

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

func (df *DFcore) ColumnTypes(colNames ...string) ([]DataTypes, error) {
	var types []DataTypes

	if colNames == nil {
		colNames = df.ColumnNames()
	}

	for ind := 0; ind < len(colNames); ind++ {
		var (
			c Column
			e error
		)
		if c, e = df.Column(colNames[ind]); e != nil {
			return nil, e
		}

		types = append(types, c.DataType())

	}

	return types, nil
}

func (df *DFcore) Column(colName string) (col Column, err error) {
	for h := df.head; h != nil; h = h.next {
		if (h.col).Name("") == colName {
			return h.col, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

func (df *DFcore) Fn(fn Ereturn) error {
	return fn()
}

/*
Save DB -> DB
     DB -> file
     Mem -> DB
     Mem -> file

*/

func (df *DFcore) CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error {
	var (
		e   error
		dts []DataTypes
	)

	if df.Context.dialect == nil {
		return fmt.Errorf("no database defined")
	}

	if cols == nil {
		cols = df.ColumnNames()
	}

	if orderBy != "" && !df.HasColumns(strings.Split(orderBy, ",")...) {
		return fmt.Errorf("not all columns present in OrderBy %s", orderBy)
	}

	if dts, e = df.ColumnTypes(cols...); e != nil {
		return e
	}

	return df.Context.dialect.Create(tableName, orderBy, cols, dts, overwrite)
}

func (df *DFcore) HasColumns(cols ...string) bool {
	dfCols := df.ColumnNames()
	for _, c := range cols {
		if !u.Has(c, "", dfCols...) {
			return false
		}
	}

	return true
}

func (df *DFcore) DoOp(opName string, inputs ...any) (Column, error) {
	var fn AnyFunction

	if fn = df.funcs.Get(opName); fn == nil {
		return nil, fmt.Errorf("op %s not defined, operation skipped", opName)
	}

	var vals []any
	for ind := 0; ind < len(inputs); ind++ {
		if c, ok := inputs[ind].(Column); ok {
			vals = append(vals, c)
		} else {
			vals = append(vals, inputs[ind])
		}
	}

	var (
		col Column
		e   error
	)

	if col, e = df.run(fn, df.Context, vals); e != nil {
		return nil, e
	}

	return col, nil
}

func (df *DFcore) Apply(resultName, opName string, replace bool, inputs ...string) error {
	var fn AnyFunction

	if fn = df.funcs.Get(opName); fn == nil {
		log.Printf("op %s to create %s not defined, operation skipped", opName, resultName)
		return nil
	}

	var vals []any
	for ind := 0; ind < len(inputs); ind++ {
		if c, e := df.Column(inputs[ind]); e == nil {
			vals = append(vals, c)
		} else {
			vals = append(vals, inputs[ind])
		}
	}

	var (
		col Column
		e   error
	)

	if col, e = df.run(fn, df.Context, vals); e != nil {
		return e
	}

	col.Name(resultName)

	return df.AppendColumn(col, replace)
}

func (df *DFcore) ValidName(columnName string) bool {
	const illegal = "!@#$%^&*()=+-;:'`/.,>< ~" + `"`
	if _, e := df.Column(columnName); e == nil {
		return false
	}

	if strings.ContainsAny(columnName, illegal) {
		return false
	}

	return true
}

func (df *DFcore) AppendColumn(col Column, replace bool) error {
	if df.Context != nil && df.Context.Len() != nil && *df.Context.Len() != col.Len() {
		return fmt.Errorf("unequal lengths in AppendColumn")
	}

	if replace {
		_ = df.DropColumns(col.Name(""))
	}

	if !df.ValidName(col.Name("")) {
		return fmt.Errorf("invalid column name: %s", col.Name(""))
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
		head:    subHead,
		funcs:   df.funcs,
		run:     df.run,
		Context: df.Context,
	}

	return subsetDF, nil
}

/////////// DataTypes

func DTFromString(nm string) DataTypes {
	const skeleton = "%v"

	var nms []string
	for ind := DataTypes(0); ind <= MaxDT; ind++ {
		nms = append(nms, fmt.Sprintf(skeleton, ind))
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

////////////////
