package df

import (
	_ "embed"
	"fmt"
	"log"
	"strings"

	u "github.com/invertedv/utilities"
)

// TODO: think about
// data types -- what if try to add a cat var?

type DF interface {
	// generic from DFcore
	AppendColumn(col Column, replace bool) error
	Apply(resultName, opName string, replace bool, inputs ...string) error
	Column(colName string) (col Column, err error)
	ColumnCount() int
	ColumnNames() []string
	ColumnTypes(cols ...string) ([]DataTypes, error)
	CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error
	DoOp(opName string, inputs ...any) (Column, error)
	DropColumns(colNames ...string) error
	Fn(fn Ereturn) error
	KeepColumns(keepColumns ...string) (*DFcore, error)
	Next(reset bool) Column
	Fns() Fns
	AppendDFcore(df2 DF) (*DFcore, error)

	// specific to underlying data source
	AppendDF(df DF) (DF, error)
	DBsave(tableName string, overwrite bool, cols ...string) error
	FileSave(fileName string) error
	MakeColumn(value any) (Column, error)
	RowCount() int
	Sort(ascending bool, keys ...string) error
	Where(indicator Column) error
}

type Ereturn func() error

// DFcore is the data plus functions to operate on it -- it is the core structure of DF that is embedded
// in specific implementations
type DFcore struct {
	head *columnList

	rowFuncs   Fns
	runRowFunc RunFn
	runDFfun   RunFn

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
	AppendRows(col Column) (Column, error)
}

// DataTypes are the types of data that the package supports
type DataTypes uint8

// values of DataTypes
const (
	DTunknown DataTypes = 0 + iota
	DTstring
	DTfloat
	DTint
	DTcategorical
	DTdate
	DTnone
	DTany
)

//go:generate stringer -type=DataTypes

// max value of DataTypes type
const MaxDT = DTany

////////// Row Function typs

type Fns []Fn

type Fn func(info bool, context *Context, inputs ...any) *FnReturn

type FnReturn struct {
	Value any

	Name   string
	Output DataTypes
	Inputs []DataTypes

	DFlevel bool

	Err error
}

type RunFn func(fn Fn, context *Context, inputs []any) (Column, error)

////////// DFCore

func NewDF(runRow, runDF RunFn, funcs Fns, cols ...Column) (df *DFcore, err error) {
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

	return &DFcore{head: head, rowFuncs: funcs, runRowFunc: runRow, runDFfun: runDF}, nil
}

// /////////// DFcore methods
func (df *DFcore) Fns() Fns {
	return df.rowFuncs
}

func (df *DFcore) RunRowFn() RunFn {
	return df.runRowFunc
}

func (df *DFcore) RunDFfn() RunFn {
	return df.runDFfun
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

func (df *DFcore) AppendDFcore(df2 DF) (*DFcore, error) {
	if df == nil {
		return df, nil
	}

	if df.ColumnCount() != df.ColumnCount() {
		return nil, fmt.Errorf("differing column counts in AppendDF")
	}

	var cols []Column

	for c := df.Next(true); c != nil; c = df.Next(false) {
		var (
			col2, nc Column
			e        error
		)
		if col2, e = df.Column(c.Name("")); e != nil {
			return nil, e
		}

		if nc, e = c.AppendRows(col2); e != nil {
			return nil, e
		}

		cols = append(cols, nc)
	}

	return NewDF(df.RunRowFn(), df.RunDFfn(), df.Fns(), cols...)
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
	var fn Fn

	if fn = df.rowFuncs.Get(opName); fn == nil {
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

	if fn(true, nil, nil).DFlevel {
		if col, e = df.runDFfun(fn, df.Context, vals); e != nil {
			return nil, e
		}
	} else {
		if col, e = df.runRowFunc(fn, df.Context, vals); e != nil {
			return nil, e
		}
	}

	return col, nil
}

func (df *DFcore) Apply(resultName, opName string, replace bool, inputs ...string) error {
	var fn Fn

	if fn = df.rowFuncs.Get(opName); fn == nil {
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

	if col, e = df.runRowFunc(fn, df.Context, vals); e != nil {
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
		head:       subHead,
		rowFuncs:   df.rowFuncs,
		runRowFunc: df.runRowFunc,
		Context:    df.Context,
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

func Compatible(x, y DataTypes, strict bool) bool {
	if x == DTany || y == DTany {
		return true
	}

	if x == y {
		return true
	}

	if strict {
		return false
	}

	if (x == DTfloat || x == DTint) && (y == DTfloat || y == DTint) {
		return true
	}

	return false
}

func (d DataTypes) IsNumeric() bool {
	return d == DTfloat || d == DTint
}

//////// Fns Methods

func (fs Fns) Get(fnName string) Fn {
	for _, f := range fs {
		if f(true, nil).Name == fnName {
			return f
		}
	}

	return nil
}

////////////////

type CategoryMap map[any]int

func (cm CategoryMap) Max() int {
	var maxVal *int
	for k, v := range cm {
		if maxVal == nil {
			maxVal = new(int)
			*maxVal = v
		}
		if k != nil && v > *maxVal {
			*maxVal = v
		}
	}

	return *maxVal
}

func (cm CategoryMap) Min() int {
	var minVal *int
	for k, v := range cm {
		if minVal == nil {
			minVal = new(int)
			*minVal = v
		}

		if k != nil && v < *minVal {
			*minVal = v
		}
	}

	return *minVal
}

////////////////
