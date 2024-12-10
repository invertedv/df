package df

import (
	_ "embed"
	"fmt"
	"strings"
)

// TODO: make summary functions return a scalar rather than a DF?
// TODO: separate mem/df.go into df.go and column.go, also for sql

// TODO: think about panic vs error
// TODO: panic needs error or just string?

type DF interface {
	// generic from DFcore
	AppendColumn(col Column, replace bool) error
	AppendDFcore(df2 DF) (*DFcore, error)

	//	Column(colName string) (col Column, err error)
	Column(colName string) Column
	ColumnCount() int
	ColumnNames() []string
	ColumnTypes(cols ...string) ([]DataTypes, error)
	Context() *Context
	Core() *DFcore
	CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error
	DoOp(opName string, inputs ...*Parsed) (any, error)
	DropColumns(colNames ...string) error
	Fns() Fns
	KeepColumns(keepColumns ...string) (*DFcore, error)
	Next(reset bool) Column
	Parse(expr string) (*Parsed, error)
	SetContext(ctx *Context)

	// specific to underlying data source

	AppendDF(df DF) (DF, error)
	Categorical(colName string, catMap CategoryMap, fuzz int, defaultVal any, levels []any) (Column, error)
	Copy() DF
	Iter(reset bool) (row []any, err error)
	MakeQuery(colNames ...string) string
	RowCount() int
	Sort(ascending bool, keys ...string) error
	String() string
	Table(sortByRows bool, cols ...string) (DF, error)
	Where(indicator Column) (DF, error)
}

// *********** DFcore ***********

// DFcore is the nucleus implementation of the DataFrame.  It does not implement all the required methods.
type DFcore struct {
	head *columnList

	appFuncs Fns

	current *columnList

	ctx *Context
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

func NewDF(funcs Fns, cols ...Column) (df *DFcore, err error) {
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

	return &DFcore{head: head, appFuncs: funcs}, nil
}

// *********** DFcore methods ***********

func (df *DFcore) AppendColumn(col Column, replace bool) error {
	if df.Column(col.Name()) != nil {
		if replace {
			_ = df.DropColumns(col.Name())
		} else {
			return fmt.Errorf("column %s already exists", col.Name())
		}
	}

	// find last column
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

func (df *DFcore) AppendDFcore(df2 DF) (*DFcore, error) {
	if df == nil {
		return nil, nil
	}

	if df.ColumnCount() != df2.ColumnCount() {
		return nil, fmt.Errorf("differing column counts in AppendDF")
	}

	var cols []Column

	for c := df.Next(true); c != nil; c = df.Next(false) {
		var (
			col2, nc Column
			e        error
		)
		if col2 = df.Column(c.Name()); col2 == nil {
			return nil, fmt.Errorf("column %s not found", c.Name())
		}

		if nc, e = c.AppendRows(col2); e != nil {
			return nil, e
		}

		cols = append(cols, nc)
	}

	return NewDF(df.Fns(), cols...)
}

func (df *DFcore) Column(colName string) Column {
	for h := df.head; h != nil; h = h.next {
		if (h.col).Name() == colName {
			return h.col
		}
	}

	return nil
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
		names = append(names, h.col.Name())
	}

	return names
}

func (df *DFcore) ColumnTypes(colNames ...string) ([]DataTypes, error) {
	var types []DataTypes

	if colNames != nil && !df.HasColumns(colNames...) {
		return nil, fmt.Errorf("some columns on in DFcore in ColumnTypes")
	}

	if colNames == nil {
		colNames = df.ColumnNames()
	}

	for ind := 0; ind < len(colNames); ind++ {
		var c Column
		if c = df.Column(colNames[ind]); c == nil {
			return nil, fmt.Errorf("column %s not found", colNames[ind])
		}

		types = append(types, c.DataType())
	}

	return types, nil
}

func (df *DFcore) Context() *Context {
	return df.ctx
}

func (df *DFcore) Copy() *DFcore {
	var cols []Column
	for c := df.Next(true); c != nil; c = df.Next(false) {
		cols = append(cols, c.Copy())
	}

	var outDF *DFcore

	outDF, _ = NewDF(df.Fns(), cols...)

	return outDF
}

func (df *DFcore) Core() *DFcore {
	return df
}

func (df *DFcore) CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error {
	if df.Context().dialect == nil {
		return fmt.Errorf("no database defined")
	}

	if cols == nil {
		cols = df.ColumnNames()
	}

	noDesc := strings.ReplaceAll(strings.ReplaceAll(orderBy, "DESC", ""), " ", "")
	if orderBy != "" && !df.HasColumns(strings.Split(noDesc, ",")...) {
		return fmt.Errorf("not all columns present in OrderBy %s", noDesc)
	}

	var (
		e   error
		dts []DataTypes
	)
	if dts, e = df.ColumnTypes(cols...); e != nil {
		return e
	}

	return df.Context().dialect.Create(tableName, noDesc, cols, dts, overwrite)
}

func (df *DFcore) DoOp(opName string, inputs ...*Parsed) (any, error) {
	var fn Fn

	if fn = df.appFuncs.Get(opName); fn == nil {
		return nil, fmt.Errorf("op %s not defined, operation skipped", opName)
	}

	var vals []Column
	for ind := 0; ind < len(inputs); ind++ {
		switch inputs[ind].Which() {
		case "DF":
			return nil, fmt.Errorf("cannot take DF as function input")
		case "Column":
			vals = append(vals, inputs[ind].AsColumn())
		}
	}

	var (
		col any
		e   error
	)
	if col, e = RunDFfn(fn, df.Context(), vals); e != nil {
		return nil, e
	}

	return col, nil
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

func (df *DFcore) Fns() Fns {
	return df.appFuncs
}

func (df *DFcore) HasColumns(cols ...string) bool {
	dfCols := df.ColumnNames()
	for _, c := range cols {
		if !Has(c, "", dfCols...) {
			return false
		}
	}

	return true
}

func (df *DFcore) KeepColumns(colNames ...string) (*DFcore, error) {
	var subHead, tail *columnList

	for ind := 0; ind < len(colNames); ind++ {
		var col Column

		if col = df.Column(colNames[ind]); col == nil {
			return nil, fmt.Errorf("column %s not found", colNames[ind])
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
		head:     subHead,
		appFuncs: df.appFuncs,
		ctx:      df.Context(),
	}

	return subsetDF, nil
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

func (df *DFcore) Parse(expr string) (*Parsed, error) {
	var (
		ot *OpTree
		e  error
	)
	if ot, e = NewOpTree(expr, df.Fns()); e != nil {
		return nil, e
	}

	if ex := ot.Build(); ex != nil {
		return nil, ex
	}

	if ex := ot.Eval(df); ex != nil {
		return nil, ex
	}

	return ot.Value(), nil
}

func (df *DFcore) SetContext(ctx *Context) {
	df.ctx = ctx
	for cx := df.Next(true); cx != nil; cx = df.Next(false) {
		ColContext(ctx)(cx.Core())
	}
}

func (df *DFcore) node(colName string) (node *columnList, err error) {
	for h := df.head; h != nil; h = h.next {
		if h.col.Name() == colName {
			return h, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}
