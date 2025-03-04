package df

import (
	_ "embed"
	"fmt"
	"strings"
)

type DF interface {
	DC

	AppendDF(df DF) (DF, error)
	By(groupBy string, fns ...string) (DF, error)
	Categorical(colName string, catMap CategoryMap, fuzz int, defaultVal any, levels []any) (Column, error)
	Copy() DF
	Iter(reset bool) (row []any, err error)
	Join(df DF, joinOn string) (DF, error)
	MakeQuery(colNames ...string) string
	RowCount() int
	SetParent() error
	Sort(ascending bool, keys ...string) error
	String() string
	Table(cols ...string) (DF, error)
	Where(indicator Column) (DF, error)
}

type DC interface {
	AppendColumn(col Column, replace bool) error
	Column(colName string) Column
	ColumnCount() int
	ColumnNames() []string
	ColumnTypes(cols ...string) ([]DataTypes, error)
	Core() *DFcore
	CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error
	Dialect() *Dialect
	DropColumns(colNames ...string) error
	First() Column
	Fns() Fns
	HasColumns(cols ...string) bool
	KeepColumns(keepColumns ...string) (*DFcore, error)
	Next() Column
	SourceDF() *DFcore
}

// *********** DFcore ***********

// DFcore is the nucleus implementation of the DataFrame.  It does not implement all the required methods.
type DFcore struct {
	head *columnList

	appFuncs Fns

	current *columnList

	dlct *Dialect

	sourceDF *DFcore
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

func NewDF(funcs Fns, cols []Column, opts ...DFopt) (df *DFcore, err error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDF")
	}

	outDF := &DFcore{appFuncs: funcs}

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

	outDF.head = head

	for _, opt := range opts {
		if e := opt(outDF); e != nil {
			return nil, e
		}
	}

	return outDF, nil
}

// *********** Setters ***********

type DFopt func(df DC) error

func DFdialect(d *Dialect) DFopt {
	return func(df DC) error {
		if df == nil {
			return fmt.Errorf("nil dataframe to DFdialect")
		}

		df.Core().dlct = d

		return nil
	}
}

func DFappendFn(f Fn) DFopt {
	return func(df DC) error {
		if df == nil {
			return fmt.Errorf("nil dataframe to DFappendFn")
		}

		df.Core().appFuncs = append(df.Core().appFuncs, f)

		return nil
	}
}

func DFsetFns(f Fns) DFopt {
	return func(df DC) error {
		if df == nil {
			return fmt.Errorf("nil dataframe to DFsetFns")
		}

		df.Core().appFuncs = f

		return nil
	}
}

func DFsetSourceDF(source DC) DFopt {
	return func(df DC) error {
		if df == nil {
			return fmt.Errorf("nil dataframe to DFsetSourceDF")
		}

		df.Core().sourceDF = source.Core()
		return nil
	}
}

// *********** Methods ***********

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

func (df *DFcore) Column(colName string) Column {
	if df.head == nil {
		return nil
	}

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

func (df *DFcore) Copy() *DFcore {
	var cols []Column
	for c := df.First(); c != nil; c = df.Next() {
		cols = append(cols, c.Copy())
	}

	var outDF *DFcore

	outDF, _ = NewDF(df.Fns(), cols, DFdialect(df.Dialect()))

	return outDF
}

func (df *DFcore) Core() *DFcore {
	return df
}

func (df *DFcore) CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error {
	if df.Dialect() == nil {
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

	return df.Dialect().Create(tableName, noDesc, cols, dts, overwrite)
}

func (df *DFcore) Dialect() *Dialect {
	return df.dlct
}

func (df *DFcore) DropColumns(colNames ...string) error {
	for _, cName := range colNames {
		var (
			node *columnList
			e    error
		)
		if node, e = df.node(cName); e != nil {
			return e
		}

		// make it an orphan
		// can't use ColParent to do this since it will call DropColumns
		node.col.Core().parent = nil

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

func (df *DFcore) First() Column {
	df.current = df.head
	return df.head.col
}

func (df *DFcore) Fns() Fns {
	return df.appFuncs
}

func (df *DFcore) HasColumns(cols ...string) bool {
	dfCols := df.ColumnNames()
	for _, c := range cols {
		if !Has(c, dfCols) {
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
		dlct:     df.Dialect(),
	}

	return subsetDF, nil
}

func (df *DFcore) Next() Column {
	if df.current.next == nil {
		df.current = nil
		return nil
	}

	df.current = df.current.next
	return df.current.col
}

func (df *DFcore) SourceDF() *DFcore {
	return df.sourceDF
}

func (df *DFcore) node(colName string) (node *columnList, err error) {
	for h := df.head; h != nil; h = h.next {
		if h.col.Name() == colName {
			return h, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}
