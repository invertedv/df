package df

import (
	_ "embed"
	"fmt"
	"iter"
)

type DF interface {
	DC

	AppendDF(df DF) (DF, error)
	By(groupBy string, fns ...string) (DF, error)
	Categorical(colName string, catMap CategoryMap, fuzz int, defaultVal any, levels []any) (Column, error)
	Copy() DF
	Iter(reset bool) (row []any, err error)
	Join(df DF, joinOn string) (DF, error)
	RowCount() int
	SetParent() error
	Sort(ascending bool, keys ...string) error
	String() string
	Table(cols ...string) (DF, error)
	Where(condition string) (DF, error)
}

type DC interface {
	AllColumns() iter.Seq[Column]
	AppendColumn(col Column, replace bool) error
	Column(colName string) Column
	ColumnNames() []string
	ColumnTypes(cols ...string) ([]DataTypes, error)
	Core() *DFcore
	Dialect() *Dialect
	DropColumns(colNames ...string) error
	Fns() Fns
	HasColumns(cols ...string) bool
	KeepColumns(colsToKeep ...string) error
	SourceDF() *DFcore
}

// *********** DFcore ***********

// DFcore is the nucleus implementation of the DataFrame.  It does not implement all the required methods.  The remaining
// methods will depend on the implementation.
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
	for ind := range len(cols) {
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

func (df *DFcore) AllColumns() iter.Seq[Column] {
	return func(yield func(Column) bool) {
		for col := df.head; col != nil; col = col.next {
			if !yield(col.col) {
				return
			}
		}
	}
}

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

	if len(colNames) == 0 {
		colNames = df.ColumnNames()
	}

	for ind := range len(colNames) {
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
	//	save := df.current
	//	for c := df.First(); c != nil; c = df.Next() {
	//		cols = append(cols, c.Copy())
	//	}
	for c := range df.AllColumns() {
		cols = append(cols, c.Copy())
	}

	//df.current = save
	var outDF *DFcore

	outDF, _ = NewDF(df.Fns(), cols,
		DFdialect(df.Dialect()), DFsetSourceDF(df.SourceDF()))

	return outDF
}

func (df *DFcore) Core() *DFcore {
	return df
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

func (df *DFcore) KeepColumns(colNames ...string) error {
	if !df.HasColumns(colNames...) {
		return fmt.Errorf("missing columns in KeepColumns")
	}

	for col := range df.AllColumns() {
		if Has(col.Name(), colNames) {
			continue
		}

		if e := df.DropColumns(col.Name()); e != nil {
			return e
		}
	}
	/*
		for col := df.First(); col != nil; col = df.Next() {
			if Has(col.Name(), colNames) {
				continue
			}

			if e := df.DropColumns(col.Name()); e != nil {
				return e
			}
		}*/

	return nil
}

func (df *DFcore) NextXXX() Column {
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
