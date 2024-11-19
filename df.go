package df

import (
	_ "embed"
	"fmt"
	"strings"
)

// TODO: think about
// consider how to handle passing a small column of parameters...
// NewColSequence ?? ...

type DF interface {
	// generic from DFcore
	AppendColumn(col Column, replace bool) error

	//	Column(colName string) (col Column, err error)
	Column(colName string) Column
	ColumnCount() int
	ColumnNames() []string
	ColumnTypes(cols ...string) ([]DataTypes, error)
	CreateTable(tableName, orderBy string, overwrite bool, cols ...string) error
	DoOp(opName string, inputs ...*Parsed) (any, error)
	DropColumns(colNames ...string) error
	Fn(fn Ereturn) error
	KeepColumns(keepColumns ...string) (*DFcore, error)
	Next(reset bool) Column
	Fns() Fns
	AppendDFcore(df2 DF) (*DFcore, error)
	Parse(expr string) (*Parsed, error)
	Context() *Context
	Core() *DFcore

	// specific to underlying data source
	AppendDF(df DF) (DF, error)
	Copy() DF
	RowCount() int
	Sort(ascending bool, keys ...string) error
	Where(indicator Column) (DF, error)
	// Categorical takes an input column of type DTint, DTdate or DTstring and creates a categorical column
	// - colName : name of column to work on
	// - catMap : existing categorical map to apply.  Use nil if there is not one.
	// - fuzz : # of counts below which to map to the defaultVal category
	// - defaultVal : level of feature to use as the "other" value. This can be an existing level of the Column or a new one.
	// - levels : array of acceptable levels.  Any level not found in this slice is mapped to the defaultVal
	Categorical(colName string, catMap CategoryMap, fuzz int, defaultVal any, levels []any) (Column, error)
	Table(sortByRows bool, cols ...string) (DF, error)
	Iter(reset bool) (row []any, err error)
	MakeQuery(colNames ...string) string
	String() string
}

//

type Ereturn func() error

// DFcore is the data plus functions to operate on it -- it is the core structure of DF that is embedded
// in specific implementations
type DFcore struct {
	head *columnList

	rowFuncs Fns
	//	runRowFunc RunFn
	runFn RunFn

	current *columnList

	ctx *Context
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

// Column interface defines the methods the columns of DFcore that must be supported
type Column interface {
	AppendRows(col Column) (Column, error)
	Copy() Column
	Data() any
	DataType() DataTypes
	Dependencies() []string
	Len() int
	Name(reNameTo string) string
	Replace(ind, repl Column) (Column, error)
	SetDependencies(d []string)
	String() string
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
	DTdf
	DTconstant
	DTany // keep as last entry
)

//go:generate stringer -type=DataTypes

// MaxDT is max value of DataTypes type
const MaxDT = DTany

// *********** Function types ***********

type Fns []Fn

type Fn func(info bool, context *Context, inputs ...any) *FnReturn

type FnReturn struct {
	Value any

	Name   string
	Output []DataTypes
	Inputs [][]DataTypes

	Varying bool

	Err error
}

type RunFn func(fn Fn, context *Context, inputs []any) (any, error)

// *********** DFcore ***********

func NewDF(runner RunFn, funcs Fns, cols ...Column) (df *DFcore, err error) {
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

	return &DFcore{head: head, rowFuncs: funcs, runFn: runner}, nil
}

// *********** DFcore methods ***********

func (df *DFcore) AppendColumn(col Column, replace bool) error {
	if replace {
		_ = df.DropColumns(col.Name(""))
	}

	if !df.ValidName(col.Name("")) {
		return fmt.Errorf("invalid column name: %s", col.Name(""))
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
		if col2 = df.Column(c.Name("")); col2 == nil {
			return nil, fmt.Errorf("column %s not found", c.Name(""))
		}

		if nc, e = c.AppendRows(col2); e != nil {
			return nil, e
		}

		cols = append(cols, nc)
	}

	return NewDF(df.Runner(), df.Fns(), cols...)
}

func (df *DFcore) Column(colName string) Column {
	for h := df.head; h != nil; h = h.next {
		if (h.col).Name("") == colName {
			return h.col
		}
	}

	//, fmt.Errorf("column %s not found", colName)
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
		names = append(names, h.col.Name(""))
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

	var (
		outDF *DFcore
		e     error
	)

	if outDF, e = NewDF(df.Runner(), df.Fns(), cols...); e != nil {
		panic(e)
	}
	context := NewContext(df.Context().Dialect(), nil)
	outDF.SetContext(context)

	// don't copy context

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

	if fn = df.rowFuncs.Get(opName); fn == nil {
		return nil, fmt.Errorf("op %s not defined, operation skipped", opName)
	}

	var vals []any
	for ind := 0; ind < len(inputs); ind++ {
		switch inputs[ind].Which() {
		case "DF":
			return nil, fmt.Errorf("cannot take DF as function input")
		case "Column":
			vals = append(vals, inputs[ind].AsColumn())
		default:
			vals = append(vals, inputs[ind].AsScalar())
		}
	}

	var (
		col any
		e   error
	)
	if col, e = df.runFn(fn, df.Context(), vals); e != nil {
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

func (df *DFcore) Fn(fn Ereturn) error {
	return fn()
}

func (df *DFcore) Fns() Fns {
	return df.rowFuncs
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
		rowFuncs: df.rowFuncs,
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

func (df *DFcore) Runner() RunFn {
	return df.runFn
}

func (df *DFcore) SetContext(c *Context) {
	df.ctx = c
}

func (df *DFcore) ValidName(columnName string) bool {
	const illegal = "!@#$%^&*()=+-;:'`/.,>< ~" + `"`
	if c := df.Column(columnName); c != nil {
		return false
	}

	if strings.ContainsAny(columnName, illegal) {
		return false
	}

	return true
}

func (df *DFcore) node(colName string) (node *columnList, err error) {
	for h := df.head; h != nil; h = h.next {
		if h.col.Name("") == colName {
			return h, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

//  *********** DataTypes functions ***********

func DTFromString(nm string) DataTypes {
	const skeleton = "%v"

	var nms []string
	for ind := DataTypes(0); ind <= MaxDT; ind++ {
		nms = append(nms, fmt.Sprintf(skeleton, ind))
	}

	pos := Position(nm, "", nms...)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}

func (d DataTypes) IsNumeric() bool {
	return d == DTfloat || d == DTint || d == DTcategorical
}

// *********** Fns Methods ***********

func (fs Fns) Get(fnName string) Fn {
	for _, f := range fs {
		if f(true, nil).Name == fnName {
			return f
		}
	}

	return nil
}

// *********** Category Map methods ***********

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
