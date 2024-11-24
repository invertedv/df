package df

import (
	_ "embed"
	"fmt"
	"strings"
)

// TODO: rethink copy column in light of ColCore
// TODO: look for "_ =" occurences

// TODO: think about panic vs error
// TODO: panic needs error or just string?

// TODO: consider how to handle passing a small column of parameters...

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

// Column interface defines the methods the columns of DFcore that must be supported
type Column interface {
	AppendRows(col Column) (Column, error)
	CategoryMap() CategoryMap
	Copy() Column
	Core() *ColCore
	Context() *Context
	Data() any
	DataType() DataTypes
	Dependencies() []string
	Len() int
	Name() string
	Replace(ind, repl Column) (Column, error)
	Rename(newName string)
	SetContext(ctx *Context)
	// TODO: do I need this?
	SetDependencies(d []string)
	String() string
}

//  *********** DataTypes ***********

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

// *********** Function types ***********

type Fn func(info bool, context *Context, inputs ...any) *FnReturn

type Fns []Fn

func (fs Fns) Get(fnName string) Fn {
	for _, f := range fs {
		if f(true, nil).Name == fnName {
			return f
		}
	}

	return nil
}

type FnReturn struct {
	Value any

	Name   string
	Output []DataTypes
	Inputs [][]DataTypes

	Varying bool

	Err error
}

type RunFn func(fn Fn, context *Context, inputs []any) (any, error)

// *********** Category Map ***********

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

// *********** ColCore ***********

// ColCore implements the nucleus of the Column interface.
type ColCore struct {
	name string
	dt   DataTypes
	ctx  *Context

	catMap    CategoryMap
	catCounts CategoryMap
	rawType   DataTypes

	dep []string
}

type COpt func(c *ColCore)

func ColDataType(dt DataTypes) COpt {
	return func(c *ColCore) {
		c.dt = dt
	}
}

func NewColCore(dt DataTypes, ops ...COpt) *ColCore {
	c := &ColCore{dt: dt}

	for _, op := range ops {
		op(c)
	}

	return c
}

func ColName(name string) COpt {
	const illegal = "!@#$%^&*()=+-;:'`/.,>< ~" + `"`

	if strings.Contains(name, illegal) {
		panic("invalid name")
	}

	return func(c *ColCore) {
		c.name = name
	}
}

func ColContext(ctx *Context) COpt {
	return func(c *ColCore) {
		c.ctx = ctx
	}
}

func ColSetDependencies(dep []string) COpt {
	return func(c *ColCore) {
		c.dep = dep
	}
}

func (c *ColCore) Dependencies() []string {
	return c.dep
}

func ColCatMap(cm CategoryMap) COpt {
	return func(c *ColCore) {
		c.catMap = cm
	}
}

func ColCatCounts(ct CategoryMap) COpt {
	return func(c *ColCore) {
		c.catCounts = ct
	}
}

func ColRawType(rt DataTypes) COpt {
	return func(c *ColCore) {
		c.rawType = rt
	}
}

func (c *ColCore) CategoryMap() CategoryMap {
	return c.catMap
}

func (c *ColCore) CategoryCounts() CategoryMap {
	return c.catCounts
}

func (c *ColCore) RawType() DataTypes {
	return c.rawType
}

func (c *ColCore) Name() string {
	return c.name
}

func (c *ColCore) DataType() DataTypes {
	return c.dt
}

func (c *ColCore) Context() *Context {
	return c.ctx
}

// *********** DFcore ***********

// DFcore is the nucleus implementation of the DataFrame.  It does not implement all the required methods.
type DFcore struct {
	head *columnList

	appFuncs Fns
	runFn    RunFn

	current *columnList

	ctx *Context
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

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

	return &DFcore{head: head, appFuncs: funcs, runFn: runner}, nil
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

	return NewDF(df.Runner(), df.Fns(), cols...)
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

	var (
		outDF *DFcore
		e     error
	)

	if outDF, e = NewDF(df.Runner(), df.Fns(), cols...); e != nil {
		panic(e)
	}

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

func (df *DFcore) Runner() RunFn {
	return df.runFn
}

func (df *DFcore) SetContext(ctx *Context) {
	df.ctx = ctx
	for cx := df.Next(true); cx != nil; cx = df.Next(false) {
		cx.SetContext(ctx)
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
