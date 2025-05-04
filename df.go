/*
The package df is an implementation of dataframes.  The central idea here is that the dataframes are defined as an interface which is
independent of the implementation of the data-handling details.

The package df defines:
  - The dataframe and column interfaces (DF, Column).
  - Implements core aspects of these.
  - Provides a parser to handle Column-valued expressions.
  - Provides for file and database IO.

Along with df, there are two sub-packages implementing DF and Column:
  - df/mem. In-memory dataframes,
  - df/sql.  SQL-database dataframes.  The current implementation covers ClickHouse and Postgres databases.

See the 
[documentation]: https://invertedv.github.io/df
for details.
*/
package df

import (
	_ "embed"
	"fmt"
	"iter"
)

type DF interface {
	// Core methods
	DC

	// AllRows iterates through the rows of the column.  It returns the row # and the values of DF that row.
	AllRows() iter.Seq2[int, []any]

	// AppendDF appends df
	AppendDF(df DF) (DF, error)

	// By creates a new DF that groups the source DF by the columns listed in groupBy and calculates fns on the groups.
	By(groupBy string, fns ...string) (DF, error)

	// Categorical creates a categorical column
	//	colName    - name of the source column
	//	catMap     - optionally supply a category map of source value -> category level
	//	fuzz       - if a source column value has counts < fuzz, then it is put in the 'other' category.
	//	defaultVal - optional source column value for the 'other' category.
	//	levels     - slice of source values to make categories from
	Categorical(colName string, catMap CategoryMap, fuzz int, defaultVal any, levels []any) (Column, error)

	Copy() DF

	// Interp interpolates the columns (xIfield,yfield) at xsField points.
	//   iDF      - input iterator (e.g. Column or DF) that yields the points to interpolate at
	//   xSfield  - column name of x values in source DF
	//   xIfield  - name of x values in iDF
	//   yfield   - column name of y values in source DF
	//   outField - column name of interpolated y's in return DF
	//
	// The output DF has two columns: xIfield, outField.
	Interp(iDF HasIter, xSfield, xIfield, yfield, outField string) (DF, error)

	// Join inner joins the df to the source DF on the joinOn fields
	//   df       - DF to join
	//   joinOn   - comma-separated list of fields to join on.
	Join(df HasIter, joinOn string) (DF, error)

	// RowCount returns # of rows in df
	RowCount() int

	// SetParent sets the Parent field of all the columns in the source DF
	SetParent() error

	// Sort sorts the source DF on sortCols
	//   ascending - if true, sorts ascending
	//   sortCols      - sortCols is a comma-separated list of fields on which to sort.
	Sort(ascending bool, sortCols string) error

	// String is expected to produce a summary of the source DF.
	String() string

	// Table returns a table based on cols.
	//   cols - comma-separated list of column names for the table.
	// The return is expected to include the columns "count" and "rate"
	Table(cols string) (DF, error)

	// Where returns a DF subset according to condition.
	Where(condition string) (DF, error)
}

type DC interface {
	// AllColumns returns an iterator across the columns.
	AllColumns() iter.Seq[Column]

	// AppendColumns appends col to the DF.
	AppendColumn(col Column, replace bool) error

	// Column returns the column colName.  Returns nil if the column doesn't exist.
	Column(colName string) Column

	// ColNames returns the names of all the columns.
	ColumnNames() []string

	// ColumnTypes returns the types of columns.  If cols is nil, returns the types for all columns.
	ColumnTypes(cols ...string) ([]DataTypes, error)

	// Core returns itself.
	Core() *DFcore

	// Dialect returns the Dialect object for DB access.
	Dialect() *Dialect

	// DropColumns drops colNames from the DF.
	DropColumns(colNames ...string) error

	// Fns returns a slice of functions that operate on columns.
	Fns() Fns

	// HasColumns returns true if the DF has all cols.
	HasColumns(cols ...string) bool

	// KeepColumns subsets DF to colsToKeep
	KeepColumns(colsToKeep ...string) error

	// sourceDF returns the source DF for this DF if this DF is a derivative (e.g. a Table).
	SourceDF() *DFcore
}

// *********** DFcore ***********

// DFcore implements DC.
type DFcore struct {
	head *columnList

	appFuncs Fns

	dlct *Dialect

	sourceDF *DFcore
}

type columnList struct {
	col Column

	prior *columnList
	next  *columnList
}

func NewDFcore(cols []Column, opts ...DFopt) (df *DFcore, err error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDF")
	}

	outDF := &DFcore{}

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

// DFopt functions are used to set DFcore options
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

	for col := range df.AllColumns() {
		if col.Name() == colName {
			return col
		}
	}

	return nil
}

func (df *DFcore) ColumnNames() []string {
	var names []string

	for col := range df.AllColumns() {
		names = append(names, col.Name())
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
	for c := range df.AllColumns() {
		cols = append(cols, c.Copy())
	}

	//df.current = save
	var outDF *DFcore

	outDF, _ = NewDFcore(cols,
		DFdialect(df.Dialect()), DFsetSourceDF(df.SourceDF()), DFsetFns(df.Fns()))

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
			df.head.prior = nil
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

	return nil
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

// ******** Additional Interfaces ********

// The HasIter interface restricts to types that have an iterator through the rows of the data.
// Save only requires an iterator to move through the rows
type HasIter interface {
	AllRows() iter.Seq2[int, []any]
}

// type HasDQdlct restricts to types that can access a DB
type HasMQdlct interface {
	MakeQuery(colNames ...string) string
	Dialect() *Dialect
}
