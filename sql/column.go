package sql

import (
	"fmt"
	"iter"
	"strings"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
)

// Col implements Column for SQL.
type Col struct {
	sql string // SQL to generate this column

	// short term signal indicating "global" function surrounds the column.
	// This instruct the function to add SQL so that the calculation is global.
	// In this case, all rows will have the same value.
	gf bool

	*d.ColCore
}

// ***************** Col - Create *****************

// NewCol creates a new *Col from SQL
//
//	dt   - data type of the column
//	dlct - Dialect to use
//	sqlx - SQL to create the column
//	opts - Column options
func NewCol(dt d.DataTypes, dlct *d.Dialect, sqlx string, opts ...d.ColOpt) (*Col, error) {
	opts = append(opts, d.ColDataType(dt))

	var (
		cc *d.ColCore
		e  error
	)
	if cc, e = d.NewColCore(opts...); e != nil {
		return nil, e
	}

	col := &Col{
		sql:     sqlx,
		ColCore: cc,
	}

	_ = d.ColDialect(dlct)

	return col, nil
}

// ***************** SQLCol - Methods *****************

func (c *Col) AllRows() iter.Seq2[int, []any] {
	return c.Data().AllRows()
}

func (c *Col) Copy() d.Column {
	n := &Col{
		sql:     c.sql,
		ColCore: c.Core().Copy(),
	}

	return n
}

func (c *Col) Core() *d.ColCore {
	return c.ColCore
}

// Data runs the SQl to pull the data.
func (c *Col) Data() *d.Vector {
	return c.DataLimit(0)
}

// DataLimit pulls the first limit rows of data.  Pulls all the data if limit=0.
func (c *Col) DataLimit(limit int) *d.Vector {
	var (
		df *m.DF
		e  error
	)

	// give it a random name if it does not have one
	if c.Name() == "" {
		_ = d.ColName(d.RandomLetters(5))(c)
	}

	mq := c.MakeQuery()
	if limit > 0 {
		mq += fmt.Sprintf(" LIMIT %d", limit)
	}

	if df, e = m.DBload(mq, c.Dialect()); e != nil {
		panic(e)
	}

	var col d.Column
	if col = df.Column(c.Name()); col == nil {
		panic(fmt.Errorf("missing column?"))
	}

	return col.(*m.Col).Data()
}

// SQL returns
//   - the name of the column if it is not a calculated field.
//   - the SQL if it is calculated. Note: this is not a complete query, just the snippet needed for the column.
func (c *Col) SQL() (snippet string, isFieldName bool) {
	if c.sql != "" {
		return c.sql, false
	}

	return c.Dialect().ToName(c.Name()), true
}

func (c *Col) Len() int {
	var (
		n  int
		ex error
	)
	if n, ex = c.Dialect().RowCount(c.MakeQuery()); ex != nil {
		panic(ex)
	}

	return n
}

// MakeQuery creates a stand-alone query that will pull the data for this column
func (c *Col) MakeQuery() string {
	if c.Parent() == nil {
		panic(fmt.Errorf("nil parent"))
	}

	df := c.Parent().(*DF)

	with := c.Dialect().WithName()
	repl := c.Dialect().WithName() // placeholder for the SELECT field
	parentSQL := df.MakeQuery()
	ssql := fmt.Sprintf("WITH %s AS (%s) SELECT %s FROM %s", with, parentSQL, repl, with)

	var selectFld string
	switch {
	case c.Name() != "" && df.Column(c.Name()) != nil:
		selectFld = c.Dialect().ToName(c.Name())
	case c.Name() != "" && c.sql != "":
		selectFld = fmt.Sprintf("%s AS %s", c.sql, c.Dialect().ToName(c.Name()))
	case c.Name() != "" && c.sql == "" && df.Column(c.Name()) == nil:
		selectFld = fmt.Sprintf("%s AS %s", c.Dialect().ToName(c.Name()), d.RandomLetters(5))
	default:
		selectFld = c.Dialect().ToName(c.Name())
	}

	qry := strings.ReplaceAll(ssql, repl, selectFld)
	return qry
}

func (c *Col) Rename(newName string) error {
	currentName := c.Dialect().ToName(c.Name())
	if e := c.Core().Rename(newName); e != nil {
		return e
	}

	// if this is just a column pull, need to keep the source name for "AS"
	if c.sql == "" {
		c.sql = currentName
	}

	return nil
}

func (c *Col) String() string {
	t := fmt.Sprintf("column: %s\ntype: %s\nlength %d\n", c.Name(), c.DataType(), c.Len())

	if cm := c.CategoryMap(); cm != nil {
		return t + cm.String()
	}

	return t + c.DataLimit(5).String()
}
