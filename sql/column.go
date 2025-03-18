package sql

import (
	"fmt"
	"strings"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
)

type Col struct {
	sql    string // SQL to generate this column
	global bool   // permanent signal that this column uses a global query
	gf     bool   // short term signal indicating "global" function surrounds the column

	*d.ColCore
}

// ***************** Col - Create *****************

func NewColSQL(dt d.DataTypes, dlct *d.Dialect, sqlx string, opts ...d.ColOpt) (*Col, error) {
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

func (c *Col) Copy() d.Column {
	n := &Col{
		sql:     c.sql,
		global:  c.global,
		ColCore: c.Core().Copy(),
	}

	return n
}

func (c *Col) Core() *d.ColCore {
	return c.ColCore
}

func (c *Col) Data() *d.Vector {
	var (
		df *m.DF
		e  error
	)

	// give it a random name if it does not have one
	if c.Name() == "" {
		_ = d.ColName(d.RandomLetters(5))(c)
	}

	if df, e = m.DBLoad(c.MakeQuery(), c.Dialect()); e != nil {
		panic(e)
	}

	var col d.Column
	if col = df.Column(c.Name()); col == nil {
		panic(fmt.Errorf("missing column?"))
	}

	return col.(*m.Col).Data()
}

func (c *Col) SQL() (snippet string, isFieldName bool) {
	if c.sql != "" {
		if c.global {
			return c.Dialect().Global(c.Parent().(*DF).SourceSQL(), c.sql), false
		}

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

// MakeQuery creates a stand-alone query that will pull this column
func (c *Col) MakeQuery() string {
	if c.Parent() == nil {
		panic("nil parent")
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
	//	if c.Parent() != nil && c.Parent().Column(newName) != nil {
	//		return fmt.Errorf("column %s already exists cannot Rename", newName)
	//	}

	oldName := c.Dialect().ToName(c.Name())
	if e := c.Core().Rename(newName); e != nil {
		return e
	}

	// if this is just a column pull, need to keep the source name for "AS"
	if c.sql == "" {
		c.sql = oldName
	}

	return nil
}

func (c *Col) String() string {
	t := fmt.Sprintf("column: %s\ntype: %s\n", c.Name(), c.DataType())

	if c.CategoryMap() != nil {
		var keys []string
		var vals []int
		for k, v := range c.CategoryMap() {
			if k == nil {
				k = "Other"
			}
			x := fmt.Sprintf("%v", k)

			keys = append(keys, x)
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + d.PrettyPrint(header, keys, vals) + "\n"
	}

	if c.DataType() != d.DTfloat {
		var (
			tab d.DF
			e   error
		)
		if tab, e = c.Parent().Table(c.Name()); e != nil {
			panic(e)
		}

		tab.Sort(true, c.Name())
		l := tab.Column(c.Name())
		cx := tab.Column("count")

		header := []string{l.Name(), c.Name()}

		cxi, _ := cx.Data().AsInt()
		strx, _ := l.Data().AsString()

		return t + d.PrettyPrint(header, strx, cxi)
	}

	cols := []string{"min", "lq", "median", "mean", "uq", "max", "n"}

	header := []string{"metric", "value"}
	vals, _ := c.Dialect().Summary(c.MakeQuery(), c.Dialect().ToName(c.Name()))

	return t + d.PrettyPrint(header, cols, vals)
}
