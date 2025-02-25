package sql

import (
	"fmt"
	"strings"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
)

// TODO: do I really need sourceDF??

type Col struct {
	sql    string // SQL to generate this column
	global bool   // permanent signal that this column uses a global query
	gf     bool   // short term signal indicating "global" function surrounds the column

	*d.ColCore
}

// ***************** Col - Create *****************

func NewColSQL(dt d.DataTypes, dlct *d.Dialect, sqlx string, opts ...d.ColOpt) (*Col, error) {
	var (
		cc *d.ColCore
		e  error
	)
	if cc, e = d.NewColCore(dt, opts...); e != nil {
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

// TODO: test this, doesn't look right
func (c *Col) AppendRows(col d.Column) (d.Column, error) {
	panicer(col)
	if c.DataType() != col.DataType() {
		return nil, fmt.Errorf("incompatible columns in AppendRows")
	}

	q1 := c.MakeQuery()
	cx := col.Copy()
	if ex := d.ColName(c.Name())(cx); ex != nil {
		return nil, ex
	}

	q2 := c.MakeQuery()

	if _, e := c.Dialect().Union(q1, q2, cx.Name()); e != nil {
		return nil, e
	}

	var (
		cc *d.ColCore
		e  error
	)
	if cc, e = d.NewColCore(c.DataType(), d.ColName(c.Name())); e != nil {
		return nil, e
	}

	outCol := &Col{
		sql:     "",
		ColCore: cc,
	}

	_ = d.ColDialect(c.Dialect())

	return outCol, nil
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

func (c *Col) SQL() string {
	if c.sql != "" {
		if c.global {
			return c.Dialect().Global(c.Parent().(*DF).SourceSQL(), c.sql)
		}

		return c.sql
	}

	return c.Name()
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
	case c.Name() != "" && c.SQL() != "":
		selectFld = fmt.Sprintf("%s AS %s", c.SQL(), c.Dialect().ToName(c.Name()))
	case c.Name() != "" && c.SQL() == "" && !d.Has(c.Name(), df.ColumnNames()):
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

// TODO: delete
func (c *Col) Replace(indicator, replacement d.Column) (d.Column, error) {
	panicer(indicator, replacement)

	if !sameSource(c, indicator) || !sameSource(c, replacement) {
		return nil, fmt.Errorf("columns not from same DF in Replace")
	}

	if c.DataType() != replacement.DataType() {
		return nil, fmt.Errorf("incompatible columns in Replace")
	}

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("indicator not type DTint in Replace")
	}

	whens := []string{fmt.Sprintf("%s > 0", indicator.Name()), "ELSE"}
	equalTo := []string{replacement.Name(), c.Name()}

	var (
		sqlx string
		e    error
	)
	if sqlx, e = c.Dialect().Case(whens, equalTo); e != nil {
		return nil, e
	}
	outCol, _ := NewColSQL(c.DataType(), c.Dialect(), sqlx)

	return outCol, nil
}

// TODO: get rid of this ... was using d.ToString(x)
func toStringX(x any) string {
	return fmt.Sprintf("%v", x)
}

func (c *Col) String() string {
	//	if c.Name() == "" {
	//		panic("column has no name")
	//	}

	t := fmt.Sprintf("column: %s\ntype: %s\n", c.Name(), c.DataType())

	if c.CategoryMap() != nil {
		var keys []string
		var vals []int
		for k, v := range c.CategoryMap() {
			if k == nil {
				k = "Other"
			}
			x := toStringX(k) // hmmm, was any? or maybe *string

			keys = append(keys, x)
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + d.PrettyPrint(header, keys, vals) + "\n"
	}

	if c.DataType() != d.DTfloat {
		df, _ := NewDFcol(nil, c.Dialect(), c.Parent().MakeQuery(), c)
		tab, _ := df.Table(c.Name())

		var (
			vals *m.DF
			e    error
		)
		if vals, e = m.DBLoad(tab.MakeQuery(), tab.Dialect()); e != nil {
			panic(e)
		}

		l := vals.Column(c.Name())
		c := vals.Column("count")

		header := []string{l.Name(), c.Name()}
		return t + d.PrettyPrint(header, l.(*m.Col).Data(), c.(*m.Col).Data())
	}

	cols := []string{"min", "lq", "median", "mean", "uq", "max", "n"}

	header := []string{"metric", "value"}
	vals, _ := c.Dialect().Summary(c.MakeQuery(), c.Name())
	return t + d.PrettyPrint(header, cols, vals)
}
