package sql

import (
	"fmt"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
)

// ***************** Col - Create *****************

func NewColSQL(name string, context *d.Context, dt d.DataTypes, sqlx string) (*Col, error) {
	if e := d.ValidName(name); e != nil {
		return nil, e
	}

	col := &Col{
		sql:     sqlx,
		ColCore: d.NewColCore(dt, d.ColName(name), d.ColContext(context)),
	}

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
	cx.Rename(c.Name())
	q2 := c.MakeQuery()

	if _, e := c.Context().Dialect().Union(q1, q2, cx.Name()); e != nil {
		return nil, e
	}
	outCol := &Col{
		sql:     "",
		ColCore: d.NewColCore(c.DataType(), d.ColName(c.Name()), d.ColContext(c.Context())),
	}

	return outCol, nil
}

func (c *Col) Copy() d.Column {
	n := &Col{
		sql: c.sql,
		//		scalarValue: c.scalarValue,
		ColCore: c.Core().Copy(),
	}

	return n
}

func (c *Col) Core() *d.ColCore {
	return c.ColCore
}

func (c *Col) Data() any {
	var (
		df *m.DF
		e  error
	)

	// give it a random name if it does not have one
	if c.Name() == "" {
		_ = c.Rename(d.RandomLetters(5))
	}

	if df, e = m.DBLoad(c.MakeQuery(), c.Context().Dialect()); e != nil {
		panic(e)
	}

	var col d.Column
	if col = df.Column(c.Name()); col == nil {
		panic(fmt.Errorf("missing column?"))
	}

	return col.(*m.Col).Data()
}

func (c *Col) SQL() any {
	if c.sql != "" {
		return c.sql
	}

	return c.Name()
}

func (c *Col) Len() int {
	var (
		n  int
		ex error
	)
	if n, ex = c.Context().Dialect().RowCount(c.MakeQuery()); ex != nil {
		panic(ex)
	}

	return n
}

func (c *Col) MakeQuery() string {
	if c.Context().Self() == nil {
		panic("nil Context")
	}

	df := c.Context().Self().(*DF)

	field := c.Name()
	if field == "" || (field != "" && !d.Has(field, "", df.ColumnNames()...)) {
		field = c.SQL().(string)
	}

	deps := c.Dependencies()

	w := d.RandomLetters(4)
	qry := fmt.Sprintf("WITH %s AS (%s) SELECT %s AS %s FROM %s", w, df.MakeQuery(deps...), field, c.Name(), w)

	return qry
}

func (c *Col) Rename(newName string) error {
	oldName := c.Name()
	if e := c.Core().Rename(newName); e != nil {
		return e
	}

	// if this is just a column pull, need to keep the source name for "AS"
	if c.sql == "" {
		c.sql = oldName
	}

	return nil
}

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
	if sqlx, e = c.Context().Dialect().Case(whens, equalTo); e != nil {
		return nil, e
	}
	outCol, _ := NewColSQL("", c.Context(), c.DataType(), sqlx)

	return outCol, nil
}

func (c *Col) String() string {
	if c.Name() == "" {
		panic("column has no name")
	}

	t := fmt.Sprintf("column: %s\ntype: %s\n", c.Name(), c.DataType())

	if c.CategoryMap() != nil {
		var keys []string
		var vals []int
		for k, v := range c.CategoryMap() {
			if k == nil {
				k = "Other"
			}
			x := *d.Any2String(k, true)

			keys = append(keys, x)
			vals = append(vals, v)
		}

		header := []string{"source", "mapped to"}
		t = t + d.PrettyPrint(header, keys, vals) + "\n"
	}

	if c.DataType() != d.DTfloat {
		df, _ := NewDFcol(nil, c.Context(), c)
		tab, _ := df.Table(false, c.Name())

		var (
			vals *m.DF
			e    error
		)
		if vals, e = m.DBLoad(tab.MakeQuery(), tab.Context().Dialect()); e != nil {
			panic(e)
		}

		l := vals.Column(c.Name())
		c := vals.Column("count")

		header := []string{l.Name(), c.Name()}
		return t + d.PrettyPrint(header, l.(*m.Col).Data(), c.(*m.Col).Data())
	}

	cols := []string{"min", "lq", "median", "mean", "uq", "max", "n"}

	header := []string{"metric", "value"}
	vals, _ := c.Context().Dialect().Summary(c.MakeQuery(), c.Name())
	return t + d.PrettyPrint(header, cols, vals)
}
