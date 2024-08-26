package df

type Context struct {
	dialect *Dialect
	n       *int

	unassigned []any
}

func NewContext(dialect *Dialect, rowCount *int, unassigned ...any) *Context {
	return &Context{
		dialect:    dialect,
		unassigned: unassigned,
		n:          rowCount,
	}
}

func (c *Context) Dialect() *Dialect {
	return c.dialect
}

func (c *Context) Len() *int {
	return c.n
}

func (c *Context) Unassigned() []any {
	return c.unassigned
}
