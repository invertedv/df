package df

type Context struct {
	dialect *Dialect
	n       *int

	unassigned []any
}

func NewContext(dialect *Dialect, rowCount int, unassigned ...any) *Context {
	var (
		np int
		nx *int
	)

	if rowCount > 0 {
		np, nx = rowCount, &np
	}

	return &Context{
		dialect:    dialect,
		unassigned: unassigned,
		n:          nx,
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
