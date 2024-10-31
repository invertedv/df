package df

type Context struct {
	self    DF
	dialect *Dialect

	unassigned []any
}

func NewContext(dialect *Dialect, df DF, unassigned ...any) *Context {
	return &Context{
		dialect:    dialect,
		unassigned: unassigned,
		self:       df,
	}
}

func (c *Context) Dialect() *Dialect {
	return c.dialect
}

func (c *Context) Unassigned() []any {
	return c.unassigned
}

func (c *Context) Self() DF {
	return c.self
}

func (c *Context) SetSelf(df DF) {
	c.self = df
}

func (c *Context) SetDialect(d *Dialect) {
	c.dialect = d
}

func (c *Context) SetUnassigned(u ...any) {
	c.unassigned = u
}
