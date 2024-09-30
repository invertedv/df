package df

type Context struct {
	dialect *Dialect
	files   *Files
	self    DF

	unassigned []any
}

func NewContext(dialect *Dialect, files *Files, df DF, unassigned ...any) *Context {
	// Fill with default values
	if files == nil {
		files = NewFiles()
	}

	return &Context{
		dialect:    dialect,
		files:      files,
		unassigned: unassigned,
		self:       df,
	}
}

func (c *Context) Dialect() *Dialect {
	return c.dialect
}

func (c *Context) Files() *Files {
	return c.files
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
