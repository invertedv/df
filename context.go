package df

type Context struct {
	dialect *Dialect
	files   *Files
	n       *int

	unassigned []any
}

func NewContext(dialect *Dialect, files *Files, rowCount *int, unassigned ...any) *Context {
	// Fill with default values
	if files == nil {
		files = NewFiles()
	}

	return &Context{
		dialect:    dialect,
		files:      files,
		unassigned: unassigned,
		n:          rowCount,
	}
}

func (c *Context) Dialect() *Dialect {
	return c.dialect
}

func (c *Context) Files() *Files {
	return c.files
}

func (c *Context) Len() *int {
	return c.n
}

func (c *Context) Unassigned() []any {
	return c.unassigned
}

func (c *Context) UpdateLen(n int) {
	*c.n = n
}
