package df

// Column interface defines the methods the columns of DFcore that must be supported
type Column interface {
	CategoryMap() CategoryMap

	Context() *Context
	DataType() DataTypes
	Dependencies() []string
	Name() string
	Rename(newName string) error

	AppendRows(col Column) (Column, error)
	Copy() Column
	Core() *ColCore
	Data() any
	Len() int
	Replace(ind, repl Column) (Column, error)
	String() string
}

// *********** ColCore ***********

// ColCore implements the nucleus of the Column interface.
type ColCore struct {
	name string
	dt   DataTypes
	ctx  *Context

	catMap    CategoryMap
	catCounts CategoryMap
	rawType   DataTypes

	dep []string
}

type COpt func(c *ColCore)

func ColDataType(dt DataTypes) COpt {
	return func(c *ColCore) {
		c.dt = dt
	}
}

func NewColCore(dt DataTypes, ops ...COpt) *ColCore {
	c := &ColCore{dt: dt}

	for _, op := range ops {
		op(c)
	}

	return c
}

// *********** Setters ***********
func ColName(name string) COpt {
	if e := ValidName(name); e != nil {
		panic(e)
	}

	return func(c *ColCore) {
		c.name = name
	}
}

func ColContext(ctx *Context) COpt {
	return func(c *ColCore) {
		c.ctx = ctx
	}
}

func colDependencies(dep []string) COpt {
	return func(c *ColCore) {
		c.dep = dep
	}
}

func (c *ColCore) Dependencies() []string {
	return c.dep
}

func ColCatMap(cm CategoryMap) COpt {
	return func(c *ColCore) {
		c.catMap = cm
	}
}

func ColCatCounts(ct CategoryMap) COpt {
	return func(c *ColCore) {
		c.catCounts = ct
	}
}

func ColRawType(rt DataTypes) COpt {
	return func(c *ColCore) {
		c.rawType = rt
	}
}

// *********** Methods ***********

func (c *ColCore) CategoryMap() CategoryMap {
	return c.catMap
}

func (c *ColCore) CategoryCounts() CategoryMap {
	return c.catCounts
}

// Core returns itself. We eed a method to return itself since DFCore struct will need these methods
func (c *ColCore) Core() *ColCore {
	return c
}

func (c *ColCore) Context() *Context {
	return c.ctx
}

func (c *ColCore) RawType() DataTypes {
	return c.rawType
}

func (c *ColCore) Name() string {
	return c.name
}

func (c *ColCore) DataType() DataTypes {
	return c.dt
}

func (c *ColCore) Copy() *ColCore {
	return NewColCore(c.DataType(),
		ColName(c.Name()),
		ColContext(c.Context()),
		colDependencies(c.Dependencies()),
		ColRawType(c.RawType()),
		ColCatMap(c.CategoryMap()),
		ColCatCounts(c.CategoryCounts()))
}

func (c *ColCore) Rename(newName string) error {
	if e := ValidName(newName); e != nil {
		return e
	}

	ColName(newName)(c)

	return nil
}

func (c *ColCore) SetContext(ctx *Context) {
	ColContext(ctx)(c)
}

// *********** Category Map ***********

type CategoryMap map[any]int

func (cm CategoryMap) Max() int {
	var maxVal *int
	for k, v := range cm {
		if maxVal == nil {
			maxVal = new(int)
			*maxVal = v
		}
		if k != nil && v > *maxVal {
			*maxVal = v
		}
	}

	return *maxVal
}

func (cm CategoryMap) Min() int {
	var minVal *int
	for k, v := range cm {
		if minVal == nil {
			minVal = new(int)
			*minVal = v
		}

		if k != nil && v < *minVal {
			*minVal = v
		}
	}

	return *minVal
}
