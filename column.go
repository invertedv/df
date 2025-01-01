package df

type CC interface {
	Core() *ColCore
	CategoryMap() CategoryMap
	DataType() DataTypes
	Dependencies() []string
	Name() string
}

// Column interface defines the methods the columns of DFcore that must be supported
type Column interface {
	CC

	AppendRows(col Column) (Column, error)
	Copy() Column
	Data() *Vector
	Len() int
	Replace(ind, repl Column) (Column, error)
	String() string
}

// *********** ColCore ***********

// ColCore implements the nucleus of the Column interface.
type ColCore struct {
	name string
	dt   DataTypes

	catMap    CategoryMap
	catCounts CategoryMap
	rawType   DataTypes

	dep []string
}

type ColOpt func(c CC)

func ColDataType(dt DataTypes) ColOpt {
	return func(c CC) {
		c.Core().dt = dt
	}
}

func NewColCore(dt DataTypes, ops ...ColOpt) *ColCore {
	c := &ColCore{dt: dt}

	for _, op := range ops {
		op(c)
	}

	return c
}

// *********** Setters ***********
func ColName(name string) ColOpt {
	return func(c CC) {
		if validName(name) {
			c.Core().name = name
		}
	}
}

func colDependencies(dep []string) ColOpt {
	return func(c CC) {
		c.Core().dep = dep
	}
}

func (c *ColCore) Dependencies() []string {
	return c.dep
}

func ColCatMap(cm CategoryMap) ColOpt {
	return func(c CC) {
		c.Core().catMap = cm
	}
}

func ColCatCounts(ct CategoryMap) ColOpt {
	return func(c CC) {
		c.Core().catCounts = ct
	}
}

func ColRawType(rt DataTypes) ColOpt {
	return func(c CC) {
		c.Core().rawType = rt
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
		colDependencies(c.Dependencies()),
		ColRawType(c.RawType()),
		ColCatMap(c.CategoryMap()),
		ColCatCounts(c.CategoryCounts()))
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
