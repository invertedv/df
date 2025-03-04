package df

import "fmt"

// Column interface defines the methods the columns
type Column interface {
	CC

	Copy() Column
	Data() *Vector
	Len() int
	String() string
}

// CC interface defines the methods of ColCore
type CC interface {
	Core() *ColCore
	CategoryMap() CategoryMap
	DataType() DataTypes
	Dependencies() []string
	Dialect() *Dialect
	Name() string
	Parent() DF
	Rename(newName string) error
	RT() ReturnTypes
}

// *********** ColCore ***********

// ColCore implements the nucleus of the Column interface.
type ColCore struct {
	name string
	dt   DataTypes
	rt   ReturnTypes

	catMap    CategoryMap
	catCounts CategoryMap
	rawType   DataTypes

	dep []string

	dlct   *Dialect
	parent DF
}

func NewColCore(dt DataTypes, ops ...ColOpt) (*ColCore, error) {
	c := &ColCore{dt: dt, rt: RTcolumn}

	for _, op := range ops {
		if e := op(c); e != nil {
			return nil, e
		}
	}

	return c, nil
}

// *********** Setters ***********

type ColOpt func(c CC) error

func ColCatCounts(ct CategoryMap) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColCatCounts")
		}

		c.Core().catCounts = ct
		return nil
	}
}

func ColCatMap(cm CategoryMap) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColCatMap")
		}

		c.Core().catMap = cm

		return nil
	}
}

func ColDataType(dt DataTypes) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColDataType")
		}

		c.Core().dt = dt

		return nil
	}
}

func ColDialect(dlct *Dialect) ColOpt {
	return func(c CC) error {
		c.Core().dlct = dlct
		return nil
	}
}

func ColName(name string) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColName")
		}

		if c.Name() != "" {
			return fmt.Errorf("column already named -- use Rename method")
		}

		if e := validName(name); e != nil {
			return e
		}

		c.Core().name = name

		return nil
	}
}

func ColParent(df DF) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColParent")
		}

		// A column with this name already exists in df and is not the column we're assigning the parent to
		if df != nil && df.Column(c.Name()) != nil && df.Column(c.Name()) != c {
			return fmt.Errorf("cant assign parent: name collision")
		}

		// can only belong to one DF
		if c.Parent() != nil && c.Parent() != df {
			_ = c.Parent().DropColumns(c.Name())
		}

		c.Core().parent = df

		return nil
	}
}

func ColRawType(rt DataTypes) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColRawType")
		}

		c.Core().rawType = rt
		return nil
	}
}

func ColReturnType(rt ReturnTypes) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColRawType")
		}
		if rt != RTcolumn && rt != RTscalar {
			return fmt.Errorf("invalid column return type: %v", rt)
		}

		c.Core().rt = rt

		return nil
	}
}

func colDependencies(dep []string) ColOpt {
	return func(c CC) error {
		c.Core().dep = dep
		return nil
	}
}

// *********** Methods ***********

func (c *ColCore) CategoryCounts() CategoryMap {
	return c.catCounts
}

func (c *ColCore) CategoryMap() CategoryMap {
	return c.catMap
}

func (c *ColCore) Copy() *ColCore {
	// don't copy parent
	cx, _ := NewColCore(c.DataType(),
		ColName(c.Name()),
		colDependencies(c.Dependencies()),
		ColRawType(c.RawType()),
		ColCatMap(c.CategoryMap()),
		ColReturnType(c.RT()),
		ColCatCounts(c.CategoryCounts()))

	return cx
}

// Core returns itself. We eed a method to return itself since DFCore struct will need these methods
func (c *ColCore) Core() *ColCore {
	return c
}

func (c *ColCore) DataType() DataTypes {
	return c.dt
}

func (c *ColCore) Dependencies() []string {
	return c.dep
}

func (c *ColCore) Dialect() *Dialect { return c.dlct }

func (c *ColCore) Name() string {
	return c.name
}

func (c *ColCore) Parent() DF {
	return c.parent
}

func (c *ColCore) RawType() DataTypes {
	return c.rawType
}

func (c *ColCore) Rename(newName string) error {
	if e := validName(newName); e != nil {
		return e
	}

	if c.Parent() != nil && c.Parent().Column(newName) != nil {
		return fmt.Errorf("column %s already exists, cannot Rename", newName)
	}

	c.name = newName

	return nil
}

func (c *ColCore) RT() ReturnTypes {
	return c.rt
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
