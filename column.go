package df

import "fmt"

// TODO: add dialect to ColCore to match DFcore

// Column interface defines the methods the columns
type Column interface {
	CC

	AppendRows(col Column) (Column, error)
	Copy() Column
	Data() *Vector
	Len() int
	// Rename is needed bc sql/column needs to do more work than just rename
	//	Rename(newName string) error
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

	dlct   *Dialect
	parent DF
}

func NewColCore(dt DataTypes, ops ...ColOpt) (*ColCore, error) {
	c := &ColCore{dt: dt}

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

		if df := c.Parent(); df != nil {
			if df.Column(name) != nil {
				return fmt.Errorf("column name already exists: %s", name)
			}
		}

		if !validName(name) {
			return fmt.Errorf("invalid column name %s", name)
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

		// TODO: think about this
		//		if c.Name() == "" && df != nil {
		//			return fmt.Errorf("column must have a name to assign parent")
		//		}

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
