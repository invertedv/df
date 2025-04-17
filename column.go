package df

import (
	"fmt"
	"iter"
	"maps"
)

// The Column interface defines the methods that columns must have.
type Column interface {

	// Core Methods
	CC

	// AllRows iterates through the rows of the column.  It returns the row # and the value of the column at that row.
	// The row value return is a slice, []any, of length 1.  This was done to be consistent with
	// the AllRows() function of DF which also returns []any.
	AllRows() iter.Seq2[int, []any]

	// Copy returns a copy of the column.
	Copy() Column

	// Data returns the contents of the column.  Column implementations that are not stored in memory (e.g. as in a database)
	//  will have to fetch the data when this method is called.
	Data() *Vector

	// Len is the length of the column.
	Len() int

	// Stringer.  This is expected to be a summary of the column.
	String() string
}

// The CC interface defines the methods of ColCore. These methods are invariant to the data that
// underlies the column.
type CC interface {
	Core() *ColCore              // Core returns itself.
	CategoryMap() CategoryMap    // CategoryMap returns a map of original value to category value.  Not nil only for dt=DTcategorical.
	DataType() DataTypes         // DataType returns the type of the column.
	Dependencies() []string      // Dependencies returns a list of columns required to calculate this column, if this is a calculated column.
	Dialect() *Dialect           // Dialect returns the Dialect object. A Dialect object is required if there is DB interaction.
	Name() string                // Name returns the column's name.
	Parent() DF                  // Parent returns the DF to which the column belongs.
	Rename(newName string) error // Rename renames the column.
}

// *********** ColCore ***********

// ColCore implements the CC interface.
type ColCore struct {
	name string    // column name
	dt   DataTypes // column data type

	catMap  CategoryMap // map of original value to category value.  Not nil only for dt=DTcategorical.
	rawType DataTypes   // data type of the source column for a categorical column.

	dep []string

	dlct   *Dialect
	parent DF // DF to which the column belongs.
}

func NewColCore(opts ...ColOpt) (*ColCore, error) {
	c := &ColCore{}

	for _, opt := range opts {
		if e := opt(c); e != nil {
			return nil, e
		}
	}

	return c, nil
}

// *********** Setters ***********

// ColOpt functions are used to set ColCore options
type ColOpt func(c CC) error

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

func ColRawType(raw DataTypes) ColOpt {
	return func(c CC) error {
		if c == nil {
			return fmt.Errorf("nil column to ColRawType")
		}

		c.Core().rawType = raw

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

func (c *ColCore) CategoryMap() CategoryMap {
	return c.catMap
}

func (c *ColCore) Copy() *ColCore {
	var cm CategoryMap
	if c.CategoryMap() != nil {
		cm = make(CategoryMap)
		maps.Copy(cm, c.CategoryMap())
	}

	// don't copy parent
	cx, _ := NewColCore(ColDataType(c.DataType()),
		ColDialect(c.Dialect()),
		ColName(c.Name()),
		colDependencies(c.Dependencies()),
		ColRawType(c.RawType()),
		ColCatMap(cm))

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

// *********** Category Map ***********

// CategoryMap maps the raw value of a categorical column to the category level
type CategoryMap map[any]int

func (cm CategoryMap) String() string {
	var keys []string
	var vals []int
	for k, v := range cm {
		if k == nil {
			continue
		}

		var x any = fmt.Sprintf("%v", k)

		keys = append(keys, x.(string))
		vals = append(vals, v)
	}
	keys = append(keys, "Other")
	vals = append(vals, -1)

	header := []string{"source", "mapped to"}

	return PrettyPrint(header, keys, vals) + "\n"
}

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
