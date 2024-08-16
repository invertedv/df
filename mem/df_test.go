package df

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() *MemDF {
	x, _ := NewMemCol("x", []float64{1, -2, 3, 0, 2, -1})
	y, _ := NewMemCol("y", []int{4, 5, 6, 1, 4, 4})
	z, _ := NewMemCol("z", []string{"p20221231", "20000101", "19900615", "20220601", "20230915", "20060310"})
	df, _ := NewMemDF(Run, StandardFunctions(), x, y, z)

	return df
}

func TestDF_Apply(t *testing.T) {
	df := makeMemDF()
	col, e := df.Column("z")
	assert.Nil(t, e)
	e1 := df.Apply("test", "cast", "DTstring", "z")
	assert.Nil(t, e1)
	c1, _ := df.Column("test")
	fmt.Println(c1.Data())
	//	assert.Nil(t, e1)
	col, e = df.Column("x")
	assert.Nil(t, e)
	col1, e1 := df.Column("y")
	_, _ = col, col1
	assert.Nil(t, e1)
	e1 = df.Apply("test1", "add", "x", "y")
	fmt.Println(df.ColumnNames())
	fmt.Println(df.ColumnCount())
	c, _ := df.Column("test1")
	fmt.Println(c.Data())
	e1 = df.Apply("xyz", "aaa", "z")
	assert.Nil(t, e1)
	e1 = df.DropColumns("test1")
	assert.Nil(t, e1)
}

func TestDF_Sort(t *testing.T) {
	df := makeMemDF()
	e := df.Sort("y", "z")
	assert.Nil(t, e)
	x, _ := df.Column("x")
	y, _ := df.Column("y")
	z, _ := df.Column("z")
	fmt.Println(x, y, z)
}
