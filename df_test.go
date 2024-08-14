package df

import (
	"fmt"
	"github.com/invertedv/df/sql"
	"testing"

	u "github.com/invertedv/utilities"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() *MemDF {
	x := []float64{1, -2, 3, 0, 2, -1}
	y := []int{4, 5, 6, 1, 4, 4}
	z := []string{"p20221231", "20000101", "19900615", "20220601", "20230915", "20060310"}

	xCol := &MemCol{
		name: "x",
		//		n:      len(x),
		dType:  DTfloat,
		data:   x,
		catMap: nil,
	}

	yCol := &MemCol{
		//		n:     len(y),
		dType: DTint,
		name:  "y",
		data:  y,
	}

	zCol := &MemCol{
		//		n:     len(y),
		dType: DTstring,
		name:  "z",
		data:  z,
	}

	// this works
	tmp, _ := NewDF(MemRun, LoadFunctions(true), xCol, yCol, zCol)

	tmp1 := &MemDF{
		//		rows:   0,
		DFcore: tmp,
	}
	_ = tmp1
	tmp2, _ := NewMemDF(MemRun, LoadFunctions(true), xCol, yCol, zCol)

	return tmp2
}

func makeSQLdf() *sql.SQLdf {
	xCol := &sql.SQLcol{
		name:   "x",
		n:      1,
		dType:  DTfloat,
		sql:    "x",
		catMap: nil,
	}

	yCol := &sql.SQLcol{
		name:   "y",
		n:      1,
		dType:  DTfloat,
		sql:    "y",
		catMap: nil,
	}

	tmp, _ := NewDF(MemRun, LoadFunctions(true), xCol, yCol)

	tmp1 := &sql.SQLdf{
		sourceSQL: "",
		DFcore:    tmp,
	}

	return tmp1
}

func TestDF_Column(t *testing.T) {
	x := []float64{1, 2, 3}
	y := []float64{4, 5, 6}
	df := makeMemDF()

	yg, ey := df.Column("y")
	assert.Nil(t, ey)
	assert.ElementsMatch(t, y, yg.Data())

	xg, ex := df.Column("x")
	assert.Nil(t, ex)
	assert.ElementsMatch(t, x, xg.Data())

	_, e := df.Column("nope")
	assert.NotNil(t, e)
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
	e1 = df.Drop("test1")
	assert.Nil(t, e1)
}

func TestDF_Subset(t *testing.T) {
	df := makeMemDF()
	df1, e := df.Subset("z", "x")
	assert.Nil(t, e)
	names := df1.ColumnNames()
	assert.True(t, u.Has("z", "", names...))
	assert.True(t, u.Has("x", "", names...))
	assert.False(t, u.Has("y", "", names...))
}
