package df

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() *MemDF {
	x := []float64{1, -2, 3}
	y := []int{4, 5, 6}
	z := []string{"p20221231", "20000101", "19900615"}

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
	tmp, _ := NewDF(xCol, yCol, zCol)

	tmp1 := &MemDF{
		rows: 0,
		DF:   tmp,
	}

	return tmp1
}

func makeSQLdf() *SQLdf {
	xCol := &SQLcol{
		name:   "x",
		n:      1,
		dType:  DTfloat,
		sql:    "x",
		catMap: nil,
	}

	yCol := &SQLcol{
		name:   "y",
		n:      1,
		dType:  DTfloat,
		sql:    "y",
		catMap: nil,
	}

	tmp, _ := NewDF(xCol, yCol)

	tmp1 := &SQLdf{
		sourceSQL: "",
		DF:        tmp,
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
	f := MemFunctions["cast"]
	col, e := df.Column("z")
	assert.Nil(t, e)
	e1 := df.Apply("test", MemRun, MemFunctions["cast"], "DTstring", "z")
	assert.Nil(t, e1)
	c1, _ := df.Column("test")
	fmt.Println(c1.Data())
	//	assert.Nil(t, e1)
	col, e = df.Column("x")
	assert.Nil(t, e)
	col1, e1 := df.Column("y")
	_, _ = col, col1
	assert.Nil(t, e1)
	f = MemFunctions["add"]
	e1 = df.Apply("test1", MemRun, f, "x", "y")
	fmt.Println(df.ColumnNames())
	fmt.Println(df.ColumnCount())
	c, _ := df.Column("test1")
	fmt.Println(c.Data())
	e1 = df.Apply("xyz", MemRun, nil, "z")
	assert.Nil(t, e1)
}

/*
func TestDFlist_Apply(t *testing.T) {
	df := makeSQLdf()
	f := SQLfunctions["addFloat"]
	col, e := df.Column("x")
	assert.Nil(t, e)
	col1, e1 := df.Column("y")
	assert.Nil(t, e1)
	e1 = df.Apply("test",MemRun, f, col, col1)
	assert.Nil(t, e1)
	_ = df
}


*/
