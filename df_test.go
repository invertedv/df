package df

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() *MemDF {
	x := []float64{1, 2, 3}
	y := []float64{4, 5, 6}

	xCol := &MemCol{
		name: "x",
		//		n:      len(x),
		dType:  DTfloat,
		data:   x,
		catMap: nil,
	}

	yCol := &MemCol{
		//		n:     len(y),
		dType: DTfloat,
		name:  "y",
		data:  y,
	}

	// this works
	tmp, _ := NewDFlist(xCol, yCol)

	tmp1 := &MemDF{
		rows:   0,
		DFlist: tmp,
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

	tmp, _ := NewDFlist(xCol, yCol)

	tmp1 := &SQLdf{
		sourceSQL: "",
		destSQL:   "",
		DFlist:    tmp,
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
	f := Functions["exp"]
	e1 := df.Apply("test", f, "x")
	assert.Nil(t, e1)
	fmt.Println(df.ColumnNames())
	fmt.Println(df.ColumnCount())
	c, _ := df.Column("test")
	fmt.Println(c.Data())
}

func TestDFlist_Apply(t *testing.T) {
	df := makeSQLdf()
	f := SQLfunctions["addFloat"]
	e1 := df.Apply("test", f, "x", "y")
	assert.Nil(t, e1)
	_ = df
}
