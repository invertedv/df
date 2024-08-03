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

func TestDF_GetColumn(t *testing.T) {
	x := []float64{1, 2, 3}
	y := []float64{4, 5, 6}
	df := makeMemDF()

	yg, ey := df.GetColumn("y")
	assert.Nil(t, ey)
	assert.ElementsMatch(t, y, yg.Data())

	xg, ex := df.GetColumn("x")
	assert.Nil(t, ex)
	assert.ElementsMatch(t, x, xg.Data())

	_, e := df.GetColumn("nope")
	assert.NotNil(t, e)
}

func TestDF_Apply(t *testing.T) {
	df := makeMemDF()
	f := Functions["exp"]
	e1 := df.Apply("test", f, "x")
	assert.Nil(t, e1)
	fmt.Println(df.GetNames())
	fmt.Println(df.NumCol())

}

func TestDFlist_Apply(t *testing.T) {
	df := makeSQLdf()
	f := SQLfunctions["exp"]
	e1 := df.Apply("test", f, "x")
	assert.Nil(t, e1)
	_ = df
}
