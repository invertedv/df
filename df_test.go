package df

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() (*DF, error) {
	x := []float64{1, 2, 3}
	y := []float64{4, 5, 6}

	xCol := &Memory{
		name:   "x",
		n:      len(x),
		dType:  DTfloat,
		data:   x,
		catMap: nil,
	}

	yCol := &Memory{
		n:     len(y),
		dType: DTfloat,
		name:  "y",
		data:  y,
	}

	return NewDF(xCol, yCol)
}

func TestNewDF(t *testing.T) {
	df, e := makeMemDF()
	assert.Nil(t, e)
	_ = df
}

func TestDF_GetColumn(t *testing.T) {
	x := []float64{1, 2, 3}
	y := []float64{4, 5, 6}
	df, _ := makeMemDF()

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
	df, _ := makeMemDF()
	//	outCol, err := df.Apply("total", MemAdd, "x", "y")
	//	assert.Nil(t, err)
	//	_ = outCol
	xc, _ := df.GetColumn("x")
	yc, _ := df.GetColumn("y")
	f := Functions["exp"]
	e1 := df.Apply("test", &f, "x")
	assert.Nil(t, e1)
	_ = yc
	_ = xc
	/*
		//	fn, e := Func1F1F("exp")
		outCol1, e2 := Oper("test", "abs", xc)
		assert.Nil(t, e2)
		_ = outCol1
		fn2, e1 := Func2F1F("add")
		assert.Nil(t, e1)

		fmt.Println(fn2(1, 2))

	*/
}
