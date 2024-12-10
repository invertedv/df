package df

import (
	"fmt"
	"time"
)

type Vector struct {
	dt DataTypes

	floats  []float64
	ints    []int
	strings []string
	dates   []time.Time
}

func NewVector(data any, n int) *Vector {
	switch x := data.(type) {
	case []float64, []float32, float64, float32:
		return &Vector{dt: DTfloat, floats: ToFloatSlc(data, n)}
	case []int, int, int8, int16, int32, int64, []int8, []int16, []int32, []int64:
		return &Vector{dt: DTint, ints: ToIntSlc(x, n)}
	case []string, string:
		return &Vector{dt: DTstring, strings: ToStringSlc(x, n)}
	case []time.Time, time.Time:
		// TODO: consider zeroing out hours/time zone here
		return &Vector{dt: DTdate, dates: ToDateSlc(x, n)}
	default:
		panic("unsupported data type in NewVector")
	}
}

func MakeVector(dt DataTypes, n int) *Vector {
	switch dt {
	case DTfloat:
		return &Vector{dt: dt, floats: make([]float64, n)}
	case DTint:
		return &Vector{dt: dt, ints: make([]int, n)}
	case DTstring:
		return &Vector{dt: dt, strings: make([]string, n)}
	case DTdate:
		return &Vector{dt: dt, dates: make([]time.Time, n)}
	default:
		panic(fmt.Errorf("cannot make Vector with data type %s", dt))
	}
}

func (v *Vector) VectorType() DataTypes {
	return v.dt
}

// TODO: allow for casting?
func (v *Vector) Set(val any, indx int) {
	if WhatAmI(val) != v.VectorType() {
		panic(fmt.Errorf("different types in set"))
	}

	if indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	switch v.VectorType() {
	case DTfloat:
		v.floats[indx] = val.(float64)
	case DTint:
		v.ints[indx] = val.(int)
	case DTstring:
		v.strings[indx] = val.(string)
	case DTdate:
		v.dates[indx] = val.(time.Time)
	default:
		panic(fmt.Errorf("error in Vector.Set"))
	}
}

func (v *Vector) SetFloat(val float64, indx int) {
	if indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.floats[indx] = val
}

func (v *Vector) SetInt(val int, indx int) {
	if indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.ints[indx] = val
}

func (v *Vector) SetString(val string, indx int) {
	if indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.strings[indx] = val
}

func (v *Vector) SetDate(val time.Time, indx int) {
	if indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.dates[indx] = val
}

func (v *Vector) Data() any {
	switch v.dt {
	case DTfloat:
		return v.floats
	case DTint:
		return v.ints
	case DTstring:
		return v.strings
	case DTdate:
		return v.dates
	default:
		panic("error in Vector.Data")
	}
}

func (v *Vector) AsFloat() []float64 {
	if v.dt == DTfloat {
		return v.floats
	}

	return ToFloatSlc(v.Data(), 0)
}

func (v *Vector) AsInt() []int {
	if v.dt == DTint {
		return v.ints
	}

	return ToIntSlc(v.Data(), 0)
}

func (v *Vector) AsString() []string {
	if v.dt == DTstring {
		return v.strings
	}

	return ToStringSlc(v.Data(), 0)
}

func (v *Vector) AsDate() []time.Time {
	if v.dt == DTdate {
		return v.dates
	}

	return ToDateSlc(v.Data(), 0)
}

func (v *Vector) Element(indx int) any {
	// handles ops like x/2 where x is a vector
	if v.Len() == 1 {
		indx = 0
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	switch v.dt {
	case DTfloat:
		return v.floats[indx]
	case DTint:
		return v.ints[indx]
	case DTstring:
		return v.strings[indx]
	case DTdate:
		return v.dates[indx]
	default:
		panic(fmt.Errorf("error in Element"))
	}
}

func (v *Vector) ElementFloat(indx int) float64 {
	// handles ops like x/2 where x is a vector
	if v.Len() == 1 {
		indx = 0
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	if v.VectorType() == DTfloat {
		return v.floats[indx]
	}

	x := v.Element(indx)
	if val := Any2Float64(x, true); val != nil {
		return *val
	}

	panic(fmt.Errorf("element is not float-able"))
}

func (v *Vector) ElementInt(indx int) int {
	// handles ops like x/2 where x is a vector
	if v.Len() == 1 {
		indx = 0
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	if v.VectorType() == DTint {
		return v.ints[indx]
	}

	x := v.Element(indx)
	if val := Any2Int(x, true); val != nil {
		return *val
	}

	panic(fmt.Errorf("element is not int-able"))
}

func (v *Vector) ElementString(indx int) string {
	// handles ops like x/2 where x is a vector
	if v.Len() == 1 {
		indx = 0
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	if v.VectorType() == DTstring {
		return v.strings[indx]
	}

	x := v.Element(indx)
	return *Any2String(x, true)
}

func (v *Vector) ElementDate(indx int) time.Time {
	// handles ops like x/2 where x is a vector
	if v.Len() == 1 {
		indx = 0
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	if v.VectorType() == DTdate {
		return v.dates[indx]
	}

	x := v.Element(indx)
	if val := Any2Date(x, true); val != nil {
		return *val
	}

	panic(fmt.Errorf("element is not date-able"))
}

func (v *Vector) Len() int {
	switch v.dt {
	case DTfloat:
		return len(v.floats)
	case DTint:
		return len(v.ints)
	case DTstring:
		return len(v.strings)
	case DTdate:
		return len(v.dates)
	default:
		panic(fmt.Errorf("unexpected error in Vector.Len"))
	}
}

func (v *Vector) Swap(i, j int) {
	switch v.dt {
	case DTfloat:
		v.floats[i], v.floats[j] = v.floats[j], v.floats[i]
	case DTint:
		v.ints[i], v.ints[j] = v.ints[j], v.ints[i]
	case DTstring:
		v.strings[i], v.strings[j] = v.strings[j], v.strings[i]
	case DTdate:
		v.dates[i], v.dates[j] = v.dates[j], v.dates[i]
	default:
		panic(fmt.Errorf("unexpected error in Vector.Len"))
	}
}

func (v *Vector) Less(i, j int) bool {
	switch v.dt {
	case DTfloat:
		return v.floats[i] < v.floats[j]
	case DTint:
		return v.ints[i] < v.ints[j]
	case DTstring:
		return v.strings[i] < v.strings[j]
	case DTdate:
		return v.dates[i].Sub(v.dates[j]).Minutes() < 0
	default:
		panic(fmt.Errorf("unexpected error in vector.Less"))
	}
}

func (v *Vector) AppendVector(vAdd *Vector) {
	if v.VectorType() != vAdd.VectorType() {
		panic("appending different vector types")
	}

	switch v.dt {
	case DTfloat:
		v.floats = append(v.floats, vAdd.floats...)
	case DTint:
		v.ints = append(v.ints, vAdd.ints...)
	case DTstring:
		v.strings = append(v.strings, vAdd.strings...)
	case DTdate:
		v.dates = append(v.dates, vAdd.dates...)
	default:
		panic(fmt.Errorf("unknown type in Vector.Append"))
	}
}

func (v *Vector) Append(data ...any) {
	for ind := 0; ind < len(data); ind++ {
		switch v.dt {
		case DTfloat:
			var x *float64
			if x = Any2Float64(data[ind], true); x == nil {
				panic(fmt.Errorf("cannot make float in Append"))
			}

			v.floats = append(v.floats, *x)
		case DTint:
			var x *int
			if x = Any2Int(data[ind], true); x == nil {
				panic(fmt.Errorf("cannot make int in Append"))
			}

			v.ints = append(v.ints, *x)
		case DTstring:
			v.strings = append(v.strings, *Any2String(data[ind], true))
		case DTdate:
			var x *time.Time
			if x = Any2Date(data[ind], true); x == nil {
				panic(fmt.Errorf("cannot make date in Append"))
			}

			v.dates = append(v.dates, *x)
		}
	}
}

func (v *Vector) Copy() *Vector {
	vCopy := &Vector{dt: v.dt}
	switch v.dt {
	case DTfloat:
		vCopy.floats = make([]float64, v.Len())
		copy(vCopy.floats, v.floats)
	case DTint:
		vCopy.ints = make([]int, v.Len())
		copy(vCopy.ints, v.ints)
	case DTstring:
		vCopy.strings = make([]string, v.Len())
		copy(vCopy.strings, v.strings)
	case DTdate:
		vCopy.dates = make([]time.Time, v.Len())
		copy(vCopy.dates, v.dates)
	default:
		panic(fmt.Errorf("unexpected error in Vector.Copy"))
	}

	return vCopy
}

func (v *Vector) Where(indic *Vector) *Vector {
	outVec := MakeVector(v.VectorType(), 0)
	for ind := 0; ind < v.Len(); ind++ {
		if indic.ElementInt(ind) > 0 {
			outVec.Append(v.Element(ind))
		}
	}

	return outVec
}
