package df

import (
	"fmt"
	"iter"
	"time"
)

// Vector is the return type for Column data.
type Vector struct {
	dt DataTypes

	data any
}

// NewVector creates a new *Vector from data, checking/converting that it is of type dt.
func NewVector(data any, dt DataTypes) (*Vector, error) {
	var (
		v  any
		ok bool
	)
	if v, ok = toSlc(data, dt); !ok {
		return nil, fmt.Errorf("cannot make vector of type %s", dt)
	}

	return &Vector{dt: dt, data: v}, nil
}

// MakeVector returns a *Vector with data of type dt and length n.
func MakeVector(dt DataTypes, n int) *Vector {
	switch dt {
	case DTfloat:
		return &Vector{dt: dt, data: make([]float64, n)}
	case DTint, DTcategorical:
		return &Vector{dt: DTint, data: make([]int, n)}
	case DTstring:
		return &Vector{dt: dt, data: make([]string, n)}
	case DTdate:
		return &Vector{dt: dt, data: make([]time.Time, n)}
	default:
		panic(fmt.Errorf("cannot make Vector with data type %s", dt))
	}
}

// *********** Methods ***********

// AllRows returns an iterator that move through the data.  It returns a slice rather than a row so that
// it's compatible with the DF iterator
func (v *Vector) AllRows() iter.Seq2[int, []any] {
	return func(yield func(int, []any) bool) {
		for ind := 0; ind < v.Len(); ind++ {
			var row []any
			row = append(row, v.Element(ind))

			if !yield(ind, row) {
				return
			}
		}
	}
}

// Append appends data (as a slice) to the vector.
func (v *Vector) Append(data ...any) error {
	vAdd, ok := toSlc(data, v.VectorType())
	if !ok {
		return fmt.Errorf("cannot convert data to %s in Append", v.VectorType())
	}

	switch v.dt {
	case DTfloat:
		v.data = append(v.data.([]float64), vAdd.([]float64)...)
	case DTint:
		v.data = append(v.data.([]int), vAdd.([]int)...)
	case DTstring:
		v.data = append(v.data.([]string), vAdd.([]string)...)
	case DTdate:
		v.data = append(v.data.([]time.Time), vAdd.([]time.Time)...)
	default:
		return fmt.Errorf("unknown type in Vector.Append")
	}

	return nil
}

// AppendVector appends a vector.
func (v *Vector) AppendVector(vAdd *Vector) error {
	if v.VectorType() != vAdd.VectorType() {
		return fmt.Errorf("appending different vector types")
	}

	switch v.dt {
	case DTfloat:
		v.data = append(v.data.([]float64), vAdd.data.([]float64)...)
	case DTint:
		v.data = append(v.data.([]int), vAdd.data.([]int)...)
	case DTstring:
		v.data = append(v.data.([]string), vAdd.data.([]string)...)
	case DTdate:
		v.data = append(v.data.([]time.Time), vAdd.data.([]time.Time)...)
	default:
		return fmt.Errorf("unknown type in Vector.Append")
	}

	return nil
}

// AsAny returns the data as an any variable.
func (v *Vector) AsAny() any {
	return v.data
}

// AsDate returns the data as a time.Time slice.  It converts to date, if needed & possible.
func (v *Vector) AsDate() ([]time.Time, error) {
	if xOut, ok := toSlc(v.data, DTdate); ok {
		return xOut.([]time.Time), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []Date")
}

// AsFloat returns the data as a []float64 slice.  It converts to float64, if needed & possible.
func (v *Vector) AsFloat() ([]float64, error) {
	if xOut, ok := toSlc(v.data, DTfloat); ok {
		return xOut.([]float64), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []Float")
}

// AsInt returns the data as a []int slice.  It converts to int, if needed & possible.
func (v *Vector) AsInt() ([]int, error) {
	if xOut, ok := toSlc(v.data, DTint); ok {
		return xOut.([]int), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []Int")
}

// AsString returns the data as a string, converting if needed.
func (v *Vector) AsString() ([]string, error) {
	if xOut, ok := toSlc(v.data, DTstring); ok {
		return xOut.([]string), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []String")
}

// Copy copies the *Vector
func (v *Vector) Copy() *Vector {
	vCopy := &Vector{dt: v.dt}
	switch v.dt {
	case DTfloat:
		x := make([]float64, v.Len())
		copy(x, v.data.([]float64))
		vCopy.data = x
	case DTint:
		x := make([]int, v.Len())
		copy(x, v.data.([]int))
		vCopy.data = x
	case DTstring:
		x := make([]string, v.Len())
		copy(x, v.data.([]string))
		vCopy.data = x
	case DTdate:
		x := make([]time.Time, v.Len())
		copy(x, v.data.([]time.Time))
		vCopy.data = x
	default:
		panic(fmt.Errorf("unexpected error in Vector.Copy"))
	}

	return vCopy
}

// Element returns the indx'th element of Vector.  It returns nil if indx is out of bounds
// if v.Len() > 1.  If v.Len() = 1, then returns the 0th element.
// This is needed for the parser when we have an op like "x/2" and we don't want to
// append a vector of 2's.
func (v *Vector) Element(indx int) any {
	// if length of the vector is 1, just return the value.
	if v.Len() == 1 {
		indx = 0
	}

	if indx < 0 || indx >= v.Len() {
		return nil
	}

	switch v.dt {
	case DTfloat:
		return v.data.([]float64)[indx]
	case DTint:
		return v.data.([]int)[indx]
	case DTstring:
		return v.data.([]string)[indx]
	case DTdate:
		return v.data.([]time.Time)[indx]
	default:
		panic(fmt.Errorf("error in Element"))
	}
}

// ElementDate returns the indx'th element as a date, converting the value, if needed & possible.
func (v *Vector) ElementDate(indx int) (*time.Time, error) {
	if val, ok := toDate(v.Element(indx)); ok {
		date := val.(time.Time)
		return &date, nil
	}

	return nil, fmt.Errorf("element is not date-able")
}

// ElementFloat returns the indx'th element as a float64, converting the value, if needed & possible.
func (v *Vector) ElementFloat(indx int) (*float64, error) {
	if val, ok := toFloat(v.Element(indx)); ok {
		x := val.(float64)
		return &x, nil
	}

	return nil, fmt.Errorf("element is not float-able")
}

// ElementInt returns the indx'th element as a int, converting the value, if needed & possible.
func (v *Vector) ElementInt(indx int) (*int, error) {
	if val, ok := toInt(v.Element(indx)); ok {
		x := val.(int)
		return &x, nil
	}

	return nil, fmt.Errorf("element is not int-able")
}

// ElementString returns the indx'th element as a string.
func (v *Vector) ElementString(indx int) (*string, error) {
	if x, ok := toString(v.Element(indx)); ok {
		s := x.(string)
		return &s, nil
	}

	return nil, fmt.Errorf("element is not string-able")
}

// Len is the length of the *Vector
func (v *Vector) Len() int {
	switch v.dt {
	case DTfloat:
		return len(v.data.([]float64))
	case DTint:
		return len(v.data.([]int))
	case DTstring:
		return len(v.data.([]string))
	case DTdate:
		return len(v.data.([]time.Time))
	default:
		return 0
	}
}

// Less returns true if element i < element j
func (v *Vector) Less(i, j int) bool {
	switch v.dt {
	case DTfloat:
		return v.data.([]float64)[i] < v.data.([]float64)[j]
	case DTint:
		return v.data.([]int)[i] < v.data.([]int)[j]
	case DTstring:
		return v.data.([]string)[i] < v.data.([]string)[j]
	case DTdate:
		return v.data.([]time.Time)[i].Sub(v.data.([]time.Time)[j]).Minutes() < 0
	default:
		panic(fmt.Errorf("unexpected error in vector.Less"))
	}
}

// SetAny sets the indx'th element to val.  Does no error checking.
func (v *Vector) SetAny(val any, indx int) {
	switch x := val.(type) {
	case float64:
		v.data.([]float64)[indx] = x
	case int:
		v.data.([]int)[indx] = x
	case string:
		v.data.([]string)[indx] = x
	case time.Time:
		v.data.([]time.Time)[indx] = x
	}
}

// SetDate sets the indx'th element to val.  Does not attempt conversion.
func (v *Vector) SetDate(val time.Time, indx int) error {
	if v.VectorType() != DTdate {
		return fmt.Errorf("vector isn't DTdate")
	}

	if indx < 0 || indx >= v.Len() {
		return fmt.Errorf("index out of range")
	}

	v.data.([]time.Time)[indx] = val

	return nil
}

// SetFloat sets the indx'th element to val.  Does not attempt conversion.
func (v *Vector) SetFloat(val float64, indx int) error {
	if v.VectorType() != DTfloat {
		return fmt.Errorf("vector isn't DTfloat")
	}

	if indx < 0 || indx >= v.Len() {
		return fmt.Errorf("index out of range")
	}

	v.data.([]float64)[indx] = val

	return nil
}

// SetInt sets the indx'th element to val.  Does not attempt conversion.
func (v *Vector) SetInt(val, indx int) error {
	if v.VectorType() != DTint {
		return fmt.Errorf("vector isn't DTint")
	}

	if indx < 0 || indx >= v.Len() {
		return fmt.Errorf("index out of range")
	}

	v.data.([]int)[indx] = val

	return nil
}

// SetString sets the indx'th element to val.  Does not attempt conversion.
func (v *Vector) SetString(val string, indx int) error {
	if v.VectorType() != DTstring {
		return fmt.Errorf("vector isn't DTstring")
	}

	if indx < 0 || indx >= v.Len() {
		return fmt.Errorf("index out of range")
	}

	v.data.([]string)[indx] = val

	return nil
}

func (v *Vector) String() string {
	s := "" //fmt.Sprintf("type: %v\nlength: %d\n\nElements:\n", v.VectorType(), v.Len())
	for ind := range min(5, v.Len()) {
		v, _ := v.ElementString(ind)
		s += fmt.Sprintf("%s\n", *v)
	}

	return s
}

// Swap swaps the ith and jth element of *Vector
func (v *Vector) Swap(i, j int) {
	switch v.dt {
	case DTfloat:
		v.data.([]float64)[i], v.data.([]float64)[j] = v.data.([]float64)[j], v.data.([]float64)[i]
	case DTint:
		v.data.([]int)[i], v.data.([]int)[j] = v.data.([]int)[j], v.data.([]int)[i]
	case DTstring:
		v.data.([]string)[i], v.data.([]string)[j] = v.data.([]string)[j], v.data.([]string)[i]
	case DTdate:
		v.data.([]time.Time)[i], v.data.([]time.Time)[j] = v.data.([]time.Time)[j], v.data.([]time.Time)[i]
	default:
		panic(fmt.Errorf("unexpected error in Vector.Len"))
	}
}

func (v *Vector) VectorType() DataTypes {
	return v.dt
}

// Where creates a new *Vector with elements from the original *Vector in which
// indic is greater than 0. indic must be type DTint.
func (v *Vector) Where(indic *Vector) *Vector {
	if indic.VectorType() != DTint {
		return nil
	}

	inds := indic.AsAny().([]int)
	outVec := MakeVector(v.VectorType(), 0)
	for ind := range v.Len() {
		if inds[ind] > 0 {
			_ = outVec.Append(v.Element(ind))
		}
	}

	return outVec
}
