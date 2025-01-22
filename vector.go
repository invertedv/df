package df

import (
	"fmt"
	"time"
)

type Vector struct {
	dt DataTypes

	data any
}

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

func (v *Vector) AsAny() any {
	return v.data
}

func (v *Vector) AsDate() ([]time.Time, error) {
	if xOut, ok := toSlc(v.data, DTdate); ok {
		return xOut.([]time.Time), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []Date")
}

func (v *Vector) AsFloat() ([]float64, error) {
	if xOut, ok := toSlc(v.data, DTfloat); ok {
		return xOut.([]float64), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []Float")
}

func (v *Vector) AsInt() ([]int, error) {
	if xOut, ok := toSlc(v.data, DTint); ok {
		return xOut.([]int), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []Int")
}

func (v *Vector) AsString() ([]string, error) {
	if xOut, ok := toSlc(v.data, DTstring); ok {
		return xOut.([]string), nil
	}

	return nil, fmt.Errorf("cannot convert to Vector to []String")
}

func (v *Vector) Coerce(to DataTypes) (*Vector, error) {
	for ind := 0; ind < v.Len(); ind++ {
		switch to {
		case DTfloat:
			var (
				x []float64
				e error
			)
			if x, e = v.AsFloat(); e != nil {
				return nil, e
			}

			return NewVector(x, DTfloat)
		case DTint:
			var (
				x []int
				e error
			)
			if x, e = v.AsInt(); e != nil {
				return nil, e
			}

			return NewVector(x, DTint)
		case DTstring:
			var (
				x []string
				e error
			)
			if x, e = v.AsString(); e != nil {
				return nil, e
			}

			return NewVector(x, DTstring)
		case DTdate:
			var (
				x []time.Time
				e error
			)
			if x, e = v.AsDate(); e != nil {
				return nil, e
			}

			return NewVector(x, DTdate)
		}
	}

	return nil, fmt.Errorf("cannot Coerce")
}

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

func (v *Vector) Data() *Vector {
	return v
}

func (v *Vector) Element(indx int) any {
	// handles ops like x/2 where x is a vector
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

func (v *Vector) ElementDate(indx int) (*time.Time, error) {
	if val, ok := toDate(v.Element(indx)); ok {
		date := val.(time.Time)
		return &date, nil
	}

	return nil, fmt.Errorf("element is not date-able")
}

func (v *Vector) ElementFloat(indx int) (*float64, error) {
	if val, ok := toFloat(v.Element(indx)); ok {
		x := val.(float64)
		return &x, nil
	}

	return nil, fmt.Errorf("element is not float-able")
}

func (v *Vector) ElementInt(indx int) (*int, error) {
	if val, ok := toInt(v.Element(indx)); ok {
		x := val.(int)
		return &x, nil
	}

	return nil, fmt.Errorf("element is not int-able")
}

func (v *Vector) ElementString(indx int) (*string, error) {
	if x, ok := toString(v.Element(indx)); ok {
		s := x.(string)
		return &s, nil
	}

	return nil, fmt.Errorf("element is not string-able")
}

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

func (v *Vector) SetAny(val any, ind int) {
	switch x := val.(type) {
	case float64:
		v.data.([]float64)[ind] = x
	case int:
		v.data.([]int)[ind] = x
	case string:
		v.data.([]string)[ind] = x
	case time.Time:
		v.data.([]time.Time)[ind] = x
	}
}

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

func (v *Vector) StringX() string {
	s := fmt.Sprintf("type: %v\nlength: %d\n\nElements:\n", v.VectorType(), v.Len())
	for ind := 0; ind < min(5, v.Len()); ind++ {
		v, _ := v.ElementString(ind)
		s += fmt.Sprintf("%s\n", *v)
	}

	if v.Len() > 5 {
		s += ".\n.\n.\n"
	}

	return s
}

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

func (v *Vector) Where(indic *Vector) *Vector {
	outVec := MakeVector(v.VectorType(), 0)
	for ind := 0; ind < v.Len(); ind++ {
		i, _ := indic.ElementInt(ind)
		if *i > 0 {
			_ = outVec.Append(v.Element(ind))
		}
	}

	return outVec
}
