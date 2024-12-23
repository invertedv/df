package df

// TODO: panic to error, ok vs error
import (
	"fmt"
	"time"
)

// TODO: add stringer

type Vector struct {
	dt DataTypes

	data any
}

func NewVector(data any, dt DataTypes) *Vector {
	var (
		v  any
		ok bool
	)
	if v, ok = toSlc(data, dt); !ok {
		panic(fmt.Errorf("cannot make vector of type %s", dt))
	}

	return &Vector{dt: dt, data: v}
}

func MakeVector(dt DataTypes, n int) *Vector {
	switch dt {
	case DTfloat:
		return &Vector{dt: dt, data: make([]float64, n)}
	case DTint:
		return &Vector{dt: dt, data: make([]int, n)}
	case DTstring:
		return &Vector{dt: dt, data: make([]string, n)}
	case DTdate:
		return &Vector{dt: dt, data: make([]time.Time, n)}
	default:
		panic(fmt.Errorf("cannot make Vector with data type %s", dt))
	}
}

func (v *Vector) VectorType() DataTypes {
	return v.dt
}

// TODO: return error

func (v *Vector) SetFloat(val float64, indx int) {
	if v.VectorType() != DTfloat {
		panic(fmt.Errorf("vector isn't DTfloat"))
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.data.([]float64)[indx] = val
}

func (v *Vector) SetInt(val, indx int) {
	if v.VectorType() != DTint {
		panic(fmt.Errorf("vector isn't DTint"))
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.data.([]int)[indx] = val
}

func (v *Vector) SetString(val string, indx int) {
	if v.VectorType() != DTstring {
		panic(fmt.Errorf("vector isn't DTstring"))
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.data.([]string)[indx] = val
}

func (v *Vector) SetDate(val time.Time, indx int) {
	if v.VectorType() != DTdate {
		panic(fmt.Errorf("vector isn't DTdate"))
	}

	if indx < 0 || indx >= v.Len() {
		panic(fmt.Errorf("index out of range"))
	}

	v.data.([]time.Time)[indx] = val
}

func (v *Vector) Data() *Vector {
	return v
}

func (v *Vector) AsAny() any {
	return v.data
}

// TODO: return error

func (v *Vector) AsFloat() []float64 {
	if xOut, ok := toSlc(v.data, DTfloat); ok {
		return xOut.([]float64)
	}

	panic(fmt.Errorf("cannot convert to Vector.AsFloat"))
}

func (v *Vector) AsInt() []int {
	if xOut, ok := toSlc(v.data, DTint); ok {
		return xOut.([]int)
	}

	panic(fmt.Errorf("cannot convert to Vector.AsInt"))
}

func (v *Vector) AsString() []string {
	if xOut, ok := toSlc(v.data, DTstring); ok {
		return xOut.([]string)
	}

	panic(fmt.Errorf("cannot convert to Vector.AsString"))
}

func (v *Vector) AsDate() []time.Time {
	if xOut, ok := toSlc(v.data, DTdate); ok {
		return xOut.([]time.Time)
	}

	panic(fmt.Errorf("cannot convert to Vector.AsDate"))
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

func (v *Vector) ElementFloat(indx int) float64 {
	if val, ok := toFloat(v.Element(indx)); ok {
		return val.(float64)
	}

	panic(fmt.Errorf("element is not float-able"))
}

func (v *Vector) ElementInt(indx int) int {
	if val, ok := toInt(v.Element(indx)); ok {
		return val.(int)
	}

	panic(fmt.Errorf("element is not int-able"))
}

func (v *Vector) ElementString(indx int) string {
	if x, ok := toString(v.Element(indx)); ok {
		return x.(string)
	}

	return ""
}

func (v *Vector) ElementDate(indx int) time.Time {
	if val, ok := toDate(v.Element(indx)); ok {
		return val.(time.Time)
	}

	panic(fmt.Errorf("element is not date-able"))
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
		panic(fmt.Errorf("unexpected error in Vector.Len"))
	}
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

func (v *Vector) AppendVector(vAdd *Vector) {
	if v.VectorType() != vAdd.VectorType() {
		panic("appending different vector types")
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
		panic(fmt.Errorf("unknown type in Vector.Append"))
	}
}

func (v *Vector) Append(data ...any) {
	for ind := 0; ind < len(data); ind++ {
		switch v.dt {
		case DTfloat:
			var (
				x  any
				ok bool
			)
			if x, ok = toFloat(data[ind]); !ok {
				panic(fmt.Errorf("cannot make float in Append"))
			}

			v.data = append(v.data.([]float64), x.(float64))
		case DTint:
			var (
				x  any
				ok bool
			)
			if x, ok = toInt(data[ind]); !ok {
				panic(fmt.Errorf("cannot make int in Append"))
			}

			v.data = append(v.data.([]int), x.(int))
		case DTstring:
			var (
				x  any
				ok bool
			)
			if x, ok = toString(data[ind]); !ok {
				panic(fmt.Errorf("cannot make string in Append"))
			}

			v.data = append(v.data.([]string), x.(string))
		case DTdate:
			var (
				xv any
				ok bool
			)
			if xv, ok = toDate(data[ind]); !ok {
				panic(fmt.Errorf("cannot make date in Append"))
			}

			v.data = append(v.data.([]time.Time), xv.(time.Time))
		}
	}
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

func (v *Vector) Where(indic *Vector) *Vector {
	outVec := MakeVector(v.VectorType(), 0)
	for ind := 0; ind < v.Len(); ind++ {
		if indic.ElementInt(ind) > 0 {
			outVec.Append(v.Element(ind))
		}
	}

	return outVec
}

func (v *Vector) Coerce(to DataTypes) *Vector {
	xOut := MakeVector(to, v.Len())
	for ind := 0; ind < v.Len(); ind++ {
		switch to {
		case DTfloat:
			return NewVector(v.AsFloat(), DTfloat)
		case DTint:
			return NewVector(v.AsInt(), DTint)
		case DTstring:
			return NewVector(v.AsString(), DTstring)
		case DTdate:
			return NewVector(v.AsDate(), DTdate)
		}
	}

	return xOut
}
