package df

import (
	"fmt"
	"time"
)

type Atomic struct {
	dt DataTypes

	f *float64
	i *int
	s *string
	d *time.Time
}

func NewAtomic(x any, dt DataTypes) *Atomic {
	switch dt {
	case DTfloat:
		if f := Any2Float64(x, true); f != nil {
			return &Atomic{dt: dt, f: f}
		}
	case DTint:
		if i := Any2Int(x, true); i != nil {
			return &Atomic{dt: dt, i: i}
		}
	case DTstring:
		if s := Any2String(x, true); s != nil {
			return &Atomic{dt: dt, s: s}
		}
	case DTdate:
		if d := Any2Date(x, true); d != nil {
			return &Atomic{dt: dt, d: d}
		}
	default:
		return nil
	}

	return nil
}

func (a *Atomic) AsFloat() *float64 {
	if a.f != nil {
		return a.f
	}

	if a.i != nil {
		return Any2Float64(*a.i, true)
	}
	if a.s != nil {
		return Any2Float64(*a.s, true)
	}
	if a.d != nil {
		return Any2Float64(*a.d, true)
	}

	return nil
}

func (a *Atomic) AsInt() *int {
	if a.i != nil {
		return a.i
	}

	if a.f != nil {
		return Any2Int(*a.f, true)
	}
	if a.s != nil {
		return Any2Int(*a.s, true)
	}
	if a.d != nil {
		return Any2Int(*a.d, true)
	}

	return nil
}

func (a *Atomic) AsString() *string {
	if a.s != nil {
		return a.s
	}

	if a.f != nil {
		return Any2String(*a.f, true)
	}
	if a.i != nil {
		return Any2String(*a.i, true)
	}
	if a.d != nil {
		return Any2String(*a.d, true)
	}

	return nil
}

func (a *Atomic) AsDate() *time.Time {
	if a.d != nil {
		return a.d
	}

	if a.i != nil {
		return Any2Date(*a.i, true)
	}
	if a.s != nil {
		return Any2Date(*a.s, true)
	}

	return nil
}

func (a *Atomic) AsAny() any {
	if a.f != nil {
		return *a.f
	}
	if a.i != nil {
		return *a.i
	}
	if a.s != nil {
		return *a.s
	}
	if a.d != nil {
		return *a.d
	}

	return nil
}

func (a *Atomic) AtomType() DataTypes {
	return a.dt
}

func (a *Atomic) Copy() *Atomic {
	return NewAtomic(a.AsAny(), a.AtomType())
}

//  *********** DataTypes ***********

// DataTypes are the types of data that the package supports
type DataTypes uint8

// values of DataTypes
const (
	DTunknown DataTypes = 0 + iota
	DTstring
	DTfloat
	DTint
	DTcategorical
	DTdate
	DTnil
	DTdf
	DTconstant
	DTany // keep as last entry
)

//go:generate stringer -type=DataTypes

// MaxDT is max value of DataTypes type
const MaxDT = DTany

func DTFromString(nm string) DataTypes {
	const skeleton = "%v"

	var nms []string
	for ind := DataTypes(0); ind <= MaxDT; ind++ {
		nms = append(nms, fmt.Sprintf(skeleton, ind))
	}

	pos := Position(nm, "", nms...)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}

func (d DataTypes) IsNumeric() bool {
	return d == DTfloat || d == DTint || d == DTcategorical
}
