package df

import "time"

type Atomic struct {
	f *float64
	i *int
	s *string
	d *time.Time
}

func NewAtomic(x any, dt DataTypes) *Atomic {
	switch dt {
	case DTfloat:
		if f := Any2Float64(x, true); f != nil {
			return &Atomic{f: f}
		}
	case DTint:
		if i := Any2Int(x, true); i != nil {
			return &Atomic{i: i}
		}
	case DTstring:
		if s := Any2String(x, true); s != nil {
			return &Atomic{s: s}
		}
	case DTdate:
		if d := Any2Date(x, true); d != nil {
			return &Atomic{d: d}
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
