package df

import (
	"fmt"
	"strings"
	"time"

	u "github.com/invertedv/utilities"
)

func ToFloat(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(float64); ok {
		return x, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if xOut, err = u.Any2Float64(xIn); err != nil {
		return nil, err
	}

	return *xOut.(*float64), nil
}

func ToInt(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(int); ok {
		return x, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if xOut, err = u.Any2Int(xIn); err != nil {
		return nil, err
	}
	return *xOut.(*int), nil
}

// Any2Date attempts to convert inVal to a date (time.Time). Returns nil if this fails.
func Any2Date(inVal any) (*time.Time, error) {
	switch x := inVal.(type) {
	case string:
		formats := []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006", "Jan 2 2006", "January 2 2006", "2006-01-02"}
		for _, fmtx := range formats {
			dt, e := time.Parse(fmtx, strings.ReplaceAll(x, "'", ""))
			if e == nil {
				return &dt, nil
			}
		}
	case time.Time:
		return &x, nil
	case int, int32, int64:
		return Any2Date(fmt.Sprintf("%d", x))
	}

	return nil, fmt.Errorf("cannot convert %v to date: Any2Date", inVal)
}

func ToDate(xIn any, cast bool) (xOut any, err error) {
	if xx, ok := xIn.(time.Time); ok {
		return xx, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if xOut, err = Any2Date(xIn); err != nil {
		return nil, err
	}
	return *xOut.(*time.Time), nil
}

func ToString(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(string); ok {
		return x, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	return u.Any2String(xIn), nil
}

func ToDataType(x any, dt DataTypes, cast bool) (xout any, err error) {
	switch dt {
	case DTfloat:
		return ToFloat(x, cast)
	case DTint:
		return ToInt(x, cast)
	case DTdate:
		return ToDate(x, cast)
	case DTstring:
		return ToString(x, cast)
	case DTany:
		return x, nil
	}

	return nil, fmt.Errorf("not supported")
}

func BestType(xIn any) (xOut any, dt DataTypes, err error) {
	if x, e := ToDataType(xIn, DTdate, true); e == nil {
		return x, DTdate, nil
	}

	if x, e := ToDataType(xIn, DTint, true); e == nil {
		return x, DTint, nil
	}

	if x, e := ToDataType(xIn, DTfloat, true); e == nil {
		return x, DTfloat, nil
	}

	if x, e := ToDataType(xIn, DTstring, true); e == nil {
		return x, DTstring, nil
	}

	return nil, DTunknown, fmt.Errorf("cannot convert to BestType")
}

func WhatAmI(val any) DataTypes {
	switch val.(type) {
	case float64, []float64:
		return DTfloat
	case int, []int:
		return DTint
	case string, []string:
		return DTstring
	case time.Time, []time.Time:
		return DTdate
	default:
		return DTunknown
	}
}

func ToColumnsXXX(cols ...any) []Column {
	var out []Column
	for ind := 0; ind < len(cols); ind++ {
		c, ok := cols[ind].(Column)
		if !ok {
			panic("input is not interface Column to ToColumns")
		}

		out = append(out, c)
	}

	return out
}

func MakeSlice(dt DataTypes, n int) any {
	var xout any
	switch dt {
	case DTfloat:
		xout = make([]float64, n)
	case DTint:
		xout = make([]int, n)
	case DTdate:
		xout = make([]time.Time, n)
	case DTstring:
		xout = make([]string, n)
	}

	return xout
}

func AppendSlice(x, xadd any, dt DataTypes) any {
	switch dt {
	case DTfloat:
		x = append(x.([]float64), xadd.(float64))
	case DTint:
		x = append(x.([]int), xadd.(int))
	case DTdate:
		x = append(x.([]time.Time), xadd.(time.Time))
	case DTstring:
		x = append(x.([]string), xadd.(string))
	}

	return x
}

func Address(data any, dt DataTypes, indx int) any {
	switch dt {
	case DTfloat:
		return &data.([]float64)[indx]
	case DTint:
		return &data.([]int)[indx]
	case DTstring:
		return &data.([]string)[indx]
	case DTdate:
		return &data.([]time.Time)[indx]
	}

	return nil
}

func GT(x, y any) (bool, error) {
	var (
		yy any
		e  error
	)

	switch dx := x.(type) {
	case float64:
		if yy, e = ToFloat(y, true); e != nil {
			return false, e
		}

		return dx > yy.(float64), nil
	case int:
		if yy, e = ToInt(y, true); e != nil {
			return false, e
		}

		return dx > yy.(int), nil
	case string:
		if yy, e = ToString(y, true); e != nil {
			return false, e
		}

		return dx > yy.(string), nil
	case time.Time:
		if yy, e = ToDate(y, true); e != nil {
			return false, e
		}

		return dx.Sub(yy.(time.Time)).Minutes() > 0, nil
	default:
		return false, fmt.Errorf("unknown type in gt")
	}
}

func Comparator(x, y any, op string) (bool, error) {
	a, ea := GT(x, y)
	if ea != nil {
		return false, ea
	}
	b, eb := GT(y, x)
	if eb != nil {
		return false, eb
	}

	switch op {
	case ">":
		return a, nil
	case "<":
		return b, nil
	case "==":
		return !a && !b, nil
	case ">=":
		return !b, nil
	case "<=":
		return !a, nil
	case "!=":
		return a || b, nil
	default:
		return false, fmt.Errorf("invalid compare operator: %s", op)
	}
}
