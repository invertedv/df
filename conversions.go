package df

import (
	"fmt"
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

func ToDate(xIn any, cast bool) (xOut any, err error) {
	if xx, ok := xIn.(time.Time); ok {
		return xx, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if xOut, err = u.Any2Date(xIn); err != nil {
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
