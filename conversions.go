package df

import (
	"fmt"
	u "github.com/invertedv/utilities"
	"time"
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

	return xOut, nil
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
	return xOut, nil
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
	return xOut, nil
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

func MakeSlice(dt DataTypes) any {
	var xout any
	switch dt {
	case DTfloat:
		xout = make([]float64, 0)
	case DTint:
		xout = make([]int, 0)
	case DTdate:
		xout = make([]time.Time, 0)
	case DTstring:
		xout = make([]string, 0)
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
