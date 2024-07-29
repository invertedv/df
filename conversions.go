package df

import (
	"fmt"
	"strconv"
	"time"
)

func toFloat(xIn any, strict bool) (xOut any, err error) {
	if x, ok := xIn.(float64); ok {
		return x, nil
	}

	if strict {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if x, ok := xIn.(int); ok {
		return float64(x), nil
	}

	if x, ok := xIn.(string); ok {
		var tmp float64
		if tmp, err = strconv.ParseFloat(x, 64); err != nil {
			return nil, err
		}
		return tmp, nil
	}

	return nil, fmt.Errorf("cannot convert type to float")
}

func toInt(xIn any, strict bool) (xOut any, err error) {
	if x, ok := xIn.(int); ok {
		return x, nil
	}

	if strict {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if x, ok := xIn.(float64); ok {
		return int(x), nil
	}

	if x, ok := xIn.(string); ok {
		var tmp int64
		if tmp, err = strconv.ParseInt(x, 10, 32); err != nil {
			return nil, err
		}
		return tmp, nil
	}

	return nil, fmt.Errorf("cannot convert type to int")
}

func toDate1(x any, strict bool) (xout any, err error) {

	if x, ok := x.(time.Time); ok {
		return x, nil
	}

	if strict {
		return nil, fmt.Errorf("conversion not allowed")
	}

	// DateFormats are formats to try when guessing the field type in Impute()
	var DateFormats = []string{"2006-01-02", "2006-1-2", "2006/01/02", "2006/1/2", "20060102", "01022006",
		"01/02/2006", "1/2/2006", "01-02-2006", "1-2-2006", "200601", "Jan 2 2006", "January 2 2006",
		"Jan 2, 2006", "January 2, 2006", time.RFC3339}

	xs, ok := x.(string)
	if !ok {
		return nil, fmt.Errorf("input not a string")
	}
	for _, format := range DateFormats {
		xout, err = time.Parse(format, xs)
		if err == nil {
			return xout, nil
		}
	}

	return xout, fmt.Errorf("cannot parse %s as date", xs)
}

func toDataType(x any, dt DataTypes, strict bool) (xout any, err error) {
	switch dt {
	case DTfloat:
		return toFloat(x, strict)
	case DTint:
		return toInt(x, strict)
	case DTdate:
		return toDate1(x, strict)
	}

	return nil, fmt.Errorf("not supported")
}

func SliceToDataType(col Column, dt DataTypes, strict bool) (xout any, err error) {
	xout = makeSlice(dt)

	for ind := 0; ind < col.N(); ind++ {
		x, e := toDataType(col.Element(ind), dt, strict)
		if e != nil {
			return nil, e
		}
		xout = appendSlice(xout, x, dt)
	}

	return xout, nil
}
