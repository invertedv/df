package df

import (
	"fmt"
	"strconv"
	"time"
)

func toFloat(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(float64); ok {
		return x, nil
	}

	if !cast {
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

func toInt(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(int); ok {
		return x, nil
	}

	if !cast {
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

func toDate(xIn any, cast bool) (xOut any, err error) {
	if xx, ok := xIn.(time.Time); ok {
		return xx, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	// DateFormats are formats to try when guessing the field type in Impute()
	var DateFormats = []string{"2006-01-02", "2006-1-2", "2006/01/02", "2006/1/2", "20060102", "01022006",
		"01/02/2006", "1/2/2006", "01-02-2006", "1-2-2006", "200601", "Jan 2 2006", "January 2 2006",
		"Jan 2, 2006", "January 2, 2006", time.RFC3339}

	xs, ok := xIn.(string)
	if !ok {
		return nil, fmt.Errorf("input not a string")
	}
	for _, format := range DateFormats {
		xOut, err = time.Parse(format, xs)
		if err == nil {
			return xOut, nil
		}
	}

	return xOut, fmt.Errorf("cannot parse %s as date", xs)
}

func toString(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(string); ok {
		return x, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	return fmt.Sprintf("%v", xIn), nil
}

func toDataType(x any, dt DataTypes, cast bool) (xout any, err error) {
	switch dt {
	case DTfloat:
		return toFloat(x, cast)
	case DTint:
		return toInt(x, cast)
	case DTdate:
		return toDate(x, cast)
	case DTstring:
		return toString(x, cast)
	}

	return nil, fmt.Errorf("not supported")
}

func SliceToDataType(col *MemCol, dt DataTypes, cast bool) (xout any, err error) {
	xout = makeSlice(dt)

	for ind := 0; ind < col.Len(); ind++ {
		x, e := toDataType(col.Element(ind), dt, cast)
		if e != nil {
			return nil, e
		}
		xout = appendSlice(xout, x, dt)
	}

	return xout, nil
}

func makeSlice(dt DataTypes) any {
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

func appendSlice(x, xadd any, dt DataTypes) any {
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

func ToColumns(cols ...any) []Column {
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
