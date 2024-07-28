package df

import (
	"fmt"
	"strconv"
	"time"
)

func SliceToDouble(xIn any) (xOut []float64, err error) {
	if x, ok := xIn.([]float64); ok {
		return x, nil
	}

	if x, ok := xIn.([]int); ok {
		y := make([]float64, len(x))
		for ind, xval := range x {
			y[ind] = float64(xval)
		}

		return y, nil
	}

	if x, ok := xIn.([]string); ok {
		y := make([]float64, len(x))
		for ind, xval := range x {
			if y[ind], err = strconv.ParseFloat(xval, 64); err != nil {
				return nil, err
			}
		}

		return y, nil
	}

	return nil, fmt.Errorf("cannot convert type to float64")
}

func SliceToInt(xIn any) (xOut []int, err error) {
	if x, ok := xIn.([]int); ok {
		return x, nil
	}

	if x, ok := xIn.([]float64); ok {
		y := make([]int, len(x))
		for ind, xval := range x {
			y[ind] = int(xval)
		}

		return y, nil
	}

	if x, ok := xIn.([]string); ok {
		y := make([]int, len(x))
		for ind, xval := range x {
			var tmp int64
			if tmp, err = strconv.ParseInt(xval, 10, 32); err != nil {
				return nil, err
			}

			y[ind] = int(tmp)
		}

		return y, nil
	}

	return nil, fmt.Errorf("cannot convert type to int")
}

func SliceToDate(xIn any) (xOut []time.Time, err error) {
	if x, ok := xIn.([]time.Time); ok {
		return x, nil
	}

	if x, ok := xIn.([]string); ok {
		y := make([]time.Time, len(x))
		for ind, xVal := range x {
			if y[ind], err = toDate(xVal); err != nil {
				return nil, err
			}
		}

		return y, nil
	}

	if x, ok := xIn.([]int); ok {
		y := make([]time.Time, len(x))
		for ind, xVal := range x {
			xStr := fmt.Sprintf("%d", xVal)
			if y[ind], err = toDate(xStr); err != nil {
				return nil, err
			}
		}

		return y, nil
	}

	return nil, fmt.Errorf("cannot convert to date")
}

func toDate(inDate string) (outDate time.Time, err error) {
	// DateFormats are formats to try when guessing the field type in Impute()
	var DateFormats = []string{"2006-01-02", "2006-1-2", "2006/01/02", "2006/1/2", "20060102", "01022006",
		"01/02/2006", "1/2/2006", "01-02-2006", "1-2-2006", "200601", "Jan 2 2006", "January 2 2006",
		"Jan 2, 2006", "January 2, 2006", time.RFC3339}

	for _, format := range DateFormats {
		outDate, err = time.Parse(format, inDate)
		if err == nil {
			return outDate, nil
		}
	}

	return outDate, fmt.Errorf("cannot parse %s as date", inDate)
}

func SliceToString(xIn any) (xOut []string, err error) {
	if x, ok := xIn.([]string); ok {
		return x, nil
	}

	if x, ok := xIn.([]float64); ok {
		y := make([]string, len(x))
		for ind, xVal := range x {
			y[ind] = fmt.Sprintf("%10.3f", xVal)
		}

		return y, nil
	}

	if x, ok := xIn.([]int); ok {
		y := make([]string, len(x))
		for ind, xVal := range x {
			y[ind] = fmt.Sprintf("%d", xVal)
		}

		return y, nil
	}

	if x, ok := xIn.([]time.Time); ok {
		y := make([]string, len(x))
		for ind, xVal := range x {
			y[ind] = xVal.Format("20060102")
		}

		return y, nil
	}

	return nil, fmt.Errorf("cannot convert to string!?")
}
