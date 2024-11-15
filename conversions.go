package df

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Any2Float64 attempts to convert inVal to float64.  Returns nil if this fails.
func Any2Float64(inVal any) (*float64, error) {
	var outVal float64

	switch x := inVal.(type) {
	case int:
		outVal = float64(x)
	case int32:
		outVal = float64(x)
	case int64:
		outVal = float64(x)
	case float32:
		outVal = float64(x)
	case float64:
		outVal = x
	case string:
		xx, e := strconv.ParseFloat(x, 64)
		if e != nil {
			return nil, e
		}
		outVal = xx
	default:
		return nil, fmt.Errorf("cannot convert %v to float64: Any2Float64", inVal)
	}

	return &outVal, nil
}

func ToFloat(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(float64); ok {
		return x, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if xOut, err = Any2Float64(xIn); err != nil {
		return nil, err
	}

	return *xOut.(*float64), nil
}

// Any2Int attempts to convert inVal to int.  Returns nil if this fails.
func Any2Int(inVal any) (*int, error) {
	var outVal int
	switch x := inVal.(type) {
	case int:
		outVal = x
	case int32:
		outVal = int(x)
	case int64:
		if x > math.MaxInt || x < math.MinInt {
			return nil, fmt.Errorf("int64 out of range: Any2Int")
		}

		outVal = int(x)
	case float32:
		if x > math.MaxInt || x < math.MinInt {
			return nil, fmt.Errorf("float32 out of range: Any2Int")
		}

		outVal = int(x)
	case float64:
		if x > math.MaxInt || x < math.MinInt {
			return nil, fmt.Errorf("float64 out of range: Any2Int")
		}

		outVal = int(x)
	case string:
		xx, e := strconv.ParseInt(x, 10, 32)
		if e != nil {
			return nil, fmt.Errorf("cannot convert %v to int: Any2Int", inVal)
		}
		outVal = int(xx)
	default:
		return nil, fmt.Errorf("cannot convert %v to int: Any2Int", inVal)
	}

	return &outVal, nil
}

func ToInt(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(int); ok {
		return x, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	if xOut, err = Any2Int(xIn); err != nil {
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

func Any2String(inVal any) string {
	switch x := inVal.(type) {
	case string:
		return strings.Trim(x, "'") // for sql
	case time.Time:
		return x.Format("2006-01-02")
	case float32, float64:
		return fmt.Sprintf("%v", x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func ToString(xIn any, cast bool) (xOut any, err error) {
	if x, ok := xIn.(string); ok {
		return x, nil
	}

	if !cast {
		return nil, fmt.Errorf("conversion not allowed")
	}

	return Any2String(xIn), nil
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
	default:
		return nil, fmt.Errorf("type not supported in ToDataType")
	}
}

func BestType(xIn any) (xOut any, dt DataTypes, err error) {
	// HERE added 11/2 WHY wasn't this here?
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

func MakeSlice(dt DataTypes, n int, initVal any) any {
	switch dt {
	case DTfloat:
		xout := make([]float64, n)
		if initVal == nil {
			return xout
		}

		for ind := 0; ind < n; ind++ {
			xout[ind] = initVal.(float64)
		}

		return xout
	case DTint, DTcategorical:
		xout := make([]int, n)
		if initVal == nil {
			return xout
		}

		for ind := 0; ind < n; ind++ {
			xout[ind] = initVal.(int)
		}

		return xout
	case DTdate:
		xout := make([]time.Time, n)
		if initVal == nil {
			return xout
		}

		for ind := 0; ind < n; ind++ {
			xout[ind] = initVal.(time.Time)
		}

		return xout
	case DTstring:
		xout := make([]string, n)
		if initVal == nil {
			return xout
		}

		for ind := 0; ind < n; ind++ {
			xout[ind] = initVal.(string)
		}

		return xout
	default:
		return nil
	}
}

func AppendSlice(x, xadd any, dt DataTypes) any {
	switch dt {
	case DTfloat:
		x = append(x.([]float64), xadd.(float64))
	case DTint, DTcategorical:
		x = append(x.([]int), xadd.(int))
	case DTdate:
		x = append(x.([]time.Time), xadd.(time.Time))
	case DTstring:
		x = append(x.([]string), xadd.(string))
	default:
		return nil
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
	default:
		return nil
	}
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

func In(check any, pop []any) bool {
	switch WhatAmI(check) {
	case DTfloat, DTint, DTstring:
		for _, val := range pop {
			if check == val {
				return true
			}
		}
	case DTdate:
		// avoids complications due to time zones
		c := check.(time.Time)
		for _, val := range pop {
			v := val.(time.Time)
			if c.Year() == v.Year() &&
				c.Month() == v.Month() &&
				c.Day() == v.Day() {
				return true
			}
		}
	default:
		return false
	}

	return false
}

func SelectFormat(x []float64) string {
	minX := math.Abs(x[0])
	maxX := math.Abs(x[0])
	for _, xv := range x {
		xva := math.Abs(xv)
		if xva < minX {
			minX = xva
		}

		if xva > maxX {
			maxX = xva
		}
	}

	rangeX := maxX - minX
	l := math.Log10(rangeX)
	var dp int
	switch {
	case l < -1:
		dp = int(math.Abs(l)+0.5) + 1
	case l > 1:
		dp = 0
	default:
		dp = 1
	}

	format := "%." + fmt.Sprintf("%d", dp) + "f"
	return format
}

func StringSlice(header string, inVal any) []string {
	const pad = 3
	c := []string{header}

	format := ""
	n := 0
	var dt DataTypes
	switch x := inVal.(type) {
	case []float64:
		format = SelectFormat(x)
		n = len(x)
		dt = DTfloat
	case []int:
		format = "%d"
		n = len(x)
		dt = DTint
	case []string:
		format = "%s"
		n = len(x)
		dt = DTstring
	case []time.Time:
		n = len(x)
		dt = DTdate
	default:
		panic(fmt.Errorf("unsupported data type"))
	}

	maxLen := len(header)
	for ind := 0; ind < n; ind++ {
		var el string
		switch x := inVal.(type) {
		case []float64:
			el = fmt.Sprintf(format, x[ind])
		case []int:
			el = fmt.Sprintf(format, x[ind])
		case []string:
			el = x[ind]
		case []time.Time:
			el = x[ind].Format("20060102")
		}

		if l := len(el); l > maxLen {
			maxLen = l
		}

		c = append(c, el)
	}

	for ind, cx := range c {
		padded := cx + strings.Repeat(" ", maxLen-len(cx)+pad)
		if dt == DTint || dt == DTfloat {
			padded = strings.Repeat(" ", maxLen-len(cx)+pad) + cx
		}
		c[ind] = padded
	}

	return c
}

func PrettyPrint(header []string, cols ...any) string {
	var colsS [][]string

	for ind := 0; ind < len(cols); ind++ {
		colsS = append(colsS, StringSlice(header[ind], cols[ind]))
	}

	out := ""
	for row := 0; row < len(colsS[0]); row++ {
		for c := 0; c < len(colsS); c++ {
			out += colsS[c][row]
		}
		out += "\n"
	}

	return out
}

// *********** Other ***********

// Has returns true if needle is in haystack
func Has(needle, delim string, haystack ...string) bool {
	return Position(needle, delim, haystack...) >= 0
}

// Slash adds a trailing slash if inStr doesn't end in a slash
func Slash(inStr string) string {
	if inStr[len(inStr)-1] == '/' {
		return inStr
	}

	return inStr + "/"
}

func Position(needle, delim string, haystack ...string) int {
	var haySlice []string
	haySlice = haystack

	if len(haystack) == 1 && delim != "" && strings.Contains(haystack[0], delim) {
		haySlice = strings.Split(haystack[0], delim)
	}

	for ind, straw := range haySlice {
		if straw == needle {
			return ind
		}
	}

	return -1
}

// RandomLetters generates a string of length "length" by randomly choosing from a-z
func RandomLetters(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	randN, err := RandUnifInt(len(letters), len(letters))
	if err != nil {
		panic(err)
	}

	name := ""
	for ind := 0; ind < length; ind++ {
		name += letters[randN[ind] : randN[ind]+1]
	}

	return name
}

// RandUnifInt generates a slice whose elements are random U[0,upper) int64's
func RandUnifInt(n, upper int) ([]int64, error) {
	const bytesPerInt = 8

	// generate random bytes
	b1 := make([]byte, bytesPerInt*n)
	if _, e := rand.Read(b1); e != nil {
		return nil, e
	}

	outInts := make([]int64, n)
	rdr := bytes.NewReader(b1)

	for ind := 0; ind < n; ind++ {
		r, e := rand.Int(rdr, big.NewInt(int64(upper)))
		if e != nil {
			return nil, e
		}
		outInts[ind] = r.Int64()
	}

	return outInts, nil
}
