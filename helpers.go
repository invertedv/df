package df

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// DateFormats is list of available formats for dates.
var DateFormats = []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006",
	"Jan 2 2006", "January 2 2006", "2006-01-02", "01/02/06"}

func Has[C comparable](needle C, haystack []C) bool {
	return Position(needle, haystack) >= 0
}

func Position[C comparable](needle C, haystack []C) int {
	for ind, straw := range haystack {
		if needle == straw {
			return ind
		}
	}

	return -1
}

// PrettyPrint returns a string where the elements of cols are aligned under the header.
// cols are expected to be a slice of either float64, int, string or time.Time
func PrettyPrint(header []string, cols ...any) string {
	var colsS [][]string

	for ind := range len(cols) {
		colsS = append(colsS, StringSlice(header[ind], cols[ind]))
	}

	out := ""
	for row := range len(colsS[0]) {
		for c := range len(colsS) {
			out += colsS[c][row]
		}
		out += "\n"
	}

	return out
}

// StringSlice converts inVal to a slice of strings, the first element is the header.
// inVal is expected to be a slice of float64, int, string or time.Time
func StringSlice(header string, inVal any) []string {
	const pad = 3
	c := []string{header}

	format := ""
	n := 0
	var dt DataTypes
	switch x := inVal.(type) {
	case []float64:
		format = selectFormat(x)
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
	for ind := range n {
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

// RandomLetters generates a string of length "length" by randomly choosing from a-z
func RandomLetters(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	randN := randUnifInt(len(letters), len(letters))
	name := ""
	for ind := range length {
		name += letters[randN[ind] : randN[ind]+1]
	}

	return name
}

// ToDataType converts x to the dt data type.
func ToDataType(x any, dt DataTypes) (any, bool) {
	switch dt {
	case DTfloat:
		if v, ok := toFloat(x); ok {
			return v.(float64), true
		}
	case DTint:
		if v, ok := toInt(x); ok {
			return v.(int), true
		}
	case DTdate:
		if v, ok := toDate(x); ok {
			return v.(time.Time), true
		}
	case DTstring:
		if v, ok := toString(x); ok {
			return v.(string), true
		}
	default:
		return x, true
	}

	return nil, false
}

// WhatAmI returns the type of val.
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



// *********** Conversions ***********

func toFloat(x any) (any, bool) {
	if f, ok := x.(float64); ok {
		return f, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanFloat() {
		return xv.Float(), true
	}

	if xv.CanInt() {
		return float64(xv.Int()), true
	}

	if xv.CanUint() {
		return float64(xv.Uint()), true
	}

	if s, ok := x.(string); ok {
		if f, e := strconv.ParseFloat(s, 64); e == nil {
			return f, true
		}
	}

	return nil, false
}

func toInt(x any) (any, bool) {
	if i, ok := x.(int); ok {
		return i, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanInt() {
		return int(xv.Int()), true
	}

	if xv.CanUint() {
		return int(xv.Uint()), true
	}

	if xv.CanFloat() {
		return int(xv.Float()), true
	}

	if s, ok := x.(string); ok {
		if i, e := strconv.ParseInt(s, 10, 64); e == nil {
			return int(i), true
		}
	}

	return nil, false
}

func toString(x any) (any, bool) {
	if s, ok := x.(string); ok {
		return s, true
	}

	if f, ok := x.(float64); ok {
		sd, dec, lf := 6, 5, 1
		if f != 0 {
			lf = max(0, int(math.Log10(math.Abs(f)))+1)
			dec = max(0, sd-lf)
		}

		format := "%" + fmt.Sprintf("%d.%df", lf, dec)
		return fmt.Sprintf(format, f), true
	}

	if i, ok := x.(int); ok {
		return fmt.Sprintf("%d", i), true
	}

	if s, ok := x.(time.Time); ok {
		return s.Format("2006-01-02"), true
	}

	return nil, false
}

func toDate(x any) (any, bool) {
	if d, ok := x.(time.Time); ok {
		return d, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanInt() {
		return toDate(fmt.Sprintf("%d", xv.Int()))
	}

	if xv.CanUint() {
		return toDate(fmt.Sprintf("%d", xv.Uint()))
	}

	if d, ok := x.(string); ok {
		for _, fmtx := range DateFormats {
			if dt, e := time.Parse(fmtx, strings.ReplaceAll(d, "'", "")); e == nil {
				return dt, true
			}
		}
	}

	return nil, false
}

func toDataType(x any, dt DataTypes) (any, bool) {
	switch dt {
	case DTfloat:
		if v, ok := toFloat(x); ok {
			return v.(float64), true
		}
	case DTint:
		if v, ok := toInt(x); ok {
			return v.(int), true
		}
	case DTdate:
		if v, ok := toDate(x); ok {
			return v.(time.Time), true
		}
	case DTstring:
		if v, ok := toString(x); ok {
			return v.(string), true
		}
	default:
		return x, true
	}

	return nil, false
}

func bestType(xIn any, dtFirst bool) (xOut any, dt DataTypes, err error) {
	if dtFirst {
		if x, ok := toDate(xIn); ok {
			return x.(time.Time), DTdate, nil
		}
	}

	if x, ok := toInt(xIn); ok {
		return x.(int), DTint, nil
	}

	if x, ok := toFloat(xIn); ok {
		return x.(float64), DTfloat, nil
	}

	if !dtFirst {
		if x, ok := toDate(xIn); ok {
			return x.(time.Time), DTdate, nil
		}
	}

	if x, ok := toString(xIn); ok {
		return x.(string), DTstring, nil
	}

	return nil, DTunknown, fmt.Errorf("cannot convert value")
}

func toSlc(xIn any, target DataTypes) (any, bool) {
	typSlc := []reflect.Type{reflect.TypeOf([]float64{}), reflect.TypeOf([]int{}), reflect.TypeOf([]string{}), reflect.TypeOf([]time.Time{})}
	toFns := []func(a any) (any, bool){toFloat, toInt, toString, toDate}

	x := reflect.ValueOf(xIn)

	var indx int
	switch target {
	case DTfloat:
		indx = 0
	case DTint:
		indx = 1
	case DTstring:
		indx = 2
	case DTdate:
		indx = 3
	default:
		return nil, false
	}

	outType := typSlc[indx]

	// nothing to do
	if x.Type() == outType {
		return xIn, true
	}

	toFn := toFns[indx]
	var xOut reflect.Value
	if x.Kind() == reflect.Slice {
		for ind := range x.Len() {
			r := x.Index(ind).Interface()
			if ind == 0 {
				xOut = reflect.MakeSlice(outType, x.Len(), x.Len())
			}
			var (
				val any
				ok  bool
			)

			if val, ok = toFn(r); !ok {
				return nil, false
			}

			xOut.Index(ind).Set(reflect.ValueOf(val))
		}

		return xOut.Interface(), true
	}

	// input is not a slice:
	if val, ok := toFn(xIn); ok {
		xOut = reflect.MakeSlice(outType, 1, 1)
		xOut.Index(0).Set(reflect.ValueOf(val))
		return xOut.Interface(), true
	}

	return nil, false
}

// *********** Other ***********

// randUnifInt generates a slice whose elements are random U[0,upper) int64's
func randUnifInt(n, upper int) []int64 {
	upFlt := float64(upper)
	outInts := make([]int64, n)

	for ind := range n {
		outInts[ind] = int64(rand.Float64() * upFlt)
	}

	return outInts
}

func validName(name string) error {
	const illegal = "!@#$%^&*()=+-;:'`/.,>< ~ " + `"`

	if strings.ContainsAny(name, illegal) {
		return fmt.Errorf("illegal column name")
	}

	return nil
}

func selectFormat(x []float64) string {
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
