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
func Any2Float64(inVal any, cast bool) *float64 {
	if v, ok := inVal.(float64); ok {
		return &v
	}

	if !cast {
		return nil
	}

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
	case string:
		var (
			xx float64
			e  error
		)
		if xx, e = strconv.ParseFloat(x, 64); e != nil {
			return nil
		}

		outVal = xx
	default:
		return nil
	}

	return &outVal
}

// Any2Int attempts to convert inVal to int.  Returns nil if this fails.
func Any2Int(inVal any, cast bool) *int {
	if v, ok := inVal.(int); ok {
		return &v
	}

	if !cast {
		return nil
	}

	var outVal int
	switch x := inVal.(type) {
	case int:
		outVal = x
	case int8:
		outVal = int(x)
	case int16:
		outVal = int(x)
	case int32:
		outVal = int(x)
	case int64:
		if x > math.MaxInt || x < math.MinInt {
			return nil
		}

		outVal = int(x)
	case float32:
		if x > math.MaxInt || x < math.MinInt {
			return nil
		}

		outVal = int(x)
	case float64:
		if x > math.MaxInt || x < math.MinInt {
			return nil
		}

		outVal = int(x)
	case string:
		var (
			xx int64
			e  error
		)
		if xx, e = strconv.ParseInt(x, 10, 32); e != nil {
			return nil
		}

		outVal = int(xx)
	default:
		return nil
	}

	return &outVal
}

func Any2String(inVal any, cast bool) *string {
	if v, ok := inVal.(string); ok {
		return &v
	}

	if !cast {
		return nil
	}

	var outVal string
	switch x := inVal.(type) {
	case time.Time:
		outVal = x.Format("2006-01-02")
		// TODO: use %f
	case float32, float64:
		outVal = fmt.Sprintf("%v", x)
	default:
		outVal = fmt.Sprintf("%v", x)
	}

	return &outVal
}

// Any2Date attempts to convert inVal to a date (time.Time). Returns nil if this fails.
func Any2Date(inVal any, cast bool) *time.Time {
	if v, ok := inVal.(time.Time); ok {
		return &v
	}

	if !cast {
		return nil
	}

	switch x := inVal.(type) {
	case string:
		formats := []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006", "Jan 2 2006", "January 2 2006", "2006-01-02"}
		for _, fmtx := range formats {
			dt, e := time.Parse(fmtx, strings.ReplaceAll(x, "'", ""))
			if e == nil {
				return &dt
			}
		}
	case int, int32, int64:
		return Any2Date(fmt.Sprintf("%d", x), true)
	}

	return nil
}

func ToDataType(x any, dt DataTypes, cast bool) any {
	var xx any
	switch dt {
	case DTfloat:
		if v := Any2Float64(x, cast); v != nil {
			xx = *v
		}
	case DTint:
		if v := Any2Int(x, cast); v != nil {
			xx = *v
		}
	case DTdate:
		if v := Any2Date(x, cast); v != nil {
			xx = *v
		}
	case DTstring:
		xx = *Any2String(x, cast)
	case DTany:
		xx = x
	}

	return xx
}

func BestType(xIn any) (xOut any, dt DataTypes, err error) {
	// HERE added 11/2 WHY wasn't this here?
	if x := ToDataType(xIn, DTdate, true); x != nil {
		return x, DTdate, nil
	}

	if x := ToDataType(xIn, DTint, true); x != nil {
		return x, DTint, nil
	}

	if x := ToDataType(xIn, DTfloat, true); x != nil {
		return x, DTfloat, nil
	}

	return ToDataType(xIn, DTstring, true), DTstring, nil
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

func ToStringSlc(xIn any, n int) []string {
	var xOut []string
	switch x := xIn.(type) {
	case []string:
		xOut = x
	case string:
		xOut = []string{x}
	default:
		panic(fmt.Errorf("input is not []string or string"))
	}

	if n > 0 && len(xOut) == 1 {
		z := xOut[0]
		xOut = make([]string, n)
		for ind := 0; ind < n; ind++ {
			xOut[ind] = z
		}
	}

	return xOut
}

func ToDateSlc(xIn any, n int) []time.Time {
	var xOut []time.Time
	switch x := xIn.(type) {
	case []time.Time:
		xOut = x
	case time.Time:
		xOut = []time.Time{x}
	default:
		panic(fmt.Errorf("input is not []time.Time or time.Time"))
	}

	if n > 0 && len(xOut) == 1 {
		z := xOut[0]
		xOut = make([]time.Time, n)
		for ind := 0; ind < n; ind++ {
			xOut[ind] = z
		}
	}

	return xOut
}

func ToFloatSlc(xIn any, n int) []float64 {
	var xOut []float64
	switch x := xIn.(type) {
	case []float64:
		xOut = x
	case float64:
		xOut = []float64{x}
	case []float32:
		xOut = make([]float64, len(x))
		for ind, xf32 := range x {
			xOut[ind] = float64(xf32)
		}
	case float32:
		xOut = []float64{float64(x)}
	default:
		panic(fmt.Errorf("input is not []float32, []float64, float32, float64"))
	}

	if n > 0 && len(xOut) == 1 {
		z := xOut[0]
		xOut = make([]float64, n)
		for ind := 0; ind < n; ind++ {
			xOut[ind] = z
		}
	}

	return xOut
}

func ToIntSlc(xIn any, n int) []int {
	var xOut []int
	switch x := xIn.(type) {
	case []int:
		xOut = x
	case int:
		xOut = []int{x}
	case int8:
		xOut = []int{int(x)}
	case int16:
		xOut = []int{int(x)}
	case int32:
		xOut = []int{int(x)}
	case int64:
		xOut = []int{int(x)}
	case []int8:
		xOut = make([]int, len(x))
		for ind, xf32 := range x {
			xOut[ind] = int(xf32)
		}
	case []int16:
		xOut = make([]int, len(x))
		for ind, xf32 := range x {
			xOut[ind] = int(xf32)
		}
	case []int32:
		xOut = make([]int, len(x))
		for ind, xf32 := range x {
			xOut[ind] = int(xf32)
		}
	case []int64:
		xOut = make([]int, len(x))
		for ind, xf32 := range x {
			xOut[ind] = int(xf32)
		}
	default:
		panic(fmt.Errorf("input is not an integer type"))
	}

	if n > 0 && len(xOut) == 1 {
		z := xOut[0]
		xOut = make([]int, n)
		for ind := 0; ind < n; ind++ {
			xOut[ind] = z
		}
	}

	return xOut
}

// *********** Other ***********

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

// Slash adds a trailing slash if inStr doesn't end in a slash
func Slash(inStr string) string {
	if inStr[len(inStr)-1] == '/' {
		return inStr
	}

	return inStr + "/"
}

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

func ValidName(name string) error {
	const illegal = "!@#$%^&*()=+-;:'`/.,>< ~ " + `"`

	if strings.ContainsAny(name, illegal) {
		return fmt.Errorf("invalid name: %s", name)
	}

	return nil
}
