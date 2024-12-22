package df

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func ToFloat(x any) (any, bool) {
	if f, ok := x.(float64); ok {
		return f, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanFloat() {
		return xv.Float(), true
	}

	// TODO: check can this ever be true?
	if xv.CanInt() {
		return float64(xv.Int()), true
	}

	if s, ok := x.(string); ok {
		if f, e := strconv.ParseFloat(s, 64); e == nil {
			return f, true
		}
	}

	return nil, false
}

func ToInt(x any) (any, bool) {
	if i, ok := x.(int); ok {
		return i, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanInt() {
		return int(xv.Int()), true
	}

	// TODO: check can this ever be true?
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

func ToString(x any) (any, bool) {
	if s, ok := x.(string); ok {
		return s, true
	}

	if f, ok := x.(float64); ok {
		return fmt.Sprintf("%0.3f", f), true
	}

	if i, ok := x.(int); ok {
		return fmt.Sprintf("%d", i), true
	}

	if s, ok := x.(time.Time); ok {
		return s.Format("2006-01-02"), true
	}

	return nil, false
}

func ToDate(x any) (any, bool) {
	if d, ok := x.(time.Time); ok {
		return d, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanInt() {
		return ToDate(fmt.Sprintf("%d", xv.Int()))
	}

	if d, ok := x.(string); ok {
		formats := []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006", "Jan 2 2006", "January 2 2006", "2006-01-02"}
		for _, fmtx := range formats {
			if dt, e := time.Parse(fmtx, strings.ReplaceAll(d, "'", "")); e == nil {
				return dt, true
			}
		}
	}

	return nil, false
}

func ToDataType(x any, dt DataTypes) any {
	var xx any
	switch dt {
	case DTfloat:
		if v, ok := ToFloat(x); ok {
			xx = v.(float64)
		}
	case DTint:
		if v, ok := ToInt(x); ok {
			xx = v.(int)
		}
	case DTdate:
		if v, ok := ToDate(x); ok {
			xx = v.(time.Time)
		}
	case DTstring:
		if v, ok := ToString(x); ok {
			xx = v.(string)
		}
	case DTany:
		xx = x
	}

	return xx
}

func BestType(xIn any) (xOut any, dt DataTypes, err error) {
	// HERE added 11/2 WHY wasn't this here?
	if x := ToDataType(xIn, DTdate); x != nil {
		return x, DTdate, nil
	}

	if x := ToDataType(xIn, DTint); x != nil {
		return x, DTint, nil
	}

	if x := ToDataType(xIn, DTfloat); x != nil {
		return x, DTfloat, nil
	}

	return ToDataType(xIn, DTstring), DTstring, nil
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

func ToSlc(xIn any, target DataTypes) (any, bool) {
	typSlc := []reflect.Type{reflect.TypeOf([]float64{}), reflect.TypeOf([]int{}), reflect.TypeOf([]string{""}), reflect.TypeOf([]time.Time{})}
	toFns := []func(a any) (any, bool){ToFloat, ToInt, ToString, ToDate}

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
		for ind := 0; ind < x.Len(); ind++ {
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
