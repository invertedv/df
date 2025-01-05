package df

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var dateFormats = []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006",
	"Jan 2 2006", "January 2 2006", "2006-01-02"}

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

	// TODO: improve # decimals choice
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
		for _, fmtx := range dateFormats {
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
	case DTany:
		return x, true
	}

	return nil, false
}

func bestType(xIn any) (xOut any, dt DataTypes, err error) {
	if x, ok := toDate(xIn); ok {
		return x.(time.Time), DTdate, nil
	}

	if x, ok := toInt(xIn); ok {
		return x.(int), DTint, nil
	}

	if x, ok := toFloat(xIn); ok {
		return x.(float64), DTfloat, nil
	}

	if x, ok := toString(xIn); ok {
		return x.(string), DTstring, nil
	}

	return nil, DTunknown, fmt.Errorf("cannot convert value")
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

func toSlc(xIn any, target DataTypes) (any, bool) {
	typSlc := []reflect.Type{reflect.TypeOf([]float64{}), reflect.TypeOf([]int{}), reflect.TypeOf([]string{""}), reflect.TypeOf([]time.Time{})}
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

// slash adds a trailing slash if inStr doesn't end in a slash
func slash(inStr string) string {
	if inStr[len(inStr)-1] == '/' {
		return inStr
	}

	return inStr + "/"
}

func has[C comparable](needle C, haystack []C) bool {
	return position(needle, haystack) >= 0
}

func position[C comparable](needle C, haystack []C) int {
	for ind, straw := range haystack {
		if needle == straw {
			return ind
		}
	}

	return -1
}

// randomLetters generates a string of length "length" by randomly choosing from a-z
func randomLetters(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	var (
		randN []int64
		e     error
	)
	if randN, e = randUnifInt(len(letters), len(letters)); e != nil {
		panic(e)
	}

	name := ""
	for ind := 0; ind < length; ind++ {
		name += letters[randN[ind] : randN[ind]+1]
	}

	return name
}

// randUnifInt generates a slice whose elements are random U[0,upper) int64's
func randUnifInt(n, upper int) ([]int64, error) {
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

func validName(name string) bool {
	const illegal = "!@#$%^&*()=+-;:'`/.,>< ~ " + `"`

	return !strings.ContainsAny(name, illegal)
}
