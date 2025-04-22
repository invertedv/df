package df

import (
	"fmt"
	"reflect"
	"time"
)

// DataTypes are the types of data that the package supports for Column elements
type DataTypes uint8

// Values of DataTypes
const (
	DTfloat DataTypes = 0 + iota
	DTint
	DTstring
	DTdate
	DTcategorical
	DTunknown // keep as last entry, OK to put new entries before
)

//go:generate stringer -type=DataTypes

// DTFromString returns the DataTypes value as given by nm
// e.g. Input "DTdate", output 3.
// Fail behavior is to return DTunknown
func DTFromString(nm string) DataTypes {
	var nms []string
	for ind := range DTunknown {
		nms = append(nms, fmt.Sprintf("%v", ind))
	}

	pos := Position(nm, nms)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}

// GetKind maps reflect.Type into d.DataType
func GetKind(fn reflect.Type) DataTypes {
	switch fn.Kind() {
	case reflect.Pointer:
		return DTunknown
	case reflect.Float64:
		return DTfloat
	case reflect.Int:
		return DTint
	case reflect.String:
		return DTstring
	case reflect.Struct:
		if fn == reflect.TypeOf(time.Time{}) {
			return DTdate
		}

		return DTunknown
	case reflect.Slice:
		return GetKind(fn.Elem())
	default:
		return DTunknown
	}
}
