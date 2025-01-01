package df

import (
	"fmt"
)

//  *********** atomic DataTypes ***********

// DataTypes are the types of data that the package supports
type DataTypes uint8

// values of DataTypes, DTany must be the last value
const (
	DTunknown DataTypes = 0 + iota
	DTstring
	DTfloat
	DTint
	DTcategorical
	DTdate
	DTnil
	DTdf
	DTany // keep as last entry
)

//go:generate stringer -type=DataTypes

// MaxDT is max value of DataTypes type
const MaxDT = DTany

func DTFromString(nm string) DataTypes {
	const skeleton = "%v"

	var nms []string
	for ind := DataTypes(0); ind <= MaxDT; ind++ {
		nms = append(nms, fmt.Sprintf(skeleton, ind))
	}

	pos := position(nm, nms)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}
