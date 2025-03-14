package df

import "fmt"

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
	DTany // keep as last entry
)

//go:generate stringer -type=DataTypes

// MaxDT is max value of DataTypes type
const MaxDT = DTany

func DTFromString(nm string) DataTypes {
	const skeleton = "%v"

	var nms []string
	for ind := range MaxDT {
		nms = append(nms, fmt.Sprintf(skeleton, ind))
	}

	pos := Position(nm, nms)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}
