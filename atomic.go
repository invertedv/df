package df

import "fmt"

// DataTypes are the types of data that the package supports
type DataTypes uint8

// values of DataTypes, DTany must be the last value
const (
	DTfloat DataTypes = 0 + iota
	DTint
	DTstring
	DTdate
	DTcategorical
	DTunknown // keep as last entry, OK to put new entries before
)

//go:generate stringer -type=DataTypes

func DTFromString(nm string) DataTypes {
	const skeleton = "%v"

	var nms []string
	for ind := range DTunknown {
		nms = append(nms, fmt.Sprintf(skeleton, ind))
	}

	pos := Position(nm, nms)
	if pos < 0 {
		return DTunknown
	}

	return DataTypes(uint8(pos))
}
