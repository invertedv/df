package df

import "math"

func addFloat(inputs ...any) (any, error) {
	return inputs[0].(float64) + inputs[1].(float64), nil
}

func addInt(inputs ...any) (any, error) {
	return inputs[0].(int) + inputs[1].(int), nil
}

func exp(xs ...any) (any, error) {
	return math.Exp(xs[0].(float64)), nil
}
