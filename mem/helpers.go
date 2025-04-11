package df

import d "github.com/invertedv/df"

func toCol(x any) *Col {
	if c, ok := x.(*Col); ok {
		return c
	}

	if s, ok := x.(*d.Scalar); ok {
		var (
			c *Col
			e error
		)
		if c, e = NewCol(s.Data(), d.ColName(s.Name())); e != nil {
			panic(e)
		}

		return c
	}

	panic("can't make column")
}

func returnCol(data any) *d.FnReturn {
	var (
		outCol *Col
		e      error
	)

	if data == nil {
		return &d.FnReturn{}
	}

	if dx, ok := data.(*d.Vector); ok {
		if dx == nil {
			return &d.FnReturn{}
		}

		if outCol, e = NewCol(dx); e != nil {
			return &d.FnReturn{Err: e}
		}
	}

	return &d.FnReturn{Value: outCol}
}

/*func loopDim(inputs ...d.Column) int {
	n := 1
	for j := range len(inputs) {
		if col, isCol := inputs[j].(*Col); isCol {
			if nn := col.Len(); nn > n {
				n = nn
			}
		}
	}

	return n
}*/

func signature(target [][]d.DataTypes, cols []d.Column) int {
	for j := range len(target) {
		ind := j
		for k := range len(target[j]) {
			trg := cols[k].DataType()

			if trg == d.DTcategorical {
				trg = d.DTint
			}

			if target[j][k] != trg {
				ind = -1
				break
			}
		}

		if ind >= 0 {
			return ind
		}
	}

	return -1
}
