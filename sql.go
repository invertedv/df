package df

type SQLcol struct {
	name  string
	n     int
	dType DataTypes
	sql   string

	catMap categoryMap
}

func (s *SQLcol) DataType() DataTypes {
	return s.dType
}

func (s *SQLcol) N() int {
	return s.n
}

func (s *SQLcol) Data() any {
	return s.sql
}

func (s *SQLcol) Name() string {
	return s.name
}

func SQLAdd(cols ...*SQLcol) (out *SQLcol, err error) {
	out = &SQLcol{
		name:   "",
		n:      0,
		dType:  0,
		sql:    "",
		catMap: nil,
	}

	return out, nil
}
