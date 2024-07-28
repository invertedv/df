package df

type SQL struct {
	name  string
	n     uint32
	dType DataTypes
	sql   string

	catMap categoryMap
}

func (s *SQL) DataType() DataTypes {
	return s.dType
}

func (s *SQL) N() uint32 {
	return s.n
}

func (s *SQL) Data() any {
	return s.sql
}

func (s *SQL) Name() string {
	return s.name
}

func SQLAdd(cols ...*SQL) (out *SQL, err error) {
	out = &SQL{
		name:   "",
		n:      0,
		dType:  0,
		sql:    "",
		catMap: nil,
	}

	return out, nil
}
