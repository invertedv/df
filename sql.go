package df

import (
	"database/sql"
	"fmt"
	"reflect"
)

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

func (s *SQLcol) Len() int {
	return s.n
}

func (s *SQLcol) Data() any {
	return s.sql
}

func (s *SQLcol) Name(renameTo string) string {
	if renameTo != "" {
		s.name = renameTo
	}

	return s.name
}

type SQLdf struct {
	sourceSQL     string
	destTableName string
	db            *sql.DB

	*DFlist
}

func NewSQLdf(query string, db *sql.DB) (*SQLdf, error) {
	df := &SQLdf{
		sourceSQL:     query,
		destTableName: "",
		db:            db,
		DFlist:        nil,
	}

	var (
		err      error
		rows     *sql.Rows
		colTypes []*sql.ColumnType
		cols     []Column
	)

	// just get one row...TRY: just query and see if it runs one row at a time
	qry := fmt.Sprintf("WITH d AS (%s) SELECT * FROM d LIMIT 1", query)
	rows, err = db.Query(qry)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	if colTypes, err = rows.ColumnTypes(); err != nil {
		return nil, err
	}

	for ind := 0; ind < len(colTypes); ind++ {
		var dt DataTypes
		switch t := colTypes[ind].ScanType().Kind(); t {
		case reflect.Float64, reflect.Float32:
			dt = DTfloat
		case reflect.Int, reflect.Int64, reflect.Int32:
			dt = DTint
		case reflect.String:
			dt = DTstring
		case reflect.Struct:
			dt = DTdate
		default:
			return nil, fmt.Errorf("unsupported db field type: %v", t)
		}

		sqlCol := &SQLcol{
			name:   colTypes[ind].Name(),
			n:      1,
			dType:  dt,
			sql:    "",
			catMap: nil,
		}

		cols = append(cols, sqlCol)

	}

	if df.DFlist, err = NewDFlist(cols...); err != nil {
		return nil, err
	}

	return df, nil
}
