package df

import (
	"database/sql"
	_ "embed"
	"fmt"
	"reflect"
	"strings"

	u "github.com/invertedv/utilities"
)

var (
	//go:embed skeletons/chCreate.txt
	chCreate string

	//go:embed skeletons/chTypes.txt
	chTypes string

	//go:embed skeletons/chFields.txt
	chFields string

	//go:embed skeletons/chDropIf.txt
	chDropIf string

	//go:embed skeletons/chInsert.xt
	chInsert string
)

type Dialect struct {
	db      *sql.DB
	dialect string

	dtTypes []string
	dbTypes []string

	create string
	insert string
	dropIf string
	exists string

	fields string
}

func NewDialect(dialect string, db *sql.DB) (*Dialect, error) {
	d := &Dialect{db: db, dialect: strings.ToLower(dialect)}

	var types string
	switch d.dialect {
	case "clickhouse":
		d.create, d.fields, d.dropIf, d.insert = chCreate, chFields, chDropIf, chInsert
		types = chTypes
	case "postgress":
		d.create = ""
	case "mysql":
		d.create = ""
	default:
		return nil, fmt.Errorf("no skeletons for database %s", dialect)
	}

	l := strings.Split(types, "\n")
	for _, lm := range l {
		if strings.Trim(lm, " ") == "" {
			continue
		}

		t := strings.Split(lm, ",")
		if len(t) != 2 {
			return nil, nil
		}

		if DTFromString(t[0]) == DTunknown {
			return nil, fmt.Errorf("unknown data type in NewDialect")
		}

		d.dtTypes = append(d.dtTypes, t[0])
		d.dbTypes = append(d.dbTypes, t[1])
	}

	return d, nil
}

func (d *Dialect) Create(tableName, orderBy string, fields []string, types []DataTypes, overwrite bool) error {
	if overwrite {
		qry := strings.Replace(d.dropIf, "?TableName", tableName, 1)
		if _, ex := d.db.Exec(qry); ex != nil {
			return ex
		}
	}

	if orderBy == "" {
		orderBy = fields[0]
	}

	create := strings.Replace(d.create, "?TableName", tableName, 1)
	create = strings.Replace(create, "?OrderBy", orderBy, 1)

	var flds []string
	for ind := 0; ind < len(fields); ind++ {
		var pos int
		if pos = u.Position(types[ind].String(), "", d.dtTypes...); pos < 0 {
			return fmt.Errorf("unknown data type %v in CreateTable", types[ind])
		}

		field := strings.Replace(d.fields, "?Field", fields[ind], 1)
		field = strings.Replace(field, "?Type", d.dbTypes[pos], 1)
		flds = append(flds, field)
	}

	create = strings.Replace(create, "?fields", strings.Join(flds, ","), 1)

	_, e := d.db.Exec(create)

	return e
}

func (d *Dialect) Insert(tableName, makeQuery, fields string) error {
	qry := strings.Replace(d.insert, "?TableName", tableName, 1)
	qry = strings.Replace(qry, "?MakeQuery", makeQuery, 1)
	qry = strings.Replace(qry, "?Fields", fields, 1)

	_, e := d.db.Exec(qry)

	return e
}

func (d *Dialect) Read(qry string) ([]any, error) {
	var (
		e     error
		names []string
		types []DataTypes
	)

	if names, types, e = d.Types(qry); e != nil {
		return nil, e
	}

	var (
		rows *sql.Rows
	)
	if rows, e = d.db.Query(qry); e != nil {
		return nil, e
	}

	r := make([]any, len(names))
	for ind := range r {
		var x any
		r[ind] = &x
	}

	var n int
	if n, e = d.RowCount(qry); e != nil {
		return nil, e
	}

	var memData []any
	for ind := 0; ind < len(types); ind++ {
		memData = append(memData, MakeSlice(types[ind], n))
	}

	xind := 0
	for rows.Next() {
		var rx []any
		for ind := 0; ind < len(types); ind++ {
			rx = append(rx, Address(memData[ind], types[ind], xind))
		}

		xind++
		if ex := rows.Scan(rx...); ex != nil {
			return nil, ex
		}
	}

	return memData, nil
}

func (d *Dialect) RowCount(qry string) (int, error) {
	const skeleton = "WITH d AS (%s) SELECT count(*) AS n FROM d"
	var n int

	q := fmt.Sprintf(skeleton, qry)
	row := d.db.QueryRow(q)
	if e := row.Scan(&n); e != nil {
		return 0, e
	}

	return n, nil
}

func (d *Dialect) Types(qry string) (fieldNames []string, fieldTypes []DataTypes, err error) {
	const skeleton = "WITH d AS (%s) SELECT * FROM d LIMIT 1"

	q := fmt.Sprintf(skeleton, qry)

	var rows *sql.Rows
	rows, err = d.db.Query(q)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	var types []*sql.ColumnType
	if types, err = rows.ColumnTypes(); err != nil {
		return nil, nil, err
	}

	for ind := 0; ind < len(types); ind++ {
		fieldNames = append(fieldNames, types[ind].Name())

		var dt DataTypes
		switch t := types[ind].ScanType().Kind(); t {
		case reflect.Float64, reflect.Float32:
			dt = DTfloat
		case reflect.Int, reflect.Int64, reflect.Int32:
			dt = DTint
		case reflect.String:
			dt = DTstring
		case reflect.Struct:
			dt = DTdate
		default:
			return nil, nil, fmt.Errorf("unsupported db field type: %v", t)
		}

		fieldTypes = append(fieldTypes, dt)
	}

	return fieldNames, fieldTypes, nil
}
