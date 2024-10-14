package df

import (
	"database/sql"
	_ "embed"
	"fmt"
	"reflect"
	"strings"
	"time"

	u "github.com/invertedv/utilities"
)

// All code interacting with a database is here

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

func (d *Dialect) DB() *sql.DB {
	return d.db
}

func (d *Dialect) DialectName() string {
	return d.dialect
}

func (d *Dialect) Close() error {
	return d.db.Close()
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
		var (
			dbType string
			e      error
		)
		if dbType, e = d.dbtype(types[ind]); e != nil {
			return e
		}

		field := strings.Replace(d.fields, "?Field", fields[ind], 1)
		field = strings.Replace(field, "?Type", dbType, 1)
		flds = append(flds, field)
	}

	create = strings.Replace(create, "?fields", strings.Join(flds, ","), 1)

	_, e := d.db.Exec(create)

	return e
}

// TODO: think about query
func (d *Dialect) Insert(tableName, makeQuery, fields string) error {
	qry := strings.Replace(d.insert, "?TableName", tableName, 1)
	qry = strings.Replace(qry, "?MakeQuery", makeQuery, 1)
	qry = strings.Replace(qry, "?Fields", fields, 1)

	_, e := d.db.Exec(qry)

	return e
}

func (d *Dialect) Rows(qry string) (rows *sql.Rows, row2Read []any, fieldNames []string, err error) {
	var (
		fieldTypes []DataTypes
		e          error
	)

	if fieldNames, fieldTypes, e = d.Types(qry); e != nil {
		return nil, nil, nil, e
	}

	if rows, e = d.db.Query(qry); e != nil {
		return nil, nil, nil, e
	}

	var addr []any
	for ind := 0; ind < len(fieldTypes); ind++ {
		var (
			vFt  float64
			vInt int
			vDt  time.Time
			vStr string
		)
		switch fieldTypes[ind] {
		case DTfloat:

			addr = append(addr, &vFt)
		case DTint:
			addr = append(addr, &vInt)
		case DTdate:
			addr = append(addr, &vDt)
		case DTstring:
			addr = append(addr, &vStr)
		default:
			return nil, nil, nil, fmt.Errorf("unknown type in Rows")
		}
	}

	return rows, addr, fieldNames, nil
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

	var rows *sql.Rows

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
		memData = append(memData, MakeSlice(types[ind], n, nil))
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
	const skeleton = "WITH d3212 AS (%s) SELECT * FROM d3212 LIMIT 1"

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

func (d *Dialect) Quote() string {
	if d.dialect == "clickhouse" {
		return "'"
	}

	return ""
}

func (d *Dialect) CastConstantXXXX(constant string, toDT DataTypes) (sql string, err error) {
	var (
		dbType string
		e      error
	)

	if _, ex := ToDataType(constant, toDT, true); ex != nil {
		return "", ex
	}

	if dbType, e = d.dbtype(toDT); e != nil {
		return "", e
	}

	if d.dialect == "clickhouse" {
		sql = fmt.Sprintf("cast(%s AS %s)", constant, dbType)
		if toDT == DTdate {
			sql = fmt.Sprintf("cast('%s' AS %s)", constant, dbType)
		}

		return sql, nil
	}

	return "", fmt.Errorf("unknown error")
}

func (d *Dialect) CastField(fieldName string, fromDT, toDT DataTypes) (sql string, err error) {
	var (
		dbType string
		e      error
	)
	if dbType, e = d.dbtype(toDT); e != nil {
		return "", e
	}

	if d.dialect == "clickhouse" {
		// is this a constant?
		if x, ex := ToDate(fieldName, true); ex == nil {
			sql = fmt.Sprintf("cast('%s' AS %s)", x.(time.Time).Format("2006-01-02"), dbType)
			return sql, nil
		}

		if fromDT == DTfloat && toDT == DTstring {
			sql = fmt.Sprintf("toDecimalString(%s, 2)", fieldName)
			return sql, nil
		}

		sql = fmt.Sprintf("cast(%s AS %s)", fieldName, dbType)
		return sql, nil
	}

	return "", fmt.Errorf("unknown error")
}

func (d *Dialect) Ifs(x, y, op string) (string, error) {
	const ops = ">,>=,<,<=,==,!="
	op = strings.ReplaceAll(op, " ", "")
	if !u.Has(op, ",", ops) {
		return "", fmt.Errorf("unknown comparison: %s", op)
	}

	if d.dialect == "clickhouse" {
		return fmt.Sprintf("toInt32(%s%s%s)", x, op, y), nil
	}

	return "", fmt.Errorf("unknown error")
}

func (d *Dialect) dbtype(dt DataTypes) (string, error) {
	pos := u.Position(dt.String(), "", d.dtTypes...)
	if pos < 0 {
		return "", fmt.Errorf("cannot find type %s to map to DB type", dt.String())
	}

	return d.dbTypes[pos], nil
}
