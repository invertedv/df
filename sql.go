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

// Can Save any DF to a table
// Can Load any query to []any

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

const (
	ch = "clickhouse"
	pg = "postgress"
	ms = "mysql"
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
	case ch:
		d.create, d.fields, d.dropIf, d.insert = chCreate, chFields, chDropIf, chInsert
		types = chTypes
	case pg:
		d.create = ""
	case ms:
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

// Case creates a CASE statement.
// - whens slice of conditions
// - vals slice of the value to set the result to if condition is true
func (d *Dialect) Case(whens, vals []string) (string, error) {
	if len(whens) != len(vals) {
		return "", fmt.Errorf("whens and vals must be same length in Dialect.Case")
	}

	var s string
	e := fmt.Errorf("unsupported db dialect")
	if d.DialectName() == ch {
		e = nil
		s = "CASE\n"
		for ind := 0; ind < len(whens); ind++ {
			s += fmt.Sprintf("WHEN %s THEN %s\n", whens[ind], vals[ind])
		}
		s += "END"
	}

	return s, e
}

func (d *Dialect) CastField(fieldName string, fromDT, toDT DataTypes) (sqlStr string, err error) {
	var (
		dbType string
		e      error
	)
	if dbType, e = d.dbtype(toDT); e != nil {
		return "", e
	}

	if d.dialect == ch {
		// is this a constant?
		if x, ex := ToDate(fieldName, true); ex == nil {
			sqlStr = fmt.Sprintf("cast('%s' AS %s)", x.(time.Time).Format("2006-01-02"), dbType)
			return sqlStr, nil
		}

		if fromDT == DTfloat && toDT == DTstring {
			sqlStr = fmt.Sprintf("toDecimalString(%s, 2)", fieldName)
			return sqlStr, nil
		}

		sqlStr = fmt.Sprintf("cast(%s AS %s)", fieldName, dbType)
		return sqlStr, nil
	}

	return "", fmt.Errorf("unknown error")
}

func (d *Dialect) Close() error {
	return d.db.Close()
}

func (d *Dialect) Create(tableName, orderBy string, fields []string, types []DataTypes, overwrite bool) error {
	e := fmt.Errorf("no implemention of Create for %s", d.DialectName())

	if d.DialectName() == ch {
		if orderBy == "" {
			orderBy = fields[0]
		}

		create := strings.Replace(d.create, "?TableName", tableName, 1)
		create = strings.Replace(create, "?OrderBy", orderBy, 1)

		var flds []string
		for ind := 0; ind < len(fields); ind++ {
			var (
				dbType string
				ex     error
			)
			if dbType, ex = d.dbtype(types[ind]); ex != nil {
				return ex
			}

			field := strings.Replace(d.fields, "?Field", fields[ind], 1)
			field = strings.Replace(field, "?Type", dbType, 1)
			flds = append(flds, field)
		}

		create = strings.Replace(create, "?fields", strings.Join(flds, ","), 1)

		_, e = d.db.Exec(create)
	}

	return e
}

func (d *Dialect) CreateTable(tableName, orderBy string, overwrite bool, df DF) error {
	var (
		e   error
		dts []DataTypes
	)

	cols := df.ColumnNames()

	noDesc := strings.ReplaceAll(strings.ReplaceAll(orderBy, "DESC", ""), " ", "")
	if orderBy != "" && !df.Core().HasColumns(strings.Split(noDesc, ",")...) {
		return fmt.Errorf("not all columns present in OrderBy %s", noDesc)
	}

	if dts, e = df.ColumnTypes(cols...); e != nil {
		return e
	}

	return df.Context().dialect.Create(tableName, noDesc, cols, dts, overwrite)
}

func (d *Dialect) DB() *sql.DB {
	return d.db
}

func (d *Dialect) DialectName() string {
	return d.dialect
}

func (d *Dialect) DropTable(tableName string) error {
	if !d.Exists(tableName) {
		return nil
	}

	qry := fmt.Sprintf("DROP TABLE %s", tableName)
	_, e := d.DB().Exec(qry)

	return e
}

func (d *Dialect) Exists(tableName string) bool {
	if d.DialectName() == ch {
		qry := fmt.Sprintf("EXISTS TABLE %s", tableName)

		res, e := d.DB().Query(qry)
		if e != nil {
			panic(e)
		}
		defer func() { _ = res.Close() }()

		var exist uint8
		res.Next()
		if ex := res.Scan(&exist); ex != nil {
			panic(ex)
		}

		if exist == 1 {
			return true
		}
	}

	return false
}

func (d *Dialect) Ifs(x, y, op string) (string, error) {
	const ops = ">,>=,<,<=,==,!="
	op = strings.ReplaceAll(op, " ", "")
	if !u.Has(op, ",", ops) {
		return "", fmt.Errorf("unknown comparison: %s", op)
	}

	if d.dialect == ch {
		return fmt.Sprintf("toInt32(%s%s%s)", x, op, y), nil
	}

	return "", fmt.Errorf("unknown error")
}

// TODO: think about query
func (d *Dialect) Insert(tableName, makeQuery, fields string) error {
	e := fmt.Errorf("db not implemented")

	if d.DialectName() == ch {
		qry := strings.Replace(d.insert, "?TableName", tableName, 1)
		qry = strings.Replace(qry, "?MakeQuery", makeQuery, 1)
		qry = strings.Replace(qry, "?Fields", fields, 1)

		_, e = d.db.Exec(qry)
	}

	return e
}

func (d *Dialect) InsertValues(tableName string, values []byte) error {
	e := fmt.Errorf("db not implemented")

	if d.DialectName() == ch {
		qry := fmt.Sprintf("INSERT INTO %s VALUES ", tableName) + string(values)
		_, e = d.db.Exec(qry)
	}

	return e
}

func (d *Dialect) Load(qry string) ([]any, error) {
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

	// change any dates to midnight UTC o.w. comparisons may not work
	for c := 0; c < len(types); c++ {
		if types[c] != DTdate {
			continue
		}

		col := memData[c].([]time.Time)
		for rx := 0; rx < n; rx++ {
			col[rx] = time.Date(col[rx].Year(), col[rx].Month(), col[rx].Day(), 0, 0, 0, 0, time.UTC)
		}
	}

	return memData, nil
}

func (d *Dialect) Quote() string {
	if d.dialect == ch {
		return "'"
	}

	return ""
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

// This should check if a query is available...then do insert or iterate

func (d *Dialect) IterSave(tableName string, df DF) error {
	const maxBuf = 10000
	var buffer []byte
	bSep := byte(',')
	bOpen := byte('(')
	bClose := byte(')')

	for eof, row := df.Iter(true); eof == false; eof, row = df.Iter(false) {
		if buffer != nil {
			buffer = append(buffer, bSep)
		}

		buffer = append(buffer, bOpen)
		for ind := 0; ind < len(row); ind++ {
			var x any
			switch xx := row[ind].(type) {
			case int, float64, string, time.Time:
				x = xx
			case *int:
				x = *xx
			case *float64:
				x = *xx
			case *string:
				x = *xx
			case *time.Time:
				x = *xx
			}
			buffer = append(append(buffer, []byte(d.ToString(x))...), bSep)
		}

		buffer[len(buffer)-1] = bClose

		if len(buffer) >= maxBuf || eof {
			if e := d.InsertValues(tableName, buffer); e != nil {
				return e
			}
			fmt.Println(string(buffer))
			buffer = nil
		}
	}

	if buffer != nil {
		if e := d.InsertValues(tableName, buffer); e != nil {
			return e
		}
	}

	return nil
}

func (d *Dialect) Save(tableName, orderBy string, overwrite bool, df DF) error {
	exists := d.Exists(tableName)

	if overwrite || !exists {
		if exists {
			if e := d.DropTable(tableName); e != nil {
				return e
			}
		}

		if e := d.CreateTable(tableName, orderBy, overwrite, df); e != nil {
			return e
		}
	}

	if qry := df.MakeQuery(); qry != "" {
		return d.Insert(tableName, df.MakeQuery(), strings.Join(df.ColumnNames(), ","))
	}

	return d.IterSave(tableName, df)
}

// ToString returns a string version of val that can be placed into SQL
func (d *Dialect) ToString(val any) string {
	if d.DialectName() == ch {
		x := Any2String(val)
		if WhatAmI(val) == DTdate || WhatAmI(val) == DTstring {
			x = fmt.Sprintf("'%s'", x)
		}
		return x
	}

	panic(fmt.Errorf("unsupported db dialect"))
}

func (d *Dialect) Types(qry string) (fieldNames []string, fieldTypes []DataTypes, err error) {
	const withLen = 4

	wn := u.RandomLetters(withLen)
	const skeleton = "WITH d3212 AS (%s) SELECT * FROM d3212 LIMIT 1"

	q := strings.ReplaceAll(fmt.Sprintf(skeleton, qry), "d3212", wn)

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

func (d *Dialect) Union(table1, table2 string, colNames ...string) (string, error) {
	e := fmt.Errorf("no implemention of Union for %s", d.DialectName())
	var sqlx string

	if d.DialectName() == ch {
		cols := strings.Join(colNames, ",")
		sqlx = fmt.Sprintf("SELECT %s FROM (%s) UNION ALL (%s)", cols, table1, table2)
		e = nil
	}

	return sqlx, e
}

func (d *Dialect) dbtype(dt DataTypes) (string, error) {
	pos := u.Position(dt.String(), "", d.dtTypes...)
	if pos < 0 {
		return "", fmt.Errorf("cannot find type %s to map to DB type", dt.String())
	}

	return d.dbTypes[pos], nil
}
