package df

import (
	"database/sql"
	_ "embed"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// Can Save any DF to a table
// Can Load any query to []any

// All code interacting with a database is here

var (
	//go:embed skeletons/clickhouse/create.txt
	chCreate string
	//go:embed skeletons/postgres/create.txt
	pgCreate string

	//go:embed skeletons/clickhouse/types.txt
	chTypes string
	//go:embed skeletons/postgres/types.txt
	pgTypes string

	//go:embed skeletons/clickhouse/fields.txt
	chFields string
	//go:embed skeletons/postgres/fields.txt
	pgFields string

	//go:embed skeletons/clickhouse/dropIf.txt
	chDropIf string
	//go:embed skeletons/postgres/dropif.txt
	pgDropIf string

	//go:embed skeletons/clickhouse/exists.txt
	chExists string
	//go:embed skeletons/postgres/exists.txt
	pgExists string

	//go:embed skeletons/clickhouse/interp.txt
	chInterp string
	//go:embed skeletons/postgres/interp.txt
	pgInterp string

	//go:embed skeletons/clickhouse/insert.txt
	chInsert string
	//go:embed skeletons/postgres/insert.txt
	pgInsert string

	//go:embed skeletons/clickhouse/seq.txt
	chSeq string
	//go:embed skeletons/postgres/seq.txt
	pgSeq string

	//go:embed skeletons/clickhouse/functions.txt
	chFunctions string
	//go:embed skeletons/postgres/functions.txt
	pgFunctions string
)

const (
	ch = "clickhouse"
	pg = "postgres"
)

type Dialect struct {
	db      *sql.DB
	dialect string

	dtTypes []string
	dbTypes []string

	create string
	insert string
	interp string
	dropIf string
	exists string
	seq    string

	fields string

	bufSize int // in MB

	functions Fmap

	defaultInt    int
	defaultFloat  float64
	defaultString string
	defaultDate   time.Time
}

func NewDialect(dialect string, db *sql.DB, opts ...DialectOpt) (*Dialect, error) {
	dialect = strings.ToLower(dialect)

	d := &Dialect{db: db,
		dialect:       dialect,
		bufSize:       1024,
		defaultInt:    math.MaxInt,
		defaultFloat:  math.MaxFloat64,
		defaultDate:   time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
		defaultString: "",
	}

	var types string
	switch d.dialect {
	case ch:
		d.create, d.fields, d.dropIf, d.insert, d.exists = chCreate, chFields, chDropIf, chInsert, chExists
		d.seq, d.interp = chSeq, chInterp
		types = chTypes
		d.functions = LoadFunctions(chFunctions)
	case pg:
		d.create, d.fields, d.dropIf, d.insert, d.exists = pgCreate, pgFields, pgDropIf, pgInsert, pgExists
		d.seq, d.interp = pgSeq, pgInterp
		types = pgTypes
		d.functions = LoadFunctions(pgFunctions)
	default:
		return nil, fmt.Errorf("no skeletons for database %s", dialect)
	}

	for lm := range strings.SplitSeq(types, "\n") {
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

	for _, opt := range opts {
		if e := opt(d); e != nil {
			return nil, e
		}
	}

	return d, nil
}

// ***************** Setters *****************

type DialectOpt func(d *Dialect) error

// DialectBuffSize sets the buffer size (in MB) for accumulating inserts
func DialectBuffSize(bufMB int) DialectOpt {
	return func(d *Dialect) error {
		if bufMB <= 0 {
			return fmt.Errorf("bad buffer size in Dialect")
		}

		d.bufSize = bufMB

		return nil
	}
}

func DialectDefaultDate(year, mon, day int) DialectOpt {
	return func(d *Dialect) error {
		if year < 1900 || year > 2200 {
			return fmt.Errorf("invalid year in default date: %d", year)
		}

		if mon < 1 || mon > 12 {
			return fmt.Errorf("invalid month in default date: %d", mon)
		}

		if day < 1 || day > 31 {
			return fmt.Errorf("invalid day in default date: %d", day)
		}

		t := time.Date(year, time.Month(mon), day, 0, 0, 0, 0, time.UTC)
		d.defaultDate = t

		return nil
	}
}

func DialectDefaultInt(deflt int) DialectOpt {
	return func(d *Dialect) error {
		d.defaultInt = deflt

		return nil
	}
}

func DialectDefaultFloat(deflt float64) DialectOpt {
	return func(d *Dialect) error {
		d.defaultFloat = deflt

		return nil
	}
}

func DialectDefaultString(deflt string) DialectOpt {
	return func(d *Dialect) error {
		d.defaultString = deflt

		return nil
	}
}

// ***************** Methods *****************

func (d *Dialect) BufSize() int {
	return d.bufSize
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
	if d.DialectName() == ch || d.DialectName() == pg {
		e = nil
		s = "CASE\n"
		for ind := range len(whens) {
			when := fmt.Sprintf("WHEN %s THEN %s\n", whens[ind], vals[ind])
			if strings.EqualFold(whens[ind], "ELSE") {
				when = fmt.Sprintf("ELSE %s\n", vals[ind])
			}
			s += when
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

	sqlStr = fmt.Sprintf("cast(%s AS %s)", fieldName, dbType)
	return sqlStr, nil
}

// CastFloat says whether floats need to be cast as such.
// Postgress will return "NUMERIC" for calculated fields which the connector loads as strings
func (d *Dialect) CastFloat() bool {
	return d.DialectName() == pg
}

func (d *Dialect) Close() error {
	return d.db.Close()
}

// options are in key:value format and are meant to replace placeholders in create.txt
func (d *Dialect) Create(tableName, orderBy string, fields []string, types []DataTypes, overwrite bool, options ...string) error {
	if d.Exists(tableName) && !overwrite {
		return fmt.Errorf("table %s exists", tableName)
	}

	if orderBy == "" {
		orderBy = d.ToName(fields[0])
	}

	create := strings.ReplaceAll(d.create, "?TableName", tableName)
	create = strings.Replace(create, "?OrderBy", orderBy, 1)
	if d.DialectName() == pg {
		create = strings.ReplaceAll(create, "?IndexName", RandomLetters(4))
	}

	var flds []string
	for ind := range len(fields) {
		var (
			dbType string
			ex     error
		)
		if dbType, ex = d.dbtype(types[ind]); ex != nil {
			return ex
		}

		field := strings.ReplaceAll(d.fields, "?Field", d.ToName(fields[ind]))
		field = strings.ReplaceAll(field, "?Type", dbType)
		flds = append(flds, field)
	}

	create = strings.Replace(create, "?fields", strings.Join(flds, ","), 1)
	for _, opt := range options {
		kv := strings.Split(opt, ":")
		if len(kv) != 2 {
			return fmt.Errorf("invalid option in Dialect.Create: %s", opt)
		}

		create = strings.ReplaceAll(create, "?"+kv[0], kv[1])
	}

	if strings.Contains(create, "?") {
		return fmt.Errorf("create still has placeholders: %s", create)
	}

	_, e := d.db.Exec(create)

	return e
}

func (d *Dialect) CreateTable(tableName, orderBy string, overwrite bool, df DF, options ...string) error {
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
	return d.Create(tableName, noDesc, cols, dts, overwrite, options...)
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

	qry := strings.ReplaceAll(d.dropIf, "?TableName", tableName)
	_, e := d.DB().Exec(qry)

	return e
}

func (d *Dialect) Exists(tableName string) bool {
	var (
		res *sql.Rows
		e   error
	)

	qry := strings.ReplaceAll(d.exists, "?TableName", tableName)

	if res, e = d.DB().Query(qry); e != nil {
		panic(e)
	}

	defer func() { _ = res.Close() }()

	var exist any
	res.Next()
	if ex := res.Scan(&exist); ex != nil {
		panic(ex)
	}

	switch x := exist.(type) {
	case int64:
		return x == 1 // for pg
	case uint8:
		return x == 1 // for ch
	}

	return (exist.(int64) == 1)
}

func (d *Dialect) Functions() Fmap {
	return d.functions
}

// Global takes SQL that normally is a scalar return (e.g. count(*), avg(x)) and surrounds it with SQL to return
// that value for every row of a query
func (d *Dialect) Global(sourceSQL, colSQL string) string {
	return fmt.Sprintf("(WITH global AS (%s) SELECT (%s) FROM global)", sourceSQL, colSQL)
}

func (d *Dialect) Insert(tableName, makeQuery, fields string) error {
	qry := strings.Replace(d.insert, "?TableName", tableName, 1)
	qry = strings.Replace(qry, "?MakeQuery", makeQuery, 1)
	qry = strings.Replace(qry, "?Fields", fields, 1)

	_, e := d.db.Exec(qry)

	return e
}

func (d *Dialect) InsertValues(tableName string, values []byte) error {
	qry := fmt.Sprintf("INSERT INTO %s VALUES ", tableName) + string(values)
	_, e := d.db.Exec(qry)

	return e
}

func (d *Dialect) Interp(sourceSQL, interpSQL, xSfield, xIfield, yField, outField string) string {
	qry := strings.ReplaceAll(d.interp, "?Source", sourceSQL)
	qry = strings.ReplaceAll(qry, "?Interp", interpSQL)
	qry = strings.ReplaceAll(qry, "?XSfield", xSfield)
	qry = strings.ReplaceAll(qry, "?XIfield", xIfield)
	qry = strings.ReplaceAll(qry, "?Yfield", yField)
	qry = strings.ReplaceAll(qry, "?OutField", outField)

	return qry
}

func (d *Dialect) IterSave(tableName string, df DF) error {
	const (
		bSep   = byte(',')
		bOpen  = byte('(')
		bClose = byte(')')
	)

	var buffer []byte
	bsize := d.bufSize * 1024 * 1024

	for _, row := range df.AllRows() {
		if buffer != nil {
			buffer = append(buffer, bSep)
		}

		buffer = append(buffer, bOpen)
		for ind := range len(row) {
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

		if bsize > 0 && len(buffer) >= bsize {
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

// Join creates an inner JOIN query.
//
//	leftSQL - SQL for left side of join
//	rightSQL - SQL for right side of join
//	leftFields - fields to keep from leftSQL
//	rightFields - fields to keep from rightSQL
//	joinField - fields to join on
func (d *Dialect) Join(leftSQL, rightSQL string, leftFields, rightFields, joinFields []string) string {
	leftAlias := d.WithName()
	rightAlias := d.WithName()
	for ind := range len(joinFields) {
		jn := joinFields[ind]
		joinFields[ind] = fmt.Sprintf("%s.%s = %s.%s", leftAlias, jn, rightAlias, jn)
	}

	for ind := range len(leftFields) {
		leftFields[ind] = fmt.Sprintf("%s.%s", leftAlias, d.ToName(leftFields[ind]))
	}

	for ind := range len(rightFields) {
		rightFields[ind] = fmt.Sprintf("%s.%s", rightAlias, d.ToName(rightFields[ind]))
	}

	selectFields := strings.Join(append(leftFields, rightFields...), ",")

	qry := fmt.Sprintf("SELECT %s FROM (%s) AS %s JOIN (%s) AS %s ON %s", selectFields,
		leftSQL, leftAlias, rightSQL, rightAlias, strings.Join(joinFields, " AND "))

	return qry
}

func (d *Dialect) Load(qry string) ([]*Vector, []string, []DataTypes, error) {
	fieldNames, fieldTypes, row2read, e1 := d.Types(qry)
	if e1 != nil {
		return nil, nil, nil, e1
	}

	var (
		n       int
		e2      error
		memData []*Vector
	)
	if n, e2 = d.RowCount(qry); e2 != nil {
		return nil, nil, nil, e2
	}

	for ind := range len(fieldTypes) {
		memData = append(memData, MakeVector(fieldTypes[ind], n))
	}

	var (
		rows *sql.Rows
		e3   error
	)
	if rows, e3 = d.db.Query(qry); e3 != nil {
		return nil, nil, nil, e3
	}

	indx := 0
	for rows.Next() {
		if e4 := rows.Scan(row2read...); e4 != nil {
			return nil, nil, nil, e4
		}

		for ind := range len(memData) {
			var z = *row2read[ind].(*any)
			if z == nil {
				switch memData[ind].dt {
				case DTfloat:
					z = d.defaultFloat
				case DTint:
					z = d.defaultInt
				case DTstring:
					z = d.defaultString
				case DTdate:
					z = d.defaultDate
				}
			}

			d.assign(memData[ind], z, indx)
		}

		indx++
	}

	// change any dates to midnight UTC o.w. comparisons may not work
	for c := range len(memData) {
		if fieldTypes[c] != DTdate {
			continue
		}

		utc(memData[c])
	}

	return memData, fieldNames, fieldTypes, nil
}

func (d *Dialect) Quantile(col string, q float64) string {
	var sqlx string
	if d.DialectName() == ch {
		sqlx = fmt.Sprintf("quantileBFloat16(%v)(%s)", q, col)
		sqlx, _ = d.CastField(sqlx, DTfloat, DTfloat)
	}

	if d.DialectName() == pg {
		sqlx = fmt.Sprintf("percentile_disc(%v) WITHIN GROUP (ORDER BY %s)", q, col)
	}

	return sqlx
}

func (d *Dialect) Quote() string {
	return "'"
}

func (d *Dialect) RowCount(qry string) (int, error) {
	const skeleton = "WITH %s AS (%s) SELECT count(*) AS n FROM %s"
	var n int

	sig := d.WithName()
	q := fmt.Sprintf(skeleton, sig, qry, sig)
	row := d.db.QueryRow(q)
	if e := row.Scan(&n); e != nil {
		return 0, e
	}

	return n, nil
}

func (d *Dialect) Rows(qry string) (rows *sql.Rows, row2Read []any, fieldNames []string, err error) {
	var e error

	if fieldNames, _, row2Read, e = d.Types(qry); e != nil {
		return nil, nil, nil, e
	}

	if rows, e = d.db.Query(qry); e != nil {
		return nil, nil, nil, e
	}

	return rows, row2Read, fieldNames, nil
}

func (d *Dialect) Save(tableName, orderBy string, overwrite bool, df DF, options ...string) error {
	exists := d.Exists(tableName)
	if exists && !overwrite {
		return fmt.Errorf("table %s exists", tableName)
	}

	if exists {
		if e := d.DropTable(tableName); e != nil {
			return e
		}
	}

	if e := d.CreateTable(tableName, orderBy, overwrite, df, options...); e != nil {
		return e
	}

	// If there's a MakeQuery method, use that
	t := reflect.TypeOf(df)
	if m, ok := t.MethodByName("MakeQuery"); ok {
		args := []reflect.Value{reflect.ValueOf(df)}
		qry := m.Func.Call(args)[0].String()
		colNames := df.ColumnNames()
		for ind, cn := range colNames {
			colNames[ind] = d.ToName(cn)
		}

		return d.Insert(tableName, qry, strings.Join(colNames, ","))
	}

	return d.IterSave(tableName, df)
}

func (d *Dialect) Seq(n int) string {
	if n <= 0 {
		return ""
	}

	return strings.ReplaceAll(d.seq, "?Upper", fmt.Sprintf("%d", n))
}

// ToName converts the raw field name to what's need for a interaction with the database.
// Specifically, Postgres requires quotes around field names that have uppercase letters
func (d *Dialect) ToName(fieldName string) string {
	if d.DialectName() == pg {
		if fieldName != strings.ToLower(fieldName) {
			return `"` + fieldName + `"`
		}
	}

	return fieldName
}

// ToString returns a string version of val that can be placed into SQL
func (d *Dialect) ToString(val any) string {
	var (
		xv any
		ok bool
	)
	if xv, ok = toString(val); !ok {
		panic(fmt.Errorf("can't make string"))
	}

	x := xv.(string)
	if WhatAmI(val) == DTdate || WhatAmI(val) == DTstring {
		x = fmt.Sprintf("'%s'", x)
	}

	return x
}

func (d *Dialect) Types(qry string) (fieldNames []string, fieldTypes []DataTypes, row2read []any, err error) {
	const skeleton = "WITH %s AS (%s) SELECT * FROM %s LIMIT 1"

	sig := d.WithName()
	q := fmt.Sprintf(skeleton, sig, qry, sig)

	var (
		r  *sql.Rows
		e0 error
	)
	if r, e0 = d.db.Query(q); e0 != nil {
		return nil, nil, nil, e0
	}
	defer func() {
		{
			_ = r.Close()
		}
	}()

	var (
		ct []*sql.ColumnType
		e1 error
	)
	if ct, e1 = r.ColumnTypes(); e1 != nil {
		return nil, nil, nil, e1
	}

	var ry []any
	for range len(ct) {
		var x any
		ry = append(ry, &x)
	}
	for r.Next() {
		if e1 := r.Scan(ry...); e1 != nil {
			return nil, nil, nil, e1
		}
	}

	var (
		names []string
		dts   []DataTypes
	)

	for ind := range len(ry) {
		names = append(names, ct[ind].Name())
		var dt DataTypes

		var z = *ry[ind].(*any)
		switch z.(type) {
		case int, int8, int16, int32, int64, *int, *int16, *int32, *int64,
			uint, uint8, uint16, uint32, uint64, *uint, *uint8, *uint16, *uint32, *uint64:
			dt = DTint
		case float32, float64, *float32, *float64:
			dt = DTfloat
		case string, *string:
			dt = DTstring
		case time.Time, *time.Time:
			dt = DTdate
		default:
			panic("OH NO bad datatype")
		}

		dts = append(dts, dt)
	}

	return names, dts, ry, nil
}

func (d *Dialect) Union(table1, table2 string, colNames ...string) (string, error) {
	e := fmt.Errorf("no implemention of Union for %s", d.DialectName())
	var sqlx string

	cols := strings.Join(colNames, ",")
	if d.DialectName() == ch {
		sqlx = fmt.Sprintf("SELECT %s FROM (%s) UNION ALL (%s)", cols, table1, table2)
		e = nil
	}

	if d.DialectName() == pg {
		sqlx = fmt.Sprintf("WITH abc AS(%s), def AS (%s) SELECT * FROM abc UNION ALL SELECT * FROM def", table1, table2)
		e = nil
	}

	return sqlx, e
}

func (d *Dialect) WithName() string {
	const wLen = 4
	return RandomLetters(wLen)
}

func (d *Dialect) dbtype(dt DataTypes) (string, error) {
	pos := Position(dt.String(), d.dtTypes)
	if pos < 0 {
		return "", fmt.Errorf("cannot find type %s to map to DB type", dt.String())
	}

	return d.dbTypes[pos], nil
}

// assign assigns the indx vector of v to be val
func (d *Dialect) assign(v *Vector, val any, indx int) {
	var e error
	switch v.VectorType() {
	case DTfloat:
		e = v.SetFloat(d.Convert(val).(float64), indx)
	case DTint:
		e = v.SetInt(d.Convert(val).(int), indx)
	case DTstring:
		e = v.SetString(d.Convert(val).(string), indx)
	case DTdate:
		e = v.SetDate(d.Convert(val).(time.Time), indx)
	}

	if e != nil {
		panic(e)
	}
}

// utc changes the entries of date slices to be midnight UTC
func utc(v *Vector) {
	var (
		col []time.Time
		e   error
	)
	if col, e = v.AsDate(); e != nil {
		panic(e)
	}

	for rx := 0; rx < v.Len(); rx++ {
		col[rx] = time.Date(col[rx].Year(), col[rx].Month(), col[rx].Day(), 0, 0, 0, 0, time.UTC)
	}
}

// assign assigns the indx vector of v to be val
func (d *Dialect) Convert(val any) any {
	switch x := val.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	case *float32:
		return float64(*x)
	case *float64:
		return *x
	case *uint:
		return int(*x)
	case *uint8:
		return int(*x)
	case *uint16:
		return int(*x)
	case *uint32:
		return int(*x)
	case *uint64:
		return int(*x)

	case uint:
		return int(x)
	case uint8:
		return int(x)
	case uint16:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)

	case *int:
		return int(*x)
	case *int8:
		return int(*x)
	case *int16:
		return int(*x)
	case *int32:
		return int(*x)
	case *int64:
		return int(*x)

	case int:
		return int(x)
	case int8:
		return int(x)
	case int16:
		return int(x)
	case int32:
		return int(x)
	case int64:
		return int(x)

	case string:
		return x
	case *string:
		return *x
	case time.Time:
		return x
	case *time.Time:
		return *x
	default:
		panic(fmt.Errorf("unsupported data type in dialect.Load"))
	}
}
