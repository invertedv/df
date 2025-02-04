package df

import (
	"database/sql"
	_ "embed"
	"fmt"
	"math"
	"strings"
	"time"
)

// Can Save any DF to a table
// Can Load any query to []any

// All code interacting with a database is here

// TODO: make a clickhouse directory
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

	//go:embed skeletons/clickhouse/insert.txt
	chInsert string
	//go:embed skeletons/postgres/insert.txt
	pgInsert string

	//go:embed skeletons/clickhouse/functions.txt
	chFunctions string
	//go:embed skeletons/postgres/functions.txt
	pgFunctions string
)

const (
	ch = "clickhouse"
	pg = "postgres"
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

	bufSize int // in MB

	functions Fmap
}

type fnSpec struct {
	Name    string
	SQL     string
	Inputs  [][]DataTypes
	Outputs []DataTypes
	RT      ReturnTypes
}

func NewDialect(dialect string, db *sql.DB) (*Dialect, error) {
	dialect = strings.ToLower(dialect)

	d := &Dialect{db: db, dialect: dialect, bufSize: 1024}

	var types string
	switch d.dialect {
	case ch:
		d.create, d.fields, d.dropIf, d.insert = chCreate, chFields, chDropIf, chInsert
		types = chTypes
		d.functions = LoadFunctions(chFunctions)
	case pg:
		d.create, d.fields, d.dropIf, d.insert = pgCreate, pgFields, pgDropIf, pgInsert
		types = pgTypes
		d.functions = LoadFunctions(pgFunctions)
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
		for ind := 0; ind < len(whens); ind++ {
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

	if d.DialectName() == ch || d.DialectName() == pg {
		// is this a constant?
		if x, ok := toDate(fieldName); ok {
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

func (d *Dialect) Count() string {
	sqlx := "count(*)"
	return sqlx
}

// options are in key:value format and are meant to replace placeholders in create.txt
func (d *Dialect) Create(tableName, orderBy string, fields []string, types []DataTypes, overwrite bool, options ...string) error {
	e := fmt.Errorf("no implemention of Create for %s", d.DialectName())

	if d.DialectName() == ch || d.DialectName() == pg {
		if d.Exists(tableName) && !overwrite {
			return fmt.Errorf("table %s exists", tableName)
		}

		if orderBy == "" {
			orderBy = fields[0]
		}

		create := strings.ReplaceAll(d.create, "?TableName", tableName)
		create = strings.Replace(create, "?OrderBy", orderBy, 1)
		if d.DialectName() == pg {
			create = strings.ReplaceAll(create, "?IndexName", RandomLetters(4))
		}

		var flds []string
		for ind := 0; ind < len(fields); ind++ {
			var (
				dbType string
				ex     error
			)
			if dbType, ex = d.dbtype(types[ind]); ex != nil {
				return ex
			}

			field := strings.ReplaceAll(d.fields, "?Field", fields[ind])
			field = strings.ReplaceAll(field, "?Type", dbType)
			flds = append(flds, field)
		}

		create = strings.Replace(create, "?fields", strings.Join(flds, ","), 1)
		for _, opt := range options {
			kv := strings.Split(opt, ":")
			if len(kv) != 2 {
				return fmt.Errorf("invalid option in Dialect.Create: %s", opt)
			}

			create = strings.ReplaceAll(create, kv[0], kv[1])
		}

		if strings.Contains(create, "?") {
			return fmt.Errorf("create still has placeholders: %s", create)
		}

		_, e = d.db.Exec(create)
	}

	return e
}

func (d *Dialect) CreateTable(tableName, orderBy string, overwrite bool, df DF, options ...string) error {
	var (
		e   error
		dts []DataTypes
	)

	cols := df.ColumnNames()

	if d.DialectName() == ch || d.DialectName() == pg {
		noDesc := strings.ReplaceAll(strings.ReplaceAll(orderBy, "DESC", ""), " ", "")
		if orderBy != "" && !df.Core().HasColumns(strings.Split(noDesc, ",")...) {
			return fmt.Errorf("not all columns present in OrderBy %s", noDesc)
		}

		if dts, e = df.ColumnTypes(cols...); e != nil {
			return e
		}
		return df.Dialect().Create(tableName, noDesc, cols, dts, overwrite, options...)
	}

	return fmt.Errorf("unknown error")
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

	if d.DialectName() == ch {
		qry := fmt.Sprintf("EXISTS TABLE %s", tableName)

		if res, e = d.DB().Query(qry); e != nil {
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

	if d.DialectName() == pg {
		qry := fmt.Sprintf("SELECT to_regclass('%s')", tableName)
		if res, e = d.DB().Query(qry); e != nil {
			panic(e)
		}

		res.Next()
		var exist any
		if ex := res.Scan(&exist); ex != nil {
			panic(ex)
		}

		if exist != nil {
			return true
		}
	}

	return false
}

func (d *Dialect) Functions() Fmap {
	return d.functions
}

// TODO: delete
func (d *Dialect) Ifs(x, y, op string) (string, error) {
	const ops = ">,>=,<,<=,==,!="
	op = strings.ReplaceAll(op, " ", "")
	if !Has(op, strings.Split(ops, ",")) {
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

	if d.DialectName() == ch || d.DialectName() == pg {
		qry := strings.Replace(d.insert, "?TableName", tableName, 1)
		qry = strings.Replace(qry, "?MakeQuery", makeQuery, 1)
		qry = strings.Replace(qry, "?Fields", fields, 1)

		_, e = d.db.Exec(qry)
	}

	return e
}

func (d *Dialect) InsertValues(tableName string, values []byte) error {
	e := fmt.Errorf("db not implemented")

	if d.DialectName() == ch || d.DialectName() == pg {
		qry := fmt.Sprintf("INSERT INTO %s VALUES ", tableName) + string(values)
		_, e = d.db.Exec(qry)
	}

	return e
}

func (d *Dialect) IterSave(tableName string, df DF) error {
	const (
		bSep   = byte(',')
		bOpen  = byte('(')
		bClose = byte(')')
	)

	var buffer []byte
	bsize := d.bufSize * 1024 * 1024

	for row, e := df.Iter(true); e == nil; row, e = df.Iter(false) {
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

	for ind := 0; ind < len(fieldTypes); ind++ {
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

		for ind := 0; ind < len(memData); ind++ {
			var z = *row2read[ind].(*any)
			if z == nil {
				switch memData[ind].dt {
				case DTfloat:
					z = math.MaxFloat64
				case DTint:
					z = math.MaxInt
				case DTstring:
					z = "!null"
				case DTdate:
					z = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
				}
			}

			assign(memData[ind], z, indx)
		}

		indx++
	}

	// change any dates to midnight UTC o.w. comparisons may not work
	for c := 0; c < len(memData); c++ {
		if fieldTypes[c] != DTdate {
			continue
		}

		utc(memData[c])
	}

	return memData, fieldNames, fieldTypes, nil
}

func (d *Dialect) Max(col string) string {
	sqlx, _ := d.CastField(fmt.Sprintf("max(%s)", col), DTfloat, DTfloat)

	return sqlx
}

func (d *Dialect) Mean(col string) string {
	sqlx, _ := d.CastField(fmt.Sprintf("avg(%s)", col), DTfloat, DTfloat)
	return sqlx
}

func (d *Dialect) Min(col string) string {
	sqlx, _ := d.CastField(fmt.Sprintf("min(%s)", col), DTfloat, DTfloat)

	return sqlx
}

func (d *Dialect) WithName() string {
	const wLen = 4
	return RandomLetters(wLen)
}

func (d *Dialect) Quantile(col string, q float64) string {
	var sqlx string
	if d.DialectName() == ch {
		sqlx = fmt.Sprintf("quantile(%v)(%s)", q, col)
	}

	sqlx, _ = d.CastField(sqlx, DTfloat, DTfloat)

	return sqlx
}

func (d *Dialect) Quote() string {
	if d.DialectName() == ch || d.DialectName() == pg {
		return "'"
	}

	return ""
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

func (d *Dialect) Summary(qry, col string) ([]float64, error) {
	const skeleton = "WITH %s AS (%s) SELECT %s FROM %s"

	minX := fmt.Sprintf("min(%s) AS min", col)
	q25 := d.Quantile(col, 0.25) + "AS q25"
	q50 := d.Quantile(col, 0.5) + "AS q50"
	q75 := d.Quantile(col, 0.75) + "AS q75"
	maxX := fmt.Sprintf("max(%s) AS max", col)
	mn := fmt.Sprintf("avg(%s) AS mean", col)
	n, _ := d.CastField("count(*)", DTint, DTfloat)
	n += " AS n"
	flds := strings.Join([]string{minX, q25, q50, mn, q75, maxX, n}, ",")

	sig := d.WithName()
	q := fmt.Sprintf(skeleton, sig, qry, flds, sig)
	row := d.db.QueryRow(q)
	var vMinX, vQ25, vQ50, vMn, vQ75, vMaxX, vN float64
	if e := row.Scan(&vMinX, &vQ25, &vQ50, &vMn, &vQ75, &vMaxX, &vN); e != nil {
		return nil, e
	}

	return []float64{vMinX, vQ25, vQ50, vMn, vQ75, vMaxX, vN}, nil
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

	if qry := df.MakeQuery(); qry != "" {
		return d.Insert(tableName, df.MakeQuery(), strings.Join(df.ColumnNames(), ","))
	}

	return d.IterSave(tableName, df)
}

// TODO: is this used?
func (d *Dialect) Seq(n int) string {
	if n <= 0 {
		return ""
	}

	if d.DialectName() == ch {
		return fmt.Sprintf("toInt32(arrayJoin(range(0,%d)))", n)
	}

	if d.DialectName() == pg {
		return fmt.Sprintf("generate_series(0,%d)", n-1)
	}

	panic(fmt.Errorf("unsupported dialect for Seq"))
}

func (d *Dialect) SetBufSize(mb int) {
	d.bufSize = mb
}

// ToString returns a string version of val that can be placed into SQL
func (d *Dialect) ToString(val any) string {
	if d.DialectName() == ch || d.DialectName() == pg {
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

	panic(fmt.Errorf("unsupported db dialect"))
}

func (d *Dialect) Types(qry string) (fieldNames []string, fieldTypes []DataTypes, row2read []any, err error) {
	const skeleton = "WITH %s AS (%s) SELECT * FROM %s LIMIT 1"

	sig := d.WithName()
	q := fmt.Sprintf(skeleton, sig, qry, sig)

	var (
		r      *sql.Rows
		ct     []*sql.ColumnType
		e0, e1 error
	)
	r, e0 = d.db.Query(q)
	defer func() {
		{
			_ = r.Close()
		}
	}()

	if e0 != nil {
		return nil, nil, nil, e0
	}
	if ct, e1 = r.ColumnTypes(); e1 != nil {
		return nil, nil, nil, e1
	}

	var ry []any
	for ind := 0; ind < len(ct); ind++ {
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

	for ind := 0; ind < len(ry); ind++ {
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

func (d *Dialect) dbtype(dt DataTypes) (string, error) {
	pos := position(dt.String(), d.dtTypes)
	if pos < 0 {
		return "", fmt.Errorf("cannot find type %s to map to DB type", dt.String())
	}

	return d.dbTypes[pos], nil
}

// assign assigns the indx vector of v to be val
func assign(v *Vector, val any, indx int) {
	switch x := val.(type) {
	case float32:
		_ = v.SetFloat(float64(x), indx)
	case float64:
		_ = v.SetFloat(x, indx)
	case *float32:
		_ = v.SetFloat(float64(*x), indx)
	case *float64:
		_ = v.SetFloat(*x, indx)
	case *uint:
		_ = v.SetInt(int(*x), indx)
	case *uint8:
		_ = v.SetInt(int(*x), indx)
	case *uint16:
		_ = v.SetInt(int(*x), indx)
	case *uint32:
		_ = v.SetInt(int(*x), indx)
	case *uint64:
		_ = v.SetInt(int(*x), indx)

	case uint:
		_ = v.SetInt(int(x), indx)
	case uint8:
		_ = v.SetInt(int(x), indx)
	case uint16:
		_ = v.SetInt(int(x), indx)
	case uint32:
		_ = v.SetInt(int(x), indx)
	case uint64:
		_ = v.SetInt(int(x), indx)

	case *int:
		_ = v.SetInt(int(*x), indx)
	case *int8:
		_ = v.SetInt(int(*x), indx)
	case *int16:
		_ = v.SetInt(int(*x), indx)
	case *int32:
		_ = v.SetInt(int(*x), indx)
	case *int64:
		_ = v.SetInt(int(*x), indx)

	case int:
		_ = v.SetInt(int(x), indx)
	case int8:
		_ = v.SetInt(int(x), indx)
	case int16:
		_ = v.SetInt(int(x), indx)
	case int32:
		_ = v.SetInt(int(x), indx)
	case int64:
		_ = v.SetInt(int(x), indx)

	case string:
		_ = v.SetString(x, indx)
	case *string:
		_ = v.SetString(*x, indx)
	case time.Time:
		_ = v.SetDate(x, indx)
	case *time.Time:
		_ = v.SetDate(*x, indx)
	default:
		panic(fmt.Errorf("unsupported data type in dialect.Load"))
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
