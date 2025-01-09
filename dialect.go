package df

import (
	"database/sql"
	_ "embed"
	"fmt"
	"reflect"
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

	//go:embed skeletons/clickhouse/types.txt
	chTypes string

	//go:embed skeletons/clickhouse/fields.txt
	chFields string

	//go:embed skeletons/clickhouse/dropIf.txt
	chDropIf string

	//go:embed skeletons/clickhouse/insert.txt
	chInsert string

	//go:embed skeletons/clickhouse/chFunctions.txt
	chFunctions string
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

	bufSize int // in MB

	functions []*fnSpec
}

type fnSpec struct {
	Name    string
	SQL     string
	Inputs  [][]DataTypes
	Outputs []DataTypes
}

func NewDialect(dialect string, db *sql.DB) (*Dialect, error) {
	dialect = strings.ToLower(dialect)

	d := &Dialect{db: db, dialect: dialect, bufSize: 1024}

	var types string
	switch d.dialect {
	case ch:
		d.create, d.fields, d.dropIf, d.insert = chCreate, chFields, chDropIf, chInsert
		types = chTypes
		d.functions = loadFunctions(chFunctions)
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
	if d.DialectName() == ch {
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

	if d.dialect == ch {
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

func (d *Dialect) Create(tableName, orderBy string, fields []string, types []DataTypes, overwrite bool) error {
	e := fmt.Errorf("no implemention of Create for %s", d.DialectName())

	if d.DialectName() == ch {
		if d.Exists(tableName) && !overwrite {
			return fmt.Errorf("table %s exists", tableName)
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

	return df.Dialect().Create(tableName, noDesc, cols, dts, overwrite)
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

		var (
			res *sql.Rows
			e   error
		)
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

	return false
}

func (d *Dialect) Functions() []*fnSpec {
	return d.functions
}

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

func (d *Dialect) Load(qry string) ([]*Vector, error) {
	var (
		e     error
		names []string
		types []DataTypes
		kinds []reflect.Kind
	)

	if names, types, kinds, e = d.Types(qry); e != nil {
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

	var memData []*Vector
	for ind := 0; ind < len(types); ind++ {
		memData = append(memData, MakeVector(types[ind], n))
	}

	xind := 0
	ry := buildRow(kinds)
	for rows.Next() {
		if ex := rows.Scan(ry...); ex != nil {
			return nil, ex
		}

		for ind := 0; ind < len(types); ind++ {
			val := castKind(ry[ind], kinds[ind])
			switch types[ind] {
			case DTfloat:
				_ = memData[ind].SetFloat(val.(float64), xind)
			case DTint:
				_ = memData[ind].SetInt(val.(int), xind)
			case DTstring:
				_ = memData[ind].SetString(val.(string), xind)
			case DTdate:
				_ = memData[ind].SetDate(val.(time.Time), xind)
			}
		}

		xind++
	}

	// change any dates to midnight UTC o.w. comparisons may not work
	for c := 0; c < len(types); c++ {
		if types[c] != DTdate {
			continue
		}

		var (
			col []time.Time
			ex  error
		)
		if col, ex = memData[c].AsDate(); ex != nil {
			return nil, ex
		}

		for rx := 0; rx < n; rx++ {
			col[rx] = time.Date(col[rx].Year(), col[rx].Month(), col[rx].Day(), 0, 0, 0, 0, time.UTC)
		}
	}

	return memData, nil
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
	if d.dialect == ch {
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

func (d *Dialect) RowNumber() string {
	if d.DialectName() == ch {
		return "toInt32(rowNumberInBlock())"
	}

	panic(fmt.Errorf("unsupported dialect in RownNumber"))
}

func (d *Dialect) Rows(qry string) (rows *sql.Rows, row2Read []any, fieldNames []string, err error) {
	var (
		kinds []reflect.Kind
		e     error
	)

	if fieldNames, _, kinds, e = d.Types(qry); e != nil {
		return nil, nil, nil, e
	}

	if rows, e = d.db.Query(qry); e != nil {
		return nil, nil, nil, e
	}

	addr := buildRow(kinds)

	return rows, addr, fieldNames, nil
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

func (d *Dialect) Seq(n int) string {
	if n <= 0 {
		return ""
	}

	if d.DialectName() == ch {
		return fmt.Sprintf("toInt32(arrayJoin(range(0,%d)))", n)
	}

	panic(fmt.Errorf("unsupported dialect for Seq"))
}

func (d *Dialect) SetBufSize(mb int) {
	d.bufSize = mb
}

// ToString returns a string version of val that can be placed into SQL
func (d *Dialect) ToString(val any) string {
	if d.DialectName() == ch {
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

func (d *Dialect) Types(qry string) (fieldNames []string, fieldTypes []DataTypes, fieldKinds []reflect.Kind, err error) {
	const skeleton = "WITH %s AS (%s) SELECT * FROM %s LIMIT 1"

	sig := d.WithName()
	q := fmt.Sprintf(skeleton, sig, qry, sig)

	var rows *sql.Rows
	rows, err = d.db.Query(q)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	var types []*sql.ColumnType
	if types, err = rows.ColumnTypes(); err != nil {
		return nil, nil, nil, err
	}

	for ind := 0; ind < len(types); ind++ {
		fieldNames = append(fieldNames, types[ind].Name())

		k := types[ind].ScanType().Kind()
		fieldKinds = append(fieldKinds, k)
		fieldTypes = append(fieldTypes, kindToDataTypes(k))
	}

	return fieldNames, fieldTypes, fieldKinds, nil
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
	pos := position(dt.String(), d.dtTypes)
	if pos < 0 {
		return "", fmt.Errorf("cannot find type %s to map to DB type", dt.String())
	}

	return d.dbTypes[pos], nil
}

// **************** Helpers **************

func buildRow(k []reflect.Kind) []any {
	var ry []any
	for ind := 0; ind < len(k); ind++ {
		switch k[ind] {
		case reflect.Int:
			var x int
			ry = append(ry, &x)
		case reflect.Int8:
			var x int8
			ry = append(ry, &x)
		case reflect.Int16:
			var x int16
			ry = append(ry, &x)
		case reflect.Int32:
			var x int32
			ry = append(ry, &x)
		case reflect.Int64:
			var x int64
			ry = append(ry, &x)
		case reflect.Uint8:
			var x uint8
			ry = append(ry, &x)
		case reflect.Uint16:
			var x uint16
			ry = append(ry, &x)
		case reflect.Uint32:
			var x uint32
			ry = append(ry, &x)
		case reflect.Uint64:
			var x uint64
			ry = append(ry, &x)
		case reflect.Float64:
			var x float64
			ry = append(ry, &x)
		case reflect.Float32:
			var x float32
			ry = append(ry, &x)
		case reflect.String:
			var x string
			ry = append(ry, &x)
		case reflect.Struct:
			var x time.Time
			ry = append(ry, &x)
		default:
			panic(fmt.Errorf("unsupported data type: %v", k[ind]))
		}
	}

	return ry
}

func kindToDataTypes(k reflect.Kind) DataTypes {
	var dt DataTypes

	switch k {
	case reflect.Float64, reflect.Float32:
		dt = DTfloat
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dt = DTint
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dt = DTint
	case reflect.String:
		dt = DTstring
	case reflect.Struct:
		dt = DTdate
	case reflect.Ptr:
		panic(fmt.Errorf("field is nullable - not supported"))
	default:
		panic(fmt.Errorf("unsupported db field type: %v", k))
	}

	return dt
}

func castKind(r any, k reflect.Kind) any {
	var out any

	switch k {
	case reflect.Int:
		out = *(r.(*int))
	case reflect.Int32:
		out = int(*(r.(*int32)))
	case reflect.Int64:
		out = int(*(r.(*int64)))
	case reflect.Int8:
		out = int(*(r.(*int8)))
	case reflect.Int16:
		out = int(*(r.(*int16)))
	case reflect.Uint8:
		out = int(*(r.(*uint8)))
	case reflect.Uint16:
		out = int(*(r.(*uint16)))
	case reflect.Uint32:
		out = int(*(r.(*uint32)))
	case reflect.Uint64:
		out = int(*(r.(*uint64)))
	case reflect.Float64:
		out = *(r).(*float64)
	case reflect.Float32:
		out = float64(*(r).(*float32))
	case reflect.String:
		out = *(r).(*string)
	case reflect.Struct:
		out = *(r).(*time.Time)
	default:
		panic("unsupported type in Load")
	}

	return out
}

func loadFunctions(fns string) []*fnSpec {
	var m []*fnSpec
	specs := strings.Split(fns, "\n")
	for _, spec := range specs {
		details := strings.Split(spec, ":")
		if len(details) != 4 {
			continue
		}

		s := &fnSpec{
			Name:    details[0],
			SQL:     details[1],
			Inputs:  parseInputs(details[2]),
			Outputs: parseOutputs(details[3]),
		}

		m = append(m, s)
	}

	return m
}

func parseInputs(inp string) [][]DataTypes {
	var outDT [][]DataTypes
	dts := strings.Split(inp, "{")
	for ind := 1; ind < len(dts); ind++ {
		s := strings.ReplaceAll(dts[ind], "},", "")
		s = strings.ReplaceAll(s, "}", "")
		outDT = append(outDT, parseOutputs(s))
	}

	return outDT
}

func parseOutputs(outp string) []DataTypes {
	var outDT []DataTypes

	outs := strings.Split(outp, ",")
	for ind := 0; ind < len(outs); ind++ {
		outDT = append(outDT, DTFromString("DT"+outs[ind]))
	}

	return outDT
}
