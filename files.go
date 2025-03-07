package df

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
)

// can Save any DF to file
// can load any file to []any
// can have a GENERIC loader that takes an iter

type Files struct {
	eol         byte
	sep         byte
	stringDelim byte
	dateFormat  string
	floatFormat string

	header bool
	peek   int
	strict bool

	defaultInt    int
	defaultFloat  float64
	defaultString string
	defaultDate   time.Time

	fileName    string
	fieldNames  []string
	fieldTypes  []DataTypes
	fieldWidths []int

	lineWidth int
	file      *os.File
	rdr       *bufio.Reader
}

func NewFiles(opts ...FileOpt) (*Files, error) {
	f := &Files{
		eol:           byte('\n'),
		sep:           byte(','),
		stringDelim:   byte('"'),
		dateFormat:    "20060102",
		floatFormat:   "%.2f",
		header:        true,
		strict:        false,
		defaultInt:    math.MaxInt,
		defaultFloat:  math.MaxFloat64,
		defaultDate:   time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
		defaultString: "",
	}

	for _, opt := range opts {
		if e := opt(f); e != nil {
			return nil, e
		}
	}

	return f, nil
}

// ***************** Setters *****************

type FileOpt func(f *Files) error

func FileDefaultDate(year, mon, day int) FileOpt {
	return func(f *Files) error {
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
		f.defaultDate = t

		return nil
	}
}

// FileDefaultInt sets the value to use for fields that fail to convert to integer if strict=false.
func FileDefaultInt(deflt int) FileOpt {
	return func(f *Files) error {
		f.defaultInt = deflt

		return nil
	}
}

func FileDefaultFloat(deflt float64) FileOpt {
	return func(f *Files) error {
		f.defaultFloat = deflt

		return nil
	}
}

func FileDefaultString(deflt string) FileOpt {
	return func(f *Files) error {
		f.defaultString = deflt

		return nil
	}
}

func FileDateFormat(format string) FileOpt {
	return func(f *Files) error {
		if !Has(format, DateFormats) {
			return fmt.Errorf("invalid date format: %s", format)
		}

		f.dateFormat = format

		return nil
	}
}

func FileEOL(eol byte) FileOpt {
	return func(f *Files) error {
		f.eol = eol

		return nil
	}
}

func FileFieldNames(fieldNames []string) FileOpt {
	return func(f *Files) error {
		for _, fn := range fieldNames {
			if e := validName(fn); e != nil {
				return e
			}
		}

		f.fieldNames = fieldNames

		return nil
	}
}

func FileFieldTypes(fieldTypes []DataTypes) FileOpt {
	return func(f *Files) error {
		f.fieldTypes = fieldTypes

		return nil
	}
}

func FileFieldWidths(fieldWidths []int) FileOpt {
	return func(f *Files) error {
		tot := 0
		for _, fw := range fieldWidths {
			tot += fw
			if fw <= 0 {
				return fmt.Errorf("fieldwidths must be positive")
			}
		}

		f.fieldWidths = fieldWidths
		f.lineWidth = tot

		return nil
	}
}

func FileFloatFormat(format string) FileOpt {
	return func(f *Files) error {
		if ok, _ := regexp.MatchString("%[0-9]?[0-9]?.[0-9]?[0-9]?f", format); !ok {
			return fmt.Errorf("invalid float format: %s", format)
		}

		f.floatFormat = format

		return nil
	}
}

func FileHeader(hasHeader bool) FileOpt {
	return func(f *Files) error {
		f.header = hasHeader

		return nil
	}
}

func FilePeek(linesToPeek int) FileOpt {
	return func(f *Files) error {
		if linesToPeek < 0 {
			return fmt.Errorf("file peek value cannot be negative")
		}

		f.peek = linesToPeek

		return nil
	}
}

func FileSep(sep byte) FileOpt {
	return func(f *Files) error {
		f.sep = sep

		return nil
	}
}

// FileStrict sets the action when a field fails to convert to its expected type.
// If true, then an error results.
// If false, the default value is substituted.
// Default: false
func FileStrict(strict bool) FileOpt {
	return func(f *Files) error {
		f.strict = strict

		return nil
	}
}

func FileStringDelim(delim byte) FileOpt {
	return func(f *Files) error {
		f.stringDelim = delim

		return nil
	}
}

// ***************** Read Methods *****************

func (f *Files) Load() ([]*Vector, error) {
	defer func() { _ = f.Close() }()
	var memData []*Vector
	for ind := range  len(f.FieldNames()) {
		memData = append(memData, MakeVector(f.FieldTypes()[ind], 0))
	}

	for {
		var (
			row any
			e1  error
		)

		if row, e1 = f.Read(); e1 != nil {
			if e1 == io.EOF {
				break
			}

			return nil, e1
		}

		r := row.([]any)
		for ind := range len(r){
			if e := memData[ind].Append(r[ind]); e != nil {
				return nil, e
			}
		}
	}

	return memData, nil
}

func (f *Files) Open(fileName string) error { //, fieldNames []string, fieldTypes []DataTypes, fieldWidths []int) error {
	var e error
	if f.fieldNames != nil && f.fieldTypes != nil && len(f.fieldNames) != len(f.fieldTypes) {
		return fmt.Errorf("fieldNames and fieldTypes not same length in Open")
	}

	if f.fieldNames != nil && f.fieldWidths != nil && len(f.fieldNames) != len(f.fieldWidths) {
		return fmt.Errorf("fieldNames and fieldWidths not same length in Open")
	}

	if f.fieldTypes != nil && f.fieldWidths != nil && len(f.fieldTypes) != len(f.fieldWidths) {
		return fmt.Errorf("fieldTypes and fieldWidths not same length in Open")
	}

	f.fileName = fileName

	if f.file, e = os.Open(fileName); e != nil {
		return e
	}

	f.rdr = bufio.NewReader(f.file)

	if len(f.FieldNames()) == 0 && !f.header {
		return fmt.Errorf("no field names specified and no header")
	}

	// skip first line if field names are supplied
	if f.header && f.FieldNames() != nil {
		if _, e1 := f.Read(); e1 != nil {
			return e1
		}
	}

	if len(f.FieldNames()) == 0 {
		if e1 := f.readHeader(); e1 != nil {
			return e1
		}
	}

	if len(f.FieldTypes()) == 0 {
		if e2 := f.detect(); e2 != nil {
			return e2
		}
	}

	if len(f.FieldTypes()) != len(f.FieldNames()) {
		return fmt.Errorf("field names and field types aren't same length")
	}

	if f.FieldWidths() != nil && len(f.FieldWidths()) != len(f.FieldNames()) {
		return fmt.Errorf("field widths and field names aren't the same length")
	}

	return nil
}

func (f *Files) Read() (any, error) {
	var (
		vals []string
		e    error
	)
	if f.FieldWidths() != nil {
		if vals, e = f.readFixed(); e != nil {
			return nil, e
		}
	} else {
		if vals, e = f.readSep(); e != nil {
			return nil, e
		}
	}

	if len(f.FieldTypes()) == 0 {
		return vals, nil
	}
	var out []any

	for ind := range len(f.FieldNames()) {
		var (
			x  any
			ok bool
		)
		fld := vals[ind]

		dt := f.FieldTypes()[ind]
		v := f.smartTrim(fld, dt)
		if x, ok = toDataType(v, dt); !ok {
			switch f.strict {
			case true:
				return nil, fmt.Errorf("conversion failed in Files.Read")
			case false:
				x = f.defaultValue(f.FieldTypes()[ind])
			}
		}

		out = append(out, x)
	}

	return out, nil
}

func (f *Files) readFixed() ([]string, error) {
	adder := 0
	if f.eol != 0 {
		adder = 1
	}

	b := make([]byte, f.lineWidth+adder)
	n, eOrEOF := f.file.Read(b)

	if n == f.lineWidth+adder {
		return f.splitFixed(b), nil
	}

	if eOrEOF == nil {
		return nil, io.EOF
	}

	return nil, eOrEOF
}

func (f *Files) readHeader() error {
	var (
		x any
		e error
	)

	if x, e = f.Read(); e != nil {
		return e
	}

	f.fieldNames = x.([]string)

	return nil
}

func (f *Files) readSep() ([]string, error) {
	var (
		line   string
		eOrEOF error
	)
	if line, eOrEOF = f.rdr.ReadString(f.eol); (eOrEOF == io.EOF && line == "") || (eOrEOF != nil && eOrEOF != io.EOF) {
		return nil, eOrEOF
	}

	var vals []string
	if vals = f.splitSep(f.dropEOL(line)); f.FieldNames() != nil && len(vals) != len(f.FieldNames()) {
		return nil, fmt.Errorf("line %s has wrong number of fields", line)
	}

	return vals, nil
}

// ***************** Write Methods *****************

func (f *Files) Create(fileName string) error {
	var e error

	f.fileName = fileName
	f.file, e = os.Create(fileName)

	return e
}

func (f *Files) Save(fileName string, df DF) error {
	defer func() { _ = f.Close() }()
	f.fieldNames = df.ColumnNames()
	f.fieldTypes, _ = df.ColumnTypes()

	var e error
	if f.file, e = os.Create(fileName); e != nil {
		return e
	}

	if ex := f.writeHeader(df.ColumnNames()); ex != nil {
		return ex
	}

	for row, eof := df.Iter(true); eof == nil; row, eof = df.Iter(false) {
		if ex := f.Write(row); ex != nil {
			return ex
		}
	}

	return nil
}

func (f *Files) Write(v []any) error {
	var line []byte
	for ind :=range len(v){
		var lx []byte

		//		var z any = *v[ind].(*any)
		switch d := v[ind].(type) {
		case float64:
			lx = []byte(fmt.Sprintf(f.floatFormat, d))
		case int, int8, int16, int32, int64:
			lx = []byte(fmt.Sprintf("%v", d))
		case time.Time:
			lx = []byte(d.Format(f.dateFormat))
		case string:
			lx = []byte(d)
			if f.stringDelim != 0 {
				lx = append([]byte{f.stringDelim}, lx...)
				lx = append(lx, f.stringDelim)
			}
		case *float64:
			lx = []byte(fmt.Sprintf(f.floatFormat, *d))
		case *int64:
			lx = []byte(fmt.Sprintf("%v", *d))
		case *time.Time:
			lx = []byte(d.Format(f.dateFormat))
		case *string:
			lx = []byte(*d)
			if f.stringDelim != 0 {
				lx = append([]byte{f.stringDelim}, lx...)
				lx = append(lx, f.stringDelim)
			}
		default:
			lx = []byte("#err#")
		}
		line = append(line, lx...)
		if ind < len(v)-1 {
			line = append(line, f.sep)
		}
	}

	if _, e := f.file.Write(line); e != nil {
		return e
	}

	_, e := f.file.Write([]byte{f.eol})

	return e
}

func (f *Files) writeHeader(fieldNames []string) error {
	if !f.header {
		return nil
	}

	if _, e := f.file.WriteString(strings.Join(fieldNames, string(rune(f.sep))) + string(rune(f.eol))); e != nil {
		return e
	}

	return nil
}

// ***************** Other Methods *****************

func (f *Files) Close() error {
	if f.file != nil {
		return f.file.Close()
	}

	return nil
}

func (f *Files) FieldNames() []string {
	return f.fieldNames
}

func (f *Files) FieldTypes() []DataTypes {
	return f.fieldTypes
}

func (f *Files) FieldWidths() []int {
	return f.fieldWidths
}

// ***************** Unexported Methods *****************

func (f *Files) defaultValue(dt DataTypes) any {
	switch dt {
	case DTint:
		return f.defaultInt
	case DTfloat:
		return f.defaultFloat
	case DTdate:
		return f.defaultDate
	case DTstring:
		return f.defaultString
	default:
		panic(fmt.Errorf("unsupported data type in files"))
	}
}

func (f *Files) detect() error {
	counts := make([]*ctr, 0)

	rn := 0
	for {
		var (
			v    any
			vals []string
			e2   error
		)

		if v, e2 = f.Read(); e2 != nil {
			if e2 == io.EOF {
				break
			}

			return e2
		}

		vals = v.([]string)

		if len(vals) != len(f.FieldNames()) {
			return fmt.Errorf("inconsistent # of fields in file")
		}

		for ind :=range len(vals) {
			var (
				dt DataTypes
				e3 error
			)
			if _, dt, e3 = bestType(vals[ind], true); e3 != nil {
				return e3
			}

			if len(counts) < ind+1 {
				counts = append(counts, &ctr{})
			}

			switch dt {
			case DTint:
				counts[ind].cInt++
			case DTfloat:
				counts[ind].cFloat++
			case DTdate:
				counts[ind].cDate++
			default:
				counts[ind].cString++
			}
		}

		rn++
		if f.peek > 0 && rn > f.peek {
			break
		}
	}

	for ind := range len(counts) {
		f.fieldTypes = append(f.fieldTypes, counts[ind].max())
	}

	_ = f.Close()
	return f.Open(f.fileName)
}

func (f *Files) dropEOL(line string) string {
	if line[len(line)-1] == f.eol {
		return line[0 : len(line)-1]
	}

	return line
}

func (f *Files) smartTrim(line string, dt DataTypes) string {
	if dt != DTstring {
		return strings.Trim(line, " ")
	}

	x := strings.Trim(line, string(f.stringDelim)+" ")
	return x
}

func (f *Files) splitSep(line string) []string {
	if !strings.Contains(line, string(f.stringDelim)) {
		return strings.Split(line, string(f.sep))
	}

	var split []string
	in := false
	start := 0
	for ind := range len(line) {
		if f.stringDelim != 0 && line[ind] == f.stringDelim {
			in = !in
		}

		if !in && line[ind] == f.sep {
			split = append(split, line[start:ind])
			start = ind + 1
		}
	}

	split = append(split, line[start:])

	return split
}

func (f *Files) splitFixed(b []byte) []string {
	var out []string
	start := 0
	for ind := range len(f.FieldWidths()) {
		fld := strings.Trim(string(b[start:start+f.FieldWidths()[ind]]), " ")
		out = append(out, fld)
		start += f.FieldWidths()[ind]
	}

	return out
}

// ****************** Used by detect *********************

type ctr struct {
	cInt    int
	cFloat  int
	cDate   int
	cString int
}

func (c *ctr) max() DataTypes {
	switch m := maxInt(c.cInt, c.cFloat, c.cDate, c.cString); m {
	case c.cDate:
		return DTdate
	case c.cInt:
		return DTint
	case c.cFloat:
		return DTfloat
	default:
		return DTstring
	}
}

// maxInt returns the maximum of ints
func maxInt(ints ...int) int {
	mx := ints[0]
	for _, i := range ints {
		if i > mx {
			mx = i
		}
	}

	return mx
}
