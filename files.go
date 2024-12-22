package df

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

// can Save any DF to file
// can load any file to []any
// can have a GENERIC loader that takes an iter

// TODO: need to make a reader.... and SQL needs an insertRow?? No, if from file must load to MemDF first then DBSave

// All code interacting with files is here

// Defaults
var (
	Sep         = ','
	EOL         = '\n'
	StringDelim = '"'
	DateFormat  = "20060102"
	FloatFormat = "%.2f"
	Header      = true
	Strict      = false

	DefaultInt    = math.MaxInt
	DefaultFloat  = math.MaxFloat64
	DefaultDate   = time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	DefaultString = ""
)

type Files struct {
	EOL         byte
	Sep         byte
	StringDelim byte
	DateFormat  string
	FloatFormat string

	Header bool
	Peek   int
	Strict bool

	DefaultInt    int
	DefaultFloat  float64
	DefaultString string
	DefaultDate   time.Time

	fileName    string
	fieldNames  []string
	fieldTypes  []DataTypes
	fieldWidths []int

	lineWidth int
	file      *os.File
	rdr       *bufio.Reader
}

// TODO: need to add check for len(fieldWidths)
// params DON'T make sense if doing a save...
func NewFiles() *Files {
	f := &Files{
		EOL:           byte(EOL),
		Sep:           byte(Sep),
		StringDelim:   byte(StringDelim),
		DateFormat:    DateFormat,
		FloatFormat:   FloatFormat,
		Header:        Header,
		Strict:        Strict,
		DefaultInt:    DefaultInt,
		DefaultFloat:  DefaultFloat,
		DefaultDate:   DefaultDate,
		DefaultString: DefaultString,
	}

	return f
}

// ***************** Read Methods *****************

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

		for ind := 0; ind < len(vals); ind++ {
			var (
				dt DataTypes
				e3 error
			)
			if _, dt, e3 = BestType(vals[ind]); e3 != nil {
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
		if f.Peek > 0 && rn > f.Peek {
			break
		}
	}

	for ind := 0; ind < len(counts); ind++ {
		f.fieldTypes = append(f.fieldTypes, counts[ind].max())
	}

	_ = f.Close()
	return f.Open(f.fileName, f.FieldNames(), f.FieldTypes(), f.FieldWidths())
}

func (f *Files) Load() ([]*Vector, error) {
	defer func() { _ = f.Close() }()
	var memData []*Vector
	for ind := 0; ind < len(f.FieldNames()); ind++ {
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
		for ind := 0; ind < len(r); ind++ {
			memData[ind].Append(r[ind])
		}
	}

	return memData, nil
}

func (f *Files) Open(fileName string, fieldNames []string, fieldTypes []DataTypes, fieldWidths []int) error {
	var e error
	if fieldNames != nil && fieldTypes != nil && len(fieldNames) != len(fieldTypes) {
		return fmt.Errorf("fieldNames and fieldTypes not same length in Open")
	}

	if fieldNames != nil && fieldWidths != nil && len(fieldNames) != len(fieldWidths) {
		return fmt.Errorf("fieldNames and fieldWidths not same length in Open")
	}

	if fieldTypes != nil && fieldWidths != nil && len(fieldTypes) != len(fieldWidths) {
		return fmt.Errorf("fieldTypes and fieldWidths not same length in Open")
	}

	f.fileName, f.fieldNames = fileName, fieldNames
	f.fieldTypes, f.fieldWidths = fieldTypes, fieldWidths

	f.lineWidth = 0
	for _, w := range fieldWidths {
		f.lineWidth += w
	}

	if f.file, e = os.Open(fileName); e != nil {
		return e
	}

	f.rdr = bufio.NewReader(f.file)

	if f.FieldNames() == nil && !f.Header {
		return fmt.Errorf("no field names specified and no header")
	}

	// skip first line if field names are supplied
	if f.Header && f.FieldNames() != nil {
		if _, e1 := f.Read(); e1 != nil {
			return e1
		}
	}

	if f.FieldNames() == nil {
		if e1 := f.readHeader(); e1 != nil {
			return e1
		}
	}

	if f.FieldTypes() == nil {
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

	if f.FieldTypes() == nil {
		return vals, nil
	}
	var out []any

	for ind := 0; ind < len(f.FieldNames()); ind++ {
		var x any
		fld := vals[ind]

		dt := f.FieldTypes()[ind]
		v := f.smartTrim(fld, dt)
		if x = ToDataType(v, dt); x == nil {
			switch f.Strict {
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
	if f.EOL != 0 {
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
	if line, eOrEOF = f.rdr.ReadString(f.EOL); (eOrEOF == io.EOF && line == "") || (eOrEOF != nil && eOrEOF != io.EOF) {
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

	if e = f.writeHeader(df.ColumnNames()); e != nil {
		return e
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
	for ind := 0; ind < len(v); ind++ {
		var lx []byte
		switch d := v[ind].(type) {
		case float64:
			lx = []byte(fmt.Sprintf(f.FloatFormat, d))
		case int:
			lx = []byte(fmt.Sprintf("%v", d))
		case time.Time:
			lx = []byte(d.Format(f.DateFormat))
		case string:
			lx = []byte(d)
			if f.StringDelim != 0 {
				lx = append([]byte{f.StringDelim}, lx...)
				lx = append(lx, f.StringDelim)
			}
		case *float64:
			lx = []byte(fmt.Sprintf(f.FloatFormat, *d))
		case *int:
			lx = []byte(fmt.Sprintf("%v", *d))
		case *time.Time:
			lx = []byte(d.Format(f.DateFormat))
		case *string:
			lx = []byte(*d)
			if f.StringDelim != 0 {
				lx = append([]byte{f.StringDelim}, lx...)
				lx = append(lx, f.StringDelim)
			}
		default:
			lx = []byte("#err#")
		}
		line = append(line, lx...)
		if ind < len(v)-1 {
			line = append(line, f.Sep)
		}
	}

	if _, e := f.file.Write(line); e != nil {
		return e
	}

	_, e := f.file.Write([]byte{f.EOL})

	return e
}

func (f *Files) writeHeader(fieldNames []string) error {
	if !f.Header {
		return nil
	}

	// TODO: place stringDelim around these?
	if _, e := f.file.WriteString(strings.Join(fieldNames, string(rune(f.Sep))) + string(rune(f.EOL))); e != nil {
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

func (f *Files) defaultValue(dt DataTypes) any {
	switch dt {
	case DTint:
		return f.DefaultInt
	case DTfloat:
		return f.DefaultFloat
	case DTdate:
		return f.DefaultDate
	case DTstring:
		return f.DefaultString
	default:
		panic(fmt.Errorf("unsupported data type in files"))
	}
}

func (f *Files) dropEOL(line string) string {
	if line[len(line)-1] == f.EOL {
		return line[0 : len(line)-1]
	}

	return line
}

func (f *Files) smartTrim(line string, dt DataTypes) string {
	if dt != DTstring {
		return strings.Trim(line, " ")
	}

	x := strings.Trim(line, string(f.StringDelim)+" ")
	return x
}

func (f *Files) splitSep(line string) []string {
	if !strings.Contains(line, string(f.StringDelim)) {
		return strings.Split(line, string(f.Sep))
	}

	var split []string
	in := false
	start := 0
	for ind := 0; ind < len(line); ind++ {
		if f.StringDelim != 0 && line[ind] == f.StringDelim {
			in = !in
		}

		if !in && line[ind] == f.Sep {
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
	for ind := 0; ind < len(f.FieldWidths()); ind++ {
		fld := strings.Trim(string(b[start:start+f.FieldWidths()[ind]]), " ")
		out = append(out, fld)
		start += f.FieldWidths()[ind]
	}

	return out
}

// ****************** used by detect *********************

type ctr struct {
	cInt    int
	cFloat  int
	cDate   int
	cString int
}

func (c *ctr) max() DataTypes {
	switch m := MaxInt(c.cInt, c.cFloat, c.cDate, c.cString); m {
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
