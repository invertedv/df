package df

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// All code interacting with files is here

const (
	Sep         = ','
	EOL         = '\n'
	StringDelim = '"'
	DateFormat  = "20060102"
	FloatFormat = "%.2f"
	Header      = true
)

type Files struct {
	FieldNames  []string
	EOL         byte
	Sep         byte
	StringDelim byte
	DateFormat  string
	FloatFormat string
	Header      bool

	file     *os.File
	fileName string
}

func NewFiles() *Files {
	f := &Files{
		EOL:         byte(EOL),
		Sep:         byte(Sep),
		StringDelim: byte(StringDelim),
		DateFormat:  DateFormat,
		FloatFormat: FloatFormat,
		Header:      Header,
	}

	return f
}

func (f *Files) Open(fileName string) error {
	var e error
	f.fileName = fileName
	f.file, e = os.Open(fileName)

	return e
}

func (f *Files) Create(fileName string) error {
	var e error
	f.fileName = fileName
	f.file, e = os.Create(fileName)

	return e
}

func (f *Files) FileName() string {
	return f.fileName
}

func (f *Files) Close() error {
	if f.file != nil {
		return f.file.Close()
	}

	return fmt.Errorf("no open files")
}

func (f *Files) WriteLine(v []any) error {
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
			lx = append([]byte{f.StringDelim}, lx...)
			lx = append(lx, f.StringDelim)
		case *float64:
			lx = []byte(fmt.Sprintf(f.FloatFormat, *d))
		case *int:
			lx = []byte(fmt.Sprintf("%v", *d))
		case *time.Time:
			lx = []byte(d.Format(f.DateFormat))
		case *string:
			lx = []byte(*d)
			lx = append([]byte{f.StringDelim}, lx...)
			lx = append(lx, f.StringDelim)
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

func (f *Files) WriteHeader() error {
	if !f.Header {
		return nil
	}

	if f.FieldNames == nil {
		return fmt.Errorf("field names or types not set in *Files")
	}

	_, e := f.file.WriteString(strings.Join(f.FieldNames, string(rune(f.Sep))) + string(rune(f.EOL)))

	return e
}
