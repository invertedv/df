package df

import (
	"database/sql"
	_ "embed"
	"fmt"
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
	switch strings.ToLower(dialect) {
	case "clickhouse":
		d.create, d.fields, d.dropIf = chCreate, chFields, chDropIf
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
			return nil, nil
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
