package df

import (
	"fmt"
	"strings"

	u "github.com/invertedv/utilities"
)

func DBTypes(list string) (DTtypes, DBtypes []string, err error) {
	l := strings.Split(list, "\n")
	for _, lm := range l {
		if strings.Trim(lm, " ") == "" {
			continue
		}

		t := strings.Split(lm, ",")
		if len(t) != 2 {
			return nil, nil, fmt.Errorf("cannot parse DBTypes line %s", lm)
		}

		if DTFromString(t[0]) == DTunknown {
			return nil, nil, fmt.Errorf("cannot parse %s into DataTypes", t[0])
		}

		DTtypes = append(DTtypes, t[0])
		DBtypes = append(DBtypes, t[1])
	}

	return DTtypes, DBtypes, nil
}

func CreateStatement(tableName, dbName string, fields []string, types []DataTypes, orderBy ...string) (string, error) {
	var (
		e                error
		dtTypes, dbTypes []string
		cSk, tSk, fSk    string
	)

	if orderBy == nil {
		orderBy = []string{fields[0]}
	}

	for _, o := range orderBy {
		if !u.Has(o, "", fields...) {
			return "", fmt.Errorf("orderBy/key field %s not in table", o)
		}
	}

	if cSk, tSk, fSk, e = Skeletons(dbName); e != nil {
		return "", e
	}

	if dtTypes, dbTypes, e = DBTypes(tSk); e != nil {
		return "", e
	}

	create := strings.Replace(cSk, "?TableName", tableName, 1)
	create = strings.Replace(create, "?OrderBy", strings.Join(orderBy, ","), 1)

	var flds []string
	for ind := 0; ind < len(fields); ind++ {
		var pos int
		if pos = u.Position(types[ind].String(), "", dtTypes...); pos < 0 {
			return "", fmt.Errorf("unknown data type %v in CreateTable", types[ind])
		}
		field := strings.Replace(fSk, "?Field", fields[ind], 1)
		field = strings.Replace(field, "?Type", dbTypes[pos], 1)
		flds = append(flds, field)
	}

	create = strings.Replace(create, "?fields", strings.Join(flds, ","), 1)
	fmt.Println(create)

	return create, nil
}
