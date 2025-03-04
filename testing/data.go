package testing

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
	s "github.com/invertedv/df/sql"
	_ "github.com/jackc/pgx/stdlib"
)

const (
	fileName   = "test.csv"
	fileNameW1 = "testFW.txt"
	fileNameW2 = "testFW1.txt"
	fileNameW3 = "testFW2.txt"
	sources    = "d1"

	pg  = "postgres"
	ch  = "clickhouse"
	mem = "mem"
)

// environment variables:
//   - host ClickHouse IP address
//   - user ClickHouse user
//   - password: ClickHouse password
//   - datapath: path to data directory in this project (df/data)
//   - tablespace: Postgres tablespace
//   - chTemp - name of Clickhouse temp table to use in testing
//   - pgTemp - name of Postgres temp table to use in testing

// list of packages to test
func pkgs() []string {
	dbs := []string{pg, mem, ch}
	srcs := strings.Split(sources, ",")
	var choices []string
	for _, db := range dbs {
		for _, src := range srcs {
			choices = append(choices, db+","+src)
		}
	}

	return choices
}

// NewConnect established a new connection to ClickHouse.
// host is IP address (assumes port 9000), memory is max_memory_usage
func newConnectCH(host, user, password string) *sql.DB {
	db := clickhouse.OpenDB(
		&clickhouse.Options{
			Addr: []string{host + ":9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: user,
				Password: password,
			},
			DialTimeout: 300 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
				Level:  0,
			},
		})

	if e := db.Ping(); e != nil {
		panic(e)
	}

	return db
}

func newConnectPG(host, user, password, dbName string) *sql.DB {
	connectionStr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", user, password, host, dbName)
	var (
		db *sql.DB
		e  error
	)
	if db, e = sql.Open("pgx", connectionStr); e != nil {
		panic(e)
	}

	if e := db.Ping(); e != nil {
		panic(e)
	}

	return db
}

func loadFile(fileName string, opts ...d.FileOpt) d.DF {
	var (
		f  *d.Files
		e3 error
	)
	if f, e3 = d.NewFiles(opts...); e3 != nil {
		panic(e3)
	}

	if e := f.Open(os.Getenv("datapath") + fileName); e != nil {
		panic(e)
	}

	var (
		df *m.DF
		e4 error
	)
	if df, e4 = m.FileLoad(f); e4 != nil {
		panic(e4)
	}

	return df
}

func loadData(which string) d.DF {
	var (
		tableName   string
		dialectName string
		db          *sql.DB
	)

	lr := strings.Split(which, ",")
	pkg, sName := lr[0], lr[1]
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	dbName := os.Getenv("db")

	switch pkg {
	case mem:
		df := loadFile(sName + ".csv")
		db = newConnectPG(host, user, password, dbName)
		dl, _ := d.NewDialect(pg, db)
		_ = d.DFdialect(dl)(df)
		return df
	case ch:
		const dbName = "testing"
		db = newConnectCH(host, user, password)
		dialectName = ch
		tableName = dbName + "." + sName
	case pg:
		db = newConnectPG(host, user, password, dbName)
		dialectName = pg
		tableName = sName
	default:
		panic("unsupported data source")
	}

	table := "SELECT * FROM " + tableName

	var (
		dialect *d.Dialect
		e       error
	)
	if dialect, e = d.NewDialect(dialectName, db); e != nil {
		panic(e)
	}

	var (
		df d.DF
		e2 error
	)

	// if data doesn't load, create the table from the csv
	if df, e2 = s.DBload(table, dialect); e2 != nil {
		dfl := loadFile(sName + ".csv")

		var opts []string
		if pkg == pg {
			opts = append(opts, "Owner:"+user)
			ts := os.Getenv("tablespace")
			opts = append(opts, "TableSpace:"+ts)
		}

		if e := dialect.Save(tableName, "k", true, dfl, opts...); e != nil {
			panic(e)
		}

		if df, e2 = s.DBload(table, dialect); e2 != nil {
			panic(e2)
		}

	}

	return df
}

// slash adds a trailing slash if inStr doesn't end in a slash
func slash(inStr string) string {
	if inStr[len(inStr)-1] == '/' {
		return inStr
	}

	return inStr + "/"
}
