package testing

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
	s "github.com/invertedv/df/sql"
	_ "github.com/jackc/pgx/stdlib"
)

// CONSIDER making .Data fetch the data for sql....
const (
	fileName   = "test.csv"
	fileNameW1 = "testFW.txt"
	fileNameW2 = "testFW1.txt"
	fileNameW3 = "testFW2.txt"
	inTableCH  = "testing.d1"
	inTablePG  = "d1"
	outTableCH = "testing.test"
	outTablePG = "public.test"

	pg  = "postgres"
	ch  = "clickhouse"
	mem = "mem"
)

// environment variables:
//   - host ClickHouse IP address
//   - user ClickHouse user
//   - password: ClickHouse password
//   - datapath: path to data directory in this project

// list of packages to test
func pkgs() []string {
	return []string{pg, mem, ch} //, mem}
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

func loadData(pkg string) d.DF {
	var (
		table string
		db    *sql.DB
	)

	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	dbName := os.Getenv("db")

	var dialectName string
	switch pkg {
	case mem:
		db = newConnectPG(host, user, password, dbName)
		dialectName = pg
		table = "SELECT * FROM " + inTablePG
	case ch:
		db = newConnectCH(host, user, password)
		dialectName = ch
		table = "SELECT * FROM " + inTableCH
	case pg:
		db = newConnectPG(host, user, password, dbName)
		dialectName = pg
		table = "SELECT * FROM " + inTablePG
	default:
		panic("unsupported data source")
	}

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
	if pkg == mem {
		if df, e2 = m.DBLoad(table, dialect); e2 != nil {
			panic(e2)
		}

		_ = d.DFdialect(dialect)(df.Core())

		return df
	}

	if df, e2 = s.DBload(table, dialect); e2 != nil {
		panic(e2)
	}

	return df
}
