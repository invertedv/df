package testing

import (
	"database/sql"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
)

// THINK about...how self interacts in context...
// CONSIDER making .Data fetch the data for sql....
const (
	fileName   = "test.csv"
	fileNameW1 = "testFW.txt"
	fileNameW2 = "testFW1.txt"
	fileNameW3 = "testFW2.txt"
	inTable    = "testing.d1"
	outTable   = "testing.test"

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
	return []string{mem} //, ch}
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

func loadData(pkg string) d.DF {
	const table = "SELECT * FROM " + inTable
	var db *sql.DB

	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")

	switch pkg {
	case ch, mem:
		db = newConnectCH(host, user, password)
	default:
		panic("unsupported database")
	}

	var (
		dialect *d.Dialect
		e       error
	)
	if dialect, e = d.NewDialect("clickhouse", db); e != nil {
		panic(e)
	}

	ctx := d.NewContext(dialect, nil, nil)

	/*if pkg != mem {
		var (
			df *s.DF
			e1 error
		)
		if df, e1 = s.DBload(table, ctx); e1 != nil {
			panic(e1)
		}
		return df
	}*/

	var (
		df *m.DF
		e2 error
	)
	if df, e2 = m.DBLoad(table, dialect); e2 != nil {
		panic(e2)
	}

	//	df.SetContext(ctx)
	d.DFcontext(ctx)(df.Core())
	df.Context().SetSelf(df)

	return df
}
