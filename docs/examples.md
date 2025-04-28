---
layout: default
title: Examples
nav_order: 3
---

## df Examples
{: .no_toc }
{: .fs-6 .fw-300 }

Note that the files df/mem/mem_test.go and df/sql/sql_test.go have examples.

### Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}
---



### Example 1: Creating a mem/df *DF from a CSV

The Files struct handles all file IO.   

- Files works with delimited files and fixed record-length files.
- The user can specify field types or files can impute them.
- The user can replace missing values with defaults or generate an error.
- Is flexible about field separators, date formats, string delimiters and EOL character.

The code below defines a file and reads it into an *m.DF.

        import (
            d "github.com/invertedv/df"
            m "github.com/invertedv/df/mem"
        )

        var (
            f  *d.Files
            e1 error
        )

        // Create a new Files struct
    	if f, e1 = d.NewFiles(); e1 != nil {
	    	panic(e1)
	    }

    	if ex := f.Open("YourFile"); ex != nil {
	    	panic(ex)
	    }

        // Load mem *DF dataframe from file
	    var (
		    df *m.DF
		    e2 error
	    )
	    if df, e2 = m.FileLoad(f); e2 != nil {
		    panic(e2)
	    }


### Example 2: Creating a df/sql *DF from an SQL query


        import (
            d "github.com/invertedv/df"
            s "github.com/invertedv/df/sql"
        )

    	table := "SELECT * FROM myTable"

	    var (
            dialect *d.Dialect
            e       error
	    )

        // db is a connector to a ClickHouse DB
    	if dialect, e = d.NewDialect("clickhouse", db); e != nil {
	    	panic(e)
	    }

        var (
            df d.DF
            e1 error
	    )

        // df is a *s.DF.  Note that the data is not in memory.
    	if df, e1 = s.DBload(table, dialect); e1 != nil {
            panic(e1)
            }

### Example 3: Creating a df/mem *DF from an SQL query

Note that the code flow here is indentical to Example 2 but that DBload is called from the df/mem
package rather than df/sql.

        import (
            d "github.com/invertedv/df"
            m "github.com/invertedv/df/mem"
        )

    	table := "SELECT * FROM myTable"

	    var (
            dialect *d.Dialect
            e       error
	    )

        // db is a connector to a ClickHouse DB
    	if dialect, e = d.NewDialect("clickhouse", db); e != nil {
	    	panic(e)
	    }

        var (
            df d.DF
            e1 error
	    )

        // df is a *m.DF.  Note that the data is in memory.
        if df, e1 = m.DBload(table, dialect); e1 != nil {
            panic(e1)
        }

### Example 4: Saving to a Permanent Table (Postgres).

Suppose we have a dataframe, df, and we wish to save it as a Postgres table. 

    var (
        ts string     // TableSpace name
        owner string  // table owner
        indx          // table index name
        key           // name of column to be the key
        table         // name of table
    )
    
	opt1 := "IndexName:i111"
	opt2 := fmt.Sprintf("TableSpace:%s", os.Getenv("tablespace"))
	opt3 := fmt.Sprintf("Owner:%s", os.Getenv("user"))
	opts = []string{opt1, opt2, opt3}

	e := dlct.Save(table, key, true, false, df, opts...)

This code will work for both a mem and sql *DF.

### Example 5: Saving to a Permanent Table (ClickHouse).

Suppose we have a dataframe, df, and we wish to save it as a ClickHouse table. 

    var (
        key           // name of column to be the key
        table         // name of table qualified by database
    )
    
	e := dlct.Save(table, key, true, false, df, opts...)

This code will work for both a mem and sql *DF.

### Example 6: Saving to a Temporary Table (Postgres & ClickHouse)

Suppose we have a dataframe, df, and we wish to save it as a temporary table. 

    var (
        key           // name of column to be the key
        table         // name of table qualified by database
    )
    
	e := dlct.Save(table, key, true, true, df)

### Example 7: The By Method

The By method creates a new DF by aggregating along the values of column(s) and running
a user-defined set of functions. The SQL analogue is "GROUP BY". The signature for the By method is:

	By(groupBy string, fns ...string) (DF, error)

The groupBy string is a comma-separated list of columns on which to group.  The fns variadic are functions
to pass to Parse, where the calculations are performed on each group.  The code below calculates the mean and
sum of for each value of x. The output df, dfBy, has 3 columns: x, my and sy.

	dfBy, e := df.By("x", "my := mean(y)", "sy := sum(y)")


Within a group, you can also run a calculation against the entire dataframe. The column "total" will have the same value
for every row -- the total number of rows in df. Hence, "prop" will be the percentage of the total dataframe in 
each combination of the levels of "a" and "b".

	dfBy, e := df.By("a,b", "cnt := count(x)","total := count(global(x))", "prop := 100.0 * float(cnt)/float(total)")

If you want to create a summary of the entire dataframe, use "" as the groupBy. The code

	dfBy, e := df.By("", "n := count(x)", "xbar := mean(x)", "std := std(x)")

returns a dataframe with one row and 3 columns - the number of rows (n) and the sample mean (xbar) and
standard deviation (std) of x.

### Example 8: The Join Method.

The signature of the Join method is:

	Join(df HasIter, joinOn string) (DF, error)

The Join method is an inner join.

df is any type that implements HasIter:

    type HasIter interface {
	    AllRows() iter.Seq2[int, []any]
    }

The interfaces DF and Column satisfy this.  

The code below joins df1 and df2 on "x".  Th

	dfJoin, e := df1.Join(df2, "x")

If df1 and df2 have columns with the same name (other than the join columns), 
the overlapping names in df2 have "DUP" appending to their name.

### Example 9: The Where Method.

The signature of the Where method is:

	Where(condition string) (DF, error)

The condition argument is a logical expression that can be evaluated by Parse. For example,

    dfSubset, e := df.Where(x > 3.0 || a == 'yes')

subsets df to rows where x > 3.0 or a == 'yes'.

### Example 10: The Parse Function.


The code below will run whether *DF is a df/mem or df/sql *DF.


        import d "github.com/invertedv/df"

        if ex:=d.Parser(df, "y := (a+b) * (c-d)"); ex!=nil {
            panic(ex)
        }

        var (
            data []float64
            e error
        )
        if data, e = df.Column("y").Data().AsFloat(); e!=nil {
            panic(e)
        }

        fmt.Println(data)

If *DF is a df/sql struct, then the line

        data := df.Column("y").Data().AsAny()

will form and execute a query to run the calculation and return the result.
