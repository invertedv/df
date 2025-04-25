---
layout: default
title: Home
nav_order: 1
---

<div style="text-align: center; font-size: 40px; color: darkgreen" >
  <img src="{{ site.baseurl }}/images/vee1c.png" width="150" height="150" class="center" /><br>
InvertedV
</div>

### Overview 

Dataframes are a common object used to hold and manipulate data for analysis. Conceptually, a dataframe consists of a set of columns. Columns, in turn, are arrays of values which are of a common type. The df package provides a flexible and extensable implementation
of dataframes. 

The df package consists of a main package, df, and two sub-packages, df/mem and df/sql.  The main package:

- defines the DF and Column interfaces.
- implements core aspects of those interfaces.
- provides a parser to evaluate expressions.
- handles file and DB I/O.

Packages df/mem and df/sql implement the full DF and Column interfaces for in-memory data and SQL databases, respectively. The distinction
between df/mem and df/sql is not the source of the data. Package mem/DF dataframes can be read from/saved to a database, for example. The distinction is where the calculations and manipulations are performed.  The df/mem package does this work in memory, while the df/sql performs it in the database.  


**Flexible**

A key aspect of the DF and Column interfaces is that they are agnostic as to the mechanisms of handling the underlying data.
Multiple implementations are possible in which the data infrastructure varies, yet the same Go code will work across them.

The parser allows flexible specification of expressions that return a column result.  The parser will work on any type that
satisfies the DF interface*.

    Parse(df, "y := exp(yhat) / (1.0 + exp(yhat))")
    Parse(df, "r := if(k==1,a,b)")
    Parse(df, "xNorm := x / global(sum(x))")
    Parse(df, "zCat := cat(z)")

*Note: the specific implementation must also provide to the parser implementations of functions, such as "sum".  The df/mem and
df/sql packages offer identical function sets.

**Extensible**

The package may be extended in several directions:
- The user may add their own functions functions to the parser.
- Additional database types can be added to the sql package.  This is done by adding support for the new DB type in the Dialect struct.
The sql package would not need to be modified.

### Package Details
**df**

The df package defines the DF and Column interfaces in two steps: a "core" (DC, CC, respectively) and "full" interface (DF, Column).  The core interface defines those methods which are independent of the details of the data architecture (*e.g.* drop columns from DF, Column name). The df package provides structs that implement the core DF and Column interfaces (DFcore, ColCore).


**df/mem**

The df/mem package implements the DF and Column interfaces for in-memory objects.

**df/sql**

The package df/sql implements the DF and Column interfaces for SQL databases. It relies on the methods of Dialect to handle the specifics
of any particular database type.

A basic design philosophy of this package is that the the storage mechanism of the data doesn't matter. A complication is that, though two database packages may use SQL, the details are likely to differ. The Dialect struct and its methods abstract these differences away.  Those methods handle the diffences between databases, hence each DB must specifically be handled there. Currently, Clickhouse and Postgres are supported. Dialect uses the standard Go sql package connector.  All communication with databases occurs through Dialect.

**Data Types**
Four data types are supported for column elements. By and large, most of the data statisticians work with is covered by these types. The four types are:

- float
- int
- string
- date

There is one additional type, "categorical", which is a mapping of the values of a source column (of type int, string or date) into int.

Note that the df/mem and df/sql packages strongly type data.  One cannot add a float and an int, for example.

### Package Files

**df package files**

- atomic.go. Defines the basic data type of Columns.
- column.go. Defines the "core" and "full" Column interfaces and implements the core Column interface as the struct ColCore.
- df.go. Defines the "core" and "full" DF interfaces and implements the core DF interface as the struct DFCore.
- dialect.go. Dialect struct handles all I/O with databases.  All of the DB communication occurs through Dialect. The Dialect struct has methods for those SQL functions that vary between databases, such as creating tables.
- files.go. Files struct handles all I/O with files.
- functions.go. Defines funcs and structs used by the parser to call functions that operate on columns.
- helpers.go. Helper funcs.
- parser.go. Defines the Parser func.
- scalar.go. Implements the full Column interface for scalars.
- skeletons/*. There is a subdirectory under skeleton for each database type that is supported -- currently, ClickHouse and Postgres. The files are skeletons (SQL with placeholders) for SQL that varies between databases, such as CREATE queries.  There is an additional file, functions.txt, provides a function mapping for the parser. It maps the function name the parser knows to the SQL equivalent, including input/output types.
- testing/*.  Tests. 
- vector.go. Implements an in-memory vector type. This is the return type for  a Column when accessing their contents (Column.Data()).

The df/mem package files:
- column.go. Defines a type that satisfies the full Column interface.
- df.go. Defines a type that satisfies the full DF interface.
- functions.go. Defines functions used by the parser.
- data/functions.txt. This file provides a function mapping for the parser. It maps the function name the parser knows to the Go function, including input/output types.

The df/sql package files:
- column.go. Defines a type that satisfies the full Column interface.
- df.go. Defines a type that satisfies the full DF interface.
- functions.go. Defines functions used by the parser.

Note that df/sql has no functions.txt.  That file exists in the skeletons directory under the database type.




### Examples

Example 1: Creating a mem/df *DF from a CSV

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


Example 2: Creating a df/sql *DF from an SQL query


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

Example 3: Creating a df/mem *DF from an SQL query


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


Example 4: Using the parser.

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
