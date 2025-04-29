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
- Additional database types can be added to the sql package. Currently, ClickHouse and Postgres are suppored.  Adding support for the new DB type requires modifying the Dialect struct.
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
Four data types are supported for column elements:

- float
- int
- string
- date

There is one additional type, "categorical", which is a mapping of the values of a source column (of type int, string or date) into int.

Note that the df/mem and df/sql packages strongly type data.  One cannot add a float and an int, for example.




