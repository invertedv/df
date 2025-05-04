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

**What Makes df Different?**

Dataframes are commonly-used objects that are used to hold and manipulate data for analysis. Conceptually, a dataframe consists of a set of columns. Columns, in turn, are arrays of values which are of a common type and length. 
Originally, implementations of dataframes such as R and pandas were designed to hold the data in memory, though these have been extended to big-data cases.

How is df different? The package df specifies interfaces for dataframes and Columns. The package is agnostic as to the mechanisms handling the underlying data. 

<div style="margin-left: 40px;margin-right: 40px;font-size: 14pt">
With this approach, the user can pull a sample of a large table, experiment with the 
data, do EDA, etc., in a fast, efficient manner. When desired, the same Go code can be run over the entire table.
</div>

The df package consists of a main package, df, and two sub-packages, df/mem and df/sql.  The main package:

- defines the DF and Column interfaces.
- implements core aspects of those interfaces.
- provides a parser to evaluate expressions.
- handles file and DB I/O.

Packages df/mem and df/sql implement the full DF and Column interfaces for in-memory data and SQL databases, respectively. The distinction
between df/mem and df/sql is not the source of the data. Package mem/DF dataframes can be read from and save to a database, for example. The distinction is where the calculations and manipulations are performed.  The df/mem package does this work in memory, while the df/sql performs it in the database.  


**Functionality**

What do you need to be able to do with a dataframe? Well, you'll want to

- Create and save them.  With df you can read/write files (such as CSV) and SQL tables.
- Manipulate the columns such as creating new columns based on the existing ones.
- Subset, sort, summarize and join the data. 

To this end,
  - df has a parser for evaluating expressions to create new columns. The parser allows flexible specification of expressions that return a column result.  The parser will work on any type that satisfies the DF interface*.

        Parse(df, "y := exp(yhat) / (1.0 + exp(yhat))")
        Parse(df, "r := if(k==1,a,b)")
        Parse(df, "xNorm := x / global(sum(x))")
        Parse(df, "zCat := cat(z)")

  - The interface specification includes methods such as Sort(), By() and Join().


*Note: the specific implementation must also provide to the parser implementations of functions, such as "sum".  The df/mem and
df/sql packages offer identical function sets.  See the Parse section of a list of supported functions.

**Extensible**

The package may be extended in several directions:
- Add new functions to the parser.
- Add additional database types can be added to the sql package. Currently, ClickHouse and Postgres are supported.  Adding support for the new DB type requires modifying the Dialect struct.
The sql package would not need to be modified.
- Build a completely new implementation of the DF and Column interfaces.

### Package Details
**df**

The df package defines the DF and Column interfaces in two steps: a core (DC, CC, respectively) and full interface (DF, Column).  The core interface defines those methods which are independent of the details of the data architecture (*e.g.* DropColumns() from DF, Name() method for a Column). The df package provides structs that implement the core DF and Column interfaces (DFcore, ColCore).


**df/mem**

The df/mem package implements the DF and Column interfaces for in-memory objects.

**df/sql**

The package df/sql implements the DF and Column interfaces for SQL databases. It relies on the methods of Dialect to handle the specifics
of any particular database type.

A basic design philosophy of this package is that the storage mechanism of the data doesn't matter. A complication is that, though two database packages may use SQL, the details are likely to differ. The Dialect struct and its methods abstract these differences away.  Those methods handle the differences between databases, hence each DB must specifically be handled there. Currently, Clickhouse and Postgres are supported. Dialect uses the standard Go sql package connector.  All communication with databases occurs through Dialect.

**Data Types**
Four data types are supported for column elements:

- float
- int
- string
- date

There is one additional type, "categorical", which is a mapping of the values of a source column (of type int, string or date) into int.

Note that the df/mem and df/sql packages strongly type data.  One cannot add a float and an int, for example.

