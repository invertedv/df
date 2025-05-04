---
layout: default
title: Package Files
nav_order: 5
---

## df Package Files


### Files
- atomic.go. Defines the basic data type of Columns.
- column.go. Defines the "core" and "full" Column interfaces and implements the core Column interface as the struct ColCore.
- df.go. Defines the "core" and "full" DF interfaces and implements the core DF interface as the struct DFCore.
- dialect.go. Dialect struct handles all I/O with databases.  All of the DB communication occurs through Dialect. The Dialect struct has methods for those SQL functions that vary between databases, such as creating tables.
- files.go. Files struct handles all I/O with files.
- functions.go. Defines funcs and structs used by the parser to call functions that operate on columns.
- helpers.go. Helper funcs.
- parser.go. Defines the Parser func.
- scalar.go. Implements the full Column interface for scalars.
- vector.go. Implements an in-memory vector type. This is the return type for  a Column when accessing their contents (Column.Data()).

### Directories

#### mem

An implementation of DF and Column for in-memory objects.

- column.go. Defines a type that satisfies the full Column interface.
- df.go. Defines a type that satisfies the full DF interface.
- functions.go. Defines functions used by the parser.
- functions.txt. This file provides a mapping for the parser. It maps the function name the parser knows to the Go function, including input/output types.
- mem_test.go

#### sql

An implementation of DF and Column for SQL databases.

- column.go. Defines a type that satisfies the full Column interface.
- df.go. Defines a type that satisfies the full DF interface.
- functions.go. Defines functions used by the parser.
- sql_test.go

Note that df/sql has no functions.txt.  Since the details of functions vary by dialect, the functions.txt files
reside in each dialect directory under the skeletons directory.

#### skeletons

There is a subdirectory under skeleton for each database type that is supported -- currently, ClickHouse and Postgres. 


These files are skeletons (SQL with placeholders) for SQL that varies between databases:

- create_temp.txt. Create a temp table. Placeholders: 
    - ?Fields  - field list for a create statement (*e.g.* name and type). See fields.txt.
    - ?OrderBy - key for the table.
- create.txt. Create a permanent table. See Dialect.Create() for usage. Mandatory placeholders: 
    - ?TableName - name of the table (including database for ClickHouse).
    - ?Fields  - field list for a create statement (*e.g.* name and type). See fields.txt.
    - ?OrderBy - key for the table.

    Postgres additional placeholders: 
    - ?IndexName. Name of the index to create with the table.
    - ?TableSpace. Name of the Table Space.
- dropif.txt. DropIf statement. Placeholder: ?TableName.
- exists_temp.txt. Returns 1 if temp table exists. Placeholder: ?TableName.
- exists.txt. Returns 1 if table exists. One or more of these make up the ?Fields placeholder of create_temp.txt and create.txt. Placeholder: ?TableName.
- fields.txt. An examplar of a field for a create statement. See Dialect.Create() for how this is used. Placeholders: 
    - ?Name. Field name.
    - ?Type. Field type.
- insert.txt. Insert into a table. See Dialect.Insert() for usage. Placeholders: 
    - ?TableName. Name of the table to insert into.
    - ?MakeQuery. Query that will generate the data to insert.
    - ?Fields. Fields from the query to insert. 
- interp.txt. Linear interpolation. Placeholders:
    - ?Source. Table with data to be interpolated.
    - ?Interp. Table with points at which to interpolate.
    - ?XSfield. X data in ?Source.
    - ?XIfield. X data in ?Interp.
    - ?Yfield. Y data in ?Source.
    - ?OutField. Name of interpolated values.
- seq.txt. Create a table with one column of range values. Placeholder: ?Upper upper end of the range. Result is 0 to ?Upper-1.
- types.txt. Mapping of DataTypes values to DB types, e.g. DTFloat,Float64 (ClickHouse), DTFloat,double precision (Postgres).

There is an additional file, functions.txt, provides a function mapping for the parser. It maps the function name the parser knows to the SQL equivalent, including input/output types.  

- functions.txt

#### testing

These tests run through dataframes using df/mem and df/sql (both ClickHouse and Postgres).


### Functions.txt Files

All functions.txt files are read by LoadFunctions() and have the same format.  These are read by LoadFunctions().

Within each line of functions.txt there are 6 fields which are colon-separated (:). The fields are:

- function name
- function spec. For df/mem this is the name of the Go function implementation. For df/sql it is the SQL to call the function.
- inputs
- outputs
- return type (C = column, S = scalar)
- varying inputs (Y = yes).

Inputs are sets of types with in braces separated by commas.

    {int,int},{float,float}

specifies the function takes two parameters which can be either {int,int} or {float,float}.

Corresponding to each set of inputs is an output type.  In the above example, if the function always
 returns a float, the output would be:

	float,float.

 Legal types are float, int, string and date.  A categorical input is an int.

If there is no input parameter, leave the field empty as in:

        ::

For example, the df/mem entry of functions.txt for std (sample standard deviation) is:

    std:github.com/invertedv/df/mem.stdFn[...]:{float},{int}:float,float:S:N

You can see, the function implementing this is github.com/invertedv/df/mem.stdFn[...].  The [...] means that this function uses generics.
For ClickHouse, the entry is:

    std:stddevSampStable(%s):{float},{int}:float,float:S:N

Note that the df/mem and df/sql entries are identical except for the function spec.
