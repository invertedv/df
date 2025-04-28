---
layout: default
title: Package Files
nav_order: 4
---

## df Package Files

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
- data/functions.txt. This file provides a mapping for the parser. It maps the function name the parser knows to the Go function, including input/output types.

The df/sql package files:
- column.go. Defines a type that satisfies the full Column interface.
- df.go. Defines a type that satisfies the full DF interface.
- functions.go. Defines functions used by the parser.

Note that df/sql has no functions.txt.  Since the details of functions vary by dialect, the functions.txt files
reside in each dialect directory under the skeletons directory.



