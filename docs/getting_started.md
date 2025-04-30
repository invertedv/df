---
layout: default
title: Getting Started
nav_order: 2
---

## Getting Started 
{: .no_toc }
{: .fs-6 .fw-300 }

### Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}
---

### Imports

    import (
        d "github.com/invertedv/df"
        m "github.com/invertedv/df/mem"
        s "github.com/invertedv/df/sql"
    )

### General Approach

1. Create a dataframe.
2. Do something.
3. Save the result.

The approach taken here is to run through some examples so that you can get the hang of it.

### Processing a CSV.


Suppose you have a CSV with these columns:

- dt. date. Type: date, date format: mm/dd/ccyy.
- status. Status at dt. Type: string. Values: C, D.
- age. Age (in months) at dt. Type: integer.
- bal. Balance at dt. Type: float.

The file we'll use, accts.csv, is in the data directory of the package.

Our task:

1. Read in the file.
2. Generate some summary statistics.
3. By age and dt calculate: 
   a. the average balance; 
   b. the percentage of the total balance that falls in this age & dt
   c. proportion of balances in status D in this age & dt.
  

