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

We'll walk through an example that:

1. Creates a dataframe.
2. Finds some summary statistics.
3. Create a derivative dataframe.
3. Saves the result.

This should provide a general orientation to how df works. The Examples section has examples of specifc methods.

### Here We Go!


Suppose you have a CSV with these columns:

- dt. date. Type: date.
- status. Status at dt. Type: string. Values: C, D.
- age. Age (in months) at dt. Type: integer.
- bal. Balance at dt. Type: float.

The file we'll use, getting_started.csv, is in the data directory of the df package.

Our task:

1. Read in the file.
2. Generate a few summary statistics:
	- Number of accounts
	- Average balance
	- Number of accounts in March
3. By age and dt calculate: 
   - the average balance
   - the percentage of balances that are in status D
   - the percentage of balances that are this age at this dt
4. Save the output to a CSV
  
This code is implemented in the function TestStart1 in mem/mem_test.go.
The setup code looks like:

    import (
        d "github.com/invertedv/df"
        m "github.com/invertedv/df/mem"
    )

This code reads the file into df. 

	var (
		f  *d.Files
		e1 error
	)
	// FilesStrict(true) instructs *Files to return an error if any value can't be coerced into the correct data type.
	// FilePeek(500) instructs *Files to examine the first 500 rows to determine data types.
	if f, e1 = d.NewFiles(d.FileStrict(true), d.FilePeek(500)); e1 != nil {
		panic(e1)
	}

	// this file is in df/data.
	fileToOpen := os.Getenv("datapath") + "getting_started.csv"
	// Since we haven't told Open about field names and types, it will read the first row as the header
	// and impute the data types.
	if ex := f.Open(fileToOpen); ex != nil {
		panic(ex)
	}

	var (
		df *DF
		e2 error
	)
	if df, e2 = FileLoad(f); e2 != nil {
		panic(e2)
	}

	fmt.Println("A quick look at what we just read in:")
	fmt.Println(df)

The output of the print is:

    A quick look at what we just read in:
    Rows: 2000

    column: acct        column: dt          column: status
    type: DTint         type: DTdate        type: DTstring
    length 2000         length 2000         length 2000
    1                   2025-03-01          C
    2                   2025-03-01          C
    3                   2025-03-01          C
    4                   2025-03-01          C
    5                   2025-03-01          D


    column: age        column: bal
    type: DTint        type: DTfloat
    length 2000        length 2000
    5                  917.588
    19                 198.274
    32                 497.489
    22                 939.373
    19                 556.778


The code to generate the summary is below:


	// using By with no grouping field produces a all-row summary
	dfSummA, e3 := df.By("", "n := count(dt)", "avgBal := mean(bal)", "nMarch := sum(if(dt==date('20250301'),1,0))")
	assert.Nil(t, e3)
	fmt.Println("Summary:")
	fmt.Println(dfSummA)

The summary output is:

    Summary:
    Rows: 1

    column: n          column: avgBal        column: nMarch
    type: DTint        type: DTfloat         type: DTint
    length 1           length 1              length 1
    2000               499.501               1000

To produce the next set of values in item 3, we must group by age and dt. To calculate the last value of item 3, we must also group by dt only. Grouping by dt
will give us the total balance by month.

	var (
		dfSumm d.DF
		e4     error
	)
	// This creates a new dataframe grouping on age. For each age & dt combination, three fields are calculated:
	//  - mb is the average balance within the age & dt.
	//  - dq is the percentage of balances at this age & dt that have status == 'D'.
	//  - balAgeDt is the total balance at each age and dt value.
	if dfSumm, e4 = df.By("age,dt", "mb := mean(bal)", "dq := 100.0 * sum(if(status=='D', bal, 0.0))/ sum(bal)", "balAgeDt := sum(bal)"); e4 != nil {
		panic(e4)
	}

	if ex := dfSumm.Sort(true, "age,dt"); ex != nil {
		panic(ex)
	}

	// now calculate the total balance by date
	var (
		dfSummDt d.DF
		e5       error
	)
	if dfSummDt, e5 = df.By("dt", "balDt := sum(bal)"); e5 != nil {
		panic(e5)
	}

	var (
		dfJoin d.DF
		e6 error
	)
	// Now join the two by dt.  We could also do
	//    dfSumm.Join(dfSummdt, "dt")
	if dfJoin, e6 = dfSummDt.Join(dfSumm, "dt"); e6!=nil {
		panic(e6)
	}

	// pAge is the percentage of balances that month that are this age.
	if ex := d.Parse(dfJoin, "pAge := 100.0 * balAgeDt / balDt"); ex!=nil {
		panic(ex)
	}

	if ex := dfJoin.Sort(true, "age,dt"); ex!=nil{
		panic(ex)
	}

	fmt.Println("Summary by age and date")
	fmt.Println(dfJoin)

The output of the print is:

    Summary by age and date
    Rows: 72

    column: dt          column: balDt        column: age
    type: DTdate        type: DTfloat        type: DTint
    length 72           length 72            length 72
    2025-03-01          497828               0
    2025-03-01          497828               1
    2025-04-01          501173               1
    2025-03-01          497828               2
    2025-04-01          501173               2


    column: mb           column: dq           column: balAgeDt
    type: DTfloat        type: DTfloat        type: DTfloat
    length 72            length 72            length 72
    448.790              8.40178              12566.1
    363.428              15.9532              7631.99
    513.056              0.00000              14365.6
    514.490              16.7029              18521.7
    540.698              8.76176              11354.7


    column: pAge
    type: DTfloat
    length 72
    2.52419
    1.53306
    2.86639
    3.72049
    2.26562

Now let's save the summary out to a CSV.

	// OK, let's save this...
	var (
		fs *d.Files
		e7 error
	)
	// Create a new Files struct to do this.
	// Write out the date, dt, in the format CCYYMMDD.
	if fs, e7 = d.NewFiles(d.FileDateFormat("20060102")); e7 != nil {
		panic(e7)
	}

	fileToSave := os.Getenv("datapath") + "getting_started_summary.csv"
	if ex := fs.Save(fileToSave, dfJoin); ex != nil {
		panic(ex)
	}

And we're done!

The Examples section will show you how to read/save to a DB table.
