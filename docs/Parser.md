---
layout: default
title: Parser
nav_order: 3
---

## Parser 
{: .no_toc }
{: .fs-6 .fw-300 }

### Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}
---

The parser evaluates an expression that returns a Column.  The signature of the Parse function is:

    Parse(df DF, equation string) error

The equation has the form

    newCol := expression

The expression is evaluated over df, the result is appended to df with the name, newCol.

The parser is strongly typed -- you cannot mix ints and floats.  You'll need to convert them
with int() and float().  Any constant with a decimal point is treated as a float.

### Parser Functions

The parser supports these functions:

**Arithmetic**

    +, -, *, /, ^

**Logic**

    ==, !=, >, >=, <, <=
    \|\|, &&, !

- **if**. if(conditon expression, rTrue any, rFalse any). if condition evaluates true, return rTrue. rTrue and rFalse must have the same type. Example:

      if(x > 4, 2, z)

  if x > 4, returns 2, o.w. returns z.

**Mathematical**

- **abs**. abs(x float \| int) float \| int
- **acos**. acos(x float) float.
- **asin**. asin(x float) float.
- **atan**. atan(x float) float.
- **atan2**. atan2(x, y float) float.
- **cos**. cos(x float) float.
- **exp**. exp(x float) float.
- **log**. log(x float) float.
- **mod**. mod(x, y int) int.  x % y.
- **round**. round(x float) int.
- **sign**. sign(x float \| int) int.  Returns -1, 0, or 1.
- **sin**. sin(x float) float.
- **sqrt**. sqrt(x float) float.
- **tan**. tan(x float) float.

**Dates**

- **addMonths**. addMonths(dt date, mon int) date. Adds mon months to dt.
- **ageMonths**. ageMonths(bDay date, asOf date) int. Age in months from bDay to asOf.
- **ageYears**. ageYears(bDay date, asOf date) int. Age in years from bDay to asOf.
- **day**. day(dt date) int. Day of month.
- **dayOfWeek**. dayOfWeek(dt date) string. Day of week. Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday.
- **makeDate**. makeDate(year int \| string, month int \| string, day int \| string) date. Make a date.
- **month**. month(dt date) int. Month of year.
- **toEndOfMonth**. toEndOfMonth(dt date) date. Moves a date to the last day of the month.
- **year**. year(dt date) int. Extracts the year from dt.

**Conversion**

- **date**. date(x string) date; date(x int) date. Converts x to a date.
- **float**. float(x int) float, float(x string) float. Converts x to float.
- **int**. int(x float) int, int(x string) int. Converts x to int.
- **string**. string(x float) string, string(x int) string, string(x string) string, string(x date) string. Converts x to string.

**Strings**

- **concat**. concat(a, b string) string. Concatenates a and b.
- **position**. position(a, b string) int. Finds the location of b in a.
- **replace**. replace(a, b, c string) string. Replaces occurences of b with c.
- **substr**. substr(a string, start, len int) string. Returns the substring of length len of a starting from the start position.

**Random Numbers**

Note, that there is not a way to set the seed for these.

- **randBern**. randBern() int. Bernouilli random numbers.
- **randBin**. randBin() int. Binomial random numbers.  This is slow in Postgres.
- **randExp**. randExp() float. Exponential(1) random numbers.
- **randNorm**. randNorm() float. N(0,1) random numbers.
- **randUnif**. randUnif() float. U(0,1) random numbers.

**Other**

- **pi**. pi() float. Returns 3.141592654.
- **probNorm**. probNorm(x float) float. Returns the CDF of the standard normal distribution evaluated at x.
- **rowNumber**. rowNumber() int. Returns a column containing the number of each row, starting with 0.

**Row-wise Summaries**

Row-wise summaries produce a single valued summary of a column. If used with Parse directly, this produces a column with a single value
repeated over the rows.  To produce a new dataframe with just one row, use by By method with an empty string as the groupBy parameter.

- **count**. count(x any) int. Counts the number of rows. Outside of the By method, this populates all rows with the length of x.
- **max**. max(x any) any. 
- **mean**. mean(x float \| int) float.
- **min**. min(x any) any.
- **lq**. lq(x float \| int) float. Lower quartile.
- **median**. median(x float \| int) float. 
- **quantile**. quantile(p float, x float \| int) float. Returns the p, 0 <= p <= 1 quantile of x.
- **std**. std(x float \| int) float. Sample standard deviation.
- **sum**. sum(x float \| int) float \| int.
- **uq**. uq(x float \| int) float. Upper quartile.
- **var**. var( x float \| int) float. Sample variance.

**Column-wise Summaries**

Column-wise summaries take a set of columns as inputs. They generate the summary row-by-row.  For instance, if 
a, b, and c are columns,

    m := colMean(a,b,c)

creates a column m whose i<sup>th</sup> element is the mean of the i<sup>th</sup> elements of a, b and c.

- **colMax**. colMax(a ...any) any. Arguments are columns.
- **colMean**. colMean(a ...float \| int) float. Arguments are columns.
- **colMedian**. colMedian(a ...float \| int) float. Arguments are columns.
- **colMin**. colMin(a ...any) any. Arguments are columns.
- **colStd**. colStd(a ...float \| int) float. Sample standard deviation. Arguments are columns.
- **colSum**. colSum(a ...float \| int) float \| int. Arguments are columns.
- **colVar**. colVar(a ...float \| int) float. Sample variance. Arguments are columns.

**The global Function**

The syntax is

    global(x)

The global function is intended for use within the By method.  It is a signal that the entire column
(all rows) is to be used in the calculation.  For example,

    df.By(groupBy, "rate := sum(x)/ sum(global(x))")

calculates the sum of x within each level of "groupBy" divided by the sum of x across all rows.  Hence, rate will sum to 1.

### Adding Functions to the Parser

A function to be run by the Parse must have type Fn:

    type Fn func(info bool, df DF, inputs ...Column) *FnReturn

The FnReturn struct is defined as:

    type FnReturn struct {
	    Value Column // return value of function
        Name string // name of function
	    // An element of Inputs is a slice of data types that the function takes as inputs.  For instance,
	    //   {DTfloat,DTint}
	    // means that the function takes 2 inputs - the first float, the second int.  And
	    //   {DTfloat,DTint},{DTfloat,DTfloat}
	    // means that the function takes 2 inputs - either float,int or float,float.
	    Inputs [][]DataTypes

	    // Output types corresponding to the input slices.
	    Output []DataTypes

	    Varying bool // if true, the number of inputs varies.

	    IsScalar bool // if true, the function reduces a column to a scalar (e.g. sum, mean)

	    Err error
}

If the function is run with info = true, then it should return these fields:

- Name
- Inputs
- Outputs
- Varying
- IsScalar

If the function is run with info = false, then it should actually run the function, returning the result
as Value (or Err if there's an error).

Once you have a function created -- call it newFn -- to make it available to Parse, use code something like this:

    import m "github.com/invertedv/df/mem"

    var (
      dlct *Dialect
      qry string
    )

    ... define dlct, qry


    fns := append(m.StandardFunctions(), newFn)
    df, e := m.DBload(qry, dlct, DFsetFns(fns))

Now newFn is available to Parse when processing on df:

    Parse(df, "newCol := newFn(<args>)")

