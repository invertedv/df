package df

import (
	"bytes"
	"encoding/binary"
	"fmt"
	d "github.com/invertedv/df"
	"hash/fnv"
	"io"
	"maps"
	"sort"
	"strings"
)

type DF struct {
	sourceQuery string
	orderBy     []*Col
	ascending   bool
	row         int

	*d.DFcore

	groupBy groups
}

type groups map[uint64]*groupVal

type groupVal struct {
	groupDF *DF

	row []any
}

func StandardFunctions() d.Fns {
	// DF returns
	fns := d.Fns{sortDF, table, where, toCat, applyCat, global}
	fns = append(fns, vectorFunctions()...)

	return fns
}

func NewDFcol(funcs d.Fns, cols []*Col, opts ...d.DFopt) (*DF, error) {
	if funcs == nil {
		funcs = StandardFunctions()
	}

	rowCount := cols[0].Len()
	var cc []d.Column
	for ind := 0; ind < len(cols); ind++ {
		if rc := cols[ind].Len(); rc > 1 && rc != rowCount {
			return nil, fmt.Errorf("all MemCols must have same length")
		}

		cc = append(cc, cols[ind])
	}

	var (
		df *d.DFcore
		e  error
	)

	if df, e = d.NewDF(funcs, cc); e != nil {
		return nil, e
	}

	outDF := &DF{DFcore: df, row: -1}

	for _, opt := range opts {
		if ex := opt(outDF); ex != nil {
			return nil, ex
		}
	}

	if ex := outDF.SetParent(); ex != nil {
		return nil, ex
	}

	return outDF, nil
}

func NewDFseq(funcs d.Fns, n int, opts ...d.DFopt) (*DF, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be positive in NewDFseq")
	}

	if funcs == nil {
		funcs = StandardFunctions()
	}

	data := make([]int, n)
	for ind := 0; ind < n; ind++ {
		data[ind] = ind
	}

	col, _ := NewCol(data, d.DTint, d.ColName("seq"))

	df, _ := NewDFcol(funcs, []*Col{col})

	for _, opt := range opts {
		if e := opt(df); e != nil {
			return nil, e
		}
	}

	return df, nil
}

func DBLoad(qry string, dlct *d.Dialect, fns ...d.Fn) (*DF, error) {
	var (
		columnNames []string
		columnTypes []d.DataTypes
		e           error
	)

	var memData []*d.Vector
	if memData, columnNames, columnTypes, e = dlct.Load(qry); e != nil {
		return nil, e
	}

	var memDF *DF
	for ind := 0; ind < len(columnTypes); ind++ {
		var col *Col

		if col, e = NewCol(memData[ind], columnTypes[ind], d.ColName(columnNames[ind])); e != nil {
			return nil, e
		}

		if ind == 0 {
			if fns == nil {
				fns = StandardFunctions()
			}
			if memDF, e = NewDFcol(fns, []*Col{col}); e != nil {
				return nil, e
			}

			continue
		}

		if ex := memDF.AppendColumn(col, false); ex != nil {
			return nil, ex
		}
	}

	memDF.sourceQuery = qry
	memDF.row = -1

	return memDF, nil
}

func FileLoad(f *d.Files) (*DF, error) {
	var (
		memData []*d.Vector
		e       error
	)
	if memData, e = f.Load(); e != nil {
		return nil, e
	}

	var memDF *DF
	for ind := 0; ind < len(f.FieldNames()); ind++ {
		var col *Col

		if col, e = NewCol(memData[ind], f.FieldTypes()[ind], d.ColName(f.FieldNames()[ind])); e != nil {
			return nil, e
		}

		if ind == 0 {
			if memDF, e = NewDFcol(StandardFunctions(), []*Col{col}); e != nil {
				return nil, e
			}

			continue
		}

		if ex := memDF.AppendColumn(col, false); ex != nil {
			return nil, ex
		}
	}

	memDF.row = -1

	return memDF, nil
}

// ***************** Methods *****************

// AppendColumn masks the DFcore version so that we can handle appending scalars
func (f *DF) AppendColumn(col d.Column, replace bool) error {
	if e := checkType(col); e != nil {
		return e
	}

	if f.RowCount() != col.Len() && col.Len() > 1 {
		return fmt.Errorf("unequal lengths in AppendColumn")
	}

	colx := col.(*Col)
	if colx.Len() == 1 {
		val := colx.Data().Element(0)
		// should not fail
		v, _ := d.NewVector(val, colx.DataType())
		for ind := 1; ind < f.RowCount(); ind++ {
			if e := v.Append(val); e != nil {
				return e
			}
		}

		colx.Vector = v
	}

	if ex := f.DFcore.AppendColumn(colx, replace); ex != nil {
		return ex
	}

	// need to wait til end to assign in case DFcore.AppendColumn needed to drop the column
	if e := d.ColParent(f)(colx); e != nil {
		return e
	}

	return nil
}

func (f *DF) AppendDF(df d.DF) (d.DF, error) {
	if _, ok := df.(*DF); !ok {
		return nil, fmt.Errorf("must be *DF to append to *DF")
	}

	var (
		dfCore *d.DFcore
		e      error
	)

	if dfCore, e = f.AppendDFcore(df.Core()); e != nil {
		return nil, e
	}

	ndf := &DF{
		sourceQuery: "",
		orderBy:     nil,
		DFcore:      dfCore,
	}

	return ndf, nil
}

func (f *DF) By(groupBy string, fns ...string) (d.DF, error) {
	if groupBy == "" {
		return nil, fmt.Errorf("must have groupBy in DF.By")
	}

	if fns == nil {
		return nil, fmt.Errorf("must have at least on function in By")
	}

	flds := strings.Split(groupBy, ",")
	var gCol []*Col

	var outVecs []*d.Vector
	for ind := 0; ind < len(flds); ind++ {
		var col d.Column
		cName := strings.ReplaceAll(flds[ind], " ", "")
		if col = f.Column(cName); col == nil {
			return nil, fmt.Errorf("missing column %s in By", cName)
		}

		gCol = append(gCol, col.(*Col))
		outVecs = append(outVecs, d.MakeVector(col.DataType(), 0))
	}

	var (
		grp groups
		e   error
	)
	if grp, e = buildGroups(f, gCol); e != nil {
		return nil, e
	}

	var left []string
	for ind := 0; ind < len(fns); ind++ {
		lr := strings.Split(fns[ind], ":=")
		left = append(left, strings.ReplaceAll(lr[0], " ", ""))
	}

	for _, v := range grp {
		for ind := 0; ind < len(fns); ind++ {
			// create group columns on first pass
			if ind == 0 {
				for ind1 := 0; ind1 < len(gCol); ind1++ {
					if e5 := outVecs[ind1].Append(v.row[ind1]); e5 != nil {
						return nil, e5
					}
				}
			}
			var e1 error

			if _, e1 = d.Parse(v.groupDF, fns[ind]); e1 != nil {
				return nil, e1
			}

			col := v.groupDF.Column(left[ind])

			if len(outVecs) < len(gCol)+ind+1 {
				outVecs = append(outVecs, d.MakeVector(col.DataType(), 0))
			}

			if e2 := outVecs[ind+len(gCol)].Append(col.Data().Element(0)); e2 != nil {
				return nil, e2
			}
		}
	}

	var cols []*Col
	names := append(flds, left...)
	for ind := 0; ind < len(outVecs); ind++ {
		var (
			col *Col
			e3  error
		)

		if col, e3 = NewCol(outVecs[ind], d.DTany, d.ColName(names[ind])); e3 != nil {
			return nil, e3
		}

		cols = append(cols, col)
	}

	var (
		outDF *DF
		e4    error
	)
	if outDF, e4 = NewDFcol(f.Fns(), cols); e4 != nil {
		return nil, e4
	}

	outDF.groupBy = grp
	_ = d.DFsetSourceDF(f)(outDF)

	return outDF, nil
}

func (f *DF) Categorical(colName string, catMap d.CategoryMap, fuzz int, defaultVal any, levels []any) (d.Column, error) {
	var col d.Column
	if col = f.Column(colName); col == nil {
		return nil, fmt.Errorf("column %s not found", colName)
	}

	if col.DataType() == d.DTfloat {
		return nil, fmt.Errorf("cannot make float to categorical")
	}

	var (
		tab d.DF
		e2  error
	)
	if tab, e2 = f.Table(colName); e2 != nil {
		return nil, e2
	}

	if e3 := tab.Sort(true, colName); e3 != nil {
		return nil, e3
	}

	// check incoming map is of the correct types
	nextInt := 0
	for k, v := range catMap {
		if k != nil && d.WhatAmI(k) != col.DataType() {
			return nil, fmt.Errorf("map and column not same data types")
		}

		if v >= nextInt {
			nextInt = v + 1
		}
	}

	// toMap is the output map
	toMap := make(d.CategoryMap)
	maps.Copy(toMap, catMap)

	if _, ok := toMap[defaultVal]; !ok {
		toMap[defaultVal] = -1
	}

	// cnts will count the frequencies of each level of toMap
	cnts := make(d.CategoryMap)

	lvls := tab.Column(colName)
	cs := tab.Column("count")
	for ind := 0; ind < tab.RowCount(); ind++ {
		lvl := lvls.(*Col).Element(ind)
		cnt, _ := cs.(*Col).ElementInt(ind)
		if levels != nil && !d.Has(lvl, levels) {
			lvl = defaultVal
		}

		if *cnt < fuzz {
			lvl = defaultVal
		}

		if _, ok := toMap[lvl]; !ok {
			toMap[lvl] = nextInt
			nextInt++
		}
		cnts[lvl] += *cnt
	}

	vec := d.MakeVector(d.DTint, 0)

	for ind := 0; ind < col.Len(); ind++ {
		inVal := col.(*Col).Element(ind)

		var (
			ok     bool
			mapVal int
		)
		// if inVal isn't in the map, map it to the default level
		if mapVal, ok = toMap[inVal]; !ok {
			mapVal = toMap[defaultVal]
		}

		if e := vec.Append(mapVal); e != nil {
			return nil, e
		}

	}

	var (
		outCol *Col
		e      error
	)

	if outCol, e = NewCol(vec, vec.VectorType()); e != nil {
		return nil, e
	}

	_ = d.ColDataType(d.DTcategorical)(outCol.ColCore)
	_ = d.ColCatMap(toMap)(outCol.ColCore)
	_ = d.ColCatCounts(cnts)(outCol.ColCore)

	return outCol, nil
}

func (f *DF) Copy() d.DF {
	dfC := f.DFcore.Copy()

	mNew := &DF{
		sourceQuery: "",
		orderBy:     nil,
		ascending:   false,
		DFcore:      dfC,
	}

	return mNew
}

func (f *DF) Iter(reset bool) (row []any, err error) {
	if reset {
		f.row = 0
	}

	if f.row+1 > f.RowCount() {
		return nil, io.EOF
	}

	for c := f.First(); c != nil; c = f.Next() {
		row = append(row, c.(*Col).Element(f.row))
	}

	f.row++

	return row, nil
}

// Len is required for sort
func (f *DF) Len() int {
	return f.RowCount()
}

func (f *DF) Less(i, j int) bool {
	for ind := 0; ind < len(f.orderBy); ind++ {
		var less bool
		if f.ascending {
			less = f.orderBy[ind].Less(i, j)

		} else {
			less = f.orderBy[ind].Less(j, i)
		}

		// if greater, it's false
		if !less {
			return false
		}

		// if < (rather than <=) it's true
		if f.orderBy[ind].Less(i, j) && !f.orderBy[ind].Less(j, i) {
			return true
		}

		// equal -- keep checking
	}

	return true
}

func (f *DF) MakeQuery(colNames ...string) string {
	return ""
}

func (f *DF) RowCount() int {
	return f.First().Len()
}

func (f *DF) SetParent() error {
	for c := f.First(); c != nil; c = f.Next() {
		if e := d.ColParent(f)(c); e != nil {
			return e
		}
	}

	return nil
}

func (f *DF) Sort(ascending bool, cols ...string) error {
	var by []*Col

	for ind := 0; ind < len(cols); ind++ {
		var x d.Column
		if x = f.Column(cols[ind]); x == nil {
			return fmt.Errorf("column %s not found", cols[ind])
		}

		by = append(by, x.(*Col))
	}

	f.orderBy = by
	f.ascending = ascending
	sort.Sort(f)

	return nil
}

func (f *DF) SourceQuery() string {
	return f.sourceQuery
}

func (f *DF) String() string {
	var sx string
	for c := f.First(); c != nil; c = f.Next() {
		sx += c.String() + "\n"
	}

	return sx
}

func (f *DF) Swap(i, j int) {
	for h := f.First(); h != nil; h = f.Next() {
		h.(*Col).Swap(i, j)
	}
}

func (f *DF) Table(cols ...string) (d.DF, error) {
	var (
		dfOut d.DF
		e     error
	)

	fn1 := fmt.Sprintf("count:=count(%s)", cols[0])
	fn2 := fmt.Sprintf("rate:=float(count)/float(count(global(%s)))", cols[0])
	if dfOut, e = f.By(strings.Join(cols, ","), fn1, fn2); e != nil {
		return nil, e
	}

	if e1 := dfOut.Sort(false, "count"); e1 != nil {
		return nil, e
	}

	return dfOut, nil
}

func (f *DF) Where(indicator d.Column) (d.DF, error) {
	if e := checkType(indicator); e != nil {
		return nil, e
	}

	if indicator.Len() != f.RowCount() {
		return nil, fmt.Errorf("indicator column wrong length. Got %d needed %d", indicator.Len(), f.RowCount())
	}

	if indicator.DataType() != d.DTint {
		return nil, fmt.Errorf("argument to Where must be int")
	}

	dfNew := f.Copy()
	i1 := indicator.(*Col)

	for col := dfNew.First(); col != nil; col = dfNew.Next() {
		cx := col.(*Col)
		cx.Vector = cx.Where(i1.Vector)
		if cx.Len() == 0 {
			return nil, fmt.Errorf("no data after applying where")
		}
	}

	return dfNew, nil
}

// ***************** Helpers *****************

func checkType(cols ...d.Column) error {
	for _, c := range cols {
		if _, ok := c.(*Col); !ok {
			return fmt.Errorf("column is wrong type: need mem/Col")
		}
	}

	return nil
}

// ****************************************************************************

func buildGroups(df *DF, gbCol []*Col) (groups, error) {
	type entry struct {
		count int
		cols  []*d.Vector
		row   []any
	}

	cn := df.ColumnNames()
	ct, _ := df.ColumnTypes()

	var inVecs []*d.Vector
	for ind := 0; ind < len(cn); ind++ {
		inVecs = append(inVecs, df.Column(cn[ind]).Data())
	}

	type oneD map[any]int64

	// the levels of each column in the table are stored in mps which maps the native value to int64
	// the byte representation of the int64 are concatenated and fed to the hash function
	var mps []oneD

	// nextIndx is the next index value to use for each column
	nextIndx := make([]int64, len(gbCol))
	for ind := 0; ind < len(gbCol); ind++ {
		mps = append(mps, make(oneD))
	}

	// tabMap is the map represenation of the table. The key is the hash value.
	tabMap := make(map[uint64]*entry)

	// buf is the 8 byte representation of the index number for a level of a column
	buf := new(bytes.Buffer)
	// h will be the hash of the bytes of the index numbers for each level of the table columns
	h := fnv.New64()

	// scan the rows to build the table
	for rowNum := 0; rowNum < gbCol[0].Len(); rowNum++ {
		// str is the byte array that is hashed, its length is 8 times the # of columns
		var str []byte

		// rowVal holds the values of the columns for that row of the table
		var rowVal []any
		for c := 0; c < len(gbCol); c++ {
			val := gbCol[c].Element(rowNum)
			rowVal = append(rowVal, val)
			var (
				cx int64
				ok bool
			)

			if cx, ok = mps[c][val]; !ok {
				mps[c][val] = nextIndx[c]
				cx = nextIndx[c]
				nextIndx[c]++
			}

			if e := binary.Write(buf, binary.LittleEndian, cx); e != nil {
				panic(e)
			}

			str = append(str, buf.Bytes()...)
			buf.Reset()
		}

		_, _ = h.Write(str)

		// increment the counter if that row is already mapped, o.w. add a new row
		// TODO: HERE, either append rows or make new entry
		entryx := h.Sum64()
		if _, ok := tabMap[entryx]; !ok {
			var vecs []*d.Vector
			for ind := 0; ind < len(cn); ind++ {
				vecs = append(vecs, d.MakeVector(ct[ind], 0))
			}

			tabMap[entryx] = &entry{
				count: 0,
				cols:  vecs,
				row:   rowVal,
			}
		}

		v := tabMap[entryx]
		for ind := 0; ind < len(cn); ind++ {
			if e := v.cols[ind].Append(inVecs[ind].Element(rowNum)); e != nil {
				return nil, e
			}
		}

		h.Reset()
	}

	var outVecs []*d.Vector
	for c := 0; c < len(gbCol); c++ {
		outVecs = append(outVecs, d.MakeVector(gbCol[c].DataType(), 0))
	}

	grp := make(groups)
	for k, v := range tabMap {
		var cols []*Col
		for ind := 0; ind < len(cn); ind++ {
			var (
				col *Col
				e1  error
			)
			if col, e1 = NewCol(v.cols[ind], ct[ind], d.ColName(cn[ind])); e1 != nil {
				return nil, e1
			}

			cols = append(cols, col)
		}

		var (
			dfg *DF
			e2  error
		)
		if dfg, e2 = NewDFcol(nil, cols); e2 != nil {
			return nil, e2
		}

		//TODO: create a "global" function that uses this DF but must return a scalar
		_ = d.DFsetSourceDF(df)(dfg)

		grp[k] = &groupVal{
			groupDF: dfg,
			row:     v.row,
		}
	}

	return grp, nil
}

//TODO: do I want to make the parent columns available or not?
//TODO: if I do, then need to make Parse a method.
//TODO: for SQL should copy the dataframe for sourceDF
