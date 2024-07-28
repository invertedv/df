package df

import (
	"fmt"
	"github.com/invertedv/utilities"
)

type DataTypes uint8

const (
	DTchar DataTypes = 0 + iota
	DTdouble
	DTinteger
	DTcategory
	DTdate
	DTdateTime
	DTtime
	DTslcChar
	DTslcDouble
	DTslcInteger
)

//go:generate stringer -type=DataTypes

type Column interface {
	Name() string
	DataType() DataTypes
	N() int
	Data() any
	To(dt DataTypes) (any, error)
	Element(row int) any
}

type OpFunc func(resultName string, cols ...Column) (Column, error)

type SaveFunc func(to string, cols ...Column) error

type LoadFunc func(from string) ([]Column, error)

type DF struct {
	ragged bool
	hasSQL bool

	head *DFlist
}

type DFlist struct {
	col Column

	prior *DFlist
	next  *DFlist
}

func loadDF(loader LoadFunc) (df *DF, err error) {

	return nil, nil
}

func NewDF(cols ...Column) (df *DF, err error) {
	if cols == nil {
		return nil, fmt.Errorf("no columns in NewDF")
	}

	var head, priorNode *DFlist
	for ind := 0; ind < len(cols); ind++ {
		node := &DFlist{
			col: cols[ind],

			prior: priorNode,
			next:  nil,
		}

		if priorNode != nil {
			priorNode.next = node
		}

		priorNode = node

		if ind == 0 {
			head = node
		}
	}

	df = &DF{head: head}

	return df, nil
}

func (dfl *DFlist) Head() *DFlist {
	var head *DFlist
	for head = dfl; head.prior != nil; head = head.prior {
	}

	return head
}

func (dfl *DFlist) Tail() *DFlist {
	var tail *DFlist
	for tail = dfl; tail.next != nil; tail = tail.next {
	}

	return tail
}

func (df *DF) getDFlist(colName string) (col *DFlist, err error) {
	for h := df.head; h != nil; h = h.next {
		if (h.col).Name() == colName {
			return h, nil
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

func (df *DF) GetColumn(colName string) (col Column, err error) {
	var dfl *DFlist
	if dfl, err = df.getDFlist(colName); err != nil {
		return nil, err
	}

	return dfl.col, nil
}

func (df *DF) Save(saver SaveFunc, to string, colNames ...string) error {
	var cols []Column
	for col := df.head; col != nil; col = df.head.next {
		if colNames == nil || utilities.Has(col.col.Name(), "", colNames...) {
			cols = append(cols, col.col)
		}
	}

	return saver(to, cols...)
}

// what about N, name, dataType
func (df *DF) Apply(resultName string, op OpFunc, colNames ...string) (out Column, err error) {
	var inCols []Column

	if op == nil {
		return nil, fmt.Errorf("no operation defined in Apply")
	}

	for ind := 0; ind < len(colNames); ind++ {
		var dfcol *DFlist

		if dfcol, err = df.getDFlist(colNames[ind]); err != nil {
			return nil, err
		}

		inCols = append(inCols, dfcol.col)
	}

	return op(resultName, inCols...)
}

// what if df is nil?
func (df *DF) Append(col Column) {
	tail := df.head.Tail()

	dfl := &DFlist{
		col:   col,
		prior: tail,
		next:  nil,
	}

	tail.next = dfl
}

func (df *DF) Drop(colName string) error {
	col, err := df.getDFlist(colName)
	if err != nil {
		return err
	}

	col.prior.next = col.next
	col.next.prior = col.prior

	return nil
}

/*

in memory vs in database

db: fetch vs run on db
on db: where put output? single query...

Load
Save

*/
