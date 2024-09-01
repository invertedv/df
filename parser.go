package df

import (
	"fmt"
	"strings"
)

type Operation uint8

const (
	Function Operation = 0 + iota
	Power
	Multiply
	Divide
	Add
	Subtract
	Not
	And
	Or
	None
)

const ops = "^#*#/#+#-#!=#==#>=#>#<=#<#&&#||#("

type OpTree struct {
	expr string
	op   Operation

	value Column

	left  *OpTree
	right *OpTree

	f *FuncReturn

	df *DFcore
}

func findMatchParen(s string) int {
	depth, start := 0, false
	for ind := 0; ind < len(s); ind++ {
		if s[ind] == '(' {
			depth++
			if depth == 1 {
				start = true
			}
			continue
		}

		if s[ind] == ')' {
			depth--
			if depth == 0 && start {
				return ind
			}
		}
	}

	return -1
}

func preParse(s string, ops []string) []any {
	var (
		op string
	)
	minV := len(s)
	position := -1
	for _, o := range ops {
		if indx := strings.Index(s, o); indx >= 0 && indx < minV {
			minV = indx
			position = indx
			op = o
		}
	}

	if position >= 0 {
		// look for paren
		var pp []any
		if position > 0 {
			pp = append(pp, s[:position])
		}

		remaining := s[position+len(op):]

		if strings.Contains(op, "(") {
			s1 := s[position:]
			pos := findMatchParen(s1)
			if pos < 0 {
				panic("what???")
			}
			op = "r: " + s[position:pos+1]
			remaining = ""
			if pos+1 < len(s1) {
				remaining = s1[pos+1:]
			}
		}

		pp = append(pp, "op:"+op)
		pp = append(pp, preParse(remaining, ops)...)
		return pp
	}

	return []any{s}
}

func NewOpTree(expression string, df *DFcore) (*OpTree, error) {
	expression = strings.ReplaceAll(expression, " ", "")
	// check for illegals

	opx := strings.Split(ops, "#")
	for _, fn := range df.Funcs() {
		opx = append(opx, fn(true, nil, nil).Name+"(")
	}

	/*	var pp []any
		pp = preParse(expression, opx)
		for ind := 0; ind < len(pp); ind++ {
			fmt.Println(pp[ind])
		}

	*/

	ot := &OpTree{expr: expression, df: df}
	return ot, nil
}

func trimParen(s string) string {
	if len(s) <= 2 {
		return s
	}

	if s[0] == '(' && s[len(s)-1] == ')' {
		return s[1 : len(s)-1]
	}

	return s
}

func (ot *OpTree) scan() (left, right, op string, err error) {
	opx := strings.Split(ops, "#")
	fns := []string{}
	for _, fn := range ot.df.Funcs() {
		//		opx = append(opx, fn(true, nil, nil).Name+"(")
		fns = append(fns, fn(true, nil, nil).Name+"(")
	}

	haveFn, fnOp := false, ""
	for _, f := range fns {
		if len(ot.expr) >= len(f) && ot.expr[:len(f)] == f {
			fmt.Println("function: ", f, " : ", ot.expr)
			haveFn, fnOp = true, f
			break
		}
	}

	for ind := len(opx) - 1; ind >= 0; ind-- {
		depth := 0
		for j := 0; j < len(ot.expr); j++ {

			val := string(ot.expr[j])
			_ = val
			if ot.expr[j] == '(' {
				depth++
			}

			if ot.expr[j] == ')' {
				depth--
			}

			if depth > 0 {
				continue
			}

			//			if len(ot.expr) > j+len(opx[ind])-1 && ot.expr[j:j+len(opx[ind])] == opx[ind] {
			if string(ot.expr[j]) == opx[ind] {
				left = trimParen(ot.expr[:j])
				right = trimParen(ot.expr[j+1:])
				op = string(ot.expr[j])
				return left, right, op, nil
			}
		}
	}

	if haveFn {
		fmt.Println("have function ", fnOp, " : ", ot.expr)
	}

	return "", "", "", nil
}

func (ot *OpTree) paren() error {
	if !strings.Contains(ot.expr, "(") {
		return nil
	}

	if strings.Count(ot.expr, "(") != strings.Count(ot.expr, ")") {
		return fmt.Errorf("mis-matched parens in %s", ot.expr)
	}

	//	strings.FieldsFunc()

	return nil
}

func (ot *OpTree) Build() error {
	if e := ot.paren(); e != nil {
		return e
	}

	l, r, op, _ := ot.scan()
	if op == "" {
		return nil
	}

	fmt.Println("left: ", l, "right: ", r, "op: ", op)

	ot.left = &OpTree{
		expr:  l,
		op:    0,
		value: nil,
		left:  nil,
		right: nil,
		df:    ot.df,
	}

	ot.right = &OpTree{
		expr:  r,
		op:    0,
		value: nil,
		left:  nil,
		right: nil,
		df:    ot.df,
	}

	_ = ot.right.Build()
	_ = ot.left.Build()

	return nil
}
