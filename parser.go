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

const ops = "^#*#/#+#-#!=#==#>#<#>#>=#<=#&&#||#("

type OpTree struct {
	expr string
	op   Operation

	value Column

	left  *OpTree
	right *OpTree

	f *FuncReturn

	df        *DFcore
	operators []string
	fnNames   []string

	fnName string
	inputs []*OpTree
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

func NewOpTree(expression string, df *DFcore) (*OpTree, error) {
	expression = strings.ReplaceAll(expression, " ", "")
	// check for illegals

	opx := strings.Split(ops, "#")
	fns := []string{}
	for _, fn := range df.Funcs() {
		//		opx = append(opx, fn(true, nil, nil).Name+"(")
		fns = append(fns, fn(true, nil, nil).Name+"(")
	}

	ot := &OpTree{expr: expression, df: df, operators: opx, fnNames: fns}
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
	haveFn, fnOp := false, ""
	for _, f := range ot.fnNames {
		if len(ot.expr) >= len(f) && ot.expr[:len(f)] == f {
			fmt.Println("function: ", f, " : ", ot.expr)
			haveFn, fnOp = true, f
			break
		}
	}

	for ind := len(ot.operators) - 1; ind >= 0; ind-- {
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

			if len(ot.expr) >= j+len(ot.operators[ind]) && ot.expr[j:j+len(ot.operators[ind])] == ot.operators[ind] {
				if ot.operators[ind] != "(" && j == 0 {
					continue
				}
				left = trimParen(ot.expr[:j])
				right = trimParen(ot.expr[j+len(ot.operators[ind]):])
				op = ot.operators[ind]
				return left, right, op, nil
			}
		}
	}

	if haveFn {
		fmt.Println("have function ", fnOp, " : ", ot.expr)

		if e := ot.makeFn(fnOp); e != nil {
			return "", "", "", e
		}
	}

	return "", "", "", nil
}

func (ot *OpTree) makeFn(fnName string) error {
	inner := strings.ReplaceAll(ot.expr, fnName, "")
	inner = inner[:len(inner)-1]
	x := strings.Split(inner, ",")

	ot.fnName = fnName[:len(fnName)-1]

	fmt.Println("Test ", ot.expr, x)
	for ind := 0; ind < len(x); ind++ {
		if x[ind] == "" {
			continue
		}
		var (
			op *OpTree
			e  error
		)
		if op, e = NewOpTree(x[ind], ot.df); e != nil {
			return e
		}

		if e = op.Build(); e != nil {
			return e
		}

		ot.inputs = append(ot.inputs, op)
	}

	return nil
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

	fmt.Println("whole:", ot.expr, "left: ", l, "right: ", r, "op: ", op)

	ot.left = &OpTree{
		expr:      l,
		op:        0,
		value:     nil,
		left:      nil,
		right:     nil,
		df:        ot.df,
		operators: ot.operators,
		fnNames:   ot.fnNames,
	}

	ot.right = &OpTree{
		expr:      r,
		op:        0,
		value:     nil,
		left:      nil,
		right:     nil,
		df:        ot.df,
		operators: ot.operators,
		fnNames:   ot.fnNames,
	}

	if e := ot.right.Build(); e != nil {
		return e
	}

	if e := ot.left.Build(); e != nil {
		return e
	}

	return nil
}
