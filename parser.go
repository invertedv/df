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

const ops = "^#*#/#+#-#!=#==#>#<#>#>=#<=#&&#||"

type OpTree struct {
	expr string
	op   string

	value Column

	left  *OpTree
	right *OpTree

	df        *DFcore
	operators []string
	fnNames   []string

	fnName   string
	fn       AnyFunction
	fnReturn *FuncReturn
	inputs   []*OpTree
}

func NewOpTree(expression string, df *DFcore) (*OpTree, error) {
	expression = strings.ReplaceAll(expression, " ", "")

	opx := strings.Split(ops, "#")
	var fns []string
	for _, fn := range df.Funcs() {
		fns = append(fns, fn(true, nil, nil).Name+"(")
	}

	ot := &OpTree{expr: expression, df: df, operators: opx, fnNames: fns}
	return ot, nil
}

// outerParen strips away parentheses that surround the entire expression.
// For example, ((a+b)) becomes a+b
// but (a+b)*3 is not changed.
func outerParen(s string) string {
	if len(s) <= 2 || s[0] != '(' {
		return s
	}

	depth := 0
	for ind := 0; ind < len(s); ind++ {
		if s[ind] == '(' {
			depth++
		}

		if s[ind] == ')' {
			depth--
		}

		if depth == 0 {
			if ind == len(s)-1 {
				return outerParen(s[1:ind])
			}
			return s
		}
	}

	return s

}

func (ot *OpTree) scan() (left, right, op string, err error) {

	// determine if expression starts with a function call
	haveFn, fnOp := false, ""
	expr := outerParen(ot.expr)
	for _, f := range ot.fnNames {
		if len(ot.expr) >= len(f) && ot.expr[:len(f)] == f {
			fmt.Println("function: ", f, " : ", ot.expr)
			haveFn, fnOp = true, f
			break
		}
	}

	// work through the operators in increasing order of precedence
	for ind := len(ot.operators) - 1; ind >= 0; ind-- {
		depth := 0
		op = ot.operators[ind]
		//		for j := 0; j < len(expr); j++ {
		// go right-to-left to avoid the a-b-c problem.
		for j := len(expr) - 1; j >= 0; j-- {
			// ignore any operators that are within parentheses
			if expr[j] == '(' {
				depth--
			}

			if expr[j] == ')' {
				depth++
			}

			if depth > 0 {
				continue
			}

			// got one?
			if len(expr) >= j+len(ot.operators[ind]) && expr[j:j+len(ot.operators[ind])] == ot.operators[ind] {
				// ignore if it is the first character
				if j == 0 {
					continue
				}

				left = expr[:j]
				right = expr[j+len(ot.operators[ind]):]

				return left, right, op, nil
			}
		}
	}

	if haveFn {
		fmt.Println("have function ", fnOp, " : ", expr)

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
	ot.op = "fn"

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
		ot.fn = ot.df.Funcs().Get(ot.fnName)
		ot.fnReturn = ot.fn(true, nil, nil)
	}

	return nil
}

func (ot *OpTree) parenError() error {
	if strings.Count(ot.expr, "(") != strings.Count(ot.expr, ")") {
		return fmt.Errorf("mis-matched parens in %s", ot.expr)
	}

	return nil
}

func (ot *OpTree) Build() error {
	if e := ot.parenError(); e != nil {
		return e
	}

	l, r, op, _ := ot.scan()
	if op == "" {
		return nil
	}

	ot.op = op

	fmt.Println("whole:", ot.expr, "left: ", l, "right: ", r, "op: ", op)

	if l != "" {
		ot.left = &OpTree{
			expr:      l,
			op:        "",
			value:     nil,
			left:      nil,
			right:     nil,
			df:        ot.df,
			operators: ot.operators,
			fnNames:   ot.fnNames,
		}
		if e := ot.left.Build(); e != nil {
			return e
		}
	}

	if r != "" {
		ot.right = &OpTree{
			expr:      r,
			op:        "",
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
	}

	return nil
}
