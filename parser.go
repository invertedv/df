package df

import (
	"fmt"
	"strings"
)

const ops = "^#*#/#+#-#!=#==#>#<#>#>=#<=#&&#||"

type OpTree struct {
	expr string
	op   string

	value Column

	left  *OpTree
	right *OpTree

	fnName   string
	fn       AnyFunction
	fnReturn *FuncReturn
	inputs   []*OpTree

	funcs     Functions
	operators []string
	fnNames   []string
}

func NewOpTree(expression string, funcs Functions) (*OpTree, error) {
	expression = strings.ReplaceAll(expression, " ", "")

	opx := strings.Split(ops, "#")
	var fns []string
	for _, fn := range funcs {
		fns = append(fns, fn(true, nil, nil).Name+"(")
	}

	ot := &OpTree{expr: expression, funcs: funcs, operators: opx, fnNames: fns}
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

func (ot *OpTree) funcIndex() (haveFn bool, fnOp string) {
	for _, f := range ot.fnNames {
		if len(ot.expr) >= len(f) && ot.expr[:len(f)] == f {
			fmt.Println("function: ", f, " : ", ot.expr)
			return true, f
		}
	}

	return false, ""
}

func (ot *OpTree) scan() (left, right, op string, err error) {
	ot.expr = outerParen(ot.expr)
	// determine if expression starts with a function call
	haveFn, fnOp := ot.funcIndex()

	// work through the operators in increasing order of precedence
	for ind := len(ot.operators) - 1; ind >= 0; ind-- {
		depth := 0
		op = ot.operators[ind]
		// go right-to-left to avoid the a-b-c problem.
		for j := len(ot.expr) - 1; j >= 0; j-- {
			// ignore any operators that are within parentheses
			if ot.expr[j] == '(' {
				depth--
			}

			if ot.expr[j] == ')' {
				depth++
			}

			if depth > 0 {
				continue
			}

			// got one?
			if len(ot.expr) >= j+len(ot.operators[ind]) && ot.expr[j:j+len(ot.operators[ind])] == ot.operators[ind] {
				// if the operator starts the expression...this may be OK or not
				if j == 0 {
					if ot.operators[ind] == "+" || ot.operators[ind] == "-" {
						continue
					}

					return "", "", "", fmt.Errorf("illegal operator placement in %s", ot.expr)
				}

				left = ot.expr[:j]
				right = ot.expr[j+len(ot.operators[ind]):]

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

func args(xIn string) ([]string, error) {
	var (
		xOut []string
		arg  string
	)
	depth, start := 0, 0
	for ind := 0; ind < len(xIn); ind++ {
		if xIn[ind] == '(' {
			depth++
		}

		if xIn[ind] == ')' {
			depth--
		}

		if depth == 0 && xIn[ind] == ',' {
			if arg = xIn[start:ind]; arg == "" {
				return nil, fmt.Errorf("bad arguments: %s", xIn)
			}

			xOut = append(xOut, arg)
			start = ind + 1
		}
	}

	if arg = xIn[start:]; arg == "" {
		return nil, fmt.Errorf("bad arguments: %s", xIn)
	}

	xOut = append(xOut, arg)

	return xOut, nil
}

func (ot *OpTree) makeFn(fnName string) error {
	inner := strings.ReplaceAll(ot.expr, fnName, "")
	inner = inner[:len(inner)-1]
	var (
		x []string
		e error
	)
	if x, e = args(inner); e != nil {
		return e
	}

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
		if op, e = NewOpTree(x[ind], ot.funcs); e != nil {
			return e
		}

		if ex := op.Build(); ex != nil {
			return ex
		}

		ot.inputs = append(ot.inputs, op)
		ot.fn = ot.funcs.Get(ot.fnName)
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
	if ex := ot.parenError(); ex != nil {
		return ex
	}

	var (
		l, r string
		err  error
	)

	l, r, ot.op, err = ot.scan()
	if err != nil {
		return err
	}

	if ot.op == "" {
		return nil
	}

	fmt.Println("whole:", ot.expr, "left: ", l, "right: ", r, "op: ", ot.op)

	if l != "" {
		ot.left = &OpTree{
			expr:      l,
			funcs:     ot.funcs,
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
			funcs:     ot.funcs,
			operators: ot.operators,
			fnNames:   ot.fnNames,
		}

		if e := ot.right.Build(); e != nil {
			return e
		}
	}

	return nil
}
