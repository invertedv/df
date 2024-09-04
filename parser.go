package df

import (
	"fmt"
	"strings"
)

const ops = "^#*#/#+#-#!=#==#>#<#>#>=#<=#&&#||"

type OpTree struct {
	expr string

	op    string
	value any

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

func orderOps() [][]string {
	const (
		l1 = "^"
		l2 = "*,/"
		l3 = "+,-"
	)
	var order [][]string
	work := []string{l1, l2, l3}

	for ind := 0; ind < len(work); ind++ {
		order = append(order, strings.Split(work[ind], ","))
	}

	return order
}

func NewOpTree(expression string, funcs Functions) (*OpTree, error) {
	expression = strings.ReplaceAll(expression, " ", "")
	//TODO : fix this
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

// isFunction determines whether the expression starts with a function
func (ot *OpTree) isFunction() (haveFn bool, fnOp string) {
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
	haveFn, fnOp := ot.isFunction()

	// TODO: some operators have equal precedence: FIRST +, - or *, /
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

	if l, r, ot.op, err = ot.scan(); err != nil {
		return err
	}

	// nothing to do
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

func (ot *OpTree) mapOp() string {
	switch ot.op {
	case "+":
		return "add"
	case "-":
		return "subtract"
	case "*":
		return "multiply"
	case "/":
		return "divide"
	case "^":
		return "pow"
	default:
		return ot.op
	}
}

func constant(xIn string) (any, error) {
	if xIn == "" {
		return xIn, nil
	}

	if len(xIn) >= 2 && xIn[0:1] == "'" && xIn[len(xIn)-1:] == "'" {
		return strings.TrimSuffix(strings.TrimPrefix(xIn, "'"), "'"), nil
	}

	v, dt, e := BestType(xIn)
	if e != nil || dt == DTunknown || dt == DTstring {
		return nil, fmt.Errorf("cannot parse %v", xIn)
	}

	return v, nil
}

func (ot *OpTree) Eval(df DF) error {
	// bottom level -- either a constant or a member of df
	if ot.op == "" && ot.fnName == "" {
		if c, e := df.Column(ot.expr); e == nil {
			ot.value = c
			return nil
		}

		var e error
		if ot.value, e = constant(ot.expr); e != nil {
			return e
		}
		return nil
	}

	// Do left/right Eval then function
	if ot.left != nil {
		if e := ot.left.Eval(df); e != nil {
			return e
		}

		if e := ot.right.Eval(df); e != nil {
			return e
		}

	}

	var (
		ex error
		c  Column
	)

	// handle functions
	if ot.inputs != nil {
		for ind := 0; ind < len(ot.inputs); ind++ {
			if e := ot.inputs[ind].Eval(df); e != nil {
				return e
			}
		}

		var inp []any
		for ind := 0; ind < len(ot.inputs); ind++ {
			inp = append(inp, ot.inputs[ind].value)
		}
		if c, ex = df.DoOp(ot.fnName, inp...); ex != nil {
			return ex
		}

		ot.value = c

		return nil
	}

	// handle the usual ops
	if c, ex = df.DoOp(ot.mapOp(), ot.left.value, ot.right.value); ex != nil {
		return ex
	}

	ot.value = c

	return nil
}

func (ot *OpTree) Value() any {
	return ot.value
}

func Parse(eqn string, df DF) error {
	lr := strings.Split(strings.ReplaceAll(eqn, " ", ""), ":=")
	if len(lr) != 2 {
		return fmt.Errorf("not an equation: %s", eqn)
	}

	var (
		ot  *OpTree
		err error
	)

	if ot, err = NewOpTree(lr[1], df.Funcs()); err != nil {
		return err
	}

	if e := ot.Build(); e != nil {
		return e
	}

	if e := ot.Eval(df); e != nil {
		return e
	}

	col := ot.Value().(Column)

	col.Name(lr[0])

	if e := df.AppendColumn(col); e != nil {
		return e
	}

	return nil
}
