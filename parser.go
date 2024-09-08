package df

import (
	"fmt"
	"strings"
)

type OpTree struct {
	value Column

	expr string

	op    string
	left  *OpTree
	right *OpTree

	fnName string
	inputs []*OpTree

	funcs   Functions
	fnNames []string

	ops operations
}

type operations [][]string

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

	col := ot.Value()
	col.Name(lr[0])

	if e := df.AppendColumn(col); e != nil {
		return e
	}

	return nil
}

func NewOpTree(expression string, funcs Functions) (*OpTree, error) {
	expression = strings.ReplaceAll(expression, " ", "")
	var fns []string
	for _, fn := range funcs {
		fns = append(fns, fn(true, nil, nil).Name+"(")
	}

	ot := &OpTree{expr: expression, funcs: funcs, ops: newOperations(), fnNames: fns}
	return ot, nil
}

func newOperations() operations {
	const (
		l7 = "!"
		l6 = "==,!=,>=,>,<=,<"
		l5 = "&&"
		l4 = "||"
		l3 = "^"
		l2 = "*,/"
		l1 = "+,-"
	)
	var order [][]string
	work := []string{l1, l2, l3, l4, l5, l6, l7}

	for ind := 0; ind < len(work); ind++ {
		order = append(order, strings.Split(work[ind], ","))
	}

	return order
}

//////////////// Exported OpTree methods

// Build creates the tree representation of the expression
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

	//	fmt.Println("whole:", ot.expr, "left: ", l, "right: ", r, "op: ", ot.op)

	if l != "" {
		ot.left = &OpTree{
			expr:    l,
			funcs:   ot.funcs,
			ops:     ot.ops,
			fnNames: ot.fnNames,
		}

		if e := ot.left.Build(); e != nil {
			return e
		}
	}

	if r != "" {
		ot.right = &OpTree{
			expr:    r,
			funcs:   ot.funcs,
			ops:     ot.ops,
			fnNames: ot.fnNames,
		}

		if e := ot.right.Build(); e != nil {
			return e
		}
	}

	if (ot.left == nil && ot.right != nil) || (ot.left != nil && ot.right == nil) {
		return fmt.Errorf("invalid expression: %s", ot.expr)
	}

	return nil
}

// Eval evaluates the expression over the dataframe df
func (ot *OpTree) Eval(df DF) error {
	// bottom level -- either a constant or a member of df
	if ot.op == "" && ot.fnName == "" {
		if c, e := df.Column(ot.expr); e == nil {
			ot.value = c
			return nil
		}

		var e error
		if ot.value, e = ot.constant(ot.expr, df); e != nil {
			return e
		}
		return nil
	}

	var (
		ex error
		c  Column
	)

	// handle function call
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

	// Do left/right Eval then op for this node
	if ot.left != nil {
		if e := ot.left.Eval(df); e != nil {
			return e
		}

		if e := ot.right.Eval(df); e != nil {
			return e
		}
	}

	// handle the usual ops
	if c, ex = df.DoOp(ot.mapOp(), ot.left.value, ot.right.value); ex != nil {
		return ex
	}

	ot.value = c

	return nil
}

// Value returns the value of the node. It will either be a Column or a scalar value.
func (ot *OpTree) Value() Column {
	return ot.value
}

// //////// Unexported OpTree methods

// TODO: make these not methods
// mapOp maps an operation to a standard function that implements it
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
	case "||":
		return "or"
	case "&&":
		return "and"
	case "!":
		return "not"
	case "==":
		return "eq"
	case "!=":
		return "ne"
	case ">=":
		return "ge"
	case ">":
		return "gt"
	case "<":
		return "lt"
	case "<=":
		return "le"
	default:
		return ot.op
	}
}

// constant handles the leaf of the OpTree when it is a constant.
// strings are surrounded by single quotes
func (ot *OpTree) constant(xIn string, df DF) (Column, error) {
	if xIn == "" {
		return nil, nil
	}

	if len(xIn) >= 2 && xIn[0:1] == "'" && xIn[len(xIn)-1:] == "'" {
		xIn = strings.TrimSuffix(strings.TrimPrefix(xIn, "'"), "'")
		return df.MakeColumn(xIn)
	}

	var (
		v  any
		dt DataTypes
		e  error
	)
	if v, dt, e = BestType(xIn); e != nil || dt == DTunknown || dt == DTstring {
		return nil, fmt.Errorf("cannot interpret %v as a constant", xIn)
	}

	return df.MakeColumn(v)
}

// outerParen strips away parentheses that surround the entire expression.
// For example, ((a+b)) becomes a+b
// but (a+b)*3 is not changed.
func (ot *OpTree) outerParen(s string) string {
	if len(s) <= 2 || s[0] != '(' {
		return s
	}

	depth, haveQuote := 0, false
	for ind := 0; ind < len(s); ind++ {
		depth, haveQuote = parenDepth(s[ind], depth, haveQuote)

		if depth == 0 {
			if ind == len(s)-1 {
				return ot.outerParen(s[1:ind])
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
			return true, f
		}
	}

	return false, ""
}

// scan determines whether the expression ot.expr is an (a) expression; (b) a function
// or a leaf and fills in the appropriate fields of ot.
func (ot *OpTree) scan() (left, right, op string, err error) {
	// strip outer parens
	ot.expr = ot.outerParen(ot.expr)

	// determine if expression starts with a function call
	haveFn, fnOp := ot.isFunction()

	// break into two expressions, if there are two
	left, right, op, err = ot.ops.find(ot.expr)
	if err != nil || op != "" {
		return left, right, op, err
	}

	if haveFn {
		if e := ot.makeFn(fnOp); e != nil {
			return "", "", "", e
		}
	}

	return "", "", "", nil
}

// args breaks a function argument string into its arguments
func (ot *OpTree) args(xIn string) ([]string, error) {
	var (
		xOut []string
		arg  string
	)
	depth, start, haveQuote := 0, 0, false
	for ind := 0; ind < len(xIn); ind++ {
		depth, haveQuote = parenDepth(xIn[ind], depth, haveQuote)

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

// makeFn populates the OpTree function fields
func (ot *OpTree) makeFn(fnName string) error {
	inner := strings.ReplaceAll(ot.expr, fnName, "")
	inner = inner[:len(inner)-1]

	var (
		x []string
		e error
	)
	if x, e = ot.args(inner); e != nil {
		return e
	}

	ot.fnName, ot.op = fnName[:len(fnName)-1], ""

	for ind := 0; ind < len(x); ind++ {
		if x[ind] == "" {
			continue
		}

		var (
			op *OpTree
			ex error
		)
		if op, ex = NewOpTree(x[ind], ot.funcs); ex != nil {
			return ex
		}

		if ex := op.Build(); ex != nil {
			return ex
		}

		ot.inputs = append(ot.inputs, op)
	}

	return nil
}

func (ot *OpTree) parenError() error {
	haveQuote, count := false, 0
	for ind := 0; ind < len(ot.expr); ind++ {
		char := ot.expr[ind : ind+1]
		if char == "'" {
			haveQuote = !haveQuote
		}

		if haveQuote {
			continue
		}

		if char == ")" {
			count++
		}

		if char == "(" {
			count--
		}
	}

	if count != 0 {
		return fmt.Errorf("mis-matched parens in %s", ot.expr)
	}

	return nil
}

// /////// operations methods

// trailingOp checks whether the end of expr is an operation
func (oper operations) trailingOp(expr string) bool {
	for j := 0; j < len(oper); j++ {
		for k := 0; k < len(oper[j]); k++ {
			if loc := strings.LastIndex(expr, oper[j][k]); loc >= 0 && loc+len(oper[j][k]) == len(expr) {
				return true
			}
		}
	}

	return false
}

// badStart checks whether the start of expr is an operation other than + or -
// if it does start with a + or -, inserts a 0 in front of it.
func (oper operations) badStart(expr *string) error {
	for j := 0; j < len(oper); j++ {
		for k := 0; k < len(oper[j]); k++ {
			if strings.Index(*expr, oper[j][k]) == 0 {
				if oper[j][k] != "+" && oper[j][k] != "-" && oper[j][k] != "!" {
					return fmt.Errorf("illegal operation in %s", *expr)
				}

				// if starts with a + or - or !, place a zero in front
				*expr = "0" + *expr

				return nil
			}
		}
	}

	return nil
}

// find finds where to split expr into two sub-expressions
func (oper operations) find(expr string) (left, right, op string, err error) {
	if expr == "" {
		return "", "", "", nil
	}

	if e := oper.badStart(&expr); e != nil {
		return "", "", "", e
	}

	for j := 0; j < len(oper); j++ {
		for k := 0; k < len(oper[j]); k++ {
			depth, haveQuote := 0, false
			for loc := len(expr) - 1; loc > 0; loc-- {

				depth, haveQuote = parenDepth(expr[loc], depth, haveQuote)
				if depth > 0 || haveQuote {
					continue
				}

				// Is this operator at position loc?
				if len(expr) < loc+len(oper[j][k]) || expr[loc:loc+len(oper[j][k])] != oper[j][k] {
					continue
				}

				if !oper.trailingOp(expr[:loc]) {
					left = expr[:loc]
					right = expr[loc+len(oper[j][k]):]
					op = oper[j][k]
					return left, right, op, nil
				}
			}
		}
	}

	return left, right, op, nil
}

// parenDepth updates the depth & haveQuote are updated based on char.
// depth counts the depth of parentheses within an expression.
// haveQuote flags whether the character is within single quotes
func parenDepth(char uint8, depthIn int, haveQuoteIn bool) (depthOut int, haveQuoteOut bool) {
	depthOut, haveQuoteOut = depthIn, haveQuoteIn
	if string(char) == "'" {
		haveQuoteOut = !haveQuoteOut
		return depthOut, haveQuoteOut
	}

	if char == ')' && !haveQuoteOut {
		depthOut++
		return depthOut, haveQuoteOut
	}

	if char == '(' && !haveQuoteOut {
		depthOut--
	}

	return depthOut, haveQuoteOut
}
