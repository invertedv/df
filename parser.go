package df

import (
	"fmt"
	"strings"
)

// Parse parses the expression expr and appends the result to df.
// Expressions have the form:
//
//	<result> := <expression>.
//
// A list of functions available is in the documentation.
func Parse(df DF, expr string) error {
	var (
		left string
		indx int
	)

	right := expr
	if indx = strings.Index(expr, ":="); indx < 0 {
		return fmt.Errorf("expression has no assignment")
	}

	left = expr[:indx]
	right = expr[indx+2:]
	left = strings.ReplaceAll(left, " ", "")

	ot := newOpTree(right, df.Fns())

	if ex := ot.build(); ex != nil {
		return ex
	}

	if ex := ot.eval(df); ex != nil {
		return ex
	}

	if ot.value == nil {
		return fmt.Errorf("parse error")
	}

	// Assign parent and dialect
	// If the parent isn't nil, that means we have a direct assignment like "a:=b" and we need to copy the column
	if ot.value.Parent() != nil {
		ot.value = ot.value.Copy()
	}

	// need to assign parent here so AppendColumns will work for sql
	_ = ColParent(df)(ot.value)
	_ = ColDialect(df.Dialect())(ot.value)

	// Need to drop existing column here so Rename will work
	if df.Column(left) != nil {
		_ = df.DropColumns(left)
	}

	if e := ot.value.Rename(left); e != nil {
		return e
	}

	return df.AppendColumn(ot.value, false)
}

// doOp runs a function
func doOp(df DF, opName string, inputs ...Column) (Column, error) {
	var fn Fn

	if fn = df.Fns().Get(opName); fn == nil {
		return nil, fmt.Errorf("op %s not defined, operation skipped", opName)
	}

	return RunDFfn(fn, df, inputs)
}

// opTree parses expressions.  The process has two steps:
//   1. Build - create a binary tree representation of the expression.
//   2. Evaluate - evaluate the expression.
type opTree struct {
	value Column

	expr string

	op    string  // op is the operation that joins the left & right pieces of the expression
	left  *opTree // left expression
	right *opTree // right expression

	fnName string // function to run
	inputs []*opTree // inputs to the function

	funcs   Fns // available functions
	fnNames []string // names of available functions

	ops operations // available operations (e.g. +, -, ...)

	dependencies []string // columns required to evaluate this node.
}

// operations is a list of operations available.  It is in order of
// precedence.  Each string slice lists operations that are of equal standing.
type operations [][]string

func newParsed(value any, dependencies ...string) Column {
	if value == nil {
		return nil
	}

	if col, ok := value.(Column); ok {
		_ = colDependencies(dependencies)(col)

		return col
	}

	return nil
}

func newOpTree(expression string, funcs Fns) *opTree {
	expression = compress(expression, " ")
	var fns []string
	for _, fn := range funcs {
		fns = append(fns, fn(true, nil, nil).Name+"(")
	}

	ot := &opTree{expr: expression, funcs: funcs, ops: newOperations(), fnNames: fns}
	return ot
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

	for ind := range len(work) {
		order = append(order, strings.Split(work[ind], ","))
	}

	return order
}

// build creates the tree representation of the expression
func (ot *opTree) build() error {
	// check parens match
	if ex := ot.parenError(); ex != nil {
		return ex
	}

	var (
		l, r string
		err  error
	)

	// scan breaks the expression into left & right, if possible
	if l, r, ot.op, err = ot.scan(); err != nil {
		return err
	}

	// nothing to do
	if ot.op == "" {
		return nil
	}

	// recurse on left and right
	if l != "" {
		ot.left = &opTree{
			expr:    l,
			funcs:   ot.funcs,
			ops:     ot.ops,
			fnNames: ot.fnNames,
		}

		if e := ot.left.build(); e != nil {
			return e
		}
	}

	if r != "" {
		ot.right = &opTree{
			expr:    r,
			funcs:   ot.funcs,
			ops:     ot.ops,
			fnNames: ot.fnNames,
		}

		if e := ot.right.build(); e != nil {
			return e
		}
	}

	if (ot.left == nil && ot.right != nil) || (ot.left != nil && ot.right == nil) {
		return fmt.Errorf("invalid expression: %s", ot.expr)
	}

	return nil
}

// eval evaluates the expression over the dataframe df
func (ot *opTree) eval(df DF) error {
	// bottom level -- either a constant or a member of df
	if ot.op == "" && ot.fnName == "" {
		if c := df.Column(ot.expr); c != nil {
			ot.dependencies = nodupAppend(ot.dependencies, c.Name())
			ot.value = newParsed(c, ot.dependencies...)
			return nil
		}

		var e error
		if ot.value, e = ot.constant(ot.expr); e != nil {
			return e
		}

		return nil
	}

	var (
		ex error
		c  any
	)

	// handle function call
	if ot.fnName != "" {
		for ind := range len(ot.inputs) {
			if e := ot.inputs[ind].eval(df); e != nil {
				return e
			}
		}

		var inp []Column
		for ind := range len(ot.inputs) {
			inp = append(inp, ot.inputs[ind].value)
			ot.dependencies = nodupAppend(ot.dependencies, ot.inputs[ind].dependencies...)
		}

		if c, ex = doOp(df, ot.fnName, inp...); ex != nil {
			return ex
		}

		ot.value = newParsed(c, ot.dependencies...)

		return nil
	}

	// Do left/right eval then op for this node
	if ot.left != nil {
		if e := ot.left.eval(df); e != nil {
			return e
		}

		if e := ot.right.eval(df); e != nil {
			return e
		}
	}

	// handle the usual ops
	var vl Column
	if ot.left != nil {
		vl = ot.left.value
		ot.dependencies = nodupAppend(ot.dependencies, ot.left.dependencies...)
	}

	var vr Column
	if ot.right != nil {
		vr = ot.right.value
		ot.dependencies = nodupAppend(ot.dependencies, ot.right.dependencies...)
	}

	// run op on left/right
	if c, ex = doOp(df, mapOp(ot.op), vl, vr); ex != nil {
		return ex
	}

	ot.value = newParsed(c, ot.dependencies...)

	return nil
}

// constant handles the leaf of the opTree when it is a constant.
// strings are surrounded by single quotes
func (ot *opTree) constant(xIn string) (Column, error) {
	if xIn == "" {
		return nil, nil
	}

	if len(xIn) >= 2 && xIn[0:1] == "'" && xIn[len(xIn)-1:] == "'" {
		xIn = strings.TrimSuffix(strings.TrimPrefix(xIn, "'"), "'")
		c, _ := NewScalar(xIn)
		return newParsed(c), nil
	}

	var (
		v  any
		dt DataTypes
		e  error
	)
	if v, dt, e = bestType(xIn, false); e != nil || dt == DTunknown || dt == DTstring {
		return nil, fmt.Errorf("cannot interpret %v as a constant...missing function?", xIn)
	}

	c, _ := NewScalar(v)

	return newParsed(c), nil
}

// outerParen strips away parentheses that surround the entire expression.
// For example, ((a+b)) becomes a+b
// but (a+b)*3 is not changed.
func (ot *opTree) outerParen(s string) string {
	if len(s) <= 2 || s[0] != '(' {
		return s
	}

	depth, haveQuote := 0, false
	for ind := range len(s) {
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
func (ot *opTree) isFunction() (haveFn bool, fnOp string) {
	for _, f := range ot.fnNames {
		if len(ot.expr) >= len(f) && ot.expr[:len(f)] == f {
			return true, f
		}
	}

	return false, ""
}

// scan determines whether the expression ot.expr is an (a) expression; (b) a function
// or a leaf and fills in the appropriate fields of ot.
func (ot *opTree) scan() (left, right, op string, err error) {
	// strip outer parens
	ot.expr = ot.outerParen(ot.expr)

	var leadingOp bool
	// break into two expressions, if there are two
	left, right, op, leadingOp = ot.ops.find(ot.expr)

	// if the operation is the first character, it can be: +, - or !
	if leadingOp {
		switch op {
		case "+":
			ot.expr = right
		case "-":
			ot.expr = fmt.Sprintf("neg(%s)", right)
		case "!":
			ot.expr = fmt.Sprintf("not(%s)", right)
		default:
			return "", "", "", fmt.Errorf("invalid leading operation: %s", op)
		}

		op = ""
	}

	if op != "" {
		return left, right, op, err
	}

	// determine if expression starts with a function call
	haveFn, fnOp := ot.isFunction()

	if haveFn {
		if e := ot.makeFn(fnOp); e != nil {
			return "", "", "", e
		}
	}

	return "", "", "", nil
}

// args breaks a function argument string into its arguments
func (ot *opTree) args(xIn string) ([]string, error) {
	var (
		xOut []string
		arg  string
	)
	depth, start, haveQuote := 0, 0, false
	for ind := range len(xIn) {
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
		// fn takes no args
		return nil, nil
	}

	xOut = append(xOut, arg)

	return xOut, nil
}

// makeFn populates the opTree function fields
func (ot *opTree) makeFn(fnName string) error {
	inner := strings.Replace(ot.expr, fnName, "", 1)
	inner = inner[:len(inner)-1]

	var (
		x []string
		e error
	)
	if x, e = ot.args(inner); e != nil {
		return e
	}

	ot.fnName, ot.op = fnName[:len(fnName)-1], ""

	for ind := range len(x) {
		if x[ind] == "" {
			continue
		}

		op := newOpTree(x[ind], ot.funcs)

		if ex := op.build(); ex != nil {
			return ex
		}

		ot.inputs = append(ot.inputs, op)
	}

	return nil
}

// parenError checks for matching parentheses.
func (ot *opTree) parenError() error {
	haveQuote, count := false, 0
	for ind := range len(ot.expr) {
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

// ************** operations methods **************

// trailingOp checks whether the end of expr is an operation
func (oper operations) trailingOp(expr string) bool {
	for j := range len(oper) {
		for k := range len(oper[j]) {
			if loc := strings.LastIndex(expr, oper[j][k]); loc >= 0 && loc+len(oper[j][k]) == len(expr) {
				return true
			}
		}
	}

	return false
}

// find finds where to split expr into two sub-expressions
func (oper operations) find(expr string) (left, right, op string, leadingOp bool) {
	if expr == "" {
		return "", "", "", false
	}

	for j := range len(oper) {
		for k := range len(oper[j]) {
			depth, haveQuote := 0, false
			for loc := len(expr) - 1; loc >= 0; loc-- {
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
					leadingOp = left == ""
					return left, right, op, leadingOp
				}
			}
		}
	}

	return left, right, op, leadingOp
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

// mapOp maps an operation to a standard function that implements it
func mapOp(op string) string {
	switch op {
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
		return op
	}
}

// nodupAppend appends xadd if it's not already in x
func nodupAppend(x []string, xadd ...string) []string {
	for _, xa := range xadd {
		if !Has(xa, x) {
			x = append(x, xa)
		}
	}

	return x
}

// compress removes target from x except when it's between single quotes
func compress(x, target string) string {
	out := ""
	hasQuote := false
	for ind := range len(x) {
		char := x[ind : ind+1]
		if char == "'" {
			hasQuote = !hasQuote
		}

		if hasQuote || (!hasQuote && char != target) {
			out += char
		}
	}

	return out
}
