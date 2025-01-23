package df

import (
	"fmt"
	"strings"
)

type Parsed struct {
	col  Column
	df   DF
	plot *Plot
}

func (p *Parsed) Value() any {
	if p.df != nil {
		return p.df
	}

	if p.col != nil {
		return p.col
	}

	if p.plot != nil {
		return p.plot
	}

	return nil
}

func (p *Parsed) DF() DF {
	return p.df
}

func (p *Parsed) Column() Column {
	return p.col
}

func (p *Parsed) Plot() *Plot { return p.plot }

func (p *Parsed) Which() ReturnTypes {
	if p.df != nil {
		return RTdataFrame
	}

	if p.col != nil {
		return RTcolumn
	}

	if p.plot != nil {
		return RTplot
	}

	return RTnone
}

func Parse(df DF, expr string) (*Parsed, error) {
	var left string
	right := expr

	if indx := strings.Index(expr, ":="); indx > 0 {
		left = expr[:indx]
		right = expr[indx+2:]
	}

	ot := newOpTree(right, df.Fns())

	if ex := ot.build(); ex != nil {
		return nil, ex
	}

	if ex := ot.eval(df); ex != nil {
		return nil, ex
	}

	// Assign parent and dialect
	if ot.value.col != nil {
		_ = ColParent(df)(ot.value.col)
		_ = ColDialect(df.Dialect())(ot.value.col)
	}

	if left == "" || ot.value.Column() == nil {
		return ot.value, nil
	}

	if e := ColName(strings.ReplaceAll(left, " ", ""))(ot.value.Column()); e != nil {
		return nil, e
	}

	if e := df.AppendColumn(ot.value.Column(), true); e != nil {
		return nil, e
	}

	return nil, nil
}

func doOp(df DF, opName string, inputs ...*Parsed) (any, error) {
	var fn Fn

	if fn = df.Fns().Get(opName); fn == nil {
		return nil, fmt.Errorf("op %s not defined, operation skipped", opName)
	}

	var vals []Column
	for ind := 0; ind < len(inputs); ind++ {
		switch inputs[ind].Which() {
		case RTdataFrame:
			return nil, fmt.Errorf("cannot take DF as function input")
		case RTcolumn:
			vals = append(vals, inputs[ind].Column())
		}
	}

	var (
		col any
		e   error
	)
	if col, e = RunDFfn(fn, df, vals); e != nil {
		return nil, e
	}

	return col, nil
}

type opTree struct {
	value *Parsed

	expr string

	op    string
	left  *opTree
	right *opTree

	fnName string
	inputs []*opTree

	funcs   Fns
	fnNames []string

	ops operations

	dependencies []string
}

type operations [][]string

func newParsed(value any, dependencies ...string) *Parsed {
	p := &Parsed{}

	if value == nil {
		return p
	}

	if _, ok := value.(DF); ok {
		p.df = value.(DF)
		return p
	}

	if col, ok := value.(Column); ok {
		_ = colDependencies(dependencies)(col) //(value.(Column).Core())
		p.col = col
		return p
	}

	if plt, ok := value.(*Plot); ok {
		p.plot = plt
		return p
	}

	return nil
}

func newOpTree(expression string, funcs Fns) *opTree {
	expression = strings.ReplaceAll(expression, " ", "")
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

	for ind := 0; ind < len(work); ind++ {
		order = append(order, strings.Split(work[ind], ","))
	}

	return order
}

// build creates the tree representation of the expression
func (ot *opTree) build() error {
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
		for ind := 0; ind < len(ot.inputs); ind++ {
			if e := ot.inputs[ind].eval(df); e != nil {
				return e
			}
		}

		var inp []*Parsed
		for ind := 0; ind < len(ot.inputs); ind++ {
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
	var vl *Parsed
	if ot.left != nil {
		vl = ot.left.value
		ot.dependencies = nodupAppend(ot.dependencies, ot.left.dependencies...)
	}

	var vr *Parsed
	if ot.right != nil {
		vr = ot.right.value
		ot.dependencies = nodupAppend(ot.dependencies, ot.right.dependencies...)
	}

	if c, ex = doOp(df, mapOp(ot.op), vl, vr); ex != nil {
		return ex
	}

	ot.value = newParsed(c, ot.dependencies...)

	return nil
}

// //////// Unexported opTree methods

// constant handles the leaf of the opTree when it is a constant.
// strings are surrounded by single quotes
func (ot *opTree) constant(xIn string) (*Parsed, error) {
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
	if v, dt, e = bestType(xIn); e != nil || dt == DTunknown || dt == DTstring {
		return nil, fmt.Errorf("cannot interpret %v as a constant", xIn)
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

	for ind := 0; ind < len(x); ind++ {
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

func (ot *opTree) parenError() error {
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

// find finds where to split expr into two sub-expressions
func (oper operations) find(expr string) (left, right, op string, leadingOp bool) {
	if expr == "" {
		return "", "", "", false
	}

	for j := 0; j < len(oper); j++ {
		for k := 0; k < len(oper[j]); k++ {
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

func nodupAppend(x []string, xadd ...string) []string {
	for _, xa := range xadd {
		if !Has(xa, x) {
			x = append(x, xa)
		}
	}

	return x
}
