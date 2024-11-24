package df

import (
	"fmt"
	"strings"
	"time"
)

type OpTree struct {
	value *Parsed

	expr string

	op    string
	left  *OpTree
	right *OpTree

	fnName string
	inputs []*OpTree

	funcs   Fns
	fnNames []string

	ops operations

	dependencies []string
}

type operations [][]string

type Parsed struct {
	which string

	scalar any
	col    Column
	df     DF
}

func NewParsed(value any, dependencies ...string) *Parsed {
	p := &Parsed{}

	if _, ok := value.(DF); ok {
		p.df = value.(DF)
		p.which = "DF"
		return p
	}

	if _, ok := value.(Column); ok {
		value.(Column).SetDependencies(dependencies)
		p.col = value.(Column)
		p.which = "Column"
		return p
	}

	p.which = "Scalar"
	// if this comes in as float or date, keep that.
	// if it's int -- could interpret as a date
	switch x := value.(type) {
	case float64:
		p.scalar = x
	case time.Time:
		p.scalar = x
	case string:
		p.scalar = x
	case int:
		p.scalar = x
	default:
		var (
			xx any
			dt DataTypes
			e  error
		)
		if xx, dt, e = BestType(value); e != nil || dt == DTunknown {
			return nil
		}

		p.scalar = xx
	}

	return p
}

func (p *Parsed) Value() any {
	if p.df != nil {
		return p.df
	}

	if p.col != nil {
		return p.col
	}

	if p.scalar != nil {
		return p.scalar
	}

	return nil
}

func (p *Parsed) AsDF() DF {
	return p.df
}

func (p *Parsed) AsColumn() Column {
	return p.col
}

func (p *Parsed) AsScalar() any {
	return p.scalar
}

func (p *Parsed) Which() string {
	return p.which
}

func ParseExpr(expr string, df *DFcore) (*Parsed, error) {
	var (
		ot *OpTree
		e  error
	)
	if ot, e = NewOpTree(expr, df.Fns()); e != nil {
		return nil, e
	}

	if ex := ot.Build(); ex != nil {
		return nil, ex
	}

	if ex := ot.Eval(df); ex != nil {
		return nil, ex
	}

	return ot.Value(), nil
}

func NewOpTree(expression string, funcs Fns) (*OpTree, error) {
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
func (ot *OpTree) Eval(df *DFcore) error {
	// bottom level -- either a constant or a member of df
	if ot.op == "" && ot.fnName == "" {
		if c := df.Column(ot.expr); c != nil {
			ot.dependencies = nodupAppend(ot.dependencies, c.Name())
			ot.value = NewParsed(c, ot.dependencies...)
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
			if e := ot.inputs[ind].Eval(df); e != nil {
				return e
			}
		}

		var inp []*Parsed
		for ind := 0; ind < len(ot.inputs); ind++ {
			inp = append(inp, ot.inputs[ind].Value())
			ot.dependencies = nodupAppend(ot.dependencies, ot.inputs[ind].dependencies...)
		}

		if c, ex = df.DoOp(ot.fnName, inp...); ex != nil {
			return ex
		}

		ot.value = NewParsed(c, ot.dependencies...)

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
	var vl *Parsed
	if ot.left != nil {
		vl = ot.left.Value()
		ot.dependencies = nodupAppend(ot.dependencies, ot.left.dependencies...)
	}

	var vr *Parsed
	if ot.right != nil {
		vr = ot.right.Value()
		ot.dependencies = nodupAppend(ot.dependencies, ot.right.dependencies...)
	}

	if c, ex = df.DoOp(mapOp(ot.op), vl, vr); ex != nil {
		return ex
	}

	ot.value = NewParsed(c, ot.dependencies...)

	return nil
}

// Value returns the value of the node. It will either be a Column or a scalar value.
func (ot *OpTree) Value() *Parsed {
	return ot.value
}

// //////// Unexported OpTree methods

// constant handles the leaf of the OpTree when it is a constant.
// strings are surrounded by single quotes
func (ot *OpTree) constant(xIn string) (*Parsed, error) {
	if xIn == "" {
		return nil, nil
	}

	if len(xIn) >= 2 && xIn[0:1] == "'" && xIn[len(xIn)-1:] == "'" {
		xIn = strings.TrimSuffix(strings.TrimPrefix(xIn, "'"), "'")
		return NewParsed(xIn), nil
	}

	var (
		v  any
		dt DataTypes
		e  error
	)
	if v, dt, e = BestType(xIn); e != nil || dt == DTunknown || dt == DTstring {
		return nil, fmt.Errorf("cannot interpret %v as a constant", xIn)
	}

	return NewParsed(v), nil
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

	var leadingOp bool
	// break into two expressions, if there are two
	left, right, op, leadingOp = ot.ops.find(ot.expr)

	// if the operation is the first character, it can be: +, - or !
	if leadingOp {
		switch op {
		case "+":
			ot.expr = fmt.Sprintf("%s", right)
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
		// fn takes no args
		return nil, nil
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
		if !Has(xa, "", x...) {
			x = append(x, xa)
		}
	}

	return x
}
