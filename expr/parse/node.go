// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Parse nodes.

package parse

import (
	"fmt"
	"strconv"
)

var textFormat = "%s" // Changed to "%q" in tests for better error messages.

// A Node is an element in the parse tree. The interface is trivial.
// The interface contains an unexported method so that only
// types local to this package can satisfy it.
type Node interface {
	Type() NodeType
	String() string
	StringAST() string
	Position() Pos // byte position of start of node in full original input string
	Check() error  // performs type checking for itself and sub-nodes
	Return() FuncType
	// Make sure only functions in this package can create Nodes.
	unexported()
}

// NodeType identifies the type of a parse tree node.
type NodeType int

// Pos represents a byte position in the original input text from which
// this template was parsed.
type Pos int

func (p Pos) Position() Pos {
	return p
}

// unexported keeps Node implementations local to the package.
// All implementations embed Pos, so this takes care of it.
func (Pos) unexported() {
}

// Type returns itself and provides an easy default implementation
// for embedding in a Node. Embedded in all non-trivial Nodes.
func (t NodeType) Type() NodeType {
	return t
}

const (
	NodeFunc   NodeType = iota // A function call.
	NodeBinary                 // Binary operator: math, logical, compare
	NodeUnary                  // Unary operator: !, -
	NodeString                 // A string constant.
	NodeNumber                 // A numerical constant.
)

// Nodes.

// FuncNode holds a function invocation.
type FuncNode struct {
	NodeType
	Pos
	Name string
	F    Func
	Args []Node
}

func newFunc(pos Pos, name string, f Func) *FuncNode {
	return &FuncNode{NodeType: NodeFunc, Pos: pos, Name: name, F: f}
}

func (c *FuncNode) append(arg Node) {
	c.Args = append(c.Args, arg)
}

func (c *FuncNode) String() string {
	s := c.Name + "("
	for i, arg := range c.Args {
		if i > 0 {
			s += ", "
		}
		s += arg.String()
	}
	s += ")"
	return s
}

func (c *FuncNode) StringAST() string {
	s := c.Name + "("
	for i, arg := range c.Args {
		if i > 0 {
			s += ", "
		}
		s += arg.StringAST()
	}
	s += ")"
	return s
}

func (c *FuncNode) Check() error {
	const errFuncType = "parse: bad argument type in %s, expected %s, got %s"
	if len(c.Args) < len(c.F.Args) {
		return fmt.Errorf("parse: not enough arguments for %s", c.Name)
	} else if len(c.Args) > len(c.F.Args) {
		return fmt.Errorf("parse: too many arguments for %s", c.Name)
	}
	for i, a := range c.Args {
		t := c.F.Args[i]
		at := a.Return()
		if t != at {
			return fmt.Errorf("parse: expected %v, got %v", t, at)
		}
		if err := a.Check(); err != nil {
			return err
		}
	}
	return nil
}

func (f *FuncNode) Return() FuncType { return f.F.Return }

// NumberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type NumberNode struct {
	NodeType
	Pos
	IsUint  bool    // Number has an unsigned integral value.
	IsFloat bool    // Number has a floating-point value.
	Uint64  uint64  // The unsigned integer value.
	Float64 float64 // The floating-point value.
	Text    string  // The original textual representation from the input.
}

func newNumber(pos Pos, text string) (*NumberNode, error) {
	n := &NumberNode{NodeType: NodeNumber, Pos: pos, Text: text}
	// Do integer test first so we get 0x123 etc.
	u, err := strconv.ParseUint(text, 0, 64) // will fail for -0.
	if err == nil {
		n.IsUint = true
		n.Uint64 = u
	}
	// If an integer extraction succeeded, promote the float.
	if n.IsUint {
		n.IsFloat = true
		n.Float64 = float64(n.Uint64)
	} else {
		f, err := strconv.ParseFloat(text, 64)
		if err == nil {
			n.IsFloat = true
			n.Float64 = f
			// If a floating-point extraction succeeded, extract the int if needed.
			if !n.IsUint && float64(uint64(f)) == f {
				n.IsUint = true
				n.Uint64 = uint64(f)
			}
		}
	}
	if !n.IsUint && !n.IsFloat {
		return nil, fmt.Errorf("illegal number syntax: %q", text)
	}
	return n, nil
}

func (n *NumberNode) String() string {
	return n.Text
}

func (n *NumberNode) StringAST() string {
	return n.String()
}

func (n *NumberNode) Check() error {
	return nil
}

func (n *NumberNode) Return() FuncType { return TYPE_SCALAR }

// StringNode holds a string constant. The value has been "unquoted".
type StringNode struct {
	NodeType
	Pos
	Quoted string // The original text of the string, with quotes.
	Text   string // The string, after quote processing.
}

func newString(pos Pos, orig, text string) *StringNode {
	return &StringNode{NodeType: NodeString, Pos: pos, Quoted: orig, Text: text}
}

func (s *StringNode) String() string {
	return s.Quoted
}

func (s *StringNode) StringAST() string {
	return s.String()
}

func (s *StringNode) Check() error {
	return nil
}

func (s *StringNode) Return() FuncType { return TYPE_STRING }

// BinaryNode holds two arguments and an operator.
type BinaryNode struct {
	NodeType
	Pos
	Args     [2]Node
	Operator item
	OpStr    string
}

func newBinary(operator item, arg1, arg2 Node) *BinaryNode {
	return &BinaryNode{NodeType: NodeBinary, Pos: operator.pos, Args: [2]Node{arg1, arg2}, Operator: operator, OpStr: operator.val}
}

func (b *BinaryNode) String() string {
	return fmt.Sprintf("%s %s %s", b.Args[0], b.Operator.val, b.Args[1])
}

func (b *BinaryNode) StringAST() string {
	return fmt.Sprintf("%s(%s, %s)", b.Operator.val, b.Args[0], b.Args[1])
}

func (b *BinaryNode) Check() error {
	t1 := b.Args[0].Return()
	t2 := b.Args[1].Return()
	if t1 == TYPE_SERIES && t2 == TYPE_SERIES {
		return fmt.Errorf("parse: type error in %s: at least one side must be a number", b)
	}
	check := t1
	if t1 == TYPE_SERIES {
		check = t2
	}
	if check != TYPE_NUMBER && check != TYPE_SCALAR {
		return fmt.Errorf("parse: type error in %s: expected a number", b)
	}
	if err := b.Args[0].Check(); err != nil {
		return err
	}
	return b.Args[1].Check()
}

func (b *BinaryNode) Return() FuncType {
	t0 := b.Args[0].Return()
	t1 := b.Args[1].Return()
	if t1 > t0 {
		return t1
	}
	return t0
}

// UnaryNode holds one argument and an operator.
type UnaryNode struct {
	NodeType
	Pos
	Arg      Node
	Operator item
	OpStr    string
}

func newUnary(operator item, arg Node) *UnaryNode {
	return &UnaryNode{NodeType: NodeUnary, Pos: operator.pos, Arg: arg, Operator: operator, OpStr: operator.val}
}

func (u *UnaryNode) String() string {
	return fmt.Sprintf("%s%s", u.Operator.val, u.Arg)
}

func (u *UnaryNode) StringAST() string {
	return fmt.Sprintf("%s(%s)", u.Operator.val, u.Arg)
}

func (u *UnaryNode) Check() error {
	switch t := u.Arg.Return(); t {
	case TYPE_NUMBER, TYPE_SERIES, TYPE_SCALAR:
		return u.Arg.Check()
	default:
		return fmt.Errorf("parse: type error in %s, expected %s, got %s", u, "number", t)
	}
}

func (u *UnaryNode) Return() FuncType {
	return u.Arg.Return()
}

// Walk invokes f on n and sub-nodes of n.
func Walk(n Node, f func(Node)) {
	f(n)
	switch n := n.(type) {
	case *BinaryNode:
		Walk(n.Args[0], f)
		Walk(n.Args[1], f)
	case *FuncNode:
		for _, a := range n.Args {
			Walk(a, f)
		}
	case *NumberNode, *StringNode:
		// Ignore.
	case *UnaryNode:
		Walk(n.Arg, f)
	default:
		panic(fmt.Errorf("other type: %T", n))
	}
}
