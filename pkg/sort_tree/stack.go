package sort_tree

type Stack struct {
	stack []*TreeNode
	base int
	top int
}

func InitStack(n int) Stack {
	stack := Stack{
		stack: make([]*TreeNode, n),
	}
	return stack
}

func (stack *Stack) Push(value *TreeNode) {
	if stack.top == len(stack.stack) {
		stack.stack = append(stack.stack, value)
	}else {
		stack.stack[stack.top] = value
	}
	stack.top ++
}

func (stack *Stack) Pop() (*TreeNode, bool) {
	if stack.top == stack.base {
		return nil, false
	}
	stack.top --
	return stack.stack[stack.top], true
}