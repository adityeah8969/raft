package command

type Command interface {
	getAction() string
	getAttr() string
	getVal() string
}
