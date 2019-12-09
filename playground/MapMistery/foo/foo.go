package foo

import(
	"fmt"
)

type FooType struct {
}

func (f FooType) Foo1() {
	fmt.Println("foo1")
}
