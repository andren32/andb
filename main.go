package main

import (
	"andb/memtable"
	"fmt"
)

func main() {
	m := memtable.NewSkiplistMemtable()
	m.Insert("a", 0, []byte{})
	fmt.Println(m.Get("a"))
	fmt.Println(m.Get("b"))
}
