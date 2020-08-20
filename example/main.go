package main

import (
	"strconv"
)

func main() {
	tree := createTestLsm()
	tree.Insert(&testEntry{data: uint32(13), str: strconv.Itoa(13)})
}
