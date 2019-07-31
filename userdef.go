package main

import (
	"test/Chord"
	"strconv"
)

// NewNode use your own node create method to overwrite
func NewNode(port int) dhtNode {
	var a DHT.Node
	a.Create_(DHT.GetLocalAddress() + ":" + strconv.Itoa(port))
	a.Create()
	return &a
}
