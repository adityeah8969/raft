package main

import (
	"log"

	"github.com/adityeah8969/raft"
)

func main() {
	err := raft.StartServing()
	if err != nil {
		log.Fatal("started serving ", err)
	}
}
