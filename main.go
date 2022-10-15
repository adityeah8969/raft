package raft

import "log"

func main() {
	err := StartServing()
	if err != nil {
		log.Fatal("started serving ", err)
	}
	return
}
