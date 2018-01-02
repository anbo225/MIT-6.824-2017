package main

import (
	"fmt"
	"time"
)

var timer *time.Timer

func hello() {
	// <-timer.C
	fmt.Println("hello")
	// timer.Reset(time.Second)
	hello()
}

func main() {
	timer = time.NewTimer(time.Second)
	hello()

}
