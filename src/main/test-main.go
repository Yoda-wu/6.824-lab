package main

import "fmt"

func main() {

	for y := 1.5; y > -1.5; y -= 0.1 {
		for x := -1.5; x < 1.5; x += 0.05 {
			a := x*x + y*y - 1
			if a*a*a-x*x*y*y*y <= 0.0 {
				fmt.Print("*")
			} else {
				fmt.Print(" ")
			}
		}
		fmt.Print("\n")
	}
}
