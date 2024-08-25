package main

import (
	"fmt"
	"workflow/graph/example"
)

func main() {
	fmt.Println("Running Task Example:")
	fmt.Println("--------------------")
	example.RunTaskExample()

	fmt.Println("\nRunning Conditional Example:")
	fmt.Println("---------------------------")
	example.RunConditionalExample()
}
