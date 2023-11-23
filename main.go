package main

import (
	"fmt"
	"github.com/maxgio92/homework-object-storage/cmd/serve"
	"os"
)

func main() {
	cmd := serve.NewCmd()

	err := cmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
