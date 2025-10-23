package main

import (
	"fmt"
	"os"

	"curator-go/internal/commands"
)

var (
    vBuild     string
)

func main() {
	if err := commands.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
