package main

import (
	"os"

	"osctl/commands"
)

var (
	vBuild string
)

func main() {
	if err := commands.Execute(); err != nil {
		os.Exit(1)
	}
}
