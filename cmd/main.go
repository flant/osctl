package main

import (
	"fmt"
	"os"

	"osctl/commands"
)

var (
	appVersion string
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		if appVersion == "" {
			appVersion = "dev"
		}
		fmt.Println(appVersion)
		os.Exit(0)
	}

	if err := commands.Execute(appVersion); err != nil {
		os.Exit(1)
	}
}
