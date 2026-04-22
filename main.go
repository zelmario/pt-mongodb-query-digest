package main

import (
	"os"

	"github.com/zelmario/pt-mongodb-query-digest/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
