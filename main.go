package main

import (
	"fmt"

	cmd "github.com/kweheliye/json2parquet/cmd"
)

func main() {
	cmd.Execute() // All CLI commands handled in cmd package
	fmt.Println("Pipeline completed successfully!")
}
