package main

import (
	"dedup-expri/context"

	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	ctx, err := context.New("./config.json")
	if err != nil {
		fmt.Println("Error loading context:", err)
		return
	}
	err = ctx.Process()
	if err != nil {
		fmt.Println("process image:", err)
		return
	}
}
