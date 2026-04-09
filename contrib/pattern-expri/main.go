package main

import (
	"flag"
	"log"
)

func main() {
	configPath := flag.String("config", "config.json", "path to image config json")
	chunksPerGroup := flag.Int("m", 1024, "number of chunks in each group")
	flag.Parse()

	err := RunAll(*configPath, *chunksPerGroup)
	if err != nil {
		log.Fatal(err)
	}
}
