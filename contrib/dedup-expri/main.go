package main

import (
	"bufio"
	"database/sql"
	"dedup-expri/builder"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var maxGB int64 = 512 * 1024 * 1024 * 1024
var count = 0

func hasNydusSuffix(name string) bool {
	return strings.HasSuffix(name, "nydus")
}

func load(name string) error {
	cmdArgs := exec.Command("sudo", "nydusify", "load", "--source", name, "--source-insecure")
	cmdArgs.Stdout = os.Stdout
	cmdArgs.Stderr = os.Stderr
	err := cmdArgs.Run()

	if err == nil {
		fmt.Println("Load command executed successfully for", name)
		return nil
	} else {
		fmt.Printf("Load command failed for %s: %v\n", name, err)
		return fmt.Errorf("load command failed")
	}
}

func loadImages(images []string) error {
	logFile, err := os.OpenFile("Result/load.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer logFile.Close()
	writer := bufio.NewWriter(logFile)
	db, err := sql.Open("sqlite3", "./chunk.db")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()
	build := builder.Builder{}
	for _, name := range images {
		var logLine string
		if hasNydusSuffix(name) {
			err := load(name)
			if err != nil {
				logLine = fmt.Sprintf("%s: load failed\n", name)
			} else {
				logLine = fmt.Sprintf("%s: load success\n", name)
			}
		} else {
			logLine = fmt.Sprintf("%s: is not nydus image\n", name)
		}
		_, _ = writer.WriteString(logLine)

		if size, err := build.CalcTotalSize(db); err != nil || size >= 4*maxGB {
			count++
			workdir := fmt.Sprintf("Result/%d", count)
			_ = os.Mkdir(workdir, 0755)
			calcAll(db, workdir)
			return writer.Flush()
		} else if size >= maxGB || size >= 2*maxGB || size >= 3*maxGB {
			count++
			workdir := fmt.Sprintf("Result/%d", count)
			_ = os.Mkdir(workdir, 0755)
			calcAll(db, workdir)
		}
	}
	count++
	workdir := fmt.Sprintf("Result/%d", count)
	_ = os.Mkdir(workdir, 0755)
	calcAll(db, workdir)
	return writer.Flush()
}

func calcAll(db *sql.DB, workdir string) error {
	File, err := os.OpenFile(fmt.Sprintf("%s/Report.txt", workdir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer File.Close()

	build := builder.Builder{}

	if err := build.CalcDeduplication(db, File); err != nil {
		return fmt.Errorf("failed to CalcDeduplication 1: %w", err)
	}
	if err := build.CalcDupSizeDistribution(db, File); err != nil {
		return fmt.Errorf("failed to CalcDupSizeDistribution 2: %w", err)
	}
	if err := build.CalcSizeDistribution(db, File); err != nil {
		return fmt.Errorf("failed to CalcSizeDistribution 3: %w", err)
	}
	if err := build.CalcCountDistribution(db, File); err != nil {
		return fmt.Errorf("failed to CalcSizeDistribution 4: %w", err)
	}
	if err := build.CalcInode(db, File); err != nil {
		return fmt.Errorf("failed to CalcInode 5: %w", err)
	}

	return nil
}

func processImage(images []string) error {
	for {
		_ = os.Remove("chunk.db")
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(images), func(i, j int) {
			images[i], images[j] = images[j], images[i]
		})
		//load image
		if err := loadImages(images); err != nil {
			return fmt.Errorf("failed to load images: %w", err)
		}
		if count > 12 {
			break
		}
	}

	return nil
}

func main() {
	//reset report
	_ = os.RemoveAll("Result")
	_ = os.Mkdir("Result", 0755)

	file, err := os.Open("image")
	if err != nil {
		fmt.Printf("Failed to read image.txt: %v\n", err)
		return
	}
	defer file.Close()

	var images []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			images = append(images, line)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read image.txt: %v\n", err)
		return
	}

	if err := processImage(images); err != nil {
		fmt.Printf("Failed to processImage: %v\n", err)
	}
}
