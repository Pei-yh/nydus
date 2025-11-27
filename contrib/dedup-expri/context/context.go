package context

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Config struct {
	WorkDir    string `json:"workDir"`
	ImagesPath string `json:"imagesPath"`
	Registry   string `json:"registry"`
}

type Context struct {
	workdir    string
	dbpath     string
	resultdir  string
	imagespath string
	registry   string
}

func New(configPath string) (*Context, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	dbpath := filepath.Join(cfg.WorkDir, "chunk.db")
	resultdir := filepath.Join(cfg.WorkDir, "Result")
	ctx := &Context{
		workdir:    cfg.WorkDir,
		dbpath:     dbpath,
		resultdir:  resultdir,
		imagespath: cfg.ImagesPath,
		registry:   cfg.Registry,
	}

	return ctx, nil
}

func hasNydusSuffix(name string) bool {
	return strings.HasSuffix(name, "nydus")
}

func (c *Context) convert(source string) error {
	target := fmt.Sprintf("%s/%s_nydus", c.registry, source)
	output := fmt.Sprintf("%s/output", c.workdir)
	cmdArgs := exec.Command("sudo", "nydusify", "convert", "--source", source, "--target", target, "--work-dir", output)
	cmdArgs.Stdout = os.Stdout
	cmdArgs.Stderr = os.Stderr
	err := cmdArgs.Run()

	if err == nil {
		fmt.Println("convert command executed successfully for", source)
		return nil
	} else {
		fmt.Printf("convert command failed for %s: %v\n", source, err)
		return fmt.Errorf("convert command failed")
	}
}

func (c *Context) load(source string) error {
	output := fmt.Sprintf("%s/output", c.workdir)
	cmdArgs := exec.Command("sudo", "nydusify", "load", "--source", source, "--source-insecure", "--dbpath", c.dbpath, "--work-dir", output)
	cmdArgs.Stdout = os.Stdout
	cmdArgs.Stderr = os.Stderr
	err := cmdArgs.Run()

	if err == nil {
		fmt.Println("Load command executed successfully for", source)
		return nil
	} else {
		fmt.Printf("Load command failed for %s: %v\n", source, err)
		return fmt.Errorf("load command failed")
	}
}

func calcAll(db *sql.DB, workdir string) error {
	File, err := os.OpenFile(fmt.Sprintf("%s/Report.txt", workdir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer File.Close()

	build := builder{}

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

func (c *Context) processImages(images []string) error {
	logPath := filepath.Join(c.resultdir, "load.log")
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer logFile.Close()

	writer := bufio.NewWriter(logFile)
	db, err := sql.Open("sqlite3", c.dbpath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	for _, source := range images {
		var logLine string
		if hasNydusSuffix(source) {
			err := c.load(source)
			if err != nil {
				logLine = fmt.Sprintf("%s: load failed\n", source)
			} else {
				logLine = fmt.Sprintf("%s: load success\n", source)
			}
		} else {
			err := c.convert(source)
			if err != nil {
				logLine = fmt.Sprintf("%s: convert failed\n", source)
			} else {
				target := fmt.Sprintf("%s/%s_nydus", c.registry, source)
				err := c.load(target)
				if err != nil {
					logLine = fmt.Sprintf("%s: convert success but load failed\n", target)
				} else {
					logLine = fmt.Sprintf("%s: convert success and load success\n", target)
				}
			}
		}
		_, _ = writer.WriteString(logLine)
	}
	calcAll(db, c.resultdir)
	return writer.Flush()
}

func get_images(dir string) ([]string, error) {
	var lines []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("failed to access %s: %v", path, err)
		}
		if d.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open %s: %v", path, err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				lines = append(lines, line)
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("failed to read %s: %v", path, err)
		}
		return nil
	})
	return lines, err
}

func (c *Context) Process() error {
	_ = os.RemoveAll(c.workdir)
	_ = os.MkdirAll(c.workdir, 0755)
	_ = os.MkdirAll(c.resultdir, 0755)

	images, err := get_images(c.imagespath)
	if err != nil {
		return fmt.Errorf("failed to load get image list from path : %w", err)
	}

	if err := c.processImages(images); err != nil {
		return fmt.Errorf("failed to load images: %w", err)
	}

	return nil
}
