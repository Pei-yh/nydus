package context

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type builder struct {
}

type bucket struct {
	label string
	min   int64
	max   int64
}

var chunkTables = []string{
	"chunk_4kb",
	"chunk_16kb",
	"chunk_64kb",
	"chunk_256kb",
	"chunk_1024kb",
	"chunk_4kb_cdc",
	"chunk_16kb_cdc",
	"chunk_64kb_cdc",
	"chunk_256kb_cdc",
}

var buckets = []bucket{
	{label: "<4KB", min: 0, max: 4*1024 - 1},
	{label: "4-16KB", min: 4 * 1024, max: 16*1024 - 1},
	{label: "16-64KB", min: 16 * 1024, max: 64*1024 - 1},
	{label: "64-256KB", min: 64 * 1024, max: 256*1024 - 1},
	{label: "256-1024KB", min: 256 * 1024, max: 1024*1024 - 1},
	{label: "=1024KB", min: 1024 * 1024, max: 1024 * 1024},
}

// 统计与inode有关的信息
// 平均文件大小
// 较大的size（大于等于1MB）带来多少重复率贡献，贡献在尺寸上的贡献
// 小于1M的实际就是文件级别去重，要怎么将其利用上
// 文件的重复是否有其他的规律，是否集中
func (c *builder) CalcInode(db *sql.DB, File *os.File) error {
	writer := bufio.NewWriter(File)
	_, _ = writer.WriteString(fmt.Sprintf("==== big inode rate Report: %s ====\n", time.Now().Format(time.RFC3339)))

	var total, duplicates int64

	query := fmt.Sprintf("SELECT SUM(size * (count - 1)) FROM %s", "chunk_1024kb")
	err := db.QueryRow(query).Scan(&total)
	if err != nil {
		log.Printf("Failed to read total size from %s: %v", "chunk_1024kb", err)
		return err
	}

	if total == 0 {
		log.Printf("Skipping %s: total is 0", "chunk_1024kb")
		return err
	}

	query = fmt.Sprintf("SELECT COALESCE(SUM(size * (count - 1)), 0) FROM %s WHERE flag = 1 OR flag = 2", "chunk_1024kb")
	err = db.QueryRow(query).Scan(&duplicates)
	if err != nil {
		log.Printf("Failed to read duplicates size from %s: %v", "chunk_1024kb", err)
		return err
	}

	rate := float64(duplicates) / float64(total) * 100
	fmt.Fprintf(writer, "For %-12s | Deduplication Rate: %.2f%% (%d/%d bytes) flag >= 1\n", "chunk_1024kb", rate, duplicates, total)

	query = fmt.Sprintf("SELECT COALESCE(SUM(size * (count - 1)), 0) FROM %s WHERE flag = 2", "chunk_1024kb")
	err = db.QueryRow(query).Scan(&duplicates)
	if err != nil {
		log.Printf("Failed to read duplicates size from %s: %v", "chunk_1024kb", err)
		return err
	}

	if total == 0 {
		log.Printf("Skipping %s: total is 0", "chunk_1024kb")
		return err
	}

	rate = float64(duplicates) / float64(total) * 100
	fmt.Fprintf(writer, "For %-12s | Deduplication Rate: %.2f%% (%d/%d bytes) flag >= 2\n", "chunk_1024kb", rate, duplicates, total)

	return writer.Flush()
}

// 统计大小分布
func (c *builder) CalcSizeDistribution(db *sql.DB, File *os.File) error {
	writer := bufio.NewWriter(File)
	_, _ = writer.WriteString(fmt.Sprintf("==== Size Distribution Report: %s ====\n", time.Now().Format(time.RFC3339)))

	for _, table := range chunkTables {
		fmt.Fprintf(writer, "Table: %s\n", table)

		var totalsize int64
		query := fmt.Sprintf("SELECT SUM(size * count) FROM %s", table)
		err := db.QueryRow(query).Scan(&totalsize)
		if err != nil || totalsize == 0 {
			fmt.Fprintf(writer, "  Failed to query total count or total = 0: %v\n\n", err)
			return writer.Flush()
		}

		for _, def := range buckets {
			var size int64
			query := fmt.Sprintf("SELECT COALESCE(SUM(size * count), 0) FROM %s WHERE size BETWEEN ? AND ?", table)
			err := db.QueryRow(query, def.min, def.max).Scan(&size)
			if err != nil {
				log.Printf("Failed to query %s in %s: %v", def.label, table, err)
				continue
			}
			percent := float64(size) / float64(totalsize) * 100
			fmt.Fprintf(writer, "  %-12s : %8d bytes (%.2f%%)\n", def.label, size, percent)
		}
	}

	return writer.Flush()
}

// 统计重复数据分布
func (c *builder) CalcDupSizeDistribution(db *sql.DB, File *os.File) error {
	writer := bufio.NewWriter(File)
	_, _ = writer.WriteString(fmt.Sprintf("==== Dup Size Distribution Report: %s ====\n", time.Now().Format(time.RFC3339)))

	for _, table := range chunkTables {
		fmt.Fprintf(writer, "Table: %s\n", table)

		var totalsize int64
		query := fmt.Sprintf("SELECT SUM(size * (count - 1)) FROM %s", table)
		err := db.QueryRow(query).Scan(&totalsize)
		if err != nil || totalsize == 0 {
			fmt.Fprintf(writer, "  Failed to query total count or total = 0: %v\n\n", err)
			continue
		}

		for _, def := range buckets {
			var dedupsize int64
			query := fmt.Sprintf("SELECT COALESCE(SUM(size * (count - 1)), 0) FROM %s WHERE size BETWEEN ? AND ?", table)
			err := db.QueryRow(query, def.min, def.max).Scan(&dedupsize)
			if err != nil {
				log.Printf("Failed to query %s in %s: %v", def.label, table, err)
				continue
			}
			percent := float64(dedupsize) / float64(totalsize) * 100
			fmt.Fprintf(writer, "  %-12s : %8d bytes (%.2f%%)\n", def.label, dedupsize, percent)
		}
	}
	return writer.Flush()
}

// 统计数量分布
func (c *builder) CalcCountDistribution(db *sql.DB, File *os.File) error {
	writer := bufio.NewWriter(File)
	_, _ = writer.WriteString(fmt.Sprintf("==== Count Distribution Report: %s ====\n", time.Now().Format(time.RFC3339)))

	for _, table := range chunkTables {
		fmt.Fprintf(writer, "Table: %s\n", table)

		var totalcount int64
		query := fmt.Sprintf("SELECT SUM(count) FROM %s", table)
		err := db.QueryRow(query).Scan(&totalcount)
		if err != nil || totalcount == 0 {
			fmt.Fprintf(writer, "  Failed to query total count or total = 0: %v\n\n", err)
			continue
		}

		for _, def := range buckets {
			var count int64
			query := fmt.Sprintf("SELECT COALESCE(SUM(count), 0) FROM %s WHERE size BETWEEN ? AND ?", table)
			err := db.QueryRow(query, def.min, def.max).Scan(&count)
			if err != nil {
				log.Printf("Failed to query %s in %s: %v", def.label, table, err)
				continue
			}
			percent := float64(count) / float64(totalcount) * 100
			fmt.Fprintf(writer, "  %-12s : %8d count (%.2f%%)\n", def.label, count, percent)
		}
	}

	return writer.Flush()
}

// 统计不同分块粒度的去重率
func (c *builder) CalcDeduplication(db *sql.DB, File *os.File) error {
	writer := bufio.NewWriter(File)
	_, _ = writer.WriteString(fmt.Sprintf("==== Deduplication rate Report: %s ====\n", time.Now().Format(time.RFC3339)))

	for _, table := range chunkTables {
		var total, duplicates int64

		query := fmt.Sprintf("SELECT SUM(size * count) FROM %s", table)
		err := db.QueryRow(query).Scan(&total)
		if err != nil {
			log.Printf("Failed to read total size from %s: %v", table, err)
			continue
		}

		query = fmt.Sprintf("SELECT SUM(size * (count - 1)) FROM %s", table)
		err = db.QueryRow(query).Scan(&duplicates)
		if err != nil {
			log.Printf("Failed to read duplicates size from %s: %v", table, err)
			continue
		}

		if total == 0 {
			log.Printf("Skipping %s: total is 0", table)
			continue
		}

		rate := float64(duplicates) / float64(total) * 100
		fmt.Fprintf(writer, "For %-12s | Deduplication Rate: %.2f%% (%d/%d bytes) \n", table, rate, duplicates, total)
	}

	return writer.Flush()
}

func (c *builder) CalcTotalSize(db *sql.DB) (int64, error) {
	var total int64
	query := fmt.Sprintf("SELECT SUM(size * count) FROM %s", "chunk_1024kb")
	err := db.QueryRow(query).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("failed to read total size from %s: %v", "chunk_1024kb", err)
	}
	return total, nil
}
