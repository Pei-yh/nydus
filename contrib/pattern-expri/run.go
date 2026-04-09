package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	sourceDBPath = "/tmp/nydus_first_access.db"
)

type ImageConfig struct {
	Image string   `json:"image"`
	Args  []string `json:"args"`
}

type ConfigItem struct {
	WorkDir string   `json:"workDir"`
	Image   string   `json:"image"`
	Args    []string `json:"args"`
}

type Chunk struct {
	ID     int
	Hash   string
	BlobID string
	Size   int64
}

type ChunkGroup struct {
	Name   string
	Chunks []Chunk
}

func RunAll(configPath string, chunksPerGroup int) error {
	if chunksPerGroup <= 0 {
		return fmt.Errorf("m must be greater than 0")
	}

	workDir, images, err := parseConfig(configPath)
	if err != nil {
		return err
	}
	dbFileSet := make(map[string]struct{}, len(images))
	dbFiles := make([]string, 0, len(images))

	for _, img := range images {
		if _, err := runImage(img); err != nil {
			return err
		}
		dbFile, err := moveDBFile(img.Image, workDir)
		if err != nil {
			return err
		}
		if _, exists := dbFileSet[dbFile]; exists {
			continue
		}
		dbFileSet[dbFile] = struct{}{}
		dbFiles = append(dbFiles, dbFile)
	}
	if len(dbFiles) == 0 {
		return fmt.Errorf("no .db files moved into work directory %s", workDir)
	}
	sort.Strings(dbFiles)

	allGroups := make([]ChunkGroup, 0)
	for _, dbFile := range dbFiles {
		chunks, err := loadChunks(dbFile)
		if err != nil {
			return err
		}
		parts := splitChunksByGroupSize(chunks, chunksPerGroup)
		if len(parts) == 0 {
			continue
		}
		imageName := strings.TrimSuffix(filepath.Base(dbFile), ".db")
		groupCount := len(parts)
		for i, part := range parts {
			allGroups = append(allGroups, ChunkGroup{
				Name:   fmt.Sprintf("%s[group_%d/%d]", imageName, i+1, groupCount),
				Chunks: part,
			})
		}
	}

	for i := 0; i < len(allGroups); i++ {
		for j := i + 1; j < len(allGroups); j++ {
			a := allGroups[i]
			b := allGroups[j]
			rate := computeOverlap(a.Chunks, b.Chunks)
			fmt.Printf("%s vs %s -> %.6f\n", a.Name, b.Name, rate)
		}
	}

	return nil
}

func splitChunksByGroupSize(chunks []Chunk, groups int) [][]Chunk {
	if len(chunks) == 0 || groups <= 0 {
		return nil
	}

	groupCount := (len(chunks) + groups - 1) / groups
	parts := make([][]Chunk, 0, groupCount)
	for start := 0; start < len(chunks); start += groups {
		end := start + groups
		if end > len(chunks) {
			end = len(chunks)
		}
		parts = append(parts, chunks[start:end])
	}

	return parts
}

func parseConfig(path string) (string, []ImageConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var items []ConfigItem
	if err := json.Unmarshal(data, &items); err != nil {
		return "", nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if len(items) == 0 {
		return "", nil, fmt.Errorf("config %s is empty", path)
	}

	workDir := filepath.Clean(strings.TrimSpace(items[0].WorkDir))
	if workDir == "" || workDir == "." {
		return "", nil, fmt.Errorf("config item 0: workDir is empty")
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("create work directory %s: %w", workDir, err)
	}

	images := make([]ImageConfig, 0, len(items)-1)
	for i := 1; i < len(items); i++ {
		img := ImageConfig{
			Image: items[i].Image,
			Args:  items[i].Args,
		}
		if strings.TrimSpace(img.Image) == "" {
			return "", nil, fmt.Errorf("config item %d: image is empty", i)
		}
		images = append(images, img)
	}
	if len(images) == 0 {
		return "", nil, fmt.Errorf("no images found in %s", path)
	}

	return workDir, images, nil
}

func runImage(img ImageConfig) (string, error) {
	cmdArgs := []string{"nerdctl", "--snapshotter", "nydus", "--insecure-registry", "run", "-it", "--rm", img.Image}
	cmdArgs = append(cmdArgs, img.Args...)

	cmd := exec.Command("sudo", cmdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("run image %s: %w", img.Image, err)
	}

	return img.Image, nil
}

func moveDBFile(image string, workDir string) (string, error) {
	baseName, err := imageToDBBaseName(image)
	if err != nil {
		return "", err
	}
	targetDB := filepath.Join(workDir, baseName+".db")

	if err := waitForFile(sourceDBPath, 3*time.Second); err != nil {
		return "", err
	}

	sourcePaths := sqliteDataFileGroup(sourceDBPath)
	targetPaths := sqliteDataFileGroup(targetDB)

	for _, targetPath := range targetPaths {
		if err := removeIfExists(targetPath); err != nil {
			return "", err
		}
	}

	for i, sourcePath := range sourcePaths {
		required := i == 0
		if err := moveFileWithFallback(sourcePath, targetPaths[i], required); err != nil {
			return "", err
		}
	}

	return targetDB, nil
}

func sqliteDataFileGroup(dbPath string) []string {
	return []string{dbPath, dbPath + "-wal", dbPath + "-shm"}
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return fmt.Errorf("remove existing %s: %w", path, err)
}

func moveFileWithFallback(src string, dst string, required bool) error {
	_, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) && !required {
			return nil
		}
		return fmt.Errorf("stat source db file %s: %w", src, err)
	}

	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	if err := copyFile(src, dst); err != nil {
		return err
	}
	if err := os.Remove(src); err != nil {
		return fmt.Errorf("remove source db file %s: %w", src, err)
	}

	return nil
}

func loadChunks(dbPath string) ([]Chunk, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db %s: %w", dbPath, err)
	}
	defer db.Close()

	chunks, err := queryChunksWithID(db)
	if err == nil {
		return chunks, nil
	}

	chunks, err = queryChunksWithoutID(db)
	if err != nil {
		return nil, fmt.Errorf("query chunks from %s: %w", dbPath, err)
	}
	return chunks, nil
}

func computeOverlap(a, b []Chunk) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	totalSize := computeUnion(a, b)
	if totalSize == 0 {
		return 0
	}

	repeatedSize := computeIntersection(a, b)
	return float64(repeatedSize) / float64(totalSize)
}

func computeUnion(a, b []Chunk) int64 {
	total := int64(0)
	hashToBlobIDs := make(map[string][]string, len(a)+len(b))

	for _, c := range a {
		blobIDs, exists := hashToBlobIDs[c.Hash]
		if !exists {
			hashToBlobIDs[c.Hash] = []string{c.BlobID}
			total += c.Size
			continue
		}

		found := false
		for _, blobID := range blobIDs {
			if blobID == c.BlobID {
				found = true
				break
			}
		}
		if found {
			continue
		}

		hashToBlobIDs[c.Hash] = append(blobIDs, c.BlobID)
		total += c.Size
	}

	for _, c := range b {
		blobIDs, exists := hashToBlobIDs[c.Hash]
		if !exists {
			hashToBlobIDs[c.Hash] = []string{c.BlobID}
			total += c.Size
			continue
		}

		found := false
		for _, blobID := range blobIDs {
			if blobID == c.BlobID {
				found = true
				break
			}
		}
		if found {
			continue
		}

		hashToBlobIDs[c.Hash] = append(blobIDs, c.BlobID)
		total += c.Size
	}

	return total
}

func computeIntersection(a, b []Chunk) int64 {
	total := int64(0)
	hashToBlobIDs := make(map[string][]string, len(a)+len(b))

	for _, c := range a {
		blobIDs, exists := hashToBlobIDs[c.Hash]
		if !exists {
			hashToBlobIDs[c.Hash] = []string{c.BlobID}
			continue
		}

		found := false
		for _, blobID := range blobIDs {
			if blobID == c.BlobID {
				found = true
				break
			}
		}
		if found {
			continue
		}

		hashToBlobIDs[c.Hash] = append(blobIDs, c.BlobID)
		total += c.Size
	}

	for _, c := range b {
		blobIDs, exists := hashToBlobIDs[c.Hash]
		if !exists {
			hashToBlobIDs[c.Hash] = []string{c.BlobID}
			continue
		}

		found := false
		for _, blobID := range blobIDs {
			if blobID == c.BlobID {
				found = true
				break
			}
		}
		if found {
			continue
		}

		hashToBlobIDs[c.Hash] = append(blobIDs, c.BlobID)
		total += c.Size
	}

	return total
}

func imageToDBBaseName(image string) (string, error) {
	img := strings.TrimSpace(image)
	if img == "" {
		return "", fmt.Errorf("image is empty")
	}

	lastSlash := strings.LastIndex(img, "/")
	repoTag := img
	if lastSlash >= 0 {
		repoTag = img[lastSlash+1:]
	}

	lastColon := strings.LastIndex(repoTag, ":")
	name := repoTag
	tag := "latest"
	if lastColon >= 0 {
		name = repoTag[:lastColon]
		tag = repoTag[lastColon+1:]
	}

	name = sanitizeForFilename(name)
	tag = sanitizeForFilename(tag)
	if name == "" || tag == "" {
		return "", fmt.Errorf("invalid image reference: %s", image)
	}

	return name + "_" + tag, nil
}

func sanitizeForFilename(s string) string {
	if s == "" {
		return ""
	}
	b := strings.Builder{}
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		isLower := ch >= 'a' && ch <= 'z'
		isUpper := ch >= 'A' && ch <= 'Z'
		isDigit := ch >= '0' && ch <= '9'
		if isLower || isUpper || isDigit || ch == '_' || ch == '-' {
			b.WriteByte(ch)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := os.Stat(path)
		if err == nil {
			return nil
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("db file not found: %s", path)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func copyFile(src string, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source db %s: %w", src, err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create target db %s: %w", dst, err)
	}
	defer func() {
		_ = out.Close()
	}()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy db from %s to %s: %w", src, dst, err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync target db %s: %w", dst, err)
	}

	return nil
}

func queryChunksWithID(db *sql.DB) ([]Chunk, error) {
	rows, err := db.Query("SELECT id, hash, blobId, chunkSize FROM access_log1")
	if err != nil {
		rows, err = db.Query("SELECT id, hash, blobId, size FROM access_log1")
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	chunks := make([]Chunk, 0)
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.ID, &c.Hash, &c.BlobID, &c.Size); err != nil {
			return nil, fmt.Errorf("scan chunk row with id: %w", err)
		}
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate chunk rows with id: %w", err)
	}
	return chunks, nil
}

func queryChunksWithoutID(db *sql.DB) ([]Chunk, error) {
	rows, err := db.Query("SELECT hash, blobId, chunkSize FROM access_log1")
	if err != nil {
		rows, err = db.Query("SELECT hash, blobId, size FROM access_log1")
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	chunks := make([]Chunk, 0)
	id := 1
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.Hash, &c.BlobID, &c.Size); err != nil {
			return nil, fmt.Errorf("scan chunk row without id: %w", err)
		}
		c.ID = id
		id++
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate chunk rows without id: %w", err)
	}
	return chunks, nil
}
