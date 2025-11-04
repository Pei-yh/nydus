package loader

import (
	"io"
	"os"
	"os/exec"
)

type BuilderOption struct {
	BootstrapPath string
	blobsPath     string
	DbPath        string
}

type Builder struct {
	BinaryPath string
	Stdout     io.Writer
	Stderr     io.Writer
}

func NewBuilder(BinaryPath string) *Builder {
	return &Builder{
		BinaryPath: BinaryPath,
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
	}
}

// Check calls `nydus-image check` to parse Nydus bootstrap
// and output debug information to specified JSON file.
func (builder *Builder) Load(option BuilderOption) error {
	// var args []string
	args := []string{
		"load",
		"--bootstrap",
		option.BootstrapPath,
		"--blobs",
		option.blobsPath,
		"--dbpath",
		option.DbPath,
		"--log-level",
		"warn",
	}

	cmd := exec.Command(builder.BinaryPath, args...)
	cmd.Stdout = builder.Stdout
	cmd.Stderr = builder.Stderr

	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}
