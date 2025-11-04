package loader

import (
	"context"
	"encoding/json"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dragonflyoss/image-service/contrib/nydusify/pkg/converter/provider"
	"github.com/dragonflyoss/image-service/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/image-service/contrib/nydusify/pkg/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Opt defines loader options.
type Opt struct {
	WorkDir        string
	Source         string
	SourceInsecure bool
	NydusImagePath string
	ExpectedArch   string
}

type Loader struct {
	Opt
	sourceParser *parser.Parser
}

func New(opt Opt) (*Loader, error) {
	var sourceParser *parser.Parser
	if opt.Source != "" {
		sourceRemote, err := provider.DefaultRemote(opt.Source, opt.SourceInsecure)
		if err != nil {
			return nil, errors.Wrap(err, "Init source image parser")
		}
		sourceParser, err = parser.New(sourceRemote, opt.ExpectedArch)
		if sourceParser == nil {
			return nil, errors.Wrap(err, "failed to create parser")
		}
	}

	loader := &Loader{
		Opt:          opt,
		sourceParser: sourceParser,
	}

	return loader, nil
}

func (loader *Loader) Load(ctx context.Context) error {
	sourceParsed, err := loader.sourceParser.Parse(ctx)
	if err != nil {
		return errors.Wrap(err, "parse source image")
	}

	if err := os.RemoveAll(loader.WorkDir); err != nil {
		return errors.Wrap(err, "clean up work directory")
	}

	if err := os.MkdirAll(loader.WorkDir, 0755); err != nil {
		return errors.Wrap(err, "create work directory")
	}

	if err := loader.Output(ctx, sourceParsed, loader.WorkDir); err != nil {
		return errors.Wrap(err, "output image information")
	}

	BootstrapPath := filepath.Join(loader.WorkDir, "bootstrap")
	blobsPath := filepath.Join(loader.WorkDir, "blobs")
	DbPath := filepath.Join("./", "chunk.db")

	builder := NewBuilder(loader.NydusImagePath)
	if err := builder.Load(BuilderOption{
		BootstrapPath: BootstrapPath,
		blobsPath:     blobsPath,
		DbPath:        DbPath,
	}); err != nil {
		return errors.Wrap(err, "invalid nydus bootstrap format")
	}
	return nil
}

func prettyDump(obj interface{}, name string) error {
	bytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(name, bytes, 0644)
}

// Output outputs OCI and Nydus image manifest, index, config to JSON file.
// Prefer to use source image to output OCI image information.
func (loader *Loader) Output(
	ctx context.Context, sourceParsed *parser.Parsed, outputPath string,
) error {
	if sourceParsed.NydusImage != nil {
		if err := prettyDump(
			sourceParsed.NydusImage.Manifest,
			filepath.Join(outputPath, "manifest.json"),
		); err != nil {
			return errors.Wrap(err, "output Nydus manifest file")
		}
		if err := prettyDump(
			sourceParsed.NydusImage.Config,
			filepath.Join(outputPath, "config.json"),
		); err != nil {
			return errors.Wrap(err, "output Nydus config file")
		}

		target := filepath.Join(outputPath, "bootstrap")
		logrus.Infof("Pulling Nydus bootstrap to %s", target)
		bootstrapReader, err := loader.sourceParser.PullNydusBootstrap(ctx, sourceParsed.NydusImage)
		if err != nil {
			return errors.Wrap(err, "pull Nydus bootstrap layer")
		}
		defer bootstrapReader.Close()

		if err := utils.UnpackFile(bootstrapReader, utils.BootstrapFileNameInLayer, target); err != nil {
			return errors.Wrap(err, "unpack Nydus bootstrap layer")
		}

		blobPath := filepath.Join(outputPath, "blobs")
		if err := os.MkdirAll(blobPath, fs.ModePerm); err != nil {
			return errors.Wrap(err, "creat work directory")
		}
		logrus.Infof("Pulling Nydus blob to %s", blobPath)
		err = loader.sourceParser.PullNydusBlob(ctx, sourceParsed.NydusImage, blobPath)
		if err != nil {
			return errors.Wrap(err, "pull Nydus blob layer")
		}
	}

	return nil
}
