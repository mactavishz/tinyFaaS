package util

import (
	"archive/zip"
	"io"
	"os"
	"path"

	"go.uber.org/zap"
)

func Unzip(src string, dest string, logger *zap.Logger) error {

	logger.Info("Unzipping", zap.String("src", src), zap.String("dest", dest))

	archive, err := zip.OpenReader(src)
	if err != nil {
		return err
	}

	// extract zip
	for _, f := range archive.File {
		logger.Info("Extracting", zap.String("file", f.Name))

		if f.FileInfo().IsDir() {
			path := path.Join(dest, f.Name)
			logger.Info("Creating directory", zap.String("dir", f.Name), zap.String("path", path))

			err = os.MkdirAll(path, 0777)
			if err != nil {
				return err
			}
			continue
		}

		// open file
		rc, err := f.Open()
		if err != nil {
			return err
		}

		// create file
		path := path.Join(dest, f.Name)
		// err = os.MkdirAll(path, 0777)
		// if err != nil {
		// return err
		// }

		// write file
		w, err := os.Create(path)
		if err != nil {
			return err
		}

		// copy
		_, err = io.Copy(w, rc)
		if err != nil {
			return err
		}

		logger.Info("Extracted file", zap.String("file", f.Name), zap.String("path", path))

		// close
		rc.Close()
		w.Close()
	}

	return nil
}
