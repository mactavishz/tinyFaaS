package util

import (
	"archive/zip"
	"io"
	"os"
	"path"

	"log/slog"
)

func Unzip(src string, dest string, logger *slog.Logger) error {

	logger.Info("Unzipping", "src", src, "dest", dest)

	archive, err := zip.OpenReader(src)
	if err != nil {
		return err
	}

	// extract zip
	for _, f := range archive.File {
		logger.Info("Extracting", "file", f.Name)

		if f.FileInfo().IsDir() {
			path := path.Join(dest, f.Name)
			logger.Info("Creating directory", "dir", f.Name, "path", path)

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

		logger.Info("Extracted file", "file", f.Name, "path", path)

		// close
		rc.Close()
		w.Close()
	}

	return nil
}
