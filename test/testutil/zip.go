package testutil

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func ZipDirectory(t *testing.T, dirPath string) []byte {
	t.Helper()

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	err := filepath.Walk(dirPath, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(dirPath, p)
		if err != nil {
			return err
		}

		w, err := zipWriter.Create(relPath)
		if err != nil {
			return err
		}

		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}

		_, err = w.Write(data)
		return err
	})

	require.NoError(t, err, "failed to create zip archive")
	require.NoError(t, zipWriter.Close(), "failed to close zip writer")
	return buf.Bytes()
}
