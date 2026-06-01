package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
)

func main() {
	port := ":8000"

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, "OK")
				slog.Info("reporting health", "status", "OK")
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return

		case http.MethodPost:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err)
				return
			}
			cmd := exec.Command("./handler.sh")
			cmd.Stdin = bytes.NewReader(data)
			output, err := cmd.CombinedOutput()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(output)
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	})

	slog.Info("server listening", "port", port)
	err := http.ListenAndServe(port, nil)
	if err != nil {
		slog.Error("failed to start server", "err", err)
		os.Exit(1)
	}
}
