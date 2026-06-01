package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"plugin"
)

var Handle func([]byte, map[string]string) (string, error)

func init() {
	p, err := plugin.Open("/usr/src/app/fn/handler.so")
	if err != nil {
		slog.Error("failed to open plugin", "err", err)
		os.Exit(1)
	}

	sym, err := p.Lookup("Handle")
	if err != nil {
		slog.Error("failed to lookup Handle symbol", "err", err)
		os.Exit(1)
	}

	var ok bool
	Handle, ok = sym.(func([]byte, map[string]string) (string, error))
	if !ok {
		slog.Error("Handle has wrong signature")
		os.Exit(1)
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

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
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err)
				return
			}

			headers := make(map[string]string)
			for k, v := range r.Header {
				headers[k] = v[0]
			}

			result, err := Handle(body, headers) // returns string, err
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, err = w.Write([]byte(result))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err)
			}

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	})

	slog.Info("server starting", "port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		slog.Error("failed to start server", "err", err)
		os.Exit(1)
	}
}
