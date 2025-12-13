package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/rproxy"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
)

var emptyBody = []byte{}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("rproxy: ")

	if len(os.Args) < 2 {
		log.Printf("invalid number of arguments")
		log.Printf("usage: ./rproxy <listen-addr>")
		os.Exit(1)
	}

	listenAddr := os.Args[1]

	r := rproxy.New()

	// Initialize autoscaler for activity tracking
	autoscalerConfig := autoscaler.NewConfigFromEnv("tinyfaas")
	if autoscalerConfig.Enabled {
		r.SetAutoScalerEnabled(true)
		log.Printf("autoscaler enabled")
	} else {
		log.Printf("autoscaler disabled")
	}
	// Create single HTTP server with multiple endpoints
	mux := http.NewServeMux()

	// Config endpoint - function registration (PUT)
	mux.HandleFunc("/config", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPut:
			var def struct {
				FunctionResource   string   `json:"name"`
				FunctionContainers []string `json:"ips"`
			}
			err := json.NewDecoder(req.Body).Decode(&def)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				log.Printf("failed to decode request: %v", err)
				return
			}

			log.Printf("registering function: %s, ips: %v", def.FunctionResource, def.FunctionContainers)

			if def.FunctionResource != "" && def.FunctionResource[0] == '/' {
				def.FunctionResource = def.FunctionResource[1:]
			}

			if len(def.FunctionContainers) == 0 {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("no container IPs provided"))
				return
			}

			err = r.Add(def.FunctionResource, def.FunctionContainers)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				log.Printf("failed to add function: %v", err)
				return
			}

			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			return
		case http.MethodDelete:
			var def struct {
				FunctionResource string `json:"name"`
			}

			err := json.NewDecoder(req.Body).Decode(&def)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				log.Printf("failed to decode request: %v", err)
				return
			}

			log.Printf("deleting function: %s", def.FunctionResource)

			if def.FunctionResource != "" && def.FunctionResource[0] == '/' {
				def.FunctionResource = def.FunctionResource[1:]
			}

			err = r.Del(def.FunctionResource)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				log.Printf("failed to delete function: %v", err)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			return
		case http.MethodPatch:
			// Clear function IPs
			var def struct {
				FunctionResource string `json:"name"`
			}

			err := json.NewDecoder(req.Body).Decode(&def)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				log.Printf("failed to decode request: %v", err)
				return
			}

			log.Printf("clearing function ips: %s", def.FunctionResource)

			if def.FunctionResource != "" && def.FunctionResource[0] == '/' {
				def.FunctionResource = def.FunctionResource[1:]
			}

			err = r.Update(def.FunctionResource)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				log.Printf("failed to delete function: %v", err)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	})

	// Function invocation endpoint - /invoke/<function-name>
	mux.HandleFunc("/invoke/", func(w http.ResponseWriter, req *http.Request) {
		// Extract function name from path
		functionName := req.URL.Path[len("/invoke/"):]

		if functionName == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("function name required"))
			return
		}

		async := req.Header.Get("X-tinyFaaS-Async") != ""

		// Determine the caller by checking the X-FaaS-Source-IP header
		// This header is set by Caddy to preserve the original source IP
		sourceIP := req.Header.Get("X-FaaS-Source-IP")
		if sourceIP == "" {
			// Fallback to RemoteAddr if header is not set
			sourceIP = util.ExtractIP(req.RemoteAddr)
		}

		log.Printf("Request IP: %s", sourceIP)
		log.Printf("invoking function: %s (async: %v)", functionName, async)

		req_body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Print(err)
			return
		}

		headers := make(map[string]string)
		for k, v := range req.Header {
			headers[k] = v[0]
		}

		s, res := r.Call(functionName, req_body, async, headers)

		w.WriteHeader(s)
		if res != nil {
			w.Write(res)
		} else {
			w.Write(emptyBody)
		}
	})

	server := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	// setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// start server in goroutine
	go func() {
		log.Println("rproxy server started on", listenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %s", err)
		}
	}()

	// wait for shutdown signal
	<-sigChan
	log.Printf("received shutdown signal, initiating graceful shutdown...")

	// create context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// gracefully shutdown server
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	log.Printf("shutdown complete, exiting")
}
