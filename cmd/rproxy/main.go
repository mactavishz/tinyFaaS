package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/coap"
	"github.com/OpenFogStack/tinyFaaS/pkg/grpc"
	tfhttp "github.com/OpenFogStack/tinyFaaS/pkg/http"
	"github.com/OpenFogStack/tinyFaaS/pkg/rproxy"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("rproxy: ")

	if len(os.Args) <= 3 {
		fmt.Println("Usage: ./rproxy <listen-addr> [<protocol>:<listen-addr>]")
		os.Exit(1)
	}

	rproxyListenAddress := os.Args[1]

	listenAddrs := make(map[string]string)

	for _, arg := range os.Args[2:] {
		prot, listenAddr, ok := strings.Cut(arg, ":")

		if !ok {
			fmt.Println("Usage: ./rproxy <listen-addr> <protocol>:<listen-addr>")
			os.Exit(1)
		}

		prot = strings.ToLower(prot)
		listenAddr = strings.ToLower(listenAddr)

		log.Printf("adding %s listener on %s", prot, listenAddr)
		listenAddrs[prot] = listenAddr
	}

	if len(listenAddrs) == 0 {
		return // nothing to do
	}

	// create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := rproxy.New()

	// CoAP
	if listenAddr, ok := listenAddrs["coap"]; ok {
		log.Printf("starting coap server on %s", listenAddr)
		go coap.Start(ctx, r, listenAddr)
	}
	// HTTP
	if listenAddr, ok := listenAddrs["http"]; ok {
		log.Printf("starting http server on %s", listenAddr)
		tfhttp.Start(ctx, r, listenAddr)
	}
	// GRPC
	if listenAddr, ok := listenAddrs["grpc"]; ok {
		log.Printf("starting grpc server on %s", listenAddr)
		grpc.Start(ctx, r, listenAddr)
	}

	server := http.NewServeMux()

	server.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		log.Printf("have request: %+v", req)

		buf := new(bytes.Buffer)
		buf.ReadFrom(req.Body)
		newStr := buf.String()

		log.Printf("have body: %s", newStr)

		var def struct {
			FunctionResource   string   `json:"name"`
			FunctionContainers []string `json:"ips"`
		}

		err := json.Unmarshal([]byte(newStr), &def)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Printf("have definition: %+v", def)

		if def.FunctionResource[0] == '/' {
			def.FunctionResource = def.FunctionResource[1:]
		}

		if len(def.FunctionContainers) > 0 {
			// "ips" field not empty: add function
			log.Printf("adding %s", def.FunctionResource)
			err = r.Add(def.FunctionResource, def.FunctionContainers)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
			return
		} else {

			log.Printf("deleting %s", def.FunctionResource)
			err = r.Del(def.FunctionResource)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return

			}
		}
	})

	// create RProxy HTTP server with graceful shutdown support
	rproxyServer := &http.Server{
		Addr:    rproxyListenAddress,
		Handler: server,
	}

	// setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// start RProxy server in goroutine
	go func() {
		log.Println("rproxy server started")
		if err := rproxyServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("rproxy server error: %s", err)
		}
	}()

	// wait for shutdown signal
	<-sigChan
	log.Printf("received shutdown signal, initiating graceful shutdown...")

	// cancel context to trigger protocol servers shutdown
	cancel()

	// create context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// gracefully shutdown RProxy server
	if err := rproxyServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("rproxy server shutdown error: %v", err)
	}

	log.Printf("shutdown complete, exiting")
}
