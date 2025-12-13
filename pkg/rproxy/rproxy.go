package rproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"sync"
	"time"

	retry "github.com/avast/retry-go/v5"
)

type Route struct {
	ips      []string
	isActive bool
}

type RProxy struct {
	routingTable      map[string]*Route
	routingTableMux   sync.RWMutex
	autoscalerEnabled bool
}

func New() *RProxy {
	return &RProxy{
		routingTable: make(map[string]*Route),
	}
}

func (r *RProxy) SetAutoScalerEnabled(enabled bool) {
	r.autoscalerEnabled = enabled
}

func (r *RProxy) Add(name string, ips []string) error {
	if len(ips) == 0 {
		return fmt.Errorf("no ips given")
	}

	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	r.routingTable[name] = &Route{
		ips:      ips,
		isActive: true,
	}
	return nil
}

func (r *RProxy) Del(name string) error {
	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	if _, ok := r.routingTable[name]; !ok {
		return fmt.Errorf("function not found")
	}

	delete(r.routingTable, name)
	return nil
}

func (r *RProxy) Update(name string) error {
	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	if _, ok := r.routingTable[name]; ok {
		r.routingTable[name].isActive = !r.routingTable[name].isActive
	}
	return nil
}

func (r *RProxy) Call(name string, payload []byte, async bool, headers map[string]string) (int, []byte) {
	r.routingTableMux.RLock()
	route, ok := r.routingTable[name]
	r.routingTableMux.RUnlock()

	if !ok {
		log.Printf("function not found: %s", name)
		return http.StatusNotFound, nil
	}

	log.Printf("have function route, ips: %s, active: %v", route.ips, route.isActive)

	// Check if function is scaled down and trigger cold start if needed
	if r.autoscalerEnabled && !route.isActive {
		log.Printf("function %s is scaled down, triggering cold start", name)
		if err := r.triggerColdStart(name); err != nil {
			log.Printf("failed to trigger cold start for %s: %v", name, err)
			return http.StatusInternalServerError, nil
		}
		log.Printf("cold start completed for %s", name)
	}

	go r.heartbeat(name)

	// choose random handler
	ip := route.ips[rand.Intn(len(route.ips))]

	log.Printf("chosen function ip: %s", ip)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/fn", ip), bytes.NewBuffer(payload))
	if err != nil {
		log.Print(err)
		return http.StatusInternalServerError, nil
	}
	for k, v := range headers {
		cleanedKey := cleanHeaderKey(k) // remove special chars from key
		req.Header.Set(cleanedKey, v)
	}

	// call function asynchronously
	if async {
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			resp.Body.Close()
		}()
		return http.StatusAccepted, nil
	}

	// call function and return results
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Print(err)
		return http.StatusInternalServerError, nil
	}

	defer resp.Body.Close()
	res_body, err := io.ReadAll(resp.Body)

	if err != nil {
		log.Print(err)
		return http.StatusInternalServerError, nil
	}

	return resp.StatusCode, res_body
}

func cleanHeaderKey(key string) string {
	// a regex pattern to match special characters
	re := regexp.MustCompile(`[:()<>@,;:\"/[\]?={} \t]`)
	// Replace special characters with an empty string
	return re.ReplaceAllString(key, "")
}

func (r *RProxy) heartbeat(name string) error {
	url := "http://127.0.0.1/system/heartbeat"

	reqData := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: name,
	}

	body, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	err = retry.New(
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.OnRetry(func(attempt uint, err error) {
			log.Printf("heartbeat attempt %d for %s failed: %v", attempt, name, err)
		}),
	).Do(
		func() error {
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return err
			}

			log.Printf("successfully triggered heartbeat for %s", name)
			return nil
		},
	)

	if err != nil {
		log.Printf("failed to trigger heartbeat for %s: %v", name, err)
		return err
	}
	return nil
}

// triggerColdStart calls the manager to scale up a function
func (r *RProxy) triggerColdStart(name string) error {
	url := "http://127.0.0.1/system/scale-up"

	reqData := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: name,
	}

	body, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	err = retry.New(
		retry.Attempts(5),
		retry.Delay(100*time.Millisecond),
		retry.OnRetry(func(attempt uint, err error) {
			log.Printf("cold start attempt %d for %s failed: %v", attempt, name, err)
		}),
	).Do(
		func() error {
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return err
			}

			log.Printf("successfully triggered cold start for %s", name)
			return nil
		},
	)
	if err != nil {
		log.Printf("failed to trigger cold start for %s: %v", name, err)
	}
	return nil
}
