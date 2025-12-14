package rproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"sync"
	"time"

	retry "github.com/avast/retry-go/v5"
	"go.uber.org/zap"
)

type Route struct {
	ips      []string
	isActive bool
}

type RProxy struct {
	routingTable      map[string]*Route
	routingTableMux   sync.RWMutex
	autoscalerEnabled bool
	logger            *zap.Logger
}

func New(logger *zap.Logger) *RProxy {
	return &RProxy{
		routingTable: make(map[string]*Route),
		logger:       logger,
	}
}

func (r *RProxy) SetAutoScalerEnabled(enabled bool) {
	r.autoscalerEnabled = enabled
}

func (r *RProxy) Add(name string, ips []string) error {
	if len(ips) == 0 {
		return fmt.Errorf("no ips given")
	}

	r.logger.Debug("adding function route", zap.String("name", name), zap.Strings("ips", ips))
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

	r.logger.Debug("deleting function route", zap.String("name", name))
	delete(r.routingTable, name)
	return nil
}

func (r *RProxy) Update(name string) error {
	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	if _, ok := r.routingTable[name]; ok {
		r.logger.Debug("updating function route", zap.String("name", name))
		r.routingTable[name].isActive = !r.routingTable[name].isActive
	}
	return nil
}

func (r *RProxy) Call(name string, payload []byte, async bool, headers map[string]string) (int, []byte) {
	r.routingTableMux.RLock()
	route, ok := r.routingTable[name]
	r.routingTableMux.RUnlock()

	if !ok {
		r.logger.Error("function not found", zap.String("name", name))
		return http.StatusNotFound, nil
	}

	r.logger.Debug("found function route", zap.Strings("ips", route.ips), zap.Bool("active", route.isActive))
	// Check if function is scaled down and trigger cold start if needed
	if r.autoscalerEnabled && !route.isActive {
		r.logger.Info("function is scaled down, triggering cold start", zap.String("name", name))
		if err := r.triggerColdStart(name); err != nil {
			r.logger.Error("failed to trigger cold start", zap.String("name", name), zap.Error(err))
			return http.StatusInternalServerError, nil
		}
		r.logger.Info("cold start completed", zap.String("name", name))
	}

	go r.heartbeat(name)

	// choose random handler
	ip := route.ips[rand.Intn(len(route.ips))]

	r.logger.Debug("chosen function ip", zap.String("ip", ip))
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/fn", ip), bytes.NewBuffer(payload))
	if err != nil {
		r.logger.Error("failed to create request", zap.Error(err))
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
		r.logger.Error("failed to invoke function", zap.Error(err))
		return http.StatusInternalServerError, nil
	}

	defer resp.Body.Close()
	res_body, err := io.ReadAll(resp.Body)

	if err != nil {
		r.logger.Error("failed to read response body", zap.Error(err))
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
			r.logger.Debug("heartbeat attempt failed", zap.Uint("attempt", attempt), zap.String("name", name), zap.Error(err))
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

			r.logger.Info("successfully triggered heartbeat", zap.String("name", name))
			return nil
		},
	)

	if err != nil {
		r.logger.Error("failed to trigger heartbeat", zap.String("name", name), zap.Error(err))
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
			r.logger.Debug("cold start attempt failed", zap.Uint("attempt", attempt), zap.String("name", name), zap.Error(err))
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

			r.logger.Info("successfully triggered cold start", zap.String("name", name))
			return nil
		},
	)
	if err != nil {
		r.logger.Error("failed to trigger cold start", zap.String("name", name), zap.Error(err))
	}
	return nil
}
