package queue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	stan "github.com/nats-io/stan.go"
)

type WorkerConfig struct {
	NATSURL       string
	ClusterID     string
	ClientID      string
	Subject       string
	QueueGroup    string
	TargetBaseURL string
	AckWait       time.Duration
	MaxInflight   int
}

type Worker struct {
	config WorkerConfig
	client *http.Client
	logger *slog.Logger

	conn stan.Conn
	sub  stan.Subscription
	mu   sync.Mutex
}

func NewWorker(config WorkerConfig, logger *slog.Logger) *Worker {
	if config.NATSURL == "" {
		config.NATSURL = "nats://127.0.0.1:4222"
	}
	if config.ClusterID == "" {
		config.ClusterID = DefaultClusterID
	}
	if config.ClientID == "" {
		config.ClientID = "tinyfaas-queue-worker"
	}
	if config.Subject == "" {
		config.Subject = DefaultSubject
	}
	if config.QueueGroup == "" {
		config.QueueGroup = DefaultQueue
	}
	if config.TargetBaseURL == "" {
		config.TargetBaseURL = "http://127.0.0.1:8000"
	}
	if config.AckWait <= 0 {
		config.AckWait = DefaultAckWait
	}
	if config.MaxInflight <= 0 {
		config.MaxInflight = DefaultMaxFlight
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		config: config,
		client: &http.Client{Timeout: 0},
		logger: logger,
	}
}

func (w *Worker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		return nil
	}

	conn, err := stan.Connect(w.config.ClusterID, w.config.ClientID, stan.NatsURL(w.config.NATSURL))
	if err != nil {
		return err
	}

	opts := []stan.SubscriptionOption{
		stan.DurableName(strings.ReplaceAll(w.config.Subject, ".", "_")),
		stan.AckWait(w.config.AckWait),
		stan.DeliverAllAvailable(),
		stan.MaxInflight(w.config.MaxInflight),
		stan.SetManualAckMode(),
	}

	sub, err := conn.QueueSubscribe(w.config.Subject, w.config.QueueGroup, w.handleMessage, opts...)
	if err != nil {
		_ = conn.Close()
		return err
	}

	w.conn = conn
	w.sub = sub
	w.logger.Info("queue worker subscribed",
		"subject", w.config.Subject,
		"queue_group", w.config.QueueGroup,
		"max_inflight", w.config.MaxInflight,
		"target", w.config.TargetBaseURL)
	return nil
}

func (w *Worker) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error
	if w.sub != nil {
		if err := w.sub.Unsubscribe(); err != nil {
			errs = append(errs, err)
		}
		w.sub = nil
	}
	if w.conn != nil {
		if err := w.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		w.conn = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("queue worker stop: %v", errs)
	}
	return nil
}

func (w *Worker) handleMessage(msg *stan.Msg) {
	w.handleMessageData(msg.Data, msg.Ack)
}

func (w *Worker) handleMessageData(data []byte, ack func() error) {
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		w.logger.Error("invalid queue message", "err", err)
		_ = ack()
		return
	}

	if req.Function == "" {
		w.logger.Error("invalid queue message: missing function")
		_ = ack()
		return
	}

	if err := w.invoke(req); err != nil {
		w.logger.Error("queued invocation transport failed", "function", req.Function, "err", err)
		return
	}
	_ = ack()
}

func (w *Worker) invoke(q Request) error {
	method := q.Method
	if method == "" {
		method = http.MethodPost
	}

	target := strings.TrimRight(w.config.TargetBaseURL, "/") + "/invoke/" + strings.TrimLeft(q.Function, "/")
	if q.Path != "" && q.Path != "/" {
		target += "/" + strings.TrimLeft(q.Path, "/")
	}
	if q.QueryString != "" {
		target += "?" + strings.TrimLeft(q.QueryString, "?")
	}

	req, err := http.NewRequest(method, target, bytes.NewReader(q.Body))
	if err != nil {
		return err
	}
	req.Header = q.Header.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Del("X-Tinyfaas-Async")
	req.Header.Set("User-Agent", "tinyfaas-queue-worker")
	if q.Host != "" {
		req.Host = q.Host
	}

	start := time.Now()
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	w.logger.Info("queued invocation completed", "function", q.Function, "status", resp.StatusCode, "duration", time.Since(start))
	return nil
}
