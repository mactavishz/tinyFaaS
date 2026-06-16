package queue

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	stan "github.com/nats-io/stan.go"
)

const (
	DefaultClusterID = "faas-cluster"
	DefaultSubject   = "faas-request"
	DefaultQueue     = "faas"
	DefaultAckWait   = 5*time.Minute + 5*time.Second
	DefaultMaxFlight = 8
)

type Request struct {
	Header      http.Header `json:"Header,omitempty"`
	Host        string      `json:"Host,omitempty"`
	Body        []byte      `json:"Body,omitempty"`
	Method      string      `json:"Method"`
	Path        string      `json:"Path,omitempty"`
	QueryString string      `json:"QueryString,omitempty"`
	Function    string      `json:"Function"`
	QueueName   string      `json:"QueueName,omitempty"`
}

type Publisher interface {
	Queue(req *Request) error
	Close() error
}

type NATSConfig struct {
	URL       string
	ClusterID string
	ClientID  string
	Subject   string
}

type NATSPublisher struct {
	conn    stan.Conn
	subject string
	mu      sync.RWMutex
	logger  *slog.Logger
}

func NewNATSPublisher(config NATSConfig, logger *slog.Logger) (*NATSPublisher, error) {
	if config.URL == "" {
		config.URL = "nats://127.0.0.1:4222"
	}
	if config.ClusterID == "" {
		config.ClusterID = DefaultClusterID
	}
	if config.ClientID == "" {
		config.ClientID = "tinyfaas-server"
	}
	if config.Subject == "" {
		config.Subject = DefaultSubject
	}
	if logger == nil {
		logger = slog.Default()
	}

	conn, err := stan.Connect(config.ClusterID, config.ClientID, stan.NatsURL(config.URL))
	if err != nil {
		return nil, err
	}

	return &NATSPublisher{conn: conn, subject: config.Subject, logger: logger}, nil
}

func (p *NATSPublisher) Queue(req *Request) error {
	if req == nil {
		return fmt.Errorf("nil queue request")
	}
	if req.Function == "" {
		return fmt.Errorf("missing function name")
	}
	if len(req.Body) > 256*1000 {
		return fmt.Errorf("request body too large for tinyFaaS async queue")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	subject := p.subject
	if req.QueueName != "" {
		subject = req.QueueName
	}

	p.mu.RLock()
	conn := p.conn
	p.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("nats publisher closed")
	}

	p.logger.Debug("queueing async invocation", "function", req.Function, "bytes", len(req.Body), "subject", subject)
	return conn.Publish(subject, body)
}

func (p *NATSPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil {
		return nil
	}
	err := p.conn.Close()
	p.conn = nil
	return err
}

type NoopPublisher struct{}

func (NoopPublisher) Queue(*Request) error {
	return fmt.Errorf("async queue not configured")
}

func (NoopPublisher) Close() error { return nil }
