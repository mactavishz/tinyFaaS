package queue

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWorkerAcksMalformedMessages(t *testing.T) {
	w := NewWorker(WorkerConfig{}, testLogger())
	acks := 0

	w.handleMessageData([]byte("{"), func() error {
		acks++
		return nil
	})

	if acks != 1 {
		t.Fatalf("expected malformed message acked once, got %d", acks)
	}
}

func TestWorkerAcksCompletedHTTPResponseAndPreservesHeaders(t *testing.T) {
	var gotPath string
	var gotQuery string
	var gotCallID string
	var gotSource string
	var gotAsyncHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotCallID = r.Header.Get("X-Call-Id")
		gotSource = r.Header.Get("X-Source-Function")
		gotAsyncHeader = r.Header.Get("X-Tinyfaas-Async")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{TargetBaseURL: server.URL}, testLogger())
	msg, err := json.Marshal(Request{
		Function:    "next",
		Method:      http.MethodPost,
		QueryString: "a=b",
		Header: http.Header{
			"X-Call-Id":         []string{"call-1"},
			"X-Source-Function": []string{"caller"},
			"X-Tinyfaas-Async":  []string{"true"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	acks := 0
	w.handleMessageData(msg, func() error {
		acks++
		return nil
	})

	if acks != 1 {
		t.Fatalf("expected completed HTTP response acked once, got %d", acks)
	}
	if gotPath != "/invoke/next" || gotQuery != "a=b" {
		t.Fatalf("unexpected target path/query: %s?%s", gotPath, gotQuery)
	}
	if gotCallID != "call-1" || gotSource != "caller" {
		t.Fatalf("expected callgraph headers preserved, callID=%q source=%q", gotCallID, gotSource)
	}
	if gotAsyncHeader != "" {
		t.Fatalf("expected async header stripped, got %q", gotAsyncHeader)
	}
}

func TestWorkerLeavesTransportFailureUnacked(t *testing.T) {
	w := NewWorker(WorkerConfig{TargetBaseURL: "http://127.0.0.1:1"}, testLogger())
	msg, err := json.Marshal(Request{Function: "next"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	acks := 0

	w.handleMessageData(msg, func() error {
		acks++
		return nil
	})

	if acks != 0 {
		t.Fatalf("expected transport failure unacked, got %d acks", acks)
	}
}
