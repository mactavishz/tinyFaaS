package gateway

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const DEFAULT_FUNCTION_NS = "tinyfaas"

type InvocationRecord struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DurationNS int64     `json:"duration_ns"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	StatusCode int       `json:"status_code"`
	Success    bool      `json:"success"`
}

type InvocationSummary struct {
	SuccessfulInvocations int            `json:"successful_invocations"`
	FailedInvocations     int            `json:"failed_invocations"`
	StatusCodes           map[string]int `json:"status_codes"`
}

type FunctionStatsResponse struct {
	Function struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"` // not used currently but aligned with faasd for future multi-namespace support
	} `json:"function"`
	Summary     InvocationSummary  `json:"summary"`
	Invocations []InvocationRecord `json:"invocations"`
}

type functionStats struct {
	invocations []InvocationRecord
	summary     InvocationSummary
}

type functionStatsStore struct {
	mu    sync.RWMutex
	stats map[string]functionStats
}

func NewFunctionStatsStore() *functionStatsStore {
	return &functionStatsStore{stats: map[string]functionStats{}}
}

func (s *functionStatsStore) Record(name string, record InvocationRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.stats[name]
	stats.invocations = append(stats.invocations, record)
	if stats.summary.StatusCodes == nil {
		stats.summary.StatusCodes = map[string]int{}
	}
	if record.Success {
		stats.summary.SuccessfulInvocations++
	} else {
		stats.summary.FailedInvocations++
	}
	stats.summary.StatusCodes[strconv.Itoa(record.StatusCode)]++
	s.stats[name] = stats
}

func (s *functionStatsStore) Get(name string) (functionStats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats, ok := s.stats[name]
	if !ok {
		return functionStats{}, false
	}
	copyStats := functionStats{
		invocations: make([]InvocationRecord, len(stats.invocations)),
		summary: InvocationSummary{
			SuccessfulInvocations: stats.summary.SuccessfulInvocations,
			FailedInvocations:     stats.summary.FailedInvocations,
			StatusCodes:           map[string]int{},
		},
	}
	copy(copyStats.invocations, stats.invocations)
	for code, count := range stats.summary.StatusCodes {
		copyStats.summary.StatusCodes[code] = count
	}
	return copyStats, true
}

func (s *functionStatsStore) Reset(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.stats, name)
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (w *statusCapturingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.written = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusCapturingResponseWriter) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (g *Gateway) recordInvocationStats(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originalPath := r.URL.Path
		name := strings.TrimPrefix(originalPath, "/fn/")
		if name == "" {
			next.ServeHTTP(w, r)
			return
		}

		startedAt := time.Now().UTC()
		capturing := &statusCapturingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(capturing, r)
		finishedAt := time.Now().UTC()

		g.stats.Record(name, InvocationRecord{
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			DurationNS: finishedAt.Sub(startedAt).Nanoseconds(),
			Method:     r.Method,
			Path:       originalPath,
			StatusCode: capturing.statusCode,
			Success:    capturing.statusCode >= 200 && capturing.statusCode < 400,
		})
	})
}

func (g *Gateway) resetFunctionStats(name string) {
	if strings.TrimSpace(name) == "" {
		return
	}
	g.stats.Reset(name)
}

func (g *Gateway) HandleFunctionStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/system/stats/function/")
	if strings.TrimSpace(name) == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	stats, ok := g.stats.Get(name)
	exists, err := g.functionExists(name)
	if err != nil {
		http.Error(w, "Failed to check function", http.StatusBadGateway)
		g.logger.Error("failed to check function for stats", zap.String("name", name), zap.Error(err))
		return
	}
	if !ok && !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !ok {
		stats = functionStats{summary: InvocationSummary{StatusCodes: map[string]int{}}, invocations: []InvocationRecord{}}
	}

	sort.Slice(stats.invocations, func(i, j int) bool {
		return stats.invocations[i].StartedAt.Before(stats.invocations[j].StartedAt)
	})

	resp := FunctionStatsResponse{Summary: stats.summary, Invocations: stats.invocations}
	resp.Function.Name = name
	resp.Function.Namespace = DEFAULT_FUNCTION_NS
	if resp.Summary.StatusCodes == nil {
		resp.Summary.StatusCodes = map[string]int{}
	}
	if resp.Invocations == nil {
		resp.Invocations = []InvocationRecord{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (g *Gateway) functionExists(name string) (bool, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+g.managerAddr()+"/function/"+name, nil)
	if err != nil {
		return false, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, nil
}
