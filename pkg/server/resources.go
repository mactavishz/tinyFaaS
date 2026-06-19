package server

import (
	"fmt"
	"math/big"
	"strings"
)

const (
	DefaultMemoryBytes  int64 = 128 * 1024 * 1024
	DefaultMemoryString       = "128Mi"

	DefaultNanoCPUs  int64 = 1_000_000_000 / 16
	DefaultCPUString       = "0.0625" // 1/16 vCPU
)

type FunctionResources struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type FunctionResourceRequest struct {
	Limits *FunctionResources `json:"limits,omitempty"`
}

type ResourceLimits struct {
	NanoCPUs    int64
	MemoryBytes int64
}

type EffectiveLimits struct {
	CPU         string `json:"cpu"`
	Memory      string `json:"memory"`
	NanoCPUs    int64  `json:"nano_cpus"`
	MemoryBytes int64  `json:"memory_bytes"`
}

func EffectiveResourceLimits(limits *FunctionResources) (FunctionResources, EffectiveLimits, ResourceLimits, error) {
	resolved := FunctionResources{}
	if limits != nil {
		resolved.CPU = strings.TrimSpace(limits.CPU)
		resolved.Memory = strings.TrimSpace(limits.Memory)
	}

	if resolved.CPU == "" {
		resolved.CPU = DefaultCPUString
	}
	if resolved.Memory == "" {
		resolved.Memory = DefaultMemoryString
	}

	nano, err := ParseCPUNano(resolved.CPU)
	if err != nil {
		return FunctionResources{}, EffectiveLimits{}, ResourceLimits{}, err
	}
	mem, err := ParseMemoryBytes(resolved.Memory)
	if err != nil {
		return FunctionResources{}, EffectiveLimits{}, ResourceLimits{}, err
	}

	effective := EffectiveLimits{
		CPU:         resolved.CPU,
		Memory:      resolved.Memory,
		NanoCPUs:    nano,
		MemoryBytes: mem,
	}
	return resolved, effective, ResourceLimits{NanoCPUs: nano, MemoryBytes: mem}, nil
}

func ParseCPUNano(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("cpu is empty")
	}

	if before, ok := strings.CutSuffix(s, "m"); ok {
		milliStr := before
		r, ok := new(big.Rat).SetString(milliStr)
		if !ok {
			return 0, fmt.Errorf("invalid cpu value: %q", s)
		}
		r.Mul(r, big.NewRat(1_000_000, 1)) // millicores -> NanoCPUs
		return ratToInt64(r, "cpu")
	}

	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return 0, fmt.Errorf("invalid cpu value: %q", s)
	}
	r.Mul(r, big.NewRat(1_000_000_000, 1))
	return ratToInt64(r, "cpu")
}

func ParseMemoryBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("memory is empty")
	}

	numStr, unitStr, err := splitNumberUnit(s)
	if err != nil {
		return 0, err
	}

	r, ok := new(big.Rat).SetString(numStr)
	if !ok {
		return 0, fmt.Errorf("invalid memory value: %q", s)
	}

	factor, ok := memoryUnitFactor(unitStr)
	if !ok {
		return 0, fmt.Errorf("invalid memory unit: %q", unitStr)
	}

	r.Mul(r, big.NewRat(factor, 1))
	return ratToInt64(r, "memory")
}

func splitNumberUnit(s string) (string, string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", fmt.Errorf("value is empty")
	}

	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= '0' && c <= '9') || c == '.' {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return "", "", fmt.Errorf("missing numeric value: %q", s)
	}

	num := strings.TrimSpace(s[:i])
	unit := strings.TrimSpace(s[i:])
	unit = strings.ToLower(unit)
	return num, unit, nil
}

func memoryUnitFactor(unit string) (int64, bool) {
	// Accept Kubernetes-style binary units and common decimal variants.
	// Default (empty unit) is bytes.
	switch unit {
	case "", "b", "byte", "bytes":
		return 1, true
	case "k", "kb":
		return 1_000, true
	case "m", "mb":
		return 1_000_000, true
	case "g", "gb":
		return 1_000_000_000, true
	case "t", "tb":
		return 1_000_000_000_000, true
	case "ki", "kib":
		return 1024, true
	case "mi", "mib":
		return 1024 * 1024, true
	case "gi", "gib":
		return 1024 * 1024 * 1024, true
	case "ti", "tib":
		return 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

func ratToInt64(r *big.Rat, field string) (int64, error) {
	if r.Sign() < 0 {
		return 0, fmt.Errorf("%s must be non-negative", field)
	}

	// Truncate toward zero. Resource quantities should be representable as whole
	// bytes and NanoCPUs.
	i := new(big.Int).Quo(r.Num(), r.Denom())
	if !i.IsInt64() {
		return 0, fmt.Errorf("%s value overflows int64", field)
	}
	return i.Int64(), nil
}
