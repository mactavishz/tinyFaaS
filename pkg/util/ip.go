package util

import (
	"net/netip"
	"strings"
)

// Docker has the following default subnet for bridge network:
// {"base":"172.17.0.0/16","size":16},
// {"base":"172.18.0.0/16","size":16},
// {"base":"172.19.0.0/16","size":16},
// {"base":"172.20.0.0/14","size":16},
// {"base":"172.24.0.0/14","size":16},
// {"base":"172.28.0.0/14","size":16},
// {"base":"192.168.0.0/16","size":20}
// more details: https://docs.docker.com/engine/network/
// Anything outside these ranges is considered external caller, i.e., end users
var dockerDefaultPools = []string{
	"172.17.0.0/16",
	"172.18.0.0/16",
	"172.19.0.0/16",
	"172.20.0.0/14",
	"172.24.0.0/14",
	"172.28.0.0/14",
	"192.168.0.0/16",
}

var privateIPBlocks []netip.Prefix

func init() {
	for _, cidr := range dockerDefaultPools {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			continue // Should not happen with hardcoded valid CIDRs
		}
		privateIPBlocks = append(privateIPBlocks, prefix)
	}
}

// extractIP extracts the IP address from an IP:port string
func ExtractIP(remoteAddr string) string {
	// Handle IPv6 addresses like [::1]:port
	if strings.HasPrefix(remoteAddr, "[") {
		if idx := strings.LastIndex(remoteAddr, "]:"); idx != -1 {
			return remoteAddr[1:idx]
		}
		return remoteAddr
	}
	// Handle IPv4 addresses like 192.168.1.1:port (exactly one colon)
	if strings.Count(remoteAddr, ":") == 1 {
		if idx := strings.LastIndex(remoteAddr, ":"); idx != -1 {
			return remoteAddr[:idx]
		}
	}
	return remoteAddr
}

// IsExternalIP checks if the given IP is an external IP address
func IsExternalIP(ip string) (bool, error) {
	private, err := IsPrivateIP(ip)
	if err != nil {
		return false, err
	}
	return !private, nil
}

func IsPrivateIP(ip string) (bool, error) {
	parsedIP, err := netip.ParseAddr(ip)
	if err != nil {
		return false, err
	}
	for _, block := range privateIPBlocks {
		if block.Contains(parsedIP) {
			return true, nil
		}
	}
	return false, nil
}
