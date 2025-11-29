package util

import "strings"

// extractIP extracts the IP address from an IP:port string
func ExtractIP(remoteAddr string) string {
	// Handle IPv6 addresses like [::1]:port
	if strings.HasPrefix(remoteAddr, "[") {
		if idx := strings.LastIndex(remoteAddr, "]:"); idx != -1 {
			return remoteAddr[1:idx]
		}
		return remoteAddr
	}
	// Handle IPv4 addresses like 192.168.1.1:port
	if idx := strings.LastIndex(remoteAddr, ":"); idx != -1 {
		return remoteAddr[:idx]
	}
	return remoteAddr
}
