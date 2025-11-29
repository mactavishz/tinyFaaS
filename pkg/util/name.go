package util

import "regexp"

// IsValidFunctionName validates a string according to RFC 1035 DNS label rules:
// - Only lowercase alphanumeric characters (a-z, 0-9) and hyphens (-)
// - Maximum length of 63 characters
// - Cannot start or end with a hyphen
// - Must have at least one character
func IsValidFunctionName(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}

	// RFC 1035: must start with a letter, contain only alphanumeric and hyphens,
	// and not end with a hyphen. We'll be lenient and allow starting with a digit
	// as is common in modern DNS implementations.
	reg := regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

	return reg.MatchString(s)
}
