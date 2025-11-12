package s3

// shouldEscape reports whether the byte should be escaped by urlEscape. It's
// similar to net/url.shouldEscape but does not escape '*' and '/'.
func shouldEscape(c byte) bool {
	// unreserved alphanumeric characters
	if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' {
		return false
	}

	switch c {
	case '-', '_', '.': // unreserved mark characters
		return false
	case '/', '*': // similar to escaping in URL path segment
		return false
	}
	return true
}

// urlEscape follows the same rules as net/url.QueryEscape, but it does not
// escape '*'.
func urlEscape(s string) string {
	hexCount := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if shouldEscape(c) {
			hexCount++
		}
	}

	if hexCount == 0 {
		return s
	}

	var buf [64]byte
	var t []byte

	required := len(s) + 2*hexCount
	if required <= len(buf) {
		t = buf[:required]
	} else {
		t = make([]byte, required)
	}

	const upperhex = "0123456789ABCDEF"

	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case shouldEscape(c):
			t[j] = '%'
			t[j+1] = upperhex[c>>4]
			t[j+2] = upperhex[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}
