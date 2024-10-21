package be

// PadOrTruncLeft pads or trims the given byte slice to the given length from the left.
func PadOrTruncLeft(b []byte, n int) []byte {
	if len(b) == n {
		return b
	}
	if len(b) > n {
		return b[len(b)-n:]
	}
	return append(make([]byte, n-len(b)), b...)
}

// PadOrTruncRight pads or trims the given byte slice to the given length from the right.
func PadOrTruncRight(b []byte, n int) []byte {
	if len(b) == n {
		return b
	}
	if len(b) > n {
		return b[:n]
	}
	return append(b, make([]byte, n-len(b))...)
}
