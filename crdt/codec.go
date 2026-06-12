package crdt

// StringEncode encodes a string element as its raw bytes.
func StringEncode(s string) ([]byte, error) { return []byte(s), nil }

// StringDecode decodes raw bytes back into a string element.
func StringDecode(b []byte) (string, error) { return string(b), nil }
