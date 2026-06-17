package antientropy

// Encode serializes a CRDT state for the wire.
type Encoder[T any] func(T) ([]byte, error)

// Decode parses a CRDT state back from its wire bytes.
type Decoder[T any] func([]byte) (T, error)
