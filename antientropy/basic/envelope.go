package basic

import "encoding/json"

// envelope is the on-the-wire frame for a basic anti-entropy message.
// Payload is a marshalled CRDT state, a delta-group or a full state, that
// the receiver merges into its own. Carrier holds the W3C trace context so
// a receive links back to the sender's gossip round.
type envelope struct {
	Payload []byte            `json:"payload"`
	Carrier map[string]string `json:"carrier,omitempty"`
}

func encodeEnvelope(e envelope) ([]byte, error) {
	return json.Marshal(e)
}

func decodeEnvelope(b []byte) (envelope, error) {
	var e envelope
	err := json.Unmarshal(b, &e)
	return e, err
}
