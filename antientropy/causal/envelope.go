package causal

import "encoding/json"

// kind tags an envelope as a delta-interval or an ack.
type kind string

const (
	kindDelta kind = "delta"
	kindAck   kind = "ack"
)

// envelope is the on-the-wire frame for a causal anti-entropy message.
// A delta carries the marshaled delta-interval in Payload and, in From & Seq,
// the sender's identifier and current sequence number. An ack carries only
// From & Seq. Carrier holds the trace context so a receive links back to the
// send that produced it.
type envelope struct {
	Kind    kind              `json:"kind"`
	From    string            `json:"from"`
	Seq     uint64            `json:"seq"`
	Payload []byte            `json:"payload,omitempty"`
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
