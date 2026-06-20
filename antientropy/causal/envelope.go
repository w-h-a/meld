package causal

import "encoding/json"

// kind tags an envelope as a selta-interval or an ack.
type kind string

const (
	kindDelta kind = "delta"
	kindAck   kind = "ack"
)

// envelope is the on-the-wire frame for a causal anti-entropy message.
// A delta carries the marshaled delta-interval in Payload and, in Seq,
// the sender's current sequence number, which is the exclusive upper bound
// of that interval. An ack carries only Seq, echoing the bound it
// received so the sender can record how far that neighbor has caught up.
// Carrier holds the trace context so a receive links back to the send
// that produced it.
type envelope struct {
	Kind    kind              `json:"kind"`
	Seq     uint64            `json:"seq"`
	Payload []byte            `json:"payload,omitempty"`
	Carrier map[string]string `json:"carrier,omitempty"`
}

func encode(e envelope) ([]byte, error) {
	return json.Marshal(e)
}

func decode(b []byte) (envelope, error) {
	var e envelope
	err := json.Unmarshal(b, &e)
	return e, err
}
