package phi

import "encoding/json"

// msgType discriminates phi protocol messages on the wire.
type msgType uint8

const (
	msgHeartbeat msgType = iota
	msgLeave
)

// envelope is the on-the-wire frame for every phi message.
// From carries the sender's current self-view. The receiver
// merges From into its members map.
// SeqNo is a monotonic per-sender counter kept only for
// ordering diagnostics. The detector judges liveness from
// arrival time, not SeqNo, so gaps and duplicates in SeqNo
// are harmless.
type envelope struct {
	Type    msgType           `json:"type"`
	From    nodeState         `json:"from"`
	SeqNo   uint64            `json:"seq_no"`
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
