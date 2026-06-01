package swim

import "encoding/json"

// msgType discrimates SWIM protocol messages on the wire.
type msgType uint8

const (
	msgPing msgType = iota
	msgAck
	msgPingReq
	msgIndirectAck
	msgState
)

// envelope is the on-the-wire frame for every SWIM protocol message.
// From carries the sender's current self-view. The receiver
// merges From into its members map.
// Target is meaningful only for msgPingReq (the node the requester
// wants pinged indirectly) and msgState (the node whose state is being
// disseminated). For msgPing, msgAck, and msgIndirectAck, it is the zero
// value and the receiver ignores it.
// SeqNo correlates msgAck back to the msgPing that prompted it, and
// msgIndirectAck back to the msgPingReq that prompted it.
type envelope struct {
	Type   msgType   `json:"type"`
	From   nodeState `json:"from"`
	Target nodeState `json:"target"`
	SeqNo  uint64    `json:"seq_no"`
}

func encode(e envelope) ([]byte, error) {
	return json.Marshal(e)
}

func decode(b []byte) (envelope, error) {
	var e envelope
	err := json.Unmarshal(b, &e)
	return e, err
}
