package causal

import "encoding/json"

// snapshot is the durable form of the node's persistent state.
type snapshot struct {
	Seq   uint64 `json:"seq"`
	State []byte `json:"state"`
}

func encodeSnapshot(s snapshot) ([]byte, error) {
	return json.Marshal(s)
}

func decodeSnapshot(b []byte) (snapshot, error) {
	var s snapshot
	err := json.Unmarshal(b, &s)
	return s, err
}
