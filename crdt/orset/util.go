package orset

import (
	"encoding/binary"
	"errors"
)

func copyTriples[T comparable](src map[triple[T]]struct{}, extra int) map[triple[T]]struct{} {
	out := make(map[triple[T]]struct{}, len(src)+extra)

	for t := range src {
		out[t] = struct{}{}
	}

	return out
}

func coalesce[T comparable](src map[triple[T]]struct{}) map[triple[T]]struct{} {
	highest := make(map[addKey[T]]uint64, len(src))
	for t := range src {
		k := addKey[T]{element: t.element, nodeID: t.nodeID}
		if c, ok := highest[k]; !ok || t.counter > c {
			highest[k] = t.counter
		}
	}

	out := make(map[triple[T]]struct{}, len(highest))
	for t := range src {
		k := addKey[T]{element: t.element, nodeID: t.nodeID}
		if t.counter == highest[k] {
			out[t] = struct{}{}
		}
	}

	return out
}

func encodeTriple[T comparable](t triple[T], encodeElement func(T) ([]byte, error)) ([]byte, error) {
	elementBytes, err := encodeElement(t.element)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0,
		binary.MaxVarintLen64+len(elementBytes)+
			binary.MaxVarintLen64+len(t.nodeID)+
			binary.MaxVarintLen64)
	buf = binary.AppendUvarint(buf, uint64(len(elementBytes)))
	buf = append(buf, elementBytes...)
	buf = binary.AppendUvarint(buf, uint64(len(t.nodeID)))
	buf = append(buf, t.nodeID...)
	buf = binary.AppendUvarint(buf, t.counter)

	return buf, nil
}

func decodeTriple[T comparable](data []byte, decodeElement func([]byte) (T, error)) (triple[T], []byte, error) {
	var t triple[T]

	elementLen, n := binary.Uvarint(data)
	if n <= 0 {
		return t, nil, errors.New("orset: invalid element length")
	}

	data = data[n:]

	if uint64(len(data)) < elementLen {
		return t, nil, errors.New("orset: element bytes truncated")
	}

	element, err := decodeElement(data[:elementLen])
	if err != nil {
		return t, nil, err
	}

	data = data[elementLen:]

	nodeIDLen, n := binary.Uvarint(data)
	if n <= 0 {
		return t, nil, errors.New("orset: invalid nodeID length")
	}

	data = data[n:]

	if uint64(len(data)) < nodeIDLen {
		return t, nil, errors.New("orset: nodeID bytes truncated")
	}

	nodeID := string(data[:nodeIDLen])

	data = data[nodeIDLen:]

	counter, n := binary.Uvarint(data)
	if n <= 0 {
		return t, nil, errors.New("orset: invalid counter")
	}

	data = data[n:]

	t.element = element
	t.nodeID = nodeID
	t.counter = counter

	return t, data, nil
}
