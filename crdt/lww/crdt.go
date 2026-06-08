// Package lww implements the Last-Writer-Wins Register CRDT.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011), Section 3.2.1
//   - Lamport, "Time, Clocks, and the Ordering of Events" (1978)
package lww

import (
	"encoding/binary"
	"errors"

	"github.com/w-h-a/meld/crdt"
)

// Make sure Register satisfies crdt.Mergeable. The witness uses a
// concrete instantiation because Go generics require one for a
// compile-time interface check.
var _ crdt.Mergeable[Register[struct{}]] = Register[struct{}]{}

// Register holds one value of type T together with a Tag that
// totally orders writes.
type Register[T any] struct {
	value T
	tag   Tag
}

func New[T any]() Register[T] {
	return Register[T]{}
}

// Value returns the current value.
func (r Register[T]) Value() T {
	return r.value
}

// Tag returns the timestamp of the current value.
func (r Register[T]) Tag() Tag {
	return r.tag
}

// Set returns a new Register holding value, tagged with a Counter
// one greater than the receiver's and Writer set to nodeID. The
// receiver is not modified. Callers pass their own node id and
// only their own.
func (r Register[T]) Set(nodeID string, value T) Register[T] {
	return Register[T]{
		value: value,
		tag:   Tag{Counter: r.tag.Counter + 1, Writer: nodeID},
	}
}

// Merge returns the Register holding the winning value and Tag.
// The winner is whichever side has the greater Tag.
//
// Worked example. Two replicas write concurrently.
//
//	a := New[string]().Set("n1", "v1")    tag=(1, n1)
//	b := New[string]().Set("n2", "v2")    tag=(1, n2)
//
// Tags tie on counter. Writer "n1" < "n2", so b wins.
//
//	a.Merge(b) = ("v2", tag=(1, n2))
//
// Later, n2 reads the merged state and writes again.
//
//	c := a.Merge(b).Set("n2", "v3")    tag=(2, n2)
//
// Compared to the original a, c has the strictly greater Counter,
// so c wins any further merge with a.
func (r Register[T]) Merge(other Register[T]) Register[T] {
	if r.tag.Less(other.tag) {
		return other
	}

	return r
}

// Marshal encodes the register for persistence or the wire. T is
// opaque to the register, so the caller supplies the value codec.
// encodeValue must not be nil.
func (r Register[T]) Marshal(encodeValue func(T) ([]byte, error)) ([]byte, error) {
	valueBytes, err := encodeValue(r.value)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0,
		binary.MaxVarintLen64+len(valueBytes)+
			binary.MaxVarintLen64+
			binary.MaxVarintLen64+len(r.tag.Writer))
	buf = binary.AppendUvarint(buf, uint64(len(valueBytes)))
	buf = append(buf, valueBytes...)
	buf = binary.AppendUvarint(buf, r.tag.Counter)
	buf = binary.AppendUvarint(buf, uint64(len(r.tag.Writer)))
	buf = append(buf, r.tag.Writer...)

	return buf, nil
}

// Unmarshal parses the byte form into the receiver. The caller
// supplies the value decoder, matching the encoder used at
// Marshal. decodeValue must not be nil.
func (r *Register[T]) Unmarshal(data []byte, decodeValue func([]byte) (T, error)) error {
	valueLen, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("lww: invalid value length")
	}

	data = data[n:]

	if uint64(len(data)) < valueLen {
		return errors.New("lww: value bytes truncated")
	}

	value, err := decodeValue(data[:valueLen])
	if err != nil {
		return err
	}

	data = data[valueLen:]

	counter, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("lww: invalid tag counter")
	}

	data = data[n:]

	writerLen, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("lww: invalid tag writer length")
	}

	data = data[n:]

	if uint64(len(data)) < writerLen {
		return errors.New("lww: tag writer bytes truncated")
	}

	writer := string(data[:writerLen])

	r.value = value
	r.tag = Tag{Counter: counter, Writer: writer}

	return nil
}
