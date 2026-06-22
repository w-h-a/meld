package merkle_test

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/util/merkle"
)

// replica models a node's local key-value store.
type replica map[string]string

// tree builds this replica's Merkle.
func (r replica) tree() merkle.Tree {
	pairs := make([]merkle.Pair, 0, len(r))
	for k, v := range r {
		sum := sha256.Sum256([]byte(v))
		pairs = append(pairs, merkle.Pair{Key: k, Hash: sum[:]})
	}
	return merkle.Build(pairs)
}

// keysIn returns this replica's keys whose token falls in any of the
// ranges. These are the keys it would send a peer to reconcile a divergence.
func (r replica) keysIn(ranges []merkle.Range) []string {
	var keys []string
	for k := range r {
		t := merkle.Token(k)
		for _, rng := range ranges {
			if t >= rng.Lo && t <= rng.Hi {
				keys = append(keys, k)
				break
			}
		}
	}
	sort.Strings(keys)
	return keys
}

func TestReconcile_IdenticalReplicasShipNothing(t *testing.T) {
	// arrange
	a := replica{"user:1": "alice", "user:2": "bob", "user:3": "carol"}
	b := replica{"user:1": "alice", "user:2": "bob", "user:3": "carol"}

	// act
	ranges := merkle.Diff(a.tree(), b.tree())

	// assert
	require.Empty(t, ranges)
}

func TestReconcile_FindsKeyMissingFromOneReplica(t *testing.T) {
	// arrange
	a := replica{"user:1": "alice", "user:2": "bob", "user:3": "carol"}
	b := replica{"user:1": "alice", "user:3": "carol"}

	// act
	ranges := merkle.Diff(a.tree(), b.tree())

	// assert. The divergence is detected and the missing key is among the
	// keys a would send. A bucket can also sweep in co-located keys (here
	// these three share one), a harmless re-send, so we check user:2 is
	// found, not that it is alone.
	require.NotEmpty(t, ranges)
	require.Contains(t, a.keysIn(ranges), "user:2")
}

func TestReconcile_FindsKeyWithDifferingValue(t *testing.T) {
	// arrange
	a := replica{"user:1": "alice", "user:2": "bob", "user:3": "carol"}
	b := replica{"user:1": "alice", "user:2": "robert", "user:3": "carol"}

	// act
	ranges := merkle.Diff(a.tree(), b.tree())

	// assert. Both sides see the divergent range and find user:2 in it, so
	// each offers its value for the consumer's CRDT merge to resolve.
	require.Contains(t, a.keysIn(ranges), "user:2")
	require.Contains(t, b.keysIn(ranges), "user:2")
}

func TestReconcile_FindsOneDifferenceAmongManyKeys(t *testing.T) {
	// arrange
	a := make(replica)
	b := make(replica)
	for i := range 500 {
		k := fmt.Sprintf("key:%d", i)
		a[k] = "v"
		b[k] = "v"
	}
	a["needle"] = "only-on-a"

	// act
	ranges := merkle.Diff(a.tree(), b.tree())

	// assert. The diff localizes to the needle's bucket. a sends it, not all
	// 500 keys: that narrowing is the whole point. It may also sweep in a
	// neighbor or two that share the bucket, which is a harmless no-op merge.
	sent := a.keysIn(ranges)
	require.Contains(t, sent, "needle")
	require.Less(t, len(sent), 10)
}

func TestBuild_RootIsOrderIndependent(t *testing.T) {
	// arrange
	pairs := []merkle.Pair{
		{Key: "user:1", Hash: []byte("a")},
		{Key: "user:2", Hash: []byte("b")},
		{Key: "user:3", Hash: []byte("c")},
	}
	reversed := []merkle.Pair{pairs[2], pairs[1], pairs[0]}

	// act
	ranges := merkle.Diff(merkle.Build(pairs), merkle.Build(reversed))

	// assert
	require.Empty(t, ranges)
}
