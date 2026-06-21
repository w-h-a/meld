package rendezvous_test

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/util/rendezvous"
)

func TestAssign_Determinism(t *testing.T) {
	// arrange
	nodes := []string{"node-a", "node-b", "node-c"}
	key := "workload-1"

	// act
	first := rendezvous.Assign(nodes, key)
	second := rendezvous.Assign(nodes, key)
	third := rendezvous.Assign(nodes, key)

	// assert
	require.Equal(t, first, second)
	require.Equal(t, second, third)
}

func TestAssign_DeterminismAcrossKeys(t *testing.T) {
	// arrange
	nodes := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	keys := []string{"redis", "nginx", "postgres", "etcd", "vault"}

	// act + assert
	for _, key := range keys {
		first := rendezvous.Assign(nodes, key)
		second := rendezvous.Assign(nodes, key)
		require.Equal(t, first, second)
	}
}

func TestAssign_StabilityOnAdd(t *testing.T) {
	// arrange
	original := []string{"node-a", "node-b", "node-c"}
	expanded := []string{"node-a", "node-b", "node-c", "node-d"}
	keys := generateKeys(1000)

	// act
	before := make(map[string]string, len(keys))
	after := make(map[string]string, len(keys))
	for _, key := range keys {
		before[key] = rendezvous.Assign(original, key)
		after[key] = rendezvous.Assign(expanded, key)
	}

	moved := movedKeys(before, after)

	// assert
	require.NotEmpty(t, moved)
	require.Less(t, len(moved), len(keys))
	for _, key := range moved {
		require.Equal(t, "node-d", after[key])
	}
}

func TestAssign_StabilityOnRemove(t *testing.T) {
	// arrange
	original := []string{"node-a", "node-b", "node-c", "node-d"}
	reduced := []string{"node-a", "node-b", "node-c"}
	keys := generateKeys(1000)

	// act
	before := make(map[string]string, len(keys))
	after := make(map[string]string, len(keys))
	for _, key := range keys {
		before[key] = rendezvous.Assign(original, key)
		after[key] = rendezvous.Assign(reduced, key)
	}

	moved := movedKeys(before, after)

	// assert
	require.NotEmpty(t, moved)
	require.Less(t, len(moved), len(keys))
	for _, key := range moved {
		require.Equal(t, "node-d", before[key])
	}
}

func TestAssign_Uniformity(t *testing.T) {
	// arrange
	nodes := []string{"node-a", "node-b", "node-c"}
	keys := generateKeys(1000)

	// act
	counts := make(map[string]int)
	for _, key := range keys {
		counts[rendezvous.Assign(nodes, key)] += 1
	}

	// assert
	fair := len(keys) / len(nodes)
	tolerance := fair / 3
	for _, node := range nodes {
		require.InDelta(t, fair, counts[node], float64(tolerance), "node %s", node)
	}
}

func TestAssign_EmptyNodes(t *testing.T) {
	// arrange
	var nodes []string

	// act
	result := rendezvous.Assign(nodes, "any-key")

	// assert
	require.Empty(t, result)
}

func TestAssignN_RanksByDescendingScore(t *testing.T) {
	// arrange
	nodes := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	key := "workload-1"

	// act
	got := rendezvous.AssignN(nodes, key, len(nodes))

	// assert. Repeatedly assigning the winner and removing it reproduces
	// the ranking, so AssignN orders by descending score.
	remaining := append([]string{}, nodes...)
	want := make([]string, 0, len(nodes))
	for len(remaining) > 0 {
		best := rendezvous.Assign(remaining, key)
		want = append(want, best)
		remaining = remove(remaining, best)
	}
	require.Equal(t, want, got)
}

func TestAssignN_NBounds(t *testing.T) {
	// arrange
	nodes := []string{"node-a", "node-b", "node-c", "node-d"}
	key := "workload-1"
	full := rendezvous.AssignN(nodes, key, len(nodes))

	// act + assert
	require.Equal(t, full[:2], rendezvous.AssignN(nodes, key, 2))      // top-n is a prefix of the full ranking
	require.Equal(t, full, rendezvous.AssignN(nodes, key, len(nodes))) // n == len returns all
	require.Equal(t, full, rendezvous.AssignN(nodes, key, 99))         // n > len returns all
	require.Nil(t, rendezvous.AssignN(nodes, key, 0))                  // n == 0 returns nil
	require.Nil(t, rendezvous.AssignN(nodes, key, -1))                 // n < 0 returns nil
	require.Nil(t, rendezvous.AssignN(nil, key, 3))                    // empty nodes returns nil
}

func TestAssignN_Determinism(t *testing.T) {
	// arrange
	nodes := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	shuffled := []string{"node-e", "node-c", "node-a", "node-d", "node-b"}
	key := "workload-1"

	// act
	first := rendezvous.AssignN(nodes, key, 3)
	second := rendezvous.AssignN(nodes, key, 3)
	reordered := rendezvous.AssignN(shuffled, key, 3)

	// assert
	require.Equal(t, first, second)
	require.Equal(t, first, reordered) // independent of the order nodes are passed in
}

func TestAssignN_StabilityOnAdd(t *testing.T) {
	// arrange
	original := []string{"node-a", "node-b", "node-c", "node-d"}
	expanded := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	keys := generateKeys(1000)
	const n = 3

	// act
	moved := 0
	for _, key := range keys {
		before := rendezvous.AssignN(original, key, n)
		after := rendezvous.AssignN(expanded, key, n)
		if slices.Equal(before, after) {
			continue
		}
		moved++
		require.Contains(t, after, "node-e") // a list changes only if the new node entered the top n
	}

	// assert
	require.Positive(t, moved)
	require.Less(t, moved, len(keys))
}

func TestAssignN_StabilityOnRemove(t *testing.T) {
	// arrange
	original := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	reduced := []string{"node-a", "node-b", "node-c", "node-d"}
	keys := generateKeys(1000)
	const n = 3

	// act
	moved := 0
	for _, key := range keys {
		before := rendezvous.AssignN(original, key, n)
		after := rendezvous.AssignN(reduced, key, n)
		if slices.Equal(before, after) {
			continue
		}
		moved++
		require.Contains(t, before, "node-e") // a list changes only if the removed node was in it
	}

	// assert
	require.Positive(t, moved)
	require.Less(t, moved, len(keys))
}

func generateKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26)) + "-" + strconv.Itoa(i)
	}
	return keys
}

func movedKeys(before, after map[string]string) []string {
	moved := []string{}
	for k, b := range before {
		if b != after[k] {
			moved = append(moved, k)
		}
	}
	return moved
}

func remove(nodes []string, target string) []string {
	out := make([]string, 0, len(nodes)-1)
	for _, n := range nodes {
		if n != target {
			out = append(out, n)
		}
	}
	return out
}
