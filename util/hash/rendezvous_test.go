package hash_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/util/hash"
)

func TestAssign_Determinism(t *testing.T) {
	// arrange
	nodes := []string{"node-a", "node-b", "node-c"}
	key := "workload-1"

	// act
	first := hash.Assign(nodes, key)
	second := hash.Assign(nodes, key)
	third := hash.Assign(nodes, key)

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
		first := hash.Assign(nodes, key)
		second := hash.Assign(nodes, key)
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
		before[key] = hash.Assign(original, key)
		after[key] = hash.Assign(expanded, key)
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
		before[key] = hash.Assign(original, key)
		after[key] = hash.Assign(reduced, key)
	}

	moved := movedKeys(before, after)

	// assert
	require.NotEmpty(t, moved)
	require.Less(t, len(moved), len(keys))
	for _, key := range moved {
		require.Equal(t, "node-d", before[key])
	}
}

func TestAssign_EmptyNodes(t *testing.T) {
	// arrange
	var nodes []string

	// act
	result := hash.Assign(nodes, "any-key")

	// assert
	require.Empty(t, result)
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
