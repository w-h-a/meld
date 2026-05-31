package swim

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/membership"
)

func TestApply_LeftIsTerminalLocally(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Left, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Alive, Incarnation: 100}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.False(t, changed)
	require.Equal(t, membership.Left, next.State)
}

func TestApply_IncomingLeftTakesEffect(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Alive, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Left, Incarnation: 5}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, membership.Left, next.State)
}

func TestApply_IncomingLeftAtHigherIncarnationAdoptsIncomingMeta(t *testing.T) {
	// arrange
	local := nodeState{
		ID:          "n1",
		Address:     "10.0.0.1:7946",
		Meta:        map[string]string{"role": "leader"},
		State:       membership.Alive,
		Incarnation: 5,
	}
	incoming := nodeState{
		ID:          "n1",
		Address:     "10.0.0.1:9999",
		Meta:        map[string]string{"role": "follower"},
		State:       membership.Left,
		Incarnation: 6,
	}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, membership.Left, next.State)
	require.Equal(t, uint64(6), next.Incarnation)
	require.Equal(t, "10.0.0.1:9999", next.Address)
	require.Equal(t, "follower", next.Meta["role"])
}

func TestApply_IncomingLeftAtLowerIncarnationPreservesLocalIdentity(t *testing.T) {
	// arrange
	local := nodeState{
		ID:          "n1",
		Address:     "10.0.0.1:7946",
		Meta:        map[string]string{"role": "leader"},
		State:       membership.Alive,
		Incarnation: 10,
	}
	incoming := nodeState{
		ID:          "n1",
		Address:     "10.0.0.1:9999",
		Meta:        map[string]string{"role": "follower"},
		State:       membership.Left,
		Incarnation: 3,
	}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, membership.Left, next.State)
	require.Equal(t, uint64(10), next.Incarnation)
	require.Equal(t, "10.0.0.1:7946", next.Address)
	require.Equal(t, "leader", next.Meta["role"])
}

func TestApply_HigherIncarnationAliveRefutesSuspect(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Suspect, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Alive, Incarnation: 6}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, membership.Alive, next.State)
	require.Equal(t, uint64(6), next.Incarnation)
}

func TestApply_HigherIncarnationAliveRefutesFailed(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Failed, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Alive, Incarnation: 7}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, membership.Alive, next.State)
	require.Equal(t, uint64(7), next.Incarnation)
}

func TestApply_IncomingAdoptsNewerMetadata(t *testing.T) {
	// arrange
	local := nodeState{
		ID:          "n1",
		Address:     "10.0.0.1:7946",
		Meta:        map[string]string{"role": "old"},
		State:       membership.Alive,
		Incarnation: 5,
	}
	incoming := nodeState{
		ID:          "n1",
		Address:     "10.0.0.1:7946",
		Meta:        map[string]string{"role": "new"},
		State:       membership.Alive,
		Incarnation: 6,
	}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, "new", next.Meta["role"])
}

func TestApply_LowerIncarnationIgnored(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Suspect, Incarnation: 10}
	incoming := nodeState{ID: "n1", State: membership.Failed, Incarnation: 5}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.False(t, changed)
	require.Equal(t, membership.Suspect, next.State)
	require.Equal(t, uint64(10), next.Incarnation)
}

func TestApply_SameIncarnation_SuspectOvertakesAlive(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Alive, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Suspect, Incarnation: 5}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, membership.Suspect, next.State)
	require.Equal(t, uint64(5), next.Incarnation)
}

func TestApply_SameIncarnation_FailedOvertakesSuspect(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Suspect, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Failed, Incarnation: 5}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.True(t, changed)
	require.Equal(t, membership.Failed, next.State)
}

func TestApply_SameIncarnation_AliveDoesNotOvertakeSuspect(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Suspect, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Alive, Incarnation: 5}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.False(t, changed)
	require.Equal(t, membership.Suspect, next.State)
}

func TestApply_SameIncarnation_AliveStaysAlive(t *testing.T) {
	// arrange
	local := nodeState{ID: "n1", State: membership.Alive, Incarnation: 5}
	incoming := nodeState{ID: "n1", State: membership.Alive, Incarnation: 5}

	// act
	next, changed := apply(local, incoming)

	// assert
	require.False(t, changed)
	require.Equal(t, local, next)
}
