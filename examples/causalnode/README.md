# causalnode: the same set, replicated causally and durably

A runnable, three-node example from the [meld](../../) library. It replicates the very same add-wins set as [orsetnode](../orsetnode), but it swaps the replicator underneath for the causal one and gives each node a durable store. The result is a set whose in-between states never skip a step and whose nodes come back after a crash exactly where they left off. It shows meld's causal anti-entropy (Algorithm 2) and its durable recovery.

## The idea

The other CRDT examples ship state with the basic anti-entropy loop (Algorithm 1). That loop sends deltas and joins whatever arrives in any order. It guarantees everyone ends up the same, but along the way a node can briefly hold a state that reflects a later change without an earlier change that came before it. For a set of workloads that is usually harmless. When you want every in-between state to make sense too, you want causal anti-entropy (Algorithm 2 in the delta paper, Almeida, Shoker, Baquero, 2016).

Causal anti-entropy makes two changes. First, each node numbers its own deltas in order, one, two, three, and keeps a per-neighbor note of how far each neighbor has acknowledged. When it is time to gossip, a node sends a neighbor only the unbroken stretch of deltas that neighbor is missing, starting right after the last one it acknowledged. The neighbor joins that stretch only when it already holds everything before it. The paper calls this the causal delta-merging condition, and it is what keeps a node from ever applying a change before the changes it depends on. So every state a node passes through reflects a whole, gap-free slice of history.

Second, the state and the sequence number are both written to disk on every change. The sequence number has to be durable, not just the state, because a node that forgot its number after a crash could reuse an old one or skip one, which would break the ordering above. With both saved, a restarted node picks up exactly where it stopped.

Recovery falls out of this design in a way you can watch. The delta buffer and the acknowledgment notes live only in memory, so a node that just restarted has no deltas queued and does not know how far its neighbors had gotten. The paper's answer, which meld follows, is that in exactly this situation the node ships its whole state to each neighbor once and lets the merge sort it out. That is the short burst of full-state sends you see right after a restart. The set itself, its members and how conflicts resolve, is the add-wins OR-Set explained in [orsetnode](../orsetnode) and is unchanged here.

## Use it in your code

The membership and transport wiring is the same as the other CRDT examples. The two new pieces are a durable store and the causal replicator. The full version is in [`main.go`](./main.go).

```go
import (
	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/causal"
	"github.com/w-h-a/meld/crdt/orset"
	"github.com/w-h-a/meld/store"
	"github.com/w-h-a/meld/store/sqlite"
)

// A durable store holds the (sequence number, state) pair across restarts.
st, _ := sqlite.New(store.WithLocation("/data/n1.db"))

// The causal replicator runs Algorithm 2. Same shape as the basic one, plus the
// store and a garbage-collection interval for the delta buffer.
r, _ := causal.New(
	antientropy.WithInitial(orset.New[string]()),
	antientropy.WithCodec(encode, decode),
	antientropy.WithTransport[orset.ORSet[string]](cg),
	antientropy.WithMembership[orset.ORSet[string]](m),
	antientropy.WithPeerAddress[orset.ORSet[string]](peerAddr),
	antientropy.WithStore[orset.ORSet[string]](st),
	antientropy.WithInterval[orset.ORSet[string]](time.Second),
	causal.WithGCInterval[orset.ORSet[string]](10*time.Second),
)

r.Start(ctx)       // loads the durable (seq, state) if one exists, then starts the loops
m.Join(ctx, seeds)

// The submit-and-read API is the same as the other CRDT demos.
r.Submit(r.State().AddDelta("n1", "nginx"))
r.Submit(r.State().RemoveDelta("nginx"))
all := r.State().Elements()
```

You never track sequence numbers or acknowledgments yourself. The replicator numbers deltas, ships the right stretch to each neighbor, applies incoming stretches in order, and persists as it goes.

## Run the demo

You need Docker with Compose. If you want to build the library or import it directly, you also need Go 1.25 or newer.

```
docker compose up --build
```

This starts three nodes and one Jaeger container. n0 is the seed, and n1 and n2 join through it. Like orsetnode, each node adds and removes workloads named `workload-0` through `workload-7` at random. The difference is that each node writes its set to a SQLite file in its own volume, so the state survives the container, and each node runs a periodic garbage collection that drops deltas every neighbor has already acknowledged.

## What you'll see

On each merge that changed something, a node prints a summary.

```
n0 merged: added=[workload-3] removed=[] elements=6
```

That reads the same as orsetnode, minus the add-wins column, because this demo is about how the set travels rather than how it resolves. The behavior worth watching is recovery, which the next section walks through in traces.

## Look in Jaeger

Open http://localhost:16686. Each node is its own service, named n0, n1, and n2.

The causal replicator records more than the basic one, so there is more to follow. A node shipping state records `antientropy.gossip`, carrying `antientropy.neighbor` (who it is sending to), `antientropy.acked` (how far that neighbor had gotten), `antientropy.seq` (how far this node has gotten), and `antientropy.delta_or_full` (whether it sent a delta stretch or the whole state). Merging an incoming stretch records `antientropy.receive_delta` with `antientropy.changed`, and an incoming acknowledgment records `antientropy.receive_ack`. Every durable write records `antientropy.persist` with the `antientropy.seq` it saved, and each garbage-collection pass records `antientropy.gc` with `antientropy.deltas_freed` and `antientropy.buffer_size`. The demo's own `crdt.element_count` and `crdt.live_triple_count` ride on the gossip and receive-delta spans.

## Try this

Watch a node recover. Note the current members in the logs, then run `docker compose restart n2`. n2 stops, leaves, and starts again with the same id against the same SQLite file. On the way back up it reloads its saved state, rejoins the cluster, and because its in-memory delta buffer is empty it ships its whole state to each neighbor once. In Jaeger, filter n2's `antientropy.gossip` spans to `antientropy.delta_or_full=full` and you will find exactly those full sends, one per neighbor, right after the restart, and nothing but delta stretches before and after. That short burst is the recovery path from the paper made visible.

Watch garbage collection keep the buffer small. Filter any node's `antientropy.gc` spans and read `antientropy.buffer_size` over time. It stays bounded, because once every neighbor has acknowledged a delta the node is free to drop it.

## Environment variables

| Var | Required | Default | Meaning |
|---|---|---|---|
| `NODE_ID` | yes | — | this node's id, also its Jaeger service name and the id its add tags carry |
| `BIND_ADDR` | yes | — | UDP address for membership and failure detection |
| `CRDT_BIND_ADDR` | yes | — | UDP address for set state |
| `PEERS` | no | — | comma-separated seed addresses to join through, empty means this node is the seed |
| `ADVERTISE_ADDR` | no | `NODE_ID:7946` | membership address peers should use to reach this node |
| `CRDT_ADVERTISE_ADDR` | no | `NODE_ID:7947` | set-state address advertised through membership Meta |
| `STORE_PATH` | no | `NODE_ID.db` | SQLite file holding the durable (sequence number, state) pair |
| `PROBE_INTERVAL` | no | `1s` | how often to probe a random peer |
| `PROBE_TIMEOUT` | no | `300ms` | how long to wait for a probe ack before probing indirectly |
| `SUSPICION_MULT` | no | `4` | suspicion timeout is this multiple of the probe interval |
| `EVENTS_INTERVAL` | no | `1s` | how often this node adds or removes an item |
| `REMOVE_PROBABILITY` | no | `0.3` | chance that a tick is a remove rather than an add |
| `WORKLOAD_POOL_SIZE` | no | `8` | how many distinct item names the demo cycles through |
| `GOSSIP_INTERVAL` | no | `1s` | how often the replicator ships to a neighbor |
| `GC_INTERVAL` | no | `10s` | how often to drop deltas every neighbor has acknowledged |
| `OTLP_ENDPOINT` | no | `jaeger:4317` | OTLP gRPC endpoint for traces |

The compose file gives each node its own volume for the SQLite file, sets `EVENTS_INTERVAL` to 5s, and slows the probe timing to `PROBE_INTERVAL=5s`, `PROBE_TIMEOUT=1s`, `SUSPICION_MULT=8`.

## See also

The causal anti-entropy algorithm is Algorithm 2 from Almeida, Shoker, Baquero, "Delta State Replicated Data Types" (2016), Section 6, which defines the delta-interval and the causal delta-merging condition the replicator enforces, and which requires the sequence number to be durable for exactly the recovery reason above. The set being replicated is the optimized OR-Set from Bieniusa, Zawirski, Preguiça, Shapiro, Baquero, Balegas, Duarte, "An Optimized Conflict-free Replicated Set" (2012). The code lives in [`antientropy/causal`](../../antientropy/causal) for the replicator, [`store/sqlite`](../../store/sqlite) for the durable store, and [`crdt/orset`](../../crdt/orset) for the set, with [`gossip/udp`](../../gossip/udp) and [`membership/swim`](../../membership/swim) for transport and membership. The sibling examples are [`swimnode`](../swimnode), [`pncounternode`](../pncounternode), [`lwwnode`](../lwwnode), and [`orsetnode`](../orsetnode).
