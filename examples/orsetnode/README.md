# orsetnode: a set where adding wins

A runnable, three-node example from the [meld](../../) library. Each node keeps adding and removing items from one shared set, the nodes gossip their copies, and they all agree on the same members. The interesting part is what happens when one node removes an item at the same moment another node adds it. The add wins and the item stays, and this demo prints those revivals so you can watch add-wins happen. It shows meld's optimized OR-Set.

## The idea

We want a set that many nodes can add to and remove from at once, with no coordinator, that still ends up the same everywhere. The easy cases are boring. The hard case is one node adding an item while another removes the same item, with neither having seen the other. There is no single correct answer here, so a design has to pick one and apply it the same way everywhere. The OR-Set picks add-wins: when an add and a remove of the same item are concurrent, the add wins and the item stays in.

The trick that makes this work is to tag every add. When a node adds an item it mints a fresh tag, which is just its node id paired with a counter that only ever goes up. The item is in the set as long as it has at least one live tag. A remove does not delete the item by name. It deletes only the specific tags it has actually seen for that item. So if another node adds the same item at the same time, that add carries a brand-new tag the remover never saw, and the remove cannot touch it. The item survives. This is add-wins, and it is also why you can remove an item and add it again later, because the new add simply brings a new tag.

That leaves one question for merging. When one copy has a tag and the other does not, did the other never see it, or did it see it and remove it? The original OR-Set answers this by keeping every removed tag forever in a tombstone pile, which grows without bound. meld uses the optimized OR-Set (Bieniusa, Zawirski, Preguiça, Shapiro, Baquero, Balegas, Duarte, 2012) instead. It throws the tombstones away and keeps a small summary of which tags each node has already seen. On merge, for a tag one side is missing, it asks that summary whether the side has seen the tag. If it has, the tag was removed, so drop it. If it has not, this is a concurrent add, so keep it. Same add-wins result, without the ever-growing pile. meld also folds repeated adds of the same item by the same node down to the newest tag, so the bookkeeping stays small.

## Use it in your code

The membership and transport wiring is the same as the other CRDT examples, with membership on one UDP port and set state on a second port advertised through membership metadata. See [pncounternode](../pncounternode) for that setup in full. The set-specific part is small. The full version is in [`main.go`](./main.go).

```go
import (
	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/basic"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/orset"
)

// The set holds strings. The replicator runs Algorithm 1 anti-entropy over it,
// shipping state to peers and merging what comes back.
r, _ := basic.New(
	antientropy.WithInitial(orset.New[string]()),
	antientropy.WithCodec(encode, decode), // s.Marshal(crdt.StringEncode) / s.Unmarshal(_, crdt.StringDecode)
	antientropy.WithTransport[orset.ORSet[string]](cg),
	antientropy.WithMembership[orset.ORSet[string]](m),
	antientropy.WithPeerAddress[orset.ORSet[string]](peerAddr),
	antientropy.WithInterval[orset.ORSet[string]](time.Second),
)

r.Start(ctx)
m.Join(ctx, seeds)

// Add mints a fresh tag under this node's id. Remove deletes only the tags this
// copy has already seen for the item.
r.Submit(r.State().AddDelta("n1", "nginx"))
r.Submit(r.State().RemoveDelta("nginx"))

// Read the members.
in := r.State().Contains("nginx")
all := r.State().Elements()
```

You never track tags yourself. You submit adds and removes and read `State()`, and the replicator ships and merges. Because merge decides each tag from the seen-summary, a lost, duplicated, or late message cannot flip an item's membership the wrong way.

## Run the demo

You need Docker with Compose. If you want to build the library or import it directly, you also need Go 1.25 or newer.

```
docker compose up --build
```

This starts three nodes and one Jaeger container. n0 is the seed, and n1 and n2 join through it. Each node treats the set as a pool of scheduled workloads named `workload-0` through `workload-7`. On every tick it picks one at random and either adds it or, with a smaller chance, removes it. Because three nodes do this at once over the same eight names, adds and removes of the same item collide often, which is exactly the case the OR-Set is built for.

## What you'll see

On each merge that changed something, a node prints a summary.

```
n0 merged: added=[workload-3] removed=[] add_wins=[workload-5] elements=6 triples=7
```

Here `added` and `removed` are the items this node gained and lost on the merge, `elements` is how many distinct items are in the set, and `triples` is how many live tags back them, which can be larger than `elements` because two nodes can each hold a tag for the same item. The one to watch is `add_wins`. The demo remembers what this node removed, and `add_wins` lists the items it removed that came back anyway, which only happens because a peer added them concurrently and that add outlived the remove. That list is add-wins caught in the act.

## Look in Jaeger

Open http://localhost:16686. Each node is its own service, named n0, n1, and n2.

When a node ships its state it records a span called `antientropy.gossip`, and when it merges an incoming copy it records one called `antientropy.receive`. The receive span carries `crdt.element_count`, `crdt.live_triple_count`, `crdt.changed`, and, when the merge moved items, `crdt.elements_added` and `crdt.elements_removed`. When a removed item comes back it also carries `crdt.add_wins` set true and `crdt.add_wins_elements`. The gossip span carries `crdt.last_added` and `crdt.last_removed`. Filter a node's `antientropy.receive` spans to `crdt.add_wins=true` and every revival is right there.

## Try this

Catch a revival. Watch the logs for an `add_wins=[...]` list that is not empty, or in Jaeger filter `antientropy.receive` spans to `crdt.add_wins=true`. Each one is an item a node had removed that a peer's concurrent add brought back. Lower `REMOVE_PROBABILITY` toward the adds and these get rarer, raise it and they get more common.

Compare tags to items. Put `crdt.element_count` and `crdt.live_triple_count` side by side on the receive spans. Triples are at least as many as elements, because several nodes can each hold a live tag for the same item, and the gap is the redundancy the OR-Set carries to make add-wins work. It stays bounded because meld folds each node's repeated adds of an item down to one tag.

## Environment variables

| Var | Required | Default | Meaning |
|---|---|---|---|
| `NODE_ID` | yes | — | this node's id, also its Jaeger service name and the id its add tags carry |
| `BIND_ADDR` | yes | — | UDP address for membership and failure detection |
| `CRDT_BIND_ADDR` | yes | — | UDP address for set state |
| `PEERS` | no | — | comma-separated seed addresses to join through, empty means this node is the seed |
| `ADVERTISE_ADDR` | no | `NODE_ID:7946` | membership address peers should use to reach this node |
| `CRDT_ADVERTISE_ADDR` | no | `NODE_ID:7947` | set-state address advertised through membership Meta |
| `PROBE_INTERVAL` | no | `1s` | how often to probe a random peer |
| `PROBE_TIMEOUT` | no | `300ms` | how long to wait for a probe ack before probing indirectly |
| `SUSPICION_MULT` | no | `4` | suspicion timeout is this multiple of the probe interval |
| `EVENTS_INTERVAL` | no | `1s` | how often this node adds or removes an item |
| `REMOVE_PROBABILITY` | no | `0.3` | chance that a tick is a remove rather than an add |
| `WORKLOAD_POOL_SIZE` | no | `8` | how many distinct item names the demo cycles through |
| `GOSSIP_INTERVAL` | no | `1s` | how often the replicator ships state to a peer |
| `OTLP_ENDPOINT` | no | `jaeger:4317` | OTLP gRPC endpoint for traces |

The compose file sets `EVENTS_INTERVAL` to 5s so the changes are easy to follow, and slows the probe timing to `PROBE_INTERVAL=5s`, `PROBE_TIMEOUT=1s`, `SUSPICION_MULT=8`.

## See also

The OR-Set and its add-wins rule come from Shapiro, Preguiça, Baquero, Zawirski, "A comprehensive study of Convergent and Commutative Replicated Data Types" (2011), Section 3.3. meld implements the tombstone-free version from Bieniusa, Zawirski, Preguiça, Shapiro, Baquero, Balegas, Duarte, "An Optimized Conflict-free Replicated Set" (2012), whose Figure 3 is the triples-plus-causal-context design used here. The anti-entropy loop that ships the state is Algorithm 1 from Almeida, Shoker, Baquero, "Delta State Replicated Data Types" (2016). The code lives in [`crdt/orset`](../../crdt/orset) for the set and [`crdt/causalcontext`](../../crdt/causalcontext) for the seen-summary, with [`antientropy/basic`](../../antientropy/basic), [`antientropy`](../../antientropy), [`gossip/udp`](../../gossip/udp), and [`membership/swim`](../../membership/swim) for replication, transport, and membership. The sibling examples are [`swimnode`](../swimnode), [`pncounternode`](../pncounternode), [`lwwnode`](../lwwnode), and [`causalnode`](../causalnode).
