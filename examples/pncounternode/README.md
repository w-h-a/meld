# pncounternode: a counter that goes up and down

A runnable, three-node example from the [meld](../../) library. Each node repeatedly nudges one shared counter up or down, the nodes gossip their copies to each other, and all three settle on the same total even though no node is in charge. It shows meld's PN-Counter together with the basic anti-entropy replicator that keeps the copies in sync.

## The idea

We want a single number that many nodes can raise and lower at the same time, with no coordinator, and we want every node to agree on the total in the end. A plain shared integer does not work. If two nodes both start at 0 and each add 1, and we later combine their copies by keeping the larger, we get 1 instead of 2. If we combine by adding, then re-sending the same copy counts it twice. Either way the number is wrong.

The fix is to stop storing one integer and store a small table instead, with one row per node. This is a G-Counter (Shapiro, Preguiça, Baquero, Zawirski, 2011). To add one, a node only ever increases its own row. The value of the counter is the sum of all the rows. To combine two copies of the table, you take the larger of each row. Because a node is the only writer of its own row, taking the larger never loses an add, and taking the larger can be done again and again, in any order, without changing the answer. That last part is what makes it safe over a network that drops, duplicates, and reorders messages. meld keeps this table as a growing list rather than a fixed array, so a node gets a row only once it has actually touched the counter. That means nodes can join the cluster whenever they like, and a node you have not yet heard from simply counts as zero, with no slot reserved in advance.

A G-Counter can only go up, because "take the larger" would quietly ignore any subtraction. To also go down, a PN-Counter keeps two of these tables. One table, `P`, records the ups, and the other, `N`, records the downs. To add one, a node increases its own row in `P`. To subtract one, it increases its own row in `N`. The value of the counter is the sum of `P` minus the sum of `N`. To combine two copies, you take the larger of each row in both tables. Because both tables are just G-Counters, the whole thing inherits the same safety, so the count can diverge for a moment while messages are in flight but always comes back to the exact total.

## Use it in your code

A node runs two things. It runs SWIM membership on one UDP port, and it runs a replicator over the counter on a second UDP port. The node advertises its second port through membership metadata, so peers learn where to send counter state without a separate address list. Here is the shape of it. The full version is in [`main.go`](./main.go).

```go
import (
	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/basic"
	"github.com/w-h-a/meld/crdt/pncounter"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/swim"
)

// Membership runs on one UDP port. The node advertises its separate CRDT-state
// port through Meta, and peers read it back to know where to send state.
mg, _ := udp.New(gossip.WithBindAddress("0.0.0.0:7946"))
m, _ := swim.New(
	membership.WithGossip(mg),
	membership.WithNodeID("n1"),
	membership.WithAdvertiseAddress("n1:7946"),
	membership.WithMeta(map[string]string{"crdt_addr": "n1:7947"}),
	// probe options omitted
)

// A second UDP port carries only marshaled PN-Counter state.
cg, _ := udp.New(gossip.WithBindAddress("0.0.0.0:7947"))

// The replicator runs Algorithm 1 anti-entropy over the counter. It owns the
// loop that ships state to peers and merges what comes back.
r, _ := basic.New(
	antientropy.WithInitial(pncounter.New()),
	antientropy.WithCodec(encode, decode),                      // c.Marshal / c.Unmarshal
	antientropy.WithTransport[pncounter.PNCounter](cg),
	antientropy.WithMembership[pncounter.PNCounter](m),
	antientropy.WithPeerAddress[pncounter.PNCounter](peerAddr), // read crdt_addr from a member's Meta
	antientropy.WithInterval[pncounter.PNCounter](2*time.Second),
)

r.Start(ctx)       // opens the CRDT transport and starts the gossip loop
m.Join(ctx, seeds) // joins the membership cluster

// Change the counter by submitting a delta tagged with this node's id, so the
// change lands in this node's own row.
r.Submit(r.State().IncrementDelta("n1"))
r.Submit(r.State().DecrementDelta("n1"))

// Read the value whenever you want. It is sum(P) - sum(N).
v := r.State().Value()
```

You never merge anything by hand. You submit deltas and read `State()`, and the replicator does the shipping and merging for you. Because merging is element-wise max, a lost, duplicated, or late message cannot corrupt the total.

## Run the demo

You need Docker with Compose. If you want to build the library or import it directly, you also need Go 1.25 or newer.

```
docker compose up --build
```

This starts three nodes and one Jaeger container. n0 is the seed, and n1 and n2 join through it. The three nodes are given different decrement probabilities on purpose, `0.3` for n0, `0.6` for n1, and `0.8` for n2, so each one pushes the counter up and down by a different mix. The point of the demo is that they still converge on the same number.

## What you'll see

On each gossip round a node prints its current reading.

```
n0 value=5 p=8 n=3
```

Here `p` is the total of every increment made anywhere in the cluster, `n` is the total of every decrement, and `value` is `p` minus `n`. Because the three nodes make different mixes of ups and downs, their readings will disagree for a moment right after a change. Watch the `value` lines from n0, n1, and n2, and you will see them drift back together after each round of gossip, because every copy is merging toward the same `P` and `N` tables.

## Look in Jaeger

Open http://localhost:16686. Each node is its own service, named n0, n1, and n2.

When a node ships its state it records a span called `antientropy.gossip`, and when it merges an incoming copy it records one called `antientropy.receive`. Both spans carry `crdt.value`, `crdt.p_sum`, and `crdt.n_sum`, which are the reading at that moment. The gossip span also carries `antientropy.algorithm`, `antientropy.delta_or_full`, `antientropy.peer_count`, and `antientropy.bytes_shipped`. To watch convergence directly, filter n2's `antientropy.receive` spans and follow `crdt.value` as it climbs toward the shared total with each merge.

## Try this

Watch the readings converge. Line up the `value` output from all three nodes. They start apart, because each node changes its own copy first, and they meet again after the next round of gossip.

Restart a node. This demo keeps nothing on disk, so a restarted node comes back with its own rows reset to zero. The total still holds, because the other nodes remember the restarted node's rows from earlier merges and hand them back the first time it gossips again. Run `docker compose restart n2` and watch the cluster's `value` stay put.

## Environment variables

| Var | Required | Default | Meaning |
|---|---|---|---|
| `NODE_ID` | yes | — | this node's id, also its Jaeger service name, and the row it writes to |
| `BIND_ADDR` | yes | — | UDP address for membership and failure detection |
| `CRDT_BIND_ADDR` | yes | — | UDP address for counter state |
| `PEERS` | no | — | comma-separated seed addresses to join through, empty means this node is the seed |
| `ADVERTISE_ADDR` | no | `NODE_ID:7946` | membership address peers should use to reach this node |
| `CRDT_ADVERTISE_ADDR` | no | `NODE_ID:7947` | counter-state address advertised through membership Meta |
| `PROBE_INTERVAL` | no | `1s` | how often to probe a random peer |
| `PROBE_TIMEOUT` | no | `300ms` | how long to wait for a probe ack before probing indirectly |
| `SUSPICION_MULT` | no | `4` | suspicion timeout is this multiple of the probe interval |
| `EVENT_INTERVAL` | no | `200ms` | how often this node raises or lowers the counter |
| `DECREMENT_PROBABILITY` | no | `0.5` | chance that a change is a decrement rather than an increment |
| `GOSSIP_INTERVAL` | no | `2s` | how often the replicator ships state to a peer |
| `OTLP_ENDPOINT` | no | `jaeger:4317` | OTLP gRPC endpoint for traces |

The compose file sets `EVENT_INTERVAL` to 5s so the changes are easy to follow, gives each node a different `DECREMENT_PROBABILITY`, and slows the probe timing to `PROBE_INTERVAL=5s`, `PROBE_TIMEOUT=1s`, `SUSPICION_MULT=8`.

## See also

The counter comes from Shapiro, Preguiça, Baquero, Zawirski, "A comprehensive study of Convergent and Commutative Replicated Data Types" (2011), Section 3.1, where Specification 6 is the G-Counter and Specification 7 is the PN-Counter. The anti-entropy loop that ships the state is Algorithm 1 from Almeida, Shoker, Baquero, "Delta State Replicated Data Types" (2016). The code lives in [`crdt/pncounter`](../../crdt/pncounter) for the counter, [`antientropy/basic`](../../antientropy/basic) for the replicator, [`antientropy`](../../antientropy) for the port interface, and [`gossip/udp`](../../gossip/udp) with [`membership/swim`](../../membership/swim) for transport and membership. The sibling examples are [`swimnode`](../swimnode), [`lwwnode`](../lwwnode), [`orsetnode`](../orsetnode), and [`causalnode`](../causalnode).
