# phinode: membership by phi accrual, a counter replicated causally

A runnable, three-node example from the [meld](../../) library. It detects failures with the phi accrual failure detector instead of SWIM, and it carries a PN-Counter replicated by the causal anti-entropy loop as its payload. It shows how phi decides a peer is gone from the rhythm of its heartbeats, how a failed peer comes back with no refute message, and why a phi cluster is seeded as a full mesh.

## The idea

SWIM finds failures by probing: one node pings another and waits for an ack. phi asks a different question. Every node broadcasts a heartbeat on a fixed interval, and every receiver just watches the arrivals. From the recent gaps between a peer's heartbeats it keeps a small statistical picture, a mean and a spread, and each time it checks a peer it turns "how long since the last heartbeat" into phi, a suspicion score. phi stays low while heartbeats arrive on time and climbs the longer one is overdue, scaled by how regular that peer has been. This is the phi accrual detector of Hayashibara, Defago, Yared, Katayama (2004).

Two thresholds turn phi into a decision, with hysteresis so a peer does not flap. A trusted peer becomes Suspect once phi reaches the high threshold. If it stays there past a dwell, it becomes Failed. A suspected peer returns to Alive once phi falls back to the low threshold. There is no probe, no ack, and no refute. A peer that went quiet and comes back recovers simply by sending again, and the receiver's rising-then-falling phi does the rest. That passive recovery is the sharpest contrast with SWIM, where a suspected node has to refute with a higher incarnation.

Two consequences shape how you run it. First, phi does not disseminate membership. A node learns a peer only from that peer's own heartbeats, never second-hand, so every node must be seeded with every other, a full mesh, not a single bootstrap. Second, a node keeps heartbeating a peer it has written off so the peer can be re-contacted if it returns, and it drops (reaps) the peer only after a longer dwell so the dead are not heartbeated forever.

The payload is a PN-Counter, a counter every node can increment and decrement, replicated with meld's causal anti-entropy (Algorithm 2) and a durable store, exactly as in [causalnode](../causalnode). The counter is here to give the membership something to carry. The star of this demo is phi.

## Use it in your code

The payload wiring is the same as the other CRDT examples. The one swap is the membership: phi.New in place of swim.New, over a TCP transport. The full version is in [`main.go`](./main.go).

```go
import (
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/tcp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/phi"
)

// phi runs over any gossip transport. Here it is TCP.
mg, _ := tcp.New(gossip.WithBindAddress("0.0.0.0:7946"))

m, _ := phi.New(
	membership.WithGossip(mg),
	membership.WithNodeID("n1"),
	membership.WithAdvertiseAddress("n1:7946"),
	membership.WithMeta(map[string]string{"crdt_addr": "n1:7947"}),
	phi.WithHeartbeatInterval(time.Second),
	phi.WithPhiHighThreshold(8.0),       // Suspect at or above this
	phi.WithPhiLowThreshold(1.0),        // recover to Alive at or below this
	phi.WithSuspectDwell(2*time.Second), // Suspect must hold this long before Failed
	phi.WithReapDwell(30*time.Second),   // drop a Failed peer after this
)

m.Join(ctx, []string{"n0:7946", "n2:7946"}) // full mesh: seed every other node

// Membership is the same port every meld consumer uses.
for _, n := range m.Members() {
	_ = n.State // Alive, Suspect, Failed, or Left
}
```

phi satisfies the same `membership.Membership` port SWIM does, so the causal replicator, the transport, and the rest of the demo are unchanged from causalnode. You only picked a different failure detector.

## Run the demo

You need Docker with Compose. To build the library or import it directly you also need Go 1.25 or newer.

```
docker compose up --build
```

This starts three nodes and one Jaeger container. The seed order is a full mesh: n0 is the bootstrap, n1 joins through n0, and n2 joins through both n0 and n1. That last seed is the phi-specific part. Without it, and with no dissemination to fill the gap, n1 and n2 would never learn each other. Each node bumps a shared PN-Counter up or down at random and ships it causally.

## What you'll see

On each gossip round a node prints its reading of the counter.

```
n0 value=4 p=11 n=7
```

`value` is increments minus decrements, and `p` and `n` are the two running totals. All three nodes converge to the same reading. In the logs you will also see membership events as nodes join, fail, and return.

## Look in Jaeger

Open http://localhost:16686. Each node is its own service, named n0, n1, and n2.

phi records four span kinds. `phi.heartbeat.send` carries `phi.heartbeat_seq` and `phi.peers_count` each interval. `phi.receive` records an arriving heartbeat with `phi.sender` and `phi.sender_seq`. `phi.check.round` records one scoring pass with `phi.peers_checked` and `phi.suspects_count`. `phi.state_transition` is the one to watch: it names the `phi.peer` whose state changed, the `phi.from_state` and `phi.to_state`, the `phi.phi_value` that triggered it, and the `phi.threshold` it crossed. Because every heartbeat carries the sender's trace context, a `phi.heartbeat.send` on one node links to a `phi.receive` on another, so you will find multi-service traces spanning n0 and n1. The causal replicator's own spans (`antientropy.gossip`, `antientropy.receive_delta`, and so on) ride alongside, carrying the demo's `crdt.value`, `crdt.p_sum`, and `crdt.n_sum`.

## Try this

Watch a failure and a passive recovery. Note the members in the logs, then run `docker compose kill n2`. n2's heartbeats stop, so on n0 and n1 the phi score for n2 climbs. Within a couple of seconds you get a `phi.state_transition` to Suspect, and after the dwell another to Failed. Filter `phi.state_transition` spans with `phi.peer=n2` in Jaeger and read `phi.phi_value` rising past `phi.threshold`.

Now run `docker compose start n2`. n2 comes back and simply resumes heartbeating. There is no refute message and nothing n2 does to argue its case. Its heartbeats reach n0 and n1, their phi for n2 drops, and you get a `phi.state_transition` back to Alive. That is the contrast with SWIM's recovery made visible: in phi the returning node stays silent about its own liveness and the detector corrects itself.

## Environment variables

| Var | Required | Default | Meaning |
|---|---|---|---|
| `NODE_ID` | yes | — | this node's id and its Jaeger service name |
| `BIND_ADDR` | yes | — | TCP address for membership and failure detection |
| `CRDT_BIND_ADDR` | yes | — | TCP address for counter state |
| `PEERS` | no | — | comma-separated seed addresses; empty means this node is the bootstrap. Seed a full mesh, since phi does not disseminate |
| `ADVERTISE_ADDR` | no | `NODE_ID:7946` | membership address peers use to reach this node |
| `CRDT_ADVERTISE_ADDR` | no | `NODE_ID:7947` | counter-state address advertised through membership Meta |
| `STORE_PATH` | no | `NODE_ID.db` | SQLite file holding the durable (sequence number, state) pair |
| `HEARTBEAT_INTERVAL` | no | `1s` | how often this node broadcasts a heartbeat |
| `PHI_HIGH_THRESHOLD` | no | `8.0` | phi at or above which a peer becomes Suspect |
| `PHI_LOW_THRESHOLD` | no | `1.0` | phi at or below which a Suspect peer returns to Alive |
| `WINDOW_SIZE` | no | `1000` | how many recent heartbeat gaps to keep per peer |
| `MIN_STDDEV` | no | `100ms` | floor on the gap spread so a very steady peer does not overreact |
| `SUSPECT_DWELL` | no | `2s` | how long a peer stays Suspect before it is marked Failed |
| `REAP_DWELL` | no | `30s` | how long a peer stays Failed before it is dropped |
| `EVENT_INTERVAL` | no | `200ms` | how often this node increments or decrements |
| `DECREMENT_PROBABILITY` | no | `0.5` | chance that a tick is a decrement rather than an increment |
| `GOSSIP_INTERVAL` | no | `1s` | how often the replicator ships to a neighbor |
| `GC_INTERVAL` | no | `10s` | how often to drop deltas every neighbor has acknowledged |
| `OTLP_ENDPOINT` | no | `jaeger:4317` | OTLP gRPC endpoint for traces |

The compose file gives each node its own volume for the SQLite file, seeds a full mesh (n0 bootstrap, n1 through n0, n2 through n0 and n1), sets `EVENT_INTERVAL` to 5s, and runs the detector at `HEARTBEAT_INTERVAL=1s` with `SUSPECT_DWELL=2s`.

## See also

The phi accrual failure detector is from Hayashibara, Defago, Yared, Katayama, "The phi Accrual Failure Detector" (2004), which defines phi as the suspicion level built from heartbeat inter-arrival times. The causal anti-entropy carrying the counter is Algorithm 2 from Almeida, Shoker, Baquero, "Delta State Replicated Data Types" (2016). The code lives in [`membership/phi`](../../membership/phi) for the detector, [`antientropy/causal`](../../antientropy/causal) for the replicator, [`crdt/pncounter`](../../crdt/pncounter) for the counter, and [`gossip/tcp`](../../gossip/tcp) for transport. The sibling examples are [`swimnode`](../swimnode), [`causalnode`](../causalnode), [`pncounternode`](../pncounternode), [`lwwnode`](../lwwnode), and [`orsetnode`](../orsetnode).
