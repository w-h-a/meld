# lwwnode: last write wins, and the other is lost

A runnable, three-node example from the [meld](../../) library. Each node now and then writes a new value into one shared cell, the nodes gossip their copies, and they all agree on a single winner. When two writes race, the cell keeps one and quietly throws the other away, and this demo prints the value it dropped so you can watch the loss happen. It shows meld's LWW-Register.

## The idea

A register is a single cell holding one value that any node may overwrite. When two writes do not overlap, the later one wins and nobody argues. The hard case is two nodes writing different values at nearly the same moment, with no coordinator to decide who was first.

Last-Writer-Wins handles this by stamping every write with a tag that puts all writes in a single order. On merge, the write with the greater tag wins, and since every node compares the same tags it makes the same choice, so the copies converge. The price is that the losing write is dropped with nothing left behind. This is fine when you only care about the newest value, such as a status or a current setting, and wrong when every write matters.

Everything turns on what the tag is. The paper (Shapiro, Preguiça, Baquero, Zawirski, 2011) stamps each write with a wall-clock time. meld does not, because clocks on different machines cannot be trusted. A machine with a fast clock would win every race, and a clock that steps backward would make good writes lose. Instead meld stamps each write with a Lamport counter, which is just a number that each write sets to one more than the highest it has seen, together with the id of the node that wrote it. Ordering by that counter means ordering by what each write had actually observed, which is the causal order the paper says the tag must respect (Lamport, 1978). When two writes carry the same counter, neither one saw the other, so they are truly concurrent, and the writer id breaks the tie the same way on every node. That tie is where a value gets lost.

## Use it in your code

The membership and transport wiring is the same as the other CRDT examples. Membership runs on one UDP port, register state runs on a second port, and the node advertises that second port through membership metadata so peers know where to send it. See [pncounternode](../pncounternode) for that setup in full. The register-specific part is small. The full version is in [`main.go`](./main.go).

```go
import (
	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/basic"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/lwwregister"
)

// The register holds a string. The replicator runs Algorithm 1 anti-entropy
// over it, shipping state to peers and merging what comes back.
r, _ := basic.New(
	antientropy.WithInitial(lwwregister.New[string]()),
	antientropy.WithCodec(encode, decode), // r.Marshal(crdt.StringEncode) / r.Unmarshal(_, crdt.StringDecode)
	antientropy.WithTransport[lwwregister.LWWRegister[string]](cg),
	antientropy.WithMembership[lwwregister.LWWRegister[string]](m),
	antientropy.WithPeerAddress[lwwregister.LWWRegister[string]](peerAddr),
	antientropy.WithInterval[lwwregister.LWWRegister[string]](time.Second),
)

r.Start(ctx)
m.Join(ctx, seeds)

// Write a new value. Set bumps the Lamport counter and stamps this node as the
// writer, so the write carries its own tag.
r.Submit(r.State().Set("n1", "amber"))

// Read the current winner and its tag.
v := r.State().Value()  // "amber"
tag := r.State().Tag()  // tag.Counter is the Lamport number, tag.Writer is the node id
```

You never compare tags yourself. You submit writes and read `State()`, and the replicator ships and merges. Because merge just keeps the greater tag, a lost, duplicated, or late message cannot change which write eventually wins.

## Run the demo

You need Docker with Compose. If you want to build the library or import it directly, you also need Go 1.25 or newer.

```
docker compose up --build
```

This starts three nodes and one Jaeger container. n0 is the seed, and n1 and n2 join through it. Every node writes a value from a small palette of "active config" colors, `blue`, `green`, `amber`, `violet`, and `crimson`. The palette is deliberately tiny so that two nodes often write near the same time and the concurrent case actually shows up.

## What you'll see

Most rounds are quiet, because a later write simply replaces an earlier one and there is nothing to report. The interesting output appears when two nodes write at the same Lamport counter. Then the tie is broken by writer id, one value is kept, the other is dropped, and the node that noticed prints the loss.

```
n0 lost concurrent write: kept="amber"(n2) dropped="blue"(n0) clock=3
```

That line says two writes both reached counter 3, the value `amber` from n2 was kept because its writer id wins the tie, and n0's own `blue` was thrown away. This is Last-Writer-Wins doing exactly what it promises, and the print is there so the loss is not invisible.

## Look in Jaeger

Open http://localhost:16686. Each node is its own service, named n0, n1, and n2.

When a node ships its state it records a span called `antientropy.gossip`, and when it merges an incoming copy it records one called `antientropy.receive`. Both carry `crdt.value`, `crdt.clock` (the Lamport counter), and `crdt.writer`. The receive span also carries `crdt.write_winner`, which is `remote` when the incoming write won, `local` when the held value won, or `tiebreak` when two writes tied on the counter. Every tiebreak also attaches an event called `lwwregister.lost_concurrent_write` holding the kept and dropped values. So Jaeger shows more than the logs do. Filter a node's `antientropy.receive` spans to `crdt.write_winner=tiebreak` and every dropped write is right there, even the ones that scrolled past in the console.

## Try this

Find the losses. In Jaeger, filter one node's `antientropy.receive` spans to `crdt.write_winner=tiebreak`. Each hit is a concurrent write where one value was silently dropped, and the attached event names the value that was kept and the value that was lost. This is the whole point of the register, made visible.

Follow a write as it spreads. Pick a write on one node from its `antientropy.gossip` span, noting its `crdt.writer` and `crdt.clock`, then look at the other nodes' `antientropy.receive` spans. You will see that same value and tag arrive a round or two later and win everywhere, which is the register converging.

## Environment variables

| Var | Required | Default | Meaning |
|---|---|---|---|
| `NODE_ID` | yes | — | this node's id, also its Jaeger service name and the writer id on its writes |
| `BIND_ADDR` | yes | — | UDP address for membership and failure detection |
| `CRDT_BIND_ADDR` | yes | — | UDP address for register state |
| `PEERS` | no | — | comma-separated seed addresses to join through, empty means this node is the seed |
| `ADVERTISE_ADDR` | no | `NODE_ID:7946` | membership address peers should use to reach this node |
| `CRDT_ADVERTISE_ADDR` | no | `NODE_ID:7947` | register-state address advertised through membership Meta |
| `PROBE_INTERVAL` | no | `1s` | how often to probe a random peer |
| `PROBE_TIMEOUT` | no | `300ms` | how long to wait for a probe ack before probing indirectly |
| `SUSPICION_MULT` | no | `4` | suspicion timeout is this multiple of the probe interval |
| `WRITE_INTERVAL` | no | `10s` | how often this node writes a new value |
| `GOSSIP_INTERVAL` | no | `1s` | how often the replicator ships state to a peer |
| `OTLP_ENDPOINT` | no | `jaeger:4317` | OTLP gRPC endpoint for traces |

The compose file sets `WRITE_INTERVAL` to 20s and slows the probe timing to `PROBE_INTERVAL=5s`, `PROBE_TIMEOUT=1s`, `SUSPICION_MULT=8`.

## See also

The register comes from Shapiro, Preguiça, Baquero, Zawirski, "A comprehensive study of Convergent and Commutative Replicated Data Types" (2011), Section 3.2.1, Specification 8. That version stamps writes with a wall clock; meld stamps them with a Lamport counter and a writer id instead, from Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System" (1978), so the order never depends on trusting machine clocks. The anti-entropy loop that ships the state is Algorithm 1 from Almeida, Shoker, Baquero, "Delta State Replicated Data Types" (2016). The code lives in [`crdt/lwwregister`](../../crdt/lwwregister) for the register, [`antientropy/basic`](../../antientropy/basic) for the replicator, [`antientropy`](../../antientropy) for the port interface, and [`gossip/udp`](../../gossip/udp) with [`membership/swim`](../../membership/swim) for transport and membership. The sibling examples are [`swimnode`](../swimnode), [`pncounternode`](../pncounternode), [`orsetnode`](../orsetnode), and [`causalnode`](../causalnode).
