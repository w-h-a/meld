# swimnode: SWIM membership on its own

This is a small example from the [meld](../../) library. Three nodes run together, but they do nothing except keep track of one another. There is no shared data and no CRDT here. Each node joins the group and then prints a line every time a peer joins, leaves, or dies. If you want to watch meld's gossip layer by itself, this is the place to start. It is also a working template you can copy when you want membership in your own program.

## The idea

Every node keeps a list of the other nodes and whether each one is alive. SWIM (Das, Gupta, and Motivala, 2002) keeps that list fresh using two mechanisms that work together. One notices when a peer has gone, and the other spreads that news to the rest of the group.

The first mechanism is failure detection. Each round, a node picks one random peer and pings it. If that peer does not answer in time, the node asks a few other peers to ping it too, in case the direct path was just congested. If nobody can reach the peer, the node starts to suspect it is gone. A node probes only one peer per round, so its detection work stays the same size whether the group has five members or five hundred.

The second mechanism is spreading the news. When a node discovers a change, such as a peer that joined, left, or failed, that one node is the one that starts telling the others. The news then passes from node to node, the way a rumor moves through a crowd, until every node has heard it. Much of it rides along on the ping messages that are already in flight, so the news spreads without a separate flood of its own. Every node ends up holding the same full picture of who is alive.

A single missed ping does not prove a peer is dead, because the peer might just be slow or briefly cut off. So instead of removing it, the node marks it as Suspect and tells the group. If the peer is actually fine, it hears the rumor and answers back that it is alive. To prove the answer is fresh and not an old echo, the peer raises its own incarnation number and sends the higher number along with the reply. Only if no such answer arrives before a timeout does Suspect turn into Failed. A node that shuts down on purpose sends one last message that says Left.

## Use it in your code

You need three things: a gossip transport, a membership built on top of it, and a loop that reacts to changes. Here is the smallest version. The full version is in [`main.go`](./main.go).

```go
import (
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/swim"
)

// 1. Create a UDP gossip transport.
g, err := udp.New(gossip.WithBindAddress("0.0.0.0:7946"))

// 2. Put SWIM membership on top of that transport.
m, err := swim.New(
	membership.WithGossip(g),
	membership.WithNodeID("n1"),
	membership.WithAdvertiseAddress("n1:7946"),
	swim.WithProbeInterval(time.Second),
	swim.WithProbeTimeout(300*time.Millisecond),
	swim.WithSuspicionMult(4),
)

// 3. Join through one or more seeds. Pass nil or an empty slice if you are the seed.
if err := m.Join(ctx, []string{"n0:7946"}); err != nil {
	// handle
}

// 4. React to membership changes as they happen.
watch, _ := m.Watch()
go func() {
	for ev := range watch {
		// ev.Type is Join, Leave, Fail, or Update.
		// ev.Node.ID and ev.Node.State tell you who changed and how.
	}
}()

// Or read a point-in-time snapshot whenever you want one.
members := m.Members()

// 5. Say goodbye cleanly when you shut down.
m.Leave(ctx)
```

The only constructors you call are `udp.New` and `swim.New`. Everything after that goes through the `membership.Membership` interface, which gives you `Join`, `Leave`, `Members`, `LocalNode`, and `Watch`. Because your code depends on that interface and not on the swim package's internals, you can swap the implementation later without changing anything above.

## Run the demo

You need Docker with Compose. If you want to build the library or import it directly, you also need Go 1.25 or newer.

```
docker compose up --build
```

This starts three nodes and one Jaeger container. n0 is the seed, so it has no peers to join. n1 and n2 join through n0 at `n0:7946`. Give it a few probe periods and every node should report that it has seen the other two come up Alive.

## What you'll see

When a node starts, it prints one line that shows how it joined.

```
n1 joined; bind=0.0.0.0:7946 advertise=n1:7946 seeds=[n0:7946]
```

After that it prints one line for each membership change. The numbers are enum values, so you have to decode them. The type is the kind of change, where 0 is Join, 1 is Leave, 2 is Fail, and 3 is Update. The state is the peer's new status, where 0 is Alive, 1 is Suspect, 2 is Failed, and 3 is Left.

```
event: type=0 node=n2 state=0
```

That line means n2 joined and is Alive. A line that read `type=2 node=n1 state=2` would mean n1 was declared Failed.

## Look in Jaeger

Open http://localhost:16686. Each node shows up as its own service, named n0, n1, and n2.

Every time a node accepts a membership change, it records a span called `swim.state_transition`. That span carries the old state, the new state, the incarnation number, and the id of the node that reported the change. When you want to know why a node changed its mind about a peer, this is the span to read. For example, you can filter n0's spans to find the exact moment it marked n1 as Suspect, and then see the incarnation number that later cleared it.

## Try this

Try a clean shutdown first. Run `docker compose stop n1`. That sends SIGTERM, so n1 calls `Leave` on its way out, and the other nodes log a Leave event with state Left almost at once.

Now try a hard failure. Run `docker compose kill n1`. That sends SIGKILL, so n1 never gets to say goodbye. The other nodes cannot reach it, so they first mark it Suspect and then, after the suspicion timeout, mark it Failed.

You can also catch a refutation. When the network drops packets, a healthy node is sometimes suspected by mistake. If you watch the logs and see a node go to Suspect and then return to Alive at a higher incarnation a moment later, you have caught that node correcting the rumor. This is the defense SWIM adds so that a false alarm does not remove a good node.

Finally, try a rejoin. Run `docker compose restart n2`. This stops n2, which makes it Leave, and then starts a fresh n2 with the same id. The other nodes take it back and mark it Alive again. This is the path the `membership/swim` rejoin fix restored, because before that fix a node that had Left could never return under the same id.

## Environment variables

| Var | Required | Default | Meaning |
|---|---|---|---|
| `NODE_ID` | yes | — | this node's id, also its Jaeger service name |
| `BIND_ADDR` | yes | — | UDP address the gossip transport listens on |
| `PEERS` | no | — | comma-separated seed addresses to join through, empty means this node is the seed |
| `ADVERTISE_ADDR` | no | `NODE_ID:7946` | address peers should use to reach this node |
| `PROBE_INTERVAL` | no | `1s` | how often to probe a random peer |
| `PROBE_TIMEOUT` | no | `300ms` | how long to wait for a probe ack before probing indirectly |
| `SUSPICION_MULT` | no | `4` | suspicion timeout is this multiple of the probe interval before Suspect becomes Failed |
| `OTLP_ENDPOINT` | no | `jaeger:4317` | OTLP gRPC endpoint for traces |

The compose file slows these down on purpose. It sets `PROBE_INTERVAL` to 5s, `PROBE_TIMEOUT` to 1s, and `SUSPICION_MULT` to 8 so that the state changes are slow enough to watch.

## See also

The protocol comes from Das, Gupta, and Motivala, "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol" (2002). The code lives in three packages. [`membership/swim`](../../membership/swim) holds the protocol, [`gossip/udp`](../../gossip/udp) holds the transport, and [`membership`](../../membership) holds the port interface. The sibling examples are [`pncounternode`](../pncounternode), [`lwwnode`](../lwwnode), [`orsetnode`](../orsetnode), and [`causalnode`](../causalnode).
