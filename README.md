# meld

Gossip, membership, and convergence primitives for distributed systems.

Go library providing gossip transport, membership, and conflict-free replicated data types.

## Packages

| Package       | Implementations                      | Use case                                     |
| ------------- | ------------------------------------ | -------------------------------------------- |
| `gossip/`     | `udp`, `tcp`                         | Point-to-point and epidemic gossip transport |
| `membership/` | `swim`, `phi`                        | Cluster membership and failure detection     |
| `crdt/`       | `orset`, `lww`, `gcounter`, `vclock` | Conflict-free replicated data types          |

Three independent primitives. Consumers compose them in their own
binaries — no meld package imports another.

## SWIM Failure Detection

SWIM (Das et al., 2002) detects failures via probe-based protocol.
Each node periodically pings a random peer. If no ack arrives, it
requests indirect probes through other members. Binary alive/suspect/dead
decisions with configurable timeouts.

```mermaid
sequenceDiagram
    participant N1 as Node 1
    participant N2 as Node 2
    participant N3 as Node 3

    Note over N1,N3: Direct Probe
    N1->>N2: ping
    N2-->>N1: ack

    Note over N1,N3: Indirect Probe (N3 unresponsive)
    N1->>N3: ping
    Note over N1,N3: timeout
    N1->>N2: ping-req(N3)
    N2->>N3: ping
    Note over N2,N3: timeout
    N2-->>N1: nack
    N1->>N1: suspect(N3)
```

## Phi Accrual Failure Detection

Phi accrual (Hayashibara et al., 2004) outputs a continuous suspicion
level (φ) derived from heartbeat inter-arrival time statistics. The
application chooses its own threshold. Self-tuning — adapts to actual
network conditions without manual timeout configuration.

```mermaid
sequenceDiagram
    participant N1 as Node 1
    participant N2 as Node 2
    participant N3 as Node 3

    Note over N1,N3: Periodic Heartbeats
    N1->>N2: heartbeat
    N2->>N3: heartbeat
    N3->>N1: heartbeat

    Note over N1,N3: Phi Computation (local per node)
    N1->>N1: record inter-arrival(N3)
    N1->>N1: φ(N3) = -log₁₀(P_later(t_now - t_last))
    Note over N1: φ(N3) = 1.2 — normal
    Note over N1: φ(N3) = 8.7 — likely failed
    Note over N1: φ(N3) > threshold — declare failed
```

## Gossip + CRDT State Convergence

Gossip spreads state epidemically. Each node periodically sends its CRDT
state to random peers. Receivers merge and re-gossip. After O(log N)
rounds, all nodes converge with high probability. Independent of
membership — works with any peer discovery mechanism.

```mermaid
sequenceDiagram
    participant N1 as Node 1
    participant N2 as Node 2
    participant N3 as Node 3

    Note over N1,N3: Epidemic Dissemination
    N1->>N2: gossip(CRDT state)
    N2->>N2: merge(local, received)
    N2->>N3: gossip(merged state)
    N3->>N3: merge(local, received)
    Note over N1,N3: All nodes converge
```

