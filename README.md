# meld

Gossip, Membership, and convergence primitives for distributed systems.

Go library providing gossip transport, membership, and conflict-free replicated data types.

## Packages

| Package       | Implementations                      | Use case                                 |
| ------------- | ------------------------------------ | ---------------------------------------- |
| `gossip/`     | `udp`, `tcp`                         | Peer-to-peer gossip transport            |
| `membership/` | `swim`, `phi`                        | Cluster membership and failure detection |
| `crdt/`       | `orset`, `lww`, `gcounter`, `vclock` | Conflict-free replicated data types      |

## Gossip + SWIM Data Flow

SWIM fuses failure detection and state dissemination into one protocol.
Probes double as gossip carriers. Binary alive/suspect/dead decisions
with manual timeout tuning.

```mermaid
sequenceDiagram
    participant N1 as Node 1
    participant N2 as Node 2
    participant N3 as Node 3

    Note over N1,N3: SWIM Failure Detection
    N1->>N2: ping
    N2-->>N1: ack
    N1->>N3: ping
    Note over N1,N3: timeout
    N1->>N2: ping-req(N3)
    N2->>N3: ping
    Note over N2,N3: timeout
    N2-->>N1: nack
    N1->>N1: suspect(N3)

    Note over N1,N3: CRDT State Gossip
    N1->>N2: gossip(CRDT state)
    N2->>N2: merge(local, received)
    N2->>N3: gossip(merged state)
    N3->>N3: merge(local, received)
    Note over N1,N3: All nodes converge
```

## Gossip + Phi Accrual Data Flow

Phi accrual separates failure detection from dissemination. Heartbeat
inter-arrival times feed a statistical model that outputs a continuous
suspicion level. The application chooses its own threshold. Self-tuning.

```mermaid
sequenceDiagram
    participant N1 as Node 1
    participant N2 as Node 2
    participant N3 as Node 3

    Note over N1,N3: Periodic Heartbeat Gossip
    N1->>N2: heartbeat + state digest
    N2-->>N1: state delta
    N2->>N3: heartbeat + state digest
    N3-->>N2: state delta

    Note over N1,N3: Phi Accrual Failure Detection (per node, local)
    N1->>N1: record inter-arrival(N3)
    N1->>N1: phi(N3) = -log10(P_later(t_now - t_last))
    Note over N1: phi(N3) = 1.2 — normal
    Note over N1: phi(N3) = 8.7 — likely failed
    Note over N1: phi(N3) > threshold — declare failed

    Note over N1,N3: CRDT State Gossip (same channel)
    N1->>N2: gossip(CRDT state)
    N2->>N2: merge(local, received)
    N2->>N3: gossip(merged state)
    N3->>N3: merge(local, received)
    Note over N1,N3: All nodes converge
```

