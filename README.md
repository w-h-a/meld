# meld

Gossip, Membership, and convergence primitives for distributed systems.

Go library providing gossip transport, membership, and conflict-free replicated data types.

## Packages

| Package       | Implementations                      | Use case                                 |
| ------------- | ------------------------------------ | ---------------------------------------- |
| `gossip/`     | `udp`, `tcp`                         | Peer-to-peer gossip transport            |
| `membership/` | `swim`, `rapid`                      | Cluster membership and failure detection |
| `crdt/`       | `orset`, `lww`, `gcounter`, `vclock` | Conflict-free replicated data types      |

## Gossip + SWIM Data Flow

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

