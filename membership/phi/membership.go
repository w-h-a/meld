// Package phi implements gossip-based membership with the
// phi accrual failure detector. Heartbeat inter-arrival times
// drive a continuous suspicion level; the application chooses
// its threshold.
//
// References:
//   - Hayashibara et al., "The φ Accrual Failure Detector" (2004)
//   - Demers et al., "Epidemic Algorithms for Replicated
//     Database Maintenance" (1987)
//   - van Renesse et al., "Efficient Reconciliation and Flow
//     Control for Anti-Entropy Protocols" (2008)
package phi
