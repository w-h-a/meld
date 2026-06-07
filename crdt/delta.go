// Delta-state extensions to the CRDT catalog.
//
// Each base CRDT in meld supports a delta-mutator pair alongside
// its standard mutator. The standard mutator returns the new full
// state. The delta-mutator returns a smaller value of the same
// base type that encodes only the change. The anti-entropy layer
// ships those deltas across the wire instead of full state.
//
// A delta is not a new kind of CRDT. The delta of a GCounter is
// itself a GCounter, just with most slots unset. The delta of an
// ORSet is an ORSet. So the catalog does not grow when delta is
// added. Each base type gains delta-mutator methods on its own.
//
// This file sits alongside Mergeable in the parent crdt package
// because the delta contract is typeclass-level. Concrete
// delta-mutator methods live on each base CRDT type in its own
// sub-package.
//
// References:
//   - Almeida, Shoker, Baquero, "Delta State Replicated Data
//     Types" (2018)
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011), Section 2.4.2 on
//     state-based emulation of operation-based objects
package crdt
