# FSPANN Architecture

This document describes the internal architecture of FSPANN, including its
state decomposition, module boundaries, query pipeline, key-lifecycle
operations, persistence model, and security scope.

---

## 1. Architectural Principle

FSPANN is organized around **routing–ciphertext orthogonality**.

This is a lifecycle invariant, not a claim that the routing index and encrypted
records are physically disconnected. Routed identifiers still resolve to
encrypted records. The invariant requires that, for a fixed indexed collection,
candidate generation remain unchanged while the cryptographic state evolves.

At epoch \(t\), the system state is:

```text
Σ_t = (I, Store_t, V_t, K_t)
```

with two logically distinct parts:

```text
Routing state:
Σ_t^R = I

Cryptographic state:
Σ_t^C = (Store_t, V_t, K_t)
```

Where:

- `I` is the fixed geometric routing structure
- `Store_t` is the encrypted record store
- `V_t` maps persistent identifiers to key versions
- `K_t` is the online epoch-key state

For a fixed query routing component `ρ_q` and routing state `I`:

```text
Route(ρ_q, Σ_t^R) = Route(ρ_q, Σ_t'^R)
```

for every pair of reachable states that differ only through rotation,
migration, or safe retirement.

Cryptographic maintenance may change ciphertexts, nonces, record versions, and
online keys. It must not change routing codes, partition membership,
representatives, probe behavior, or routed candidate identifiers.

The invariant applies to cryptographic maintenance over a fixed indexed
collection. Ordinary insertions, deletions, and geometric index updates are
outside this claim.

---

## 2. Logical Roles and Trust Boundaries

FSPANN uses three logical roles.

### Data owner

The data owner:

- supplies the plaintext vector collection
- constructs the routing structure
- encrypts records
- controls the master secret
- derives and activates epoch keys
- authorizes migration and retirement

### Honest-but-curious server

The server:

- stores routing metadata
- stores versioned encrypted records
- performs geometric candidate routing
- returns encrypted candidates
- executes maintenance-visible storage updates

The server is not trusted with:

- plaintext vectors
- plaintext query contents beyond visible routing material
- plaintext candidates produced during trusted refinement
- the master secret
- safely retired epoch keys

### Client or trusted refinement component

The trusted refinement component:

- generates query tokens
- receives a bounded encrypted candidate set
- decrypts records under their stored versions
- computes exact distances
- returns the approximate top-\(k\) result
- emits the touched set of successfully authenticated identifiers

The refinement component may be colocated with the client or deployed inside
another trusted execution boundary. The architecture does not require the
honest-but-curious server to see plaintext candidates.

---

## 3. Repository Modules

The implementation is organized as a multi-module Maven project.

```text
.
├── api/             # System facade and lifecycle orchestration
├── common/          # Shared models, utilities, and abstractions
├── config/          # Configuration loading and validation
├── crypto/          # AES-GCM and encrypted-record handling
├── index/           # Geometric encoding, partition construction, and routing
├── keymanagement/   # Key derivation, active-version control, rotation, retirement
├── loader/          # Streaming dataset ingestion
├── query/           # Token generation, candidate retrieval, refinement, evaluation
├── it/              # Integration and security-oriented tests
├── data/            # Local datasets, excluded from version control
├── metadata/        # Local metadata state, excluded from version control
└── Results/         # Generated experiment outputs, excluded from version control
```

The principal dependency pattern is:

```text
                         api / system facade
                                  │
          ┌───────────────────────┼────────────────────────┐
          │                       │                        │
   config + loader          index + query       crypto + keymanagement
          │                       │                        │
          └───────────────────────┴────────────────────────┘
                             common
```

The `api` module coordinates the lifecycle but should not merge the geometric
and cryptographic state models. Shared record and configuration abstractions
belong in `common`.

---

## 4. Core Data Structures

### 4.1 Indexed collection

The indexed collection is:

```text
D = {(id_i, x_i)} for i = 1 ... N
```

where `id_i` is a persistent identifier and `x_i` is a \(d\)-dimensional
vector.

### 4.2 Encrypted record

Each stored vector is represented as:

```text
EP_i = (id_i, v_i, iv_i, ct_i, d)
```

with:

```text
ct_i  = AEAD.Enc(K_{v_i}, iv_i, x_i, aad_i)
aad_i = concat(id_i, v_i, d)
```

The prototype uses AES-GCM.

Associated data bind:

- the persistent identifier
- the key version
- the vector dimensionality

This prevents substitution of these fields while allowing records from multiple
epochs to coexist during migration.

### 4.3 Key derivation

Epoch keys are derived from an owner-controlled master secret:

```text
K_v = PRF(K_M, v)
```

The current model assumes `K_M` remains uncompromised. Master-secret rotation
is a possible extension but is not part of the present theorem or implementation
guarantee.

### 4.4 Version map

`V_t[id_i]` records the epoch version required to decrypt the encrypted record
stored under `id_i`.

The persistent identifier is the direct lookup key for both:

```text
Store_t[id_i]
V_t[id_i]
```

No separate identifier-translation index is required.

---

## 5. Routing State

The routing state is built from geometric information rather than ciphertext
state.

For every table/division pair `(a, b)`, the coding function:

```text
G_{a,b}: R^d -> {0,1}^m
```

maps a vector to an \(m\)-bit routing code. A sortable mapping orders the codes
and supports partition construction.

Each partition contains:

- a sortable key range
- persistent identifier memberships
- a representative code
- routing metadata needed for bounded neighboring probes

The fixed routing state includes:

- coding functions and seeds
- sortable routing keys
- partition boundaries
- representatives
- identifier memberships
- probe rules

The routing state excludes:

- ciphertext payloads
- nonces
- record versions
- migration status
- online epoch keys

---

## 6. Cryptographic State

The evolving cryptographic state contains:

```text
Store_t
V_t
K_t
```

### `Store_t`

The identifier-keyed encrypted record store.

### `V_t`

The identifier-to-version map.

### `K_t`

The online epoch-key state, including:

- the active epoch version
- currently available epoch keys
- retirement status
- per-version live-record counts

Rotation, migration, and retirement update only these cryptographic components.

---

## 7. Seven-Primitive Interface

FSPANN is defined through:

```text
Setup, TokenGen, Route, Refine, Rotate, Migrate, Retire
```

### Setup

```text
Setup(1^κ, D, θ) -> Σ_0
```

Responsibilities:

- initialize routing-code functions
- construct and fix the routing structure
- derive the initial epoch key
- encrypt each vector under the initial version
- initialize the version map and live-record counts

### TokenGen

```text
TokenGen(q, k, K_t) -> τ_q
```

Produces:

- visible routing material `ρ_q`
- target size `k`
- active version metadata
- protected query material for trusted refinement

### Route

```text
Route(ρ_q, I) -> C_q
```

Uses only routing material and fixed routing state to produce candidate
identifiers.

### Refine

```text
Refine(τ_q, C_q, Store_t, K_t) -> (N~_k(q), T_t)
```

Responsibilities:

- select at most `B` routed candidates
- authenticate and decrypt each record under its stored version
- compute exact distances
- return the approximate top-\(k\) result
- return the touched set `T_t`

### Rotate

```text
Rotate(Σ_t)
```

Responsibilities:

- derive the next epoch key
- activate the new version
- leave all routing state unchanged
- leave existing records under their stored versions

### Migrate

```text
Migrate(Σ_t, M, v')
```

For every outdated identifier in a selected maintenance set:

1. load the encrypted record and stored version
2. authenticate and decrypt under the old version
3. re-encrypt the same vector under the active version
4. use a fresh nonce and updated associated data
5. atomically replace ciphertext and version metadata
6. decrement the old-version live-record count
7. increment the new-version live-record count

The maintenance set may be:

- a query-generated touched set
- a scheduled batch
- another explicitly selected set

### Retire

```text
Retire(Σ_t, u)
```

Retirement succeeds only when:

- `u` is older than the active version
- the live-record count for `u` is zero
- no live ciphertext still depends on `K_u`

The obsolete key is then removed from online key state and marked retired.

---

## 8. End-to-End Query Pipeline

1. The client or trusted component receives query vector `q`.
2. `TokenGen` computes the routing representation `ρ_q`.
3. Protected query material is created for trusted refinement.
4. The server executes `Route(ρ_q, I)`.
5. Bounded neighboring probes return candidate identifiers `C_q`.
6. A refinement subset `F_q` is selected with `|F_q| <= B`.
7. For each `id_i` in `F_q`, the trusted component loads:
   `EP_i = (id_i, v_i, iv_i, ct_i, d)`.
8. Associated data are reconstructed as `concat(id_i, v_i, d)`.
9. AES-GCM authenticates and decrypts the record under `K_{v_i}`.
10. Exact distance to `q` is computed.
11. Successfully authenticated records are added to the score set and touched
    set.
12. The smallest-distance `k` identifiers are returned.
13. The touched set may drive query-triggered migration.

Cryptographic refinement costs \(O(Bd)\), independent of the total number of
stored vectors when the refinement budget is bounded.

---

## 9. Key-Lifecycle Pipeline

### 9.1 Rotation

Rotation:

- advances the active version
- derives one new epoch key
- costs \(O(1)\) with respect to database size
- does not rewrite records
- does not alter routing state

### 9.2 Mixed-version operation

After rotation:

- new or migrated records use the active version
- untouched records retain older versions
- refinement resolves the correct key from each record's version
- multiple non-retired versions may coexist

### 9.3 Selective migration

For a maintenance set `M`, migration costs:

```text
O(|M| d)
```

Under query-triggered migration:

```text
M = T_t
T_t subset_of F_q
|M| <= B
```

Therefore, added query-triggered maintenance is bounded by \(O(Bd)\), rather
than the \(O(Nd)\) cost of full-store re-encryption.

### 9.4 Safe retirement

An epoch becomes safely retired only after:

1. every live dependent ciphertext has migrated or become unreachable
2. its live-record count reaches zero
3. its key is removed from online state

Forward security begins only after this boundary. Old-version records are not
protected against exposure of their still-available epoch keys while
mixed-version refinement remains active.

---

## 10. Persistence and Consistency

The prototype uses RocksDB for metadata persistence and a version-aware
encrypted-record store.

The architecture requires the following consistency properties:

- persistent identifiers remain stable across migration
- ciphertext and version metadata are replaced atomically
- failed migration must leave the previous record valid
- successful migration must remove the record's dependency on the old key
- per-version live-record counts must remain consistent with stored versions
- retirement must be rejected while any live dependency remains
- restore must reconstruct a state in which record versions, online keys, and
  routing metadata agree

The exact persistence protocol is implemented in the repository and should be
validated through integration and restore tests. Documentation should not imply
stronger crash-consistency guarantees than those covered by the implementation
and tests.

---

## 11. Complexity Summary

Let:

- `N` be the number of vectors
- `d` be the vector dimension
- `B` be the refinement budget
- `M` be a selected migration set
- `P` be the number of partitions per table/division
- `ρ` be the probe budget
- `b` be the maximum partition size

Key complexity properties are:

| Operation | Complexity |
| --- | --- |
| Rotation | \(O(1)\) in \(N\) |
| Bounded refinement | \(O(Bd)\) plus top-\(k\) selection |
| Selective migration | \(O(|M|d)\) |
| Query-triggered migration | At most \(O(Bd)\) |
| Full-store re-encryption | \(O(Nd)\) |
| Retirement eligibility | \(O(1)\) per epoch with live-record counts |

When routing probes, partition sizes, and refinement budgets are bounded, query
processing avoids a linear scan over `N`.

---

## 12. Security Model

FSPANN uses an adaptive honest-but-curious server model.

### Protected information

During normal operation, the design protects:

- plaintext vector contents
- protected query contents beyond visible routing material
- plaintext candidates inside trusted refinement
- plaintext exact distances inside trusted refinement
- the master secret
- safely retired epoch keys

After exposure of the current online epoch key, the forward-security guarantee
protects historical ciphertexts from safely retired epochs.

### Declared leakage

The server may observe:

- public parameters
- record counts and ciphertext lengths
- fixed routing metadata
- routing tokens
- partition-probe traces
- candidate identifiers
- refinement identifiers
- access order
- accessed key versions
- activated and retired version labels
- migrated identifiers
- version transitions
- observable update sizes
- retirement outcomes

Adaptive correlation may reveal:

- coarse geometric locality
- repeated-query behavior
- candidate overlap
- access correlations
- frequently accessed records
- migration progress

FSPANN does not provide ORAM/PIR-style access-pattern hiding.

### Out of scope

- malicious protocol deviation
- denial of service
- hardware side channels
- master-secret compromise
- master-secret rotation guarantees
- access-pattern obliviousness
- arbitrary index updates under the fixed-routing invariant

---

## 13. Empirical Operating Envelope

The paper reports the following representative FSPANN operating points:

| Profile | Dataset | Budget | Recall@10 | ART |
| --- | --- | ---: | ---: | ---: |
| Interactive | SIFT1M | 6K | 0.502 | 745.7 ms |
| Balanced | SIFT1M | 20K | 0.838 | 2828.0 ms |
| High recall | SIFT1M | 22K | 0.879 | 4186.0 ms |
| High recall | GloVe-100 | 24K | 0.427 | 3620.0 ms |
| High recall | RedCaps | 28K | 0.1197 | 5607.2 ms |

The principal measured cost is authenticated decryption and exact refinement,
which account for approximately 89–93% of query latency. Routing and candidate
transfer account for approximately 7–11%.

Selective migration of a selected one-million-record outdated maintenance set
completes in approximately 31–41 seconds without rebuilding routing codes,
partitions, or representatives.

The full evaluation pipeline completes on SIFT1B subsets up to 75M vectors.
The 100M target run stops near 92.6M because of JVM heap exhaustion, which is
an implementation-level memory limitation rather than a formal scalability
boundary.

---

## 14. Architectural Invariants

The implementation should preserve the following invariants:

1. `Route` depends only on `ρ_q` and `I`.
2. Rotation does not modify routing state.
3. Migration does not modify routing state.
4. Retirement does not modify routing state.
5. Persistent identifiers do not change during migration.
6. Ciphertext and version metadata change atomically.
7. A retired key has zero live-record dependencies.
8. Refinement uses each record's stored key version.
9. Query-triggered migration is bounded by the touched refinement set.
10. Current-key exposure does not imply recovery of safely retired keys.
11. Master-secret compromise is not covered by the current security theorem.
12. Routing invariance applies to a fixed indexed collection.

These invariants are the core conditions that connect the implementation to the
paper's system and security arguments.
