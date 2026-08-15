# FSPANN: Forward-Secure and Privacy-Preserving ANN Search with Routing–Ciphertext Orthogonality

FSPANN is a Java implementation of lifecycle-aware approximate nearest neighbor
(ANN) retrieval over encrypted high-dimensional vectors. It combines bounded
geometric candidate generation with versioned authenticated encryption, gradual
ciphertext migration, and safe epoch-key retirement.

The project is designed for long-lived encrypted vector databases in which keys
must evolve without forcing reconstruction of the ANN routing structure.

---

## Scope

FSPANN targets encrypted vector retrieval for applications such as semantic
search, retrieval-augmented generation, recommendation, multimodal retrieval,
and large-scale embedding analytics.

Its central contribution is **routing–ciphertext orthogonality**, defined as a
lifecycle invariant:

> For a fixed indexed collection, ANN candidate-generation semantics remain
> unchanged while ciphertexts, record versions, and online epoch keys evolve
> through rotation, selective migration, and safe retirement.

This is not a claim that routing and encrypted storage are physically
disconnected. Routed identifiers still resolve to encrypted records. The
invariant requires cryptographic maintenance not to alter routing codes,
partition membership, representatives, probe behavior, or routed candidate
identifiers.

Ordinary insertions, deletions, and geometric index updates remain operations of
the underlying ANN index and are outside this fixed-collection invariance claim.

---

## Core Capabilities

### Stable routing under key evolution

- Multi-table binary-code partition routing
- Fixed routing codes, partitions, representatives, and identifier memberships
- Candidate generation independent of ciphertext contents, nonces, versions,
  and migration status
- No routing-index reconstruction during normal rotation, migration, or
  retirement

### Version-aware encrypted storage

Each vector is stored as a versioned authenticated-encryption record:

```text
EP_i = (id_i, v_i, iv_i, ct_i, d)
ct_i = AEAD.Enc(K_{v_i}, iv_i, x_i, aad_i)
aad_i = concat(id_i, v_i, d)
```

The prototype uses AES-GCM. Associated data bind the ciphertext to its
persistent identifier, key version, and dimensionality.

### Bounded trusted refinement

- Routing returns a candidate identifier set
- At most `B` encrypted records are selected for refinement
- Each record is decrypted under its stored key version
- Exact distances are computed only inside the client-side or otherwise trusted
  refinement component
- Query-time cryptographic work is bounded by the refinement budget rather than
  the full database size

### Cryptographic lifecycle operations

FSPANN defines seven system primitives:

```text
Setup, TokenGen, Route, Refine, Rotate, Migrate, Retire
```

The lifecycle operations are:

- **Rotate**: derive and activate a new epoch key without modifying routing
  state
- **Migrate**: authenticate, decrypt, and re-encrypt outdated records in a
  selected touched or scheduled maintenance set
- **Retire**: remove an obsolete epoch key from online key state only after no
  live ciphertext remains dependent on that version

### Forward security after safe retirement

Exposure of the current online epoch key does not reveal historical ciphertexts
from safely retired epochs, assuming:

- PRF key separation
- AES-GCM confidentiality
- an uncompromised master secret
- removal of the retired epoch key from online state
- no remaining live ciphertext under the retired version

Master-secret compromise and master-secret rotation are outside the current
formal guarantee.

---

## Repository Structure

The implementation is organized as a multi-module Maven project.

```text
.
├── api/             # System facade and lifecycle orchestration
├── common/          # Shared models, utilities, and abstractions
├── config/          # Configuration loading and validation
├── crypto/          # AES-GCM and encrypted-record handling
├── index/           # Geometric encoding, partition construction, and routing
├── keymanagement/   # Key derivation, version control, rotation, and retirement
├── loader/          # Streaming dataset ingestion
├── query/           # Token generation, candidate retrieval, refinement, evaluation
├── it/              # Integration and security-oriented tests
├── data/            # Local datasets, excluded from version control
├── metadata/        # Local metadata state, excluded from version control
└── Results/         # Generated experiment outputs, excluded from version control
```

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the detailed state model,
component boundaries, query path, and key-lifecycle workflow.

---

## System Roles

FSPANN uses three logical roles.

### Data owner

- Constructs the routing structure
- Encrypts the vector collection
- Controls the master secret and epoch-key lifecycle
- Authorizes rotation, migration, and retirement

### Honest-but-curious server

- Stores routing metadata and encrypted records
- Executes geometric candidate routing
- Returns encrypted candidate records
- Observes the routing and maintenance information declared in the leakage model

The server is not trusted with plaintext vectors, plaintexts produced during
trusted refinement, the master secret, or safely retired epoch keys.

### Client or trusted refinement component

- Generates query tokens
- Receives a bounded encrypted candidate set
- Decrypts each candidate under its recorded version
- Computes exact distances
- Returns the approximate top-\(k\) result and the touched set

---

## Query Workflow

1. The query vector is mapped to the same routing representation used during
   indexing.
2. The server probes the nearest routing partitions and bounded neighboring
   partitions.
3. Routing produces a candidate identifier set.
4. At most `B` encrypted records are selected for trusted refinement.
5. Each selected record is authenticated and decrypted under its stored version.
6. Exact distances are computed over the decrypted candidates.
7. The top-\(k\) identifiers are returned.
8. Successfully authenticated identifiers form the touched set used by optional
   selective migration.

---

## Key Evolution Workflow

1. `Rotate` derives and activates a new epoch key.
2. Existing records remain under their stored versions, allowing mixed-version
   retrieval.
3. Query-generated touched sets or scheduled maintenance sets identify outdated
   records.
4. `Migrate` authenticates each selected old record, decrypts it under its stored
   key version, and re-encrypts it under the active version with a fresh nonce
   and updated associated data.
5. Ciphertext and version metadata are replaced atomically under the same
   persistent identifier.
6. Per-version live-record counts are updated.
7. `Retire` removes an obsolete key only when its live-record count reaches zero.

Routing codes, partitions, representatives, probe behavior, and routed
identifiers remain unchanged throughout these cryptographic operations.

---

## Security Scope and Declared Leakage

FSPANN uses an adaptive honest-but-curious server model. The server follows the
protocol but may observe:

- public parameters and record counts
- ciphertext lengths
- fixed routing metadata
- routing tokens
- partition-probe traces
- candidate and refinement identifiers
- access order
- accessed key versions
- activated and retired version labels
- migrated identifiers
- version transitions
- observable update sizes
- retirement outcomes

Across repeated queries and epochs, these observations may reveal coarse
geometric locality, repeated-query behavior, candidate overlap, access
correlations, frequently accessed records, and migration progress.

FSPANN does **not** provide ORAM/PIR-style access-pattern hiding.

During normal operation, vector contents remain confidential beyond the
declared leakage. In the current-key compromise experiment, current-epoch
confidentiality is not claimed after exposure of the current online epoch key.
The forward-security guarantee applies to historical ciphertexts from safely
retired epochs.

---

## Build

### Requirements

- Java 21
- Maven
- A platform supported by the RocksDB Java bindings
- Sufficient memory and storage for the selected dataset and profile

### Compile and install

```bash
mvn clean install
```

Dataset paths, dimensions, routing parameters, refinement budgets, key-rotation
settings, and output locations are profile-driven. Use the committed
configuration profiles and experiment runners associated with the desired
dataset.

Before launching a full-scale experiment, verify the installation with the
repository's integration and smoke-test targets.

---

## Configuration

Typical configuration fields include:

- dataset path and file format
- vector count and dimensionality
- query and ground-truth paths
- top-\(k\)
- routing-table and division counts
- routing-code length
- partition and probe budgets
- refinement budget
- active key version and rotation policy
- migration mode
- restore or query-only mode
- output and profiling paths
- deterministic seeds

Configuration files used for reported experiments should be treated as part of
the reproduction artifact.

---

## Evaluated Datasets

The final paper evaluates FSPANN on:

- **SIFT1M**, 128-dimensional descriptor vectors
- **GloVe-100**, 100-dimensional word embeddings
- **RedCaps**, 512-dimensional multimodal embeddings
- **SIFT1B subsets**, used for scalability experiments

Datasets are not included in the repository. Obtain them from their official
sources and configure local paths before running an experiment.

---

## Representative Results

All average response time values below were measured on an Intel Xeon
E5-2630 v4 platform with 40 logical cores at 2.20 GHz and 380 GiB RAM using
10,000 warmed-up queries.

| Profile | Dataset | Refinement budget | Recall@10 | ART |
| --- | --- | ---: | ---: | ---: |
| Interactive | SIFT1M | 6K | 0.502 | 745.7 ms |
| Balanced | SIFT1M | 20K | 0.838 | 2828.0 ms |
| High recall | SIFT1M | 22K | 0.879 | 4186.0 ms |
| High recall | GloVe-100 | 24K | 0.427 | 3620.0 ms |
| High recall | RedCaps | 28K | 0.1197 | 5607.2 ms |

Additional observations:

- The strongest reported distance ratios are approximately 1.010 on SIFT1M,
  1.037 on GloVe-100, and 1.018 on RedCaps.
- Authenticated decryption and exact refinement account for approximately
  89–93% of measured query latency.
- Routing and candidate transfer account for approximately 7–11%.
- Selective migration of a selected one-million-record outdated maintenance set
  completes in approximately 31–41 seconds.
- Measured total system-storage footprints are 1.03–1.09 GB for SIFT1M,
  0.98–1.03 GB for GloVe-100, and 3.35–3.41 GB for RedCaps.
- The complete evaluation pipeline finishes on SIFT1B subsets up to 75M
  vectors.
- The 100M target run stops near 92.6M vectors because of JVM heap exhaustion,
  an implementation-level memory limitation rather than a formal algorithmic
  boundary.

FSPANN does not claim universal recall–latency dominance. The evaluation
measures the cost of combining encrypted ANN retrieval with routing-invariant
key evolution, mixed-version refinement, selective migration, and safe
retirement.

---

## Current Limitations

- The system is routing-visible rather than access-pattern oblivious.
- Query latency is dominated by authenticated decryption and exact refinement.
- The current implementation is CPU-oriented.
- Performance depends on routing, probing, and refinement configuration.
- Master-secret compromise is outside the current security theorem.
- Routing invariance is stated for cryptographic maintenance over a fixed
  indexed collection.
- The current prototype has a JVM-memory limitation at the largest tested scale.
- FSPANN is a research prototype, not a drop-in replacement for a production
  vector database.

---

## Reproducibility Notes

For a clean reproduction:

1. Build from a fresh clone.
2. Confirm the documented Java and Maven versions.
3. Run the integration and smoke-test targets.
4. Download datasets from their official sources.
5. Update only local dataset and output paths in the committed profiles.
6. Preserve the committed routing parameters, budgets, and seeds.
7. Run one dataset profile at a time unless sufficient memory is available.
8. Record the exact commit, JVM options, hardware, and output directory.
9. Compare generated Recall@10, Ratio, ART, migration, and storage summaries
   with the reported operating points.
10. Do not commit datasets, generated ciphertexts, RocksDB state, keystores, or
    machine-specific paths.

---

## Project Status

This repository accompanies the ICICS 2026 submission:

**FSPANN: Forward-Secure and Privacy-Preserving ANN Search with Routing–Ciphertext Orthogonality**

The repository is intended for review, inspection, and experimental reproduction. It should not be interpreted as providing stronger privacy or performance guarantees than those stated above.
