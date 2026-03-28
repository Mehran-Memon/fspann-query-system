# Secure Vector Search Engine with Forward Security — Architecture

This document describes the internal architecture of the FSPANN system: its structural decomposition, the layered design principle that separates routing from cryptographic state, the full component map, the query pipeline, and the lifecycle of key evolution.

---

## Core design principle: routing–ciphertext orthogonality

The central architectural decision in FSPANN is that **geometric routing must not depend on ciphertext state**.

The system maintains two independent layers:

**Routing layer (geometric)**
- LSH-based projection functions
- Partition boundaries and routing codes
- Sortable keys derived from vector geometry
- Fixed public parameters that do not change under key rotation

**Payload layer (cryptographic)**
- AES-GCM encrypted vector payloads
- Per-record key version metadata `(id, kv, iv, ct, d)`
- Version authority and master secret keystore
- Ciphertext migration and safe key deletion

Key derivation follows `kv = PRF(Kmaster, v)`, and ciphertext per record follows `c(v)i = Ekv(xi)`. Because routing depends only on vector geometry and fixed public parameters, the routing topology is unaffected by ciphertext evolution. Key rotation, selective re-encryption, and safe key deletion can all proceed without touching the index.

---

## Module structure

The system is organised as a multi-module Maven project. The seven core modules have explicit service boundaries and minimal cross-cutting dependencies.

```text
.
├── api/             # System facade and global lifecycle orchestration
├── common/          # Shared models, utilities, and abstractions
├── config/          # Configuration loading, validation, and profile management
├── crypto/          # AES-GCM authenticated encryption and encrypted record handling
├── index/           # Geometric encoding, partition construction, and routing services
├── keymanagement/   # Key derivation, version control, rotation policies, and safe deletion
├── loader/          # Streaming dataset ingestion (.fvecs / .bvecs / CSV)
├── query/           # Query token generation, candidate retrieval, decrypt-and-refine, evaluation
├── it/              # Integration tests, security validation, adversarial test workflows
├── data/            # Local datasets (ignored in VCS)
├── metadata/        # Local metadata (ignored in VCS)
└── Results/         # Generated experimental outputs (ignored in VCS)
```

The module dependency structure is hub-and-spoke. The `api` module acts as the system facade and holds the global lifecycle. All other modules depend on `common`. The three functional pillars — configuration and evaluation, geometric routing, and cryptography — are kept separate so each can evolve independently.

```text
                        api / system facade
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
  config + loader       index + query         crypto + keymanagement
  evaluation, profiler   routing, ANN           encryption, rotation
        │                     │                     │
        └─────────────────────┴─────────────────────┘
                          common + persistence
```

---

## Component map

The following is a full decomposition of FSPANN's internal components, grouped by pillar.

### Configuration, input, and evaluation

| Component | Responsibility |
| --- | --- |
| Configuration loader | JSON loading, SHA-256 profile validation, schema enforcement |
| Streaming dataset loader | Batch-iterator ingestion of `.fvecs`, `.bvecs`, and CSV vector files |
| Queries + ground truth | Evaluation inputs for recall and ratio measurement |
| Profiler / Micrometer | Recall, ratio, and latency export to CSV |
| Integration / security tests | Restore, rotation, and adversarial validation workflows |

### Geometric routing, ingestion, and query execution

| Component | Responsibility |
| --- | --- |
| `GFunctionRegistry` | Projection sampling with deterministic seed |
| `PartitionedIndexService` | Routing tables and immutable partition construction |
| Geometric encoding `xi → C(xi)` | `MapTo63Bit` encoding of vectors into sortable routing codes |
| Index finalisation | Sort keys, compute partition boundaries, lock for query mode |
| `QueryTokenFactory` | Routing codes and encrypted query token generation |
| Candidate retrieval | Binary search over sorted keys, neighbour probing |
| Bounded decrypt-and-refine | Configurable candidate budget, exact distance computation, top-K selection, adaptive retry |

### Cryptography, key lifecycle, and re-encryption

| Component | Responsibility |
| --- | --- |
| `AesGcmCryptoService` | AES-GCM authenticated encryption with AAD binding |
| `KeyRotationServiceImpl` | Master-to-version derivation `KM ⇒ Kv`, active version control |
| `EncryptedPoint` | Per-record struct: `(id, kv, iv, ct, d)` |
| Rotation triggers | `opsThreshold` and `ageThreshold` policy enforcement |
| Selective re-encryption | Lazy migration `Ku → Kv` triggered by access batches |
| `ReencryptionTracker` | Touched-set `T` and per-version usage accounting |
| Safe key deletion | Delete only when no ciphertext references remain for a version |

### Persistence, crash consistency, and restore

| Component | Responsibility |
| --- | --- |
| `RocksDBMetadataManager` | Atomic `WriteBatch` commits for version, shard, dimension, and bucket metadata |
| Ciphertext filesystem | Versioned directory layout: `baseDir/v_k/id.point` |
| Crash recovery | Load keys, open DB, call `restoreIndexFromDisk()` |
| Write protocol | Metadata commit → temp file write → atomic move → obsolete cleanup |
| Keystore | Master secret and version authority persistence |

---

## System flow: data owner to client

At the highest level, FSPANN connects three principals.

**Data owner** uploads vector data. The system encrypts each vector under the current active key version and stores the encrypted subsets with associated metadata in the cloud server. Forward security state — versioned ciphertexts and routing structures — lives on the untrusted server.

**Cloud server** holds all encrypted state. It performs partitioned LSH query filtering without access to plaintext vectors. When a query arrives, it returns encrypted result candidates to the client. After query batches, access-based selective re-encryption is triggered on touched records, migrating outdated ciphertexts to the current version.

**Client** generates a query token consisting of `LSH(q) + Enc(q)` — the routing code and encrypted query — and sends it to the server. The server returns candidate encrypted payloads. The client decrypts the candidates locally and performs bounded distance refinement to produce the final ANN results.

---

## Execution lifecycle

The system follows a strict ordered lifecycle enforced by the `ForwardSecureANNSystem` facade.

| Phase | Key actions |
| --- | --- |
| `SETUP` | Load configuration; initialise projection and routing functions; open RocksDB metadata and keystore |
| `INDEX` | Stream vectors in batches; compute `C(xi)`; encrypt under active version; persist metadata and ciphertext payloads |
| `FINALIZE` | Sort routing keys; compute immutable partition boundaries; transition to query mode |
| `QUERY` | Construct query token; route to bounded candidate set; fetch encrypted payloads; decrypt-and-refine; return top-K |
| `S-R.E` | Rotate active key version; lazily re-encrypt touched ciphertexts; preserve routing topology |
| `EXPORT` | Write profiling summaries; export recall, ratio, latency, and experimental artifacts |
| `SHUTDOWN` | Flush state; close RocksDB; support restore-from-disk and query-only restart |

---

## Query pipeline

1. Query vector `q` is converted into the same geometric routing representation used during indexing via `QueryTokenFactory`
2. Binary search over sorted routing keys identifies the relevant partition and neighbour partitions
3. A bounded candidate identifier set is produced within the configured refinement budget
4. Encrypted payloads `(id, kv, iv, ct, d)` for all candidates are fetched from the versioned ciphertext filesystem
5. Each candidate is decrypted using its recorded key version `kv` via `AesGcmCryptoService`
6. Exact distances are computed over decrypted vectors
7. Top-K results are returned to the client
8. If enabled, touched records with outdated key versions are added to the re-encryption tracker

Cryptographic work is bounded by the candidate budget, not the dataset size.

---

## Key evolution and selective re-encryption

Key rotation does not require stopping the system or rebuilding the index. The sequence is:

1. `KeyRotationServiceImpl` derives a new version `Kv+1 = PRF(Kmaster, v+1)` and promotes it to active
2. Newly ingested or re-encrypted records are written under `Kv+1`; existing records retain their version tag
3. `ReencryptionTracker` accumulates a touched set `T` across query batches
4. After each batch, accessed records in `T` that carry obsolete version tags are lazily migrated: decrypt under `Ku`, re-encrypt under `Kv+1`, write atomically
5. Once `ReencryptionTracker` confirms no remaining ciphertext references to version `Ku`, the key is safely deleted

The routing layer is unaffected throughout. Partition boundaries, routing codes, and the sortable key structure remain unchanged.

---

## Security model

The system is designed under an **honest-but-curious server** model.

**Protected by the design**
- Vector payload contents through AES-GCM authenticated encryption with AAD binding
- Historical ciphertext confidentiality after safe deletion of obsolete keys
- Record integrity bound to associated metadata via AAD

**Observable / leakage side**

The system does not eliminate all leakage. Observable server-side behaviour includes partition access patterns, candidate-set sizes, version metadata in record headers, and the access patterns of the selective re-encryption phase. FSPANN is a **leakage-aware secure ANN design**, not a fully oblivious retrieval protocol. Stronger obliviousness extensions are identified as future work.

---

## Technology stack

| Layer | Technology |
| --- | --- |
| Language | Java 21 |
| Build | Maven (multi-module) |
| Metadata store | RocksDB via `WriteBatch` atomic commits |
| Encryption | AES-GCM with AAD-bound authenticated encryption |
| Routing / ANN | Partitioned LSH-style geometric routing with `MapTo63Bit` encoding |
| Profiling | Micrometer with CSV export |
| Testing | JUnit-based integration, restore, rotation, and adversarial validation |
