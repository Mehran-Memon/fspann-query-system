# FSPANN — Forward-Secure Privacy-Preserving Approximate Nearest Neighbour Search

A systems-oriented implementation of encrypted ANN retrieval with forward-secure key evolution over high-dimensional vector data.

---

## Overview

FSPANN is a Java-based system for approximate nearest neighbour (ANN) retrieval over encrypted high-dimensional vectors. It is designed for settings where vector embeddings are operationally useful but also sensitive — semantic retrieval, retrieval-augmented generation (RAG), recommendation, and large-scale embedding search.

The system combines sub-linear geometric routing with authenticated encryption and version-aware key evolution. Its central design principle is that **routing state and ciphertext state are deliberately separated**, so cryptographic key rotation does not require rebuilding the ANN index. This allows the system to support forward security while preserving stable query behaviour and practical search performance.

---

## Problem Statement

Modern vector databases are increasingly used as infrastructure for AI systems, but they are usually optimised for speed rather than long-term security. In many deployments, embeddings are stored in plaintext or under static protection. This creates an important risk: if current key material is exposed, historical vector data may also become exposed.

The main systems question addressed here is:

> How can we support efficient ANN retrieval over encrypted high-dimensional vectors while allowing keys to evolve over time, without forcing index reconstruction after every key update?

FSPANN answers that question through a forward-secure retrieval architecture in which geometric routing remains stable while ciphertexts and key versions evolve independently.

---

## Why This Matters in Modern AI

Vector search now underpins many AI workflows, including semantic document retrieval, hybrid search, multimodal retrieval, recommendation pipelines, and RAG. In these settings, embeddings may encode sensitive signals derived from proprietary documents, user activity, internal corpora, or confidential knowledge bases.

This creates a gap in current infrastructure:

- Plaintext vector databases provide strong performance but weak confidentiality
- Heavy cryptographic retrieval methods provide stronger security but often become too expensive at scale
- Many secure ANN designs couple routing with encrypted representation, making key rotation operationally costly

FSPANN is aimed at that gap. It explores a design that preserves practical ANN behaviour while introducing forward-secure cryptographic maintenance.

---

## Core Features

**Forward-secure key evolution**
- Versioned encryption keys derived from a master secret
- Safe retirement of obsolete keys after ciphertext migration
- Protection against retrospective exposure of older ciphertext epochs after current-key compromise

**Stable ANN routing under key rotation**
- Routing depends on geometric structure and fixed public parameters
- Ciphertext evolution does not alter partition boundaries or routing topology
- No index rebuild required after normal key rotation

**Partition-based approximate retrieval**
- Geometric coding and ordered partition construction for candidate generation
- Bounded decrypt-and-refine pipeline
- Multi-probe style local partition expansion during search

**Version-aware retrieval**
- Stored encrypted records carry explicit key-version metadata
- Mixed-version ciphertext populations coexist safely
- Query-time decryption resolves the correct key version per candidate

**Lazy selective re-encryption**
- Ciphertext migration amortised into normal access rather than forced as a blocking full-dataset operation
- Accessed vectors re-encrypted incrementally under newer key versions

**Persistent metadata and recovery support**
- RocksDB-backed metadata layer
- Version-aware encrypted payload storage
- Restart and recovery workflows for restoring queryable state after interruption

**Evaluation and profiling support**
- Ground-truth-based quality evaluation
- Recall, ratio, and latency export
- Smoke-test and experiment runner support

---

## Design Principle: Routing–Ciphertext Orthogonality

The key architectural idea is that **geometric routing must not depend on ciphertext state**.

In conventional secure retrieval designs, encrypted representation and index structure are often tightly coupled, meaning key rotation can force expensive re-encryption alongside index mutation or full reconstruction.

FSPANN avoids that by separating:

**Geometric state**
- Projection functions
- Routing codes
- Sortable keys
- Partition layout

**Cryptographic state**
- Key versions
- Encrypted vector payloads
- Ciphertext migration
- Safe key deletion

Because of that separation, the routing layer remains stable even while the cryptographic layer evolves.

---

## Architecture

The implementation is organised as a multi-module Maven project with explicit service boundaries.

```text
.
├── api/             # System facade and executable entry-point orchestration
├── common/          # Shared models, utilities, and common abstractions
├── config/          # Configuration loading, validation, and profile management
├── crypto/          # Authenticated encryption, encrypted record handling, and crypto utilities
├── index/           # Geometric encoding, partition construction, and routing services
├── it/              # Integration tests, security validation, and experiment-oriented test workflows
├── keymanagement/   # Key derivation, active-version control, rotation policies, and safe deletion
├── loader/          # Streaming dataset ingestion for supported vector formats
├── query/           # Query token generation, candidate retrieval, decrypt-and-refine, and evaluation
├── data/            # Local datasets (ignored in VCS)
├── metadata/        # Local metadata (ignored in VCS)
└── Results/         # Generated outputs (ignored in VCS)
```

---

## Execution Lifecycle

The system follows an ordered lifecycle.

1. **Setup** — load validated configuration; initialise projection and routing functions; open metadata and keystore state
2. **Indexing / ingestion** — stream vectors in batches; compute geometric representations; encrypt under the current active version; persist metadata and encrypted payloads
3. **Finalisation** — sort routing keys; construct immutable partition boundaries; switch from ingestion mode to query mode
4. **Querying** — construct a query token; route geometrically to a bounded candidate set; decrypt and refine; return top-k nearest neighbours
5. **Key evolution and migration** — rotate active key version; selectively re-encrypt outdated ciphertexts; preserve routing topology during cryptographic maintenance
6. **Evaluation / export** — write profiling summaries; export recall, ratio, latency, and experimental artifacts
7. **Shutdown / recovery** — flush state; close metadata resources; support restore-from-disk and query-only restart workflows

---

## Query Pipeline

1. A query vector is converted into the geometric routing representation used during indexing
2. The routing layer identifies the relevant partition and nearby candidate partitions
3. A bounded candidate identifier set is produced
4. Encrypted payloads for those candidates are fetched
5. Each candidate is decrypted using its recorded key version
6. Exact distances are computed over the decrypted candidates
7. The top-k results are returned
8. If enabled, touched outdated ciphertexts may be migrated to the current version

Expensive cryptographic work is bounded by the configured refinement budget rather than the total dataset size.

---

## Security Model

The system is designed under an **honest-but-curious server** model. The server is assumed to follow the prescribed protocol correctly while attempting to infer information from stored state and execution behaviour.

**Protected**
- Vector payload contents through authenticated encryption
- Historical ciphertext confidentiality after safe deletion of obsolete keys
- Integrity of encrypted records bound to associated metadata

**Observable / leakage side**

Like other practical sub-linear retrieval systems, the design does not eliminate all leakage. Observable behaviour may include partition access patterns, candidate-set sizes, version metadata, and migration-side access patterns.

The system is best understood as a **leakage-aware secure ANN design**, not a fully oblivious retrieval protocol.

---

## Technology Stack

| Layer          | Technology                                          |
| -------------- | --------------------------------------------------- |
| Language       | Java 21                                             |
| Build          | Maven                                               |
| Metadata store | RocksDB                                             |
| Encryption     | AES-GCM                                             |
| Routing / ANN  | Partitioned geometric routing with LSH-style coding |
| Profiling      | Micrometer and profiler exports                     |
| Testing        | JUnit-based integration and adversarial validation  |

---

## Build Instructions

### Prerequisites

- Java 21
- Maven
- RocksDB JNI dependencies available for your environment
- Sufficient RAM depending on dataset scale and configuration

### Build

```bash
mvn clean install
```

If your packaging uses a specific module entry point rather than a root fat JAR, run from the appropriate module or with the module-specific Maven goal used in your repository.

---

## Running the System

Use the actual entry path for your `api` module. If your repository provides experiment scripts or profile-driven runners, prefer those over a generic invocation.

```bash
# Build
mvn clean install

# Run the API/system facade module
mvn -pl api exec:java
```

If you use shell scripts for smoke tests or dataset sweeps, document those commands here instead.

---

## Configuration

Because the system is profile-driven, configuration should be treated as part of the experimental artifact, not just runtime convenience. Typical runtime configuration covers:

- Dataset path and format
- Vector dimensionality
- Top-k query size
- Refinement bound / candidate budget
- Routing parameters and probe settings
- Key rotation thresholds
- Profiling and export toggles
- Restore / query-only mode

---

## Datasets

The evaluation is centred on large-scale vector retrieval benchmarks:

- **SIFT** — 128-dimensional local descriptor vectors
- **GloVe** — pre-trained word embedding vectors
- **RedCaps** — 512-dimensional image-text embedding vectors

The thesis evaluates FSPANN on SIFT and RedCaps and analyses the trade-off among recall, latency, and security-aware refinement cost. GloVe is included in the experimental setup; recall figures for that dataset are reported in the full thesis rather than summarised here.

---

## Results

All figures below are taken directly from thesis-measured results.

- On **SIFT**, the system reaches **Recall@10 up to 0.879**
- Supports **interactive configurations with query latency as low as 400 ms**
- On **RedCaps (512 dimensions)**, reaches **Recall@10 up to 0.120**
- Complete key rotation costs approximately **381–414 seconds per million vectors**
- Selective re-encryption introduces **no measurable additional per-query overhead** under the reported evaluation setting

| Dataset         | Dimension | Representative Result                                       |
| --------------- | --------: | ----------------------------------------------------------- |
| SIFT            |       128 | Recall@10 up to 0.879; interactive latency as low as 400 ms |
| RedCaps         |       512 | Recall@10 up to 0.120                                       |
| Key maintenance |         — | Full key rotation ~381–414 s per million vectors            |

For complete tables and plots, see [`RESULTS.md`](./RESULTS.md).

---

## What This Project Demonstrates

This repository demonstrates more than a secure storage wrapper. It shows the implementation of a system that combines:

- **ANN retrieval engineering** — geometric routing preserved under encryption
- **Persistent metadata management** — RocksDB-backed index state
- **Forward-secure cryptographic maintenance** — key evolution without index rebuild
- **Bounded decrypt-and-refine querying** — controlled cryptographic work per query
- **Experiment orchestration and evaluation** — profile-driven runners and dataset sweeps
- **Integration and adversarial testing** — end-to-end correctness and security validation

This makes it relevant not only to secure retrieval research, but also to production-oriented AI infrastructure roles where vector search, retrieval systems, and privacy-aware design matter.

---

## Current Limitations

- Query-time cryptographic work remains significant compared with plaintext ANN systems
- The design is not fully oblivious and leaks routing-side access patterns
- The current implementation is CPU-oriented
- Performance and security trade-offs remain configuration-sensitive
- This is a specialised secure ANN architecture, not a drop-in replacement for mainstream vector databases

---

## Future Work

- Crypto acceleration for query-time decryption
- Tighter candidate-reduction and adaptive probing strategies
- Hybrid routing with stronger ANN backends
- GPU-assisted refinement
- Broader vector-database integration
- Stronger leakage mitigation and obliviousness extensions
- Production-facing API and deployment wrappers

---

## Thesis Context

This repository is closely aligned with the system described in the thesis on forward-secure privacy-preserving ANN retrieval over high-dimensional data. The thesis formalises the architectural principles, describes the implementation in detail, and reports the full evaluation used to validate the system design.

---

## Summary

FSPANN is a systems-oriented implementation of encrypted ANN retrieval with forward-secure key evolution. Its central contribution is not merely that vectors are encrypted, but that cryptographic maintenance is introduced **without destabilising the geometric routing layer** required for practical ANN search.

That separation is the core reason this project is technically differentiated from both plaintext ANN engines and conventional secure retrieval designs.
