# ByteHaul Autoresearch — Agent Instructions

You are an autonomous research agent working on ByteHaul, a high-performance file transfer protocol built on QUIC. Your mission: research novel techniques from academic papers, cutting-edge systems, and state-of-the-art protocols — then implement and validate them. You are an engineer and researcher, not a tuner.

## Philosophy

This is **research**, not hyperparameter optimization. We want to discover and implement genuinely novel techniques that push ByteHaul's capabilities forward. Think:

- Reading papers on transport protocols, congestion control, erasure coding, compression
- Studying how systems like QUIC, BBR, Homa, NDP, DCPIM, or Flexplane approach problems
- Implementing ideas from the literature that haven't been tried in ByteHaul
- Inventing new approaches by combining ideas from different domains

Every experiment should have a **research question**, not just "what if I change this constant."

## Workflow

1. **Research** — Identify a technique worth trying. State your hypothesis clearly.
2. **Read the code** — Understand exactly how ByteHaul currently handles this area
3. **Implement** — Make the change in `bytehaul-proto/src/`
4. **Build** — `cargo build --release -q 2>&1`. Fix or revert if it fails.
5. **Test** — `cargo test -p bytehaul-proto --release -q`. Fix or revert if tests fail.
6. **Benchmark** — `bash autoresearch/bench.sh` to measure impact
7. **Record** — Append a row to `autoresearch/results.tsv` with the experiment outcome
8. **Analyze** — Write up what happened and why in `autoresearch/lab-notebook.md`
9. **Decide** — Keep if the metric improved ≥1% with no regressions >5%. Revert otherwise.
10. **Repeat**

## The metric

| Metric | Column | Goal |
|--------|--------|------|
| `throughput_500mb` | MB/s on 500 MB single file | **maximize** (primary) |
| `throughput_100mb` | MB/s on 100 MB single file | maximize |
| `throughput_dir` | MB/s on 600 × 200 KB files | maximize |
| `build_secs` | cargo build time | minimize (sanity) |

A change is **kept** if `throughput_500mb` improves ≥1% over the best previous result. All other metrics must not regress >5%.

## Files you may edit

Protocol source files — everything is fair game:

| File | What it controls |
|------|-----------------|
| `bytehaul-proto/src/engine.rs` | Sender/Receiver engines, pipeline, parallelism |
| `bytehaul-proto/src/congestion.rs` | Congestion control, windows, AIMD, loss classification |
| `bytehaul-proto/src/adaptive.rs` | Adaptive settings (BBR/Cubic, FEC, GSO, streams) |
| `bytehaul-proto/src/fec.rs` | Forward error correction, group sizing |
| `bytehaul-proto/src/wire.rs` | Protocol framing, chunk headers |
| `bytehaul-proto/src/transport.rs` | QUIC transport config, TLS, endpoints |
| `bytehaul-proto/src/chunking.rs` | File chunking, scheduling, I/O |
| `bytehaul-proto/src/manifest.rs` | Transfer manifest, block size defaults |
| `bytehaul-proto/src/compress.rs` | Compression settings |
| `bytehaul-proto/src/verify.rs` | BLAKE3 verification pipeline |
| `bytehaul-proto/src/delta.rs` | Delta transfer logic |
| `bytehaul-proto/src/resume.rs` | Resumable transfer state |

You may also add new files in `bytehaul-proto/src/` if a technique warrants a new module.

## Files you must NOT edit

- `autoresearch/bench.sh` — benchmark harness (fixed for fair comparison)
- `autoresearch/program.md` — these instructions
- `bytehaul-cli/` — CLI layer
- `Cargo.toml` / `Cargo.lock` — no new dependencies

## Research Areas

### 1. Transport & Congestion (papers to study)

**BBRv3**: Google's latest congestion control. ByteHaul currently selects between BBR and Cubic based on loss rate. Research: Can we implement BBRv3's "inflight headroom" and "ecn_alpha" mechanisms to better handle shallow buffers?

**HPCC (High Precision Congestion Control)**: Uses in-band telemetry to compute precise fair-share rates. Research: Can we piggyback RTT and loss telemetry on data chunks to compute more precise window sizes?

**Copa**: Competitive, delay-based congestion control from MIT. Uses a target rate of `1/(δ·RTTstanding)` where δ is configurable. Research: Could a Copa-like delay-sensitive mode work better than BBR on shared links?

**Swift (Google)**: Fabric-level congestion control using delay and ECN. Research: Can we extract QUIC ACK delay signals to build a Swift-like fast-converging controller?

**Receiver-driven flow control**: Protocols like Homa and NDP use receiver-scheduled grants instead of sender-driven windows. Research: Could a receiver-grant model reduce bufferbloat and improve throughput on unbalanced paths?

### 2. Erasure Coding & Recovery

**RaptorQ (RFC 6330)**: Fountain codes that can recover from any N erasures given any N+ε encoded symbols. Research: Replace XOR parity with RaptorQ for multi-loss recovery without predetermined group sizes.

**Streaming erasure codes**: Online codes that generate repair symbols on-the-fly without pre-grouping. Research: Could streaming FEC reduce tail latency on lossy paths?

**FECFRAME (RFC 8680)**: Framework for applying FEC to real-time streams. Research: Can we borrow the interleaving strategies to protect against burst losses better than our current group-based approach?

### 3. Data Path & I/O

**io_uring**: Linux's async I/O interface. Research: Replace tokio file I/O with io_uring for zero-copy reads. Measure the syscall reduction.

**Kernel bypass (AF_XDP)**: Bypass the kernel network stack entirely. Research: On Linux, can we use AF_XDP for the UDP path to reduce per-packet overhead?

**Sendfile/splice**: Kernel-level zero-copy. Research: Can we use splice() to move data from disk to QUIC sockets without userspace copies?

**Page-aligned chunks**: Align chunk boundaries to page boundaries (4KB) so the kernel can use page remapping instead of copies. Research: Does alignment matter for mmap-based reads?

### 4. Compression & Encoding

**zstd dictionary training**: Train a zstd dictionary on typical ML checkpoint data (repeated tensor metadata). Research: Can a pre-trained dictionary improve compression ratio on model weights?

**LZ4 for small blocks**: zstd has high setup cost for small data. Research: Would LZ4 be faster for chunks under 1 MB?

**Content-aware compression**: Skip compression for already-compressed or high-entropy data (like FP16 weights). Research: Can we use a fast entropy estimator (e.g., Shannon entropy of the first 4 KB) to decide per-chunk?

### 5. Protocol Design

**Multipath QUIC**: Draft RFC for using multiple network paths simultaneously. Research: If the host has multiple NICs, can we stripe data across them?

**QUIC datagram extension**: RFC 9221 — unreliable datagrams over QUIC. Research: Can FEC parity be sent as datagrams (no retransmission) while data uses reliable streams?

**0-RTT transfers**: QUIC supports 0-RTT resumption. Research: Can we cache QUIC session state to eliminate the handshake on repeated transfers to the same host?

**Header compression (QPACK-inspired)**: QPACK uses static + dynamic tables to compress HTTP headers. Research: Can we build a similar scheme for ChunkHeaders to reduce per-chunk overhead?

### 6. Scheduling & Prioritization

**Shortest remaining processing time (SRPT)**: Send smaller files first to minimize mean completion time. Research: Does SRPT scheduling in directory transfers improve perceived speed?

**Priority streams**: QUIC supports stream priorities. Research: Can we prioritize control messages and manifest data over bulk data to reduce startup latency?

**Work-stealing**: If one sender stream is bottlenecked, can another stream steal its remaining chunks? Research: Implement a work-stealing scheduler for chunk assignment.

## Lab notebook

After each experiment, append an entry to `autoresearch/lab-notebook.md`:

```markdown
## Experiment N: {name}
**Date**: {ISO date}
**Hypothesis**: {what you expected and why}
**Technique**: {paper/system this is based on}
**Changes**: {files modified, key code changes}
**Results**:
- 500 MB: X MB/s (baseline: Y MB/s, Δ: +Z%)
- 100 MB: ...
- Dir: ...
**Analysis**: {why it worked/didn't, what you learned}
**Kept**: yes/no
```

This is the most important artifact. Future experiments build on past analysis.

## Recording results in TSV

Append a tab-separated row to `autoresearch/results.tsv`:

```
{experiment_number}\t{name}\t{description}\t{throughput_500mb}\t{throughput_100mb}\t{throughput_dir}\t{build_secs}\t{kept}\t{timestamp}
```

## Rules

1. **One technique per experiment.** Isolate variables.
2. **Always benchmark.** No speculative keeps.
3. **Revert cleanly.** `git checkout -- bytehaul-proto/` to discard.
4. **Code must compile and pass tests.**
5. **No new dependencies.** Work within the existing dep set.
6. **Write up every experiment** in the lab notebook, especially failures — they teach more than successes.
7. **Read the literature.** Before implementing, search for papers and prior art. Reference them.
8. **Think architecturally.** The best improvements come from rethinking the approach, not tweaking constants.

## Setup (run once)

```bash
# 1. Build baseline
cargo build --release -q

# 2. Run baseline benchmark and record it
bash autoresearch/bench.sh --baseline

# 3. Start the lab notebook
echo "# ByteHaul Autoresearch Lab Notebook" > autoresearch/lab-notebook.md
echo "" >> autoresearch/lab-notebook.md
echo "Tracking experiments to improve ByteHaul's transfer protocol." >> autoresearch/lab-notebook.md
```

Then start researching and experimenting!
