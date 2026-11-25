# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| `0.1.x` | Yes |

## Threat model and scope

`simraft-kv` implements the crash-fault Raft consensus model rather
than a Byzantine fault model. Cluster peers and client endpoints
operate inside a trusted network perimeter without built-in TLS
encryption or peer authentication. Production deployments that cross
untrusted networks must place peer and client TCP listeners behind
mutual TLS or an authenticated private network overlay.

Within that crash-fault boundary, every crate enforces defensive
resource and integrity bounds in safe Rust.

- All five crates compile with `#![forbid(unsafe_code)]`.
- Wire frames (`simraft-wire`), client TCP messages (`simraft-node`),
  and key-value command and snapshot payloads (`simraft-kv`) enforce
  a hard 16 MiB size ceiling before allocating payload buffers and
  reject trailing bytes.
- Peer sockets and client sockets in `simraft-node` enforce read and
  write timeouts and bounded internal channels so stalled or
  disconnected peers cannot exhaust memory.
- Uncommitted log entries and client deduplication sessions are
  bounded by `Config::max_uncommitted_entries` and
  `Config::max_sessions`.
- Wire frames, durable hard-state records, and durable log records
  carry domain-separated IEEE CRC-32 checksums, and snapshot payloads
  carry a domain-separated SHA-256 digest.

## Reporting a vulnerability

Do not open a public GitHub issue for suspected security
vulnerabilities, memory exhaustion bugs, consensus safety violations,
or panic paths reachable from untrusted network bytes. Report
vulnerabilities privately through GitHub Security Advisories on the
`david-engelmann/simraft-kv` repository with a minimal reproduction
or simulator seed when applicable. Reports receive an initial
response within seven days.
