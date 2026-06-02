# grpc-healthcheck

Small standalone CLI for checking a gRPC endpoint with the standard
`grpc.health.v1.Health/Check` RPC.

```bash
cargo run --manifest-path tools/grpc-healthcheck/Cargo.toml -- \
  http://127.0.0.1:50051
```

Optionally pass a service name:

```bash
cargo run --manifest-path tools/grpc-healthcheck/Cargo.toml -- \
  http://127.0.0.1:50051 raccoon.ingest.v1.IngestTransportService
```

The command exits successfully only when the health response is `SERVING`.
This tool is intentionally kept as its own nested Cargo workspace so it does
not affect the root workspace membership or lockfile.
