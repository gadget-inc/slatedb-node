# slatedb-node

High-performance Node.js bindings for [SlateDB](https://github.com/slatedb/slatedb) implemented as a native Rust extension using `napi-rs`.

- Native extension (no subprocess bridge)
- Async TypeScript API
- Supports SlateDB DB/Reader/Snapshot/Transaction/Admin primitives
- Compiled with `slatedb` feature `all` (object store + compression feature set)

## Installation

```bash
pnpm add slatedb-node
```

## Quick Start

```ts
import { WriteBatch, open } from "slatedb-node";

const db = await open({ path: "example-db", url: "memory:///" });

await db.put("user:1", "alice");
console.log((await db.get("user:1"))?.toString());

const batch = new WriteBatch();
batch.put("user:2", "bob");
batch.delete("user:1");
await db.write(batch);

const iter = await db.scanPrefix("user:");
for await (const [key, value] of iter) {
  console.log(key.toString(), value.toString());
}

await db.close();
```

## Opening Databases

```ts
import { open } from "slatedb-node";

const db = await open({
  path: "my-db",
  url: "file:///tmp/slatedb-store",
  settingsPath: "./SlateDb.toml", // optional
  settings: {
    flush_interval: "50ms",
    manifest_poll_interval: "1s",
  }, // optional object overrides
});
```

## Reader and Admin APIs

```ts
import { open, openAdmin, openReader } from "slatedb-node";

const db = await open({ path: "my-db", url: "file:///tmp/slatedb-store" });
const checkpoint = await db.createCheckpoint("durable");

const reader = await openReader({
  path: "my-db",
  url: "file:///tmp/slatedb-store",
  checkpointId: checkpoint.id,
});

const admin = await openAdmin({ path: "my-db", url: "file:///tmp/slatedb-store" });
console.log(await admin.listCheckpoints());

await reader.close();
await db.close();
```

## Errors

Errors are mapped to typed classes:

- `TransactionError`
- `ClosedError`
- `UnavailableError`
- `InvalidError`
- `DataError`
- `InternalError`

## Development

Requirements:

- Node.js 20+
- Rust toolchain
- `pnpm`

Commands:

```bash
pnpm install
pnpm run build
pnpm test
```

## Prebuilt Binaries

The package is configured for `@napi-rs/cli` prebuilt targets:

- macOS x64 / arm64
- Linux x64 / arm64 (gnu + musl)
- Windows x64 / arm64 (msvc)

CI can build and publish artifacts with `napi prepublish -t npm`.
