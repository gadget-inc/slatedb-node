import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { ClosedError, InvalidError, SlateDB, WriteBatch, open, openAdmin, openReader } from "../src/index.ts";

const tempPaths: string[] = [];

const createTempDir = async (prefix: string): Promise<string> => {
  const path = await mkdtemp(join(tmpdir(), prefix));
  tempPaths.push(path);
  return path;
};

const toFileUrl = (path: string): string => `file://${path}`;

afterEach(async () => {
  await Promise.all(
    tempPaths.splice(0, tempPaths.length).map(async (path) => {
      await rm(path, { recursive: true, force: true });
    }),
  );
});

describe("slatedb-node bindings", () => {
  it("opens with open() and performs CRUD", async () => {
    const db = await open({ path: "crud-db", url: "memory:///" });

    await db.put("user:1", "alice");
    expect((await db.get("user:1"))?.toString()).toBe("alice");

    await db.delete("user:1");
    expect(await db.get("user:1")).toBeNull();

    await db.close();
    await expect(db.get("user:1")).rejects.toBeInstanceOf(ClosedError);
  });

  it("supports batched writes and iterator seek", async () => {
    const db = await SlateDB.open({ path: "batch-db", url: "memory:///" });

    const batch = new WriteBatch();
    batch.put("k1", "v1");
    batch.put("k2", "v2");
    batch.put("k3", "v3");
    await db.write(batch);

    const iterator = await db.scanPrefix("k");
    expect(await iterator.next()).toEqual([Buffer.from("k1"), Buffer.from("v1")]);

    await iterator.seek("k3");
    expect(await iterator.next()).toEqual([Buffer.from("k3"), Buffer.from("v3")]);
    expect(await iterator.next()).toBeNull();

    await iterator.close();
    await db.close();
  });

  it("loads settings from file and object", async () => {
    const settingsDir = await createTempDir("slatedb-node-settings-");
    const settingsPath = join(settingsDir, "SlateDb.json");
    await writeFile(
      settingsPath,
      JSON.stringify({
        flush_interval: "50ms",
        manifest_poll_interval: "100ms",
      }),
      "utf8",
    );

    const dbFromFile = await open({
      path: "settings-file-db",
      url: "memory:///",
      settingsPath,
    });
    await dbFromFile.put("file-key", "value");
    expect((await dbFromFile.get("file-key"))?.toString()).toBe("value");
    await dbFromFile.close();

    const dbFromObject = await open({
      path: "settings-object-db",
      url: "memory:///",
      settings: {
        flush_interval: "20ms",
        manifest_poll_interval: "100ms",
      },
    });
    await dbFromObject.put("obj-key", "value");
    expect((await dbFromObject.get("obj-key"))?.toString()).toBe("value");
    await dbFromObject.close();

    await expect(
      open({
        path: "bad-settings-db",
        url: "memory:///",
        settings: {
          flush_interval: 123,
        },
      }),
    ).rejects.toBeInstanceOf(InvalidError);
  });

  it("smokes reader and admin bindings against local file object store", async () => {
    const storeDir = await createTempDir("slatedb-node-store-");
    const url = toFileUrl(storeDir);

    const db = await open({ path: "shared-db", url });
    await db.put("user:1", "alice");
    const checkpoint = await db.createCheckpoint("durable");

    const reader = await openReader({
      path: "shared-db",
      url,
      checkpointId: checkpoint.id,
    });

    expect((await reader.get("user:1"))?.toString()).toBe("alice");

    const admin = await openAdmin({ path: "shared-db", url });
    const checkpoints = await admin.listCheckpoints();
    expect(checkpoints.some((item) => item.id === checkpoint.id)).toBe(true);

    const manifest = await admin.readManifest();
    expect(manifest === null || typeof manifest === "string").toBe(true);

    const seq = await admin.getSequenceForTimestamp(Date.now(), false);
    expect(seq === null || typeof seq === "number").toBe(true);

    await reader.close();
    await db.close();
  });

  it("smokes admin compaction and gc endpoints", async () => {
    const storeDir = await createTempDir("slatedb-node-admin-");
    const url = toFileUrl(storeDir);

    const db = await open({ path: "admin-db", url });
    await db.put("k", "v");
    await db.close();

    const admin = await openAdmin({ path: "admin-db", url });

    const submitted = await admin.submitCompaction({
      sources: [{ sortedRun: 3 }],
      destination: 3,
    });

    const parsedSubmitted = JSON.parse(submitted) as { id: string };
    expect(typeof parsedSubmitted.id).toBe("string");

    const readBack = await admin.readCompaction(parsedSubmitted.id);
    expect(readBack).not.toBeNull();

    await admin.runGcOnce({
      manifest: { minAgeMs: 0 },
      wal: { minAgeMs: 0 },
      compacted: { minAgeMs: 0 },
    });
  });

  it("maps invalid URL errors to InvalidError", async () => {
    await expect(open({ path: "bad-url-db", url: ":invalid:" })).rejects.toBeInstanceOf(InvalidError);
  });
});
