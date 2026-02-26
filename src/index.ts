/* eslint-disable func-style, @typescript-eslint/consistent-type-definitions */
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const native = require("../native.cjs") as NativeModule;

type Binary = Buffer | Uint8Array | string;

type NativeErrorClass = new (message: string, cause?: unknown) => SlateDBError;

interface NativeModule {
  openNative(options: NativeOpenOptions): Promise<NativeSlateDB>;
  openReaderNative(options: NativeReaderOpenOptions): Promise<NativeSlateDBReader>;
  openAdminNative(options: NativeAdminOpenOptions): Promise<NativeSlateDBAdmin>;
  NativeWriteBatch: new () => NativeWriteBatch;
}

interface NativeOpenOptions {
  path: string;
  url?: string;
  envFile?: string;
  settingsPath?: string;
  settings?: Record<string, unknown>;
}

interface NativeReaderOpenOptions {
  path: string;
  url?: string;
  envFile?: string;
  checkpointId?: string;
  manifestPollIntervalMs?: number;
  checkpointLifetimeMs?: number;
  maxMemtableBytes?: number;
  skipWalReplay?: boolean;
  options?: Record<string, unknown>;
}

interface NativeAdminOpenOptions {
  path: string;
  url?: string;
  envFile?: string;
  walUrl?: string;
}

interface NativeReadOptions {
  durabilityFilter?: "memory" | "remote";
  dirty?: boolean;
  cacheBlocks?: boolean;
}

interface NativeScanOptions extends NativeReadOptions {
  readAheadBytes?: number;
  maxFetchTasks?: number;
}

interface NativeWriteOptions {
  awaitDurable?: boolean;
}

interface NativePutOptions {
  ttlMs?: number;
  noExpiry?: boolean;
}

interface NativeMergeOptions {
  ttlMs?: number;
  noExpiry?: boolean;
}

interface NativeCheckpointOptions {
  lifetimeMs?: number;
  source?: string;
  name?: string;
}

interface NativeGcDirectoryOptions {
  intervalMs?: number;
  minAgeMs?: number;
}

interface NativeGcOptions {
  manifest?: NativeGcDirectoryOptions;
  wal?: NativeGcDirectoryOptions;
  compacted?: NativeGcDirectoryOptions;
  compactions?: NativeGcDirectoryOptions;
}

interface NativeCompactionSource {
  sortedRun?: number;
  sst?: string;
}

interface NativeCompactionSpec {
  sources: NativeCompactionSource[];
  destination: number;
}

interface NativeKeyValue {
  key: Buffer;
  value: Buffer;
}

interface NativeWriteHandle {
  seq: number;
  createTs: number;
}

interface NativeCheckpointCreateResult {
  id: string;
  manifestId: number;
}

interface NativeCheckpoint {
  id: string;
  manifestId: number;
  expireTimeMs: number | null;
  createTimeMs: number;
  name?: string;
}

interface NativeSlateDBIterator {
  next(): Promise<NativeKeyValue | null>;
  seek(key: Buffer): Promise<void>;
  close(): Promise<void>;
}

interface NativeWriteBatch {
  put(key: Buffer, value: Buffer): void;
  putWithOptions(key: Buffer, value: Buffer, options?: NativePutOptions): void;
  delete(key: Buffer): void;
  merge(key: Buffer, value: Buffer): void;
  mergeWithOptions(key: Buffer, value: Buffer, options?: NativeMergeOptions): void;
}

interface NativeSlateDBSnapshot {
  get(key: Buffer): Promise<Buffer | null>;
  getWithOptions(key: Buffer, options?: NativeReadOptions): Promise<Buffer | null>;
  scan(start: Buffer, end?: Buffer): Promise<NativeSlateDBIterator>;
  scanWithOptions(start: Buffer, end?: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  scanPrefix(prefix: Buffer): Promise<NativeSlateDBIterator>;
  scanPrefixWithOptions(prefix: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  close(): Promise<void>;
}

interface NativeSlateDBTransaction {
  get(key: Buffer): Promise<Buffer | null>;
  getWithOptions(key: Buffer, options?: NativeReadOptions): Promise<Buffer | null>;
  scan(start: Buffer, end?: Buffer): Promise<NativeSlateDBIterator>;
  scanWithOptions(start: Buffer, end?: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  scanPrefix(prefix: Buffer): Promise<NativeSlateDBIterator>;
  scanPrefixWithOptions(prefix: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  put(key: Buffer, value: Buffer): Promise<void>;
  putWithOptions(key: Buffer, value: Buffer, options?: NativePutOptions): Promise<void>;
  delete(key: Buffer): Promise<void>;
  merge(key: Buffer, value: Buffer): Promise<void>;
  mergeWithOptions(key: Buffer, value: Buffer, options?: NativeMergeOptions): Promise<void>;
  markRead(keys: Buffer[]): Promise<void>;
  unmarkWrite(keys: Buffer[]): Promise<void>;
  commit(writeOptions?: NativeWriteOptions): Promise<NativeWriteHandle | null>;
  rollback(): Promise<void>;
  close(): Promise<void>;
}

interface NativeSlateDBReader {
  get(key: Buffer): Promise<Buffer | null>;
  getWithOptions(key: Buffer, options?: NativeReadOptions): Promise<Buffer | null>;
  scan(start: Buffer, end?: Buffer): Promise<NativeSlateDBIterator>;
  scanWithOptions(start: Buffer, end?: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  scanPrefix(prefix: Buffer): Promise<NativeSlateDBIterator>;
  scanPrefixWithOptions(prefix: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  close(): Promise<void>;
}

interface NativeSlateDBAdmin {
  readManifest(id?: number): Promise<string | null>;
  listManifests(start?: number, end?: number): Promise<string>;
  readCompactions(id?: number): Promise<string | null>;
  listCompactions(start?: number, end?: number): Promise<string>;
  readCompaction(id: string, compactionsId?: number): Promise<string | null>;
  submitCompaction(spec: NativeCompactionSpec): Promise<string>;
  createCheckpoint(options?: NativeCheckpointOptions): Promise<NativeCheckpointCreateResult>;
  listCheckpoints(name?: string): Promise<NativeCheckpoint[]>;
  refreshCheckpoint(id: string, lifetimeMs?: number): Promise<void>;
  deleteCheckpoint(id: string): Promise<void>;
  getTimestampForSequence(seq: number, roundUp?: boolean): Promise<number | null>;
  getSequenceForTimestamp(timestampMs: number, roundUp?: boolean): Promise<number | null>;
  createClone(parentPath: string, parentCheckpoint?: string): Promise<void>;
  runGcOnce(options?: NativeGcOptions): Promise<void>;
  runGc(options?: NativeGcOptions): Promise<void>;
}

interface NativeSlateDB {
  get(key: Buffer): Promise<Buffer | null>;
  getWithOptions(key: Buffer, options?: NativeReadOptions): Promise<Buffer | null>;
  put(key: Buffer, value: Buffer): Promise<NativeWriteHandle>;
  putWithOptions(key: Buffer, value: Buffer, putOptions?: NativePutOptions, writeOptions?: NativeWriteOptions): Promise<NativeWriteHandle>;
  delete(key: Buffer): Promise<NativeWriteHandle>;
  deleteWithOptions(key: Buffer, writeOptions?: NativeWriteOptions): Promise<NativeWriteHandle>;
  merge(key: Buffer, value: Buffer): Promise<NativeWriteHandle>;
  mergeWithOptions(
    key: Buffer,
    value: Buffer,
    mergeOptions?: NativeMergeOptions,
    writeOptions?: NativeWriteOptions,
  ): Promise<NativeWriteHandle>;
  write(batch: NativeWriteBatch): Promise<NativeWriteHandle>;
  writeWithOptions(batch: NativeWriteBatch, writeOptions?: NativeWriteOptions): Promise<NativeWriteHandle>;
  scan(start: Buffer, end?: Buffer): Promise<NativeSlateDBIterator>;
  scanWithOptions(start: Buffer, end?: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  scanPrefix(prefix: Buffer): Promise<NativeSlateDBIterator>;
  scanPrefixWithOptions(prefix: Buffer, options?: NativeScanOptions): Promise<NativeSlateDBIterator>;
  snapshot(): Promise<NativeSlateDBSnapshot>;
  begin(isolation?: string): Promise<NativeSlateDBTransaction>;
  flush(): Promise<void>;
  flushWithOptions(flushType?: "wal" | "memtable"): Promise<void>;
  metrics(): Promise<Record<string, number>>;
  createCheckpoint(scope?: "all" | "durable", options?: NativeCheckpointOptions): Promise<NativeCheckpointCreateResult>;
  status(): Promise<void>;
  close(): Promise<void>;
}

function toBuffer(value: Binary): Buffer {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (typeof value === "string") {
    return Buffer.from(value);
  }
  return Buffer.from(value);
}

function mapError(error: unknown): Error {
  const message = error instanceof Error ? error.message : String(error);

  for (const [prefix, ErrorClass] of errorClassMap) {
    if (message.startsWith(prefix)) {
      return new ErrorClass(message.slice(prefix.length).trim(), error);
    }
  }

  if (error instanceof Error) {
    return error;
  }

  return new SlateDBError(message, error);
}

async function call<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    throw mapError(error);
  }
}

function mapWriteHandle(handle: NativeWriteHandle): WriteHandle {
  return {
    seq: handle.seq,
    createTs: handle.createTs,
  };
}

function mapCheckpointResult(result: NativeCheckpointCreateResult): CheckpointCreateResult {
  return {
    id: result.id,
    manifestId: result.manifestId,
  };
}

function mapCheckpoint(checkpoint: NativeCheckpoint): Checkpoint {
  return {
    id: checkpoint.id,
    manifestId: checkpoint.manifestId,
    expireTimeMs: checkpoint.expireTimeMs,
    createTimeMs: checkpoint.createTimeMs,
    name: checkpoint.name,
  };
}

export class SlateDBError extends Error {
  public cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = new.target.name;
    this.cause = cause;
  }
}

export class TransactionError extends SlateDBError {}
export class ClosedError extends SlateDBError {}
export class UnavailableError extends SlateDBError {}
export class InvalidError extends SlateDBError {}
export class DataError extends SlateDBError {}
export class InternalError extends SlateDBError {}

const errorClassMap: Array<[string, NativeErrorClass]> = [
  ["TransactionError:", TransactionError],
  ["ClosedError:", ClosedError],
  ["UnavailableError:", UnavailableError],
  ["InvalidError:", InvalidError],
  ["DataError:", DataError],
  ["InternalError:", InternalError],
];

export interface ReadOptions {
  durabilityFilter?: "memory" | "remote";
  dirty?: boolean;
  cacheBlocks?: boolean;
}

export interface ScanOptions extends ReadOptions {
  readAheadBytes?: number;
  maxFetchTasks?: number;
}

export interface WriteOptions {
  awaitDurable?: boolean;
}

export interface PutOptions {
  ttlMs?: number;
  noExpiry?: boolean;
}

export interface MergeOptions {
  ttlMs?: number;
  noExpiry?: boolean;
}

export interface CheckpointOptions {
  lifetimeMs?: number;
  source?: string;
  name?: string;
}

export interface GcDirectoryOptions {
  intervalMs?: number;
  minAgeMs?: number;
}

export interface GcOptions {
  manifest?: GcDirectoryOptions;
  wal?: GcDirectoryOptions;
  compacted?: GcDirectoryOptions;
  compactions?: GcDirectoryOptions;
}

export interface CompactionSource {
  sortedRun?: number;
  sst?: string;
}

export interface CompactionSpec {
  sources: CompactionSource[];
  destination: number;
}

export interface WriteHandle {
  seq: number;
  createTs: number;
}

export interface CheckpointCreateResult {
  id: string;
  manifestId: number;
}

export interface Checkpoint {
  id: string;
  manifestId: number;
  expireTimeMs: number | null;
  createTimeMs: number;
  name?: string;
}

export interface SlateDBOpenOptions {
  path: string;
  url?: string;
  envFile?: string;
  settingsPath?: string;
  settings?: Record<string, unknown>;
}

export interface SlateDBReaderOpenOptions {
  path: string;
  url?: string;
  envFile?: string;
  checkpointId?: string;
  manifestPollIntervalMs?: number;
  checkpointLifetimeMs?: number;
  maxMemtableBytes?: number;
  skipWalReplay?: boolean;
  options?: Record<string, unknown>;
}

export interface SlateDBAdminOpenOptions {
  path: string;
  url?: string;
  envFile?: string;
  walUrl?: string;
}

export class SlateDBIterator implements AsyncIterable<[Buffer, Buffer]> {
  private readonly native: NativeSlateDBIterator;

  constructor(nativeIterator: NativeSlateDBIterator) {
    this.native = nativeIterator;
  }

  async next(): Promise<[Buffer, Buffer] | null> {
    const kv = await call(() => this.native.next());
    if (kv == null) {
      return null;
    }
    return [kv.key, kv.value];
  }

  async seek(key: Binary): Promise<void> {
    await call(() => this.native.seek(toBuffer(key)));
  }

  async close(): Promise<void> {
    await call(() => this.native.close());
  }

  [Symbol.asyncIterator](): AsyncIterator<[Buffer, Buffer]> {
    return {
      next: async () => {
        const value = await this.next();
        if (value == null) {
          return {
            done: true,
            value: undefined,
          };
        }
        return {
          done: false,
          value,
        };
      },
    };
  }
}

export class WriteBatch {
  private readonly native: NativeWriteBatch;

  constructor() {
    this.native = new native.NativeWriteBatch();
  }

  put(key: Binary, value: Binary): void {
    this.native.put(toBuffer(key), toBuffer(value));
  }

  putWithOptions(key: Binary, value: Binary, options?: PutOptions): void {
    this.native.putWithOptions(toBuffer(key), toBuffer(value), options);
  }

  delete(key: Binary): void {
    this.native.delete(toBuffer(key));
  }

  merge(key: Binary, value: Binary): void {
    this.native.merge(toBuffer(key), toBuffer(value));
  }

  mergeWithOptions(key: Binary, value: Binary, options?: MergeOptions): void {
    this.native.mergeWithOptions(toBuffer(key), toBuffer(value), options);
  }

  getNative(): NativeWriteBatch {
    return this.native;
  }
}

export class SlateDBSnapshot {
  private readonly native: NativeSlateDBSnapshot;

  constructor(nativeSnapshot: NativeSlateDBSnapshot) {
    this.native = nativeSnapshot;
  }

  async get(key: Binary): Promise<Buffer | null> {
    return call(() => this.native.get(toBuffer(key)));
  }

  async getWithOptions(key: Binary, options?: ReadOptions): Promise<Buffer | null> {
    return call(() => this.native.getWithOptions(toBuffer(key), options));
  }

  async scan(start: Binary, end?: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scan(toBuffer(start), end == null ? undefined : toBuffer(end)));
    return new SlateDBIterator(iterator);
  }

  async scanWithOptions(start: Binary, end?: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanWithOptions(toBuffer(start), end == null ? undefined : toBuffer(end), options));
    return new SlateDBIterator(iterator);
  }

  async scanPrefix(prefix: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefix(toBuffer(prefix)));
    return new SlateDBIterator(iterator);
  }

  async scanPrefixWithOptions(prefix: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefixWithOptions(toBuffer(prefix), options));
    return new SlateDBIterator(iterator);
  }

  async close(): Promise<void> {
    await call(() => this.native.close());
  }
}

export class SlateDBTransaction {
  private readonly native: NativeSlateDBTransaction;

  constructor(nativeTransaction: NativeSlateDBTransaction) {
    this.native = nativeTransaction;
  }

  async get(key: Binary): Promise<Buffer | null> {
    return call(() => this.native.get(toBuffer(key)));
  }

  async getWithOptions(key: Binary, options?: ReadOptions): Promise<Buffer | null> {
    return call(() => this.native.getWithOptions(toBuffer(key), options));
  }

  async scan(start: Binary, end?: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scan(toBuffer(start), end == null ? undefined : toBuffer(end)));
    return new SlateDBIterator(iterator);
  }

  async scanWithOptions(start: Binary, end?: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanWithOptions(toBuffer(start), end == null ? undefined : toBuffer(end), options));
    return new SlateDBIterator(iterator);
  }

  async scanPrefix(prefix: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefix(toBuffer(prefix)));
    return new SlateDBIterator(iterator);
  }

  async scanPrefixWithOptions(prefix: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefixWithOptions(toBuffer(prefix), options));
    return new SlateDBIterator(iterator);
  }

  async put(key: Binary, value: Binary): Promise<void> {
    await call(() => this.native.put(toBuffer(key), toBuffer(value)));
  }

  async putWithOptions(key: Binary, value: Binary, options?: PutOptions): Promise<void> {
    await call(() => this.native.putWithOptions(toBuffer(key), toBuffer(value), options));
  }

  async delete(key: Binary): Promise<void> {
    await call(() => this.native.delete(toBuffer(key)));
  }

  async merge(key: Binary, value: Binary): Promise<void> {
    await call(() => this.native.merge(toBuffer(key), toBuffer(value)));
  }

  async mergeWithOptions(key: Binary, value: Binary, options?: MergeOptions): Promise<void> {
    await call(() => this.native.mergeWithOptions(toBuffer(key), toBuffer(value), options));
  }

  async markRead(keys: Binary[]): Promise<void> {
    await call(() => this.native.markRead(keys.map(toBuffer)));
  }

  async unmarkWrite(keys: Binary[]): Promise<void> {
    await call(() => this.native.unmarkWrite(keys.map(toBuffer)));
  }

  async commit(writeOptions?: WriteOptions): Promise<WriteHandle | null> {
    const handle = await call(() => this.native.commit(writeOptions));
    return handle == null ? null : mapWriteHandle(handle);
  }

  async rollback(): Promise<void> {
    await call(() => this.native.rollback());
  }

  async close(): Promise<void> {
    await call(() => this.native.close());
  }
}

export class SlateDBReader {
  private readonly native: NativeSlateDBReader;

  constructor(nativeReader: NativeSlateDBReader) {
    this.native = nativeReader;
  }

  async get(key: Binary): Promise<Buffer | null> {
    return call(() => this.native.get(toBuffer(key)));
  }

  async getWithOptions(key: Binary, options?: ReadOptions): Promise<Buffer | null> {
    return call(() => this.native.getWithOptions(toBuffer(key), options));
  }

  async scan(start: Binary, end?: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scan(toBuffer(start), end == null ? undefined : toBuffer(end)));
    return new SlateDBIterator(iterator);
  }

  async scanWithOptions(start: Binary, end?: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanWithOptions(toBuffer(start), end == null ? undefined : toBuffer(end), options));
    return new SlateDBIterator(iterator);
  }

  async scanPrefix(prefix: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefix(toBuffer(prefix)));
    return new SlateDBIterator(iterator);
  }

  async scanPrefixWithOptions(prefix: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefixWithOptions(toBuffer(prefix), options));
    return new SlateDBIterator(iterator);
  }

  async close(): Promise<void> {
    await call(() => this.native.close());
  }
}

export class SlateDBAdmin {
  private readonly native: NativeSlateDBAdmin;

  constructor(nativeAdmin: NativeSlateDBAdmin) {
    this.native = nativeAdmin;
  }

  async readManifest(id?: number): Promise<string | null> {
    return call(() => this.native.readManifest(id));
  }

  async listManifests(start?: number, end?: number): Promise<string> {
    return call(() => this.native.listManifests(start, end));
  }

  async readCompactions(id?: number): Promise<string | null> {
    return call(() => this.native.readCompactions(id));
  }

  async listCompactions(start?: number, end?: number): Promise<string> {
    return call(() => this.native.listCompactions(start, end));
  }

  async readCompaction(id: string, compactionsId?: number): Promise<string | null> {
    return call(() => this.native.readCompaction(id, compactionsId));
  }

  async submitCompaction(spec: CompactionSpec): Promise<string> {
    return call(() => this.native.submitCompaction(spec));
  }

  async createCheckpoint(options?: CheckpointOptions): Promise<CheckpointCreateResult> {
    const result = await call(() => this.native.createCheckpoint(options));
    return mapCheckpointResult(result);
  }

  async listCheckpoints(name?: string): Promise<Checkpoint[]> {
    const checkpoints = await call(() => this.native.listCheckpoints(name));
    return checkpoints.map(mapCheckpoint);
  }

  async refreshCheckpoint(id: string, lifetimeMs?: number): Promise<void> {
    await call(() => this.native.refreshCheckpoint(id, lifetimeMs));
  }

  async deleteCheckpoint(id: string): Promise<void> {
    await call(() => this.native.deleteCheckpoint(id));
  }

  async getTimestampForSequence(seq: number, roundUp?: boolean): Promise<number | null> {
    return call(() => this.native.getTimestampForSequence(seq, roundUp));
  }

  async getSequenceForTimestamp(timestampMs: number, roundUp?: boolean): Promise<number | null> {
    return call(() => this.native.getSequenceForTimestamp(timestampMs, roundUp));
  }

  async createClone(parentPath: string, parentCheckpoint?: string): Promise<void> {
    await call(() => this.native.createClone(parentPath, parentCheckpoint));
  }

  async runGcOnce(options?: GcOptions): Promise<void> {
    await call(() => this.native.runGcOnce(options));
  }

  async runGc(options?: GcOptions): Promise<void> {
    await call(() => this.native.runGc(options));
  }
}

export class SlateDB {
  private readonly native: NativeSlateDB;

  constructor(nativeDb: NativeSlateDB) {
    this.native = nativeDb;
  }

  static async open(options: SlateDBOpenOptions): Promise<SlateDB> {
    return open(options);
  }

  async get(key: Binary): Promise<Buffer | null> {
    return call(() => this.native.get(toBuffer(key)));
  }

  async getWithOptions(key: Binary, options?: ReadOptions): Promise<Buffer | null> {
    return call(() => this.native.getWithOptions(toBuffer(key), options));
  }

  async put(key: Binary, value: Binary): Promise<WriteHandle> {
    const handle = await call(() => this.native.put(toBuffer(key), toBuffer(value)));
    return mapWriteHandle(handle);
  }

  async putWithOptions(key: Binary, value: Binary, putOptions?: PutOptions, writeOptions?: WriteOptions): Promise<WriteHandle> {
    const handle = await call(() => this.native.putWithOptions(toBuffer(key), toBuffer(value), putOptions, writeOptions));
    return mapWriteHandle(handle);
  }

  async delete(key: Binary): Promise<WriteHandle> {
    const handle = await call(() => this.native.delete(toBuffer(key)));
    return mapWriteHandle(handle);
  }

  async deleteWithOptions(key: Binary, writeOptions?: WriteOptions): Promise<WriteHandle> {
    const handle = await call(() => this.native.deleteWithOptions(toBuffer(key), writeOptions));
    return mapWriteHandle(handle);
  }

  async merge(key: Binary, value: Binary): Promise<WriteHandle> {
    const handle = await call(() => this.native.merge(toBuffer(key), toBuffer(value)));
    return mapWriteHandle(handle);
  }

  async mergeWithOptions(key: Binary, value: Binary, mergeOptions?: MergeOptions, writeOptions?: WriteOptions): Promise<WriteHandle> {
    const handle = await call(() => this.native.mergeWithOptions(toBuffer(key), toBuffer(value), mergeOptions, writeOptions));
    return mapWriteHandle(handle);
  }

  async write(batch: WriteBatch): Promise<WriteHandle> {
    const handle = await call(() => this.native.write(batch.getNative()));
    return mapWriteHandle(handle);
  }

  async writeWithOptions(batch: WriteBatch, writeOptions?: WriteOptions): Promise<WriteHandle> {
    const handle = await call(() => this.native.writeWithOptions(batch.getNative(), writeOptions));
    return mapWriteHandle(handle);
  }

  async scan(start: Binary, end?: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scan(toBuffer(start), end == null ? undefined : toBuffer(end)));
    return new SlateDBIterator(iterator);
  }

  async scanWithOptions(start: Binary, end?: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanWithOptions(toBuffer(start), end == null ? undefined : toBuffer(end), options));
    return new SlateDBIterator(iterator);
  }

  async scanPrefix(prefix: Binary): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefix(toBuffer(prefix)));
    return new SlateDBIterator(iterator);
  }

  async scanPrefixWithOptions(prefix: Binary, options?: ScanOptions): Promise<SlateDBIterator> {
    const iterator = await call(() => this.native.scanPrefixWithOptions(toBuffer(prefix), options));
    return new SlateDBIterator(iterator);
  }

  async snapshot(): Promise<SlateDBSnapshot> {
    const snapshot = await call(() => this.native.snapshot());
    return new SlateDBSnapshot(snapshot);
  }

  async begin(isolation?: "si" | "snapshot" | "ssi" | "serializable" | "serializable_snapshot"): Promise<SlateDBTransaction> {
    const txn = await call(() => this.native.begin(isolation));
    return new SlateDBTransaction(txn);
  }

  async flush(): Promise<void> {
    await call(() => this.native.flush());
  }

  async flushWithOptions(flushType?: "wal" | "memtable"): Promise<void> {
    await call(() => this.native.flushWithOptions(flushType));
  }

  async metrics(): Promise<Record<string, number>> {
    return call(() => this.native.metrics());
  }

  async createCheckpoint(scope?: "all" | "durable", options?: CheckpointOptions): Promise<CheckpointCreateResult> {
    const result = await call(() => this.native.createCheckpoint(scope, options));
    return mapCheckpointResult(result);
  }

  async status(): Promise<void> {
    await call(() => this.native.status());
  }

  async close(): Promise<void> {
    await call(() => this.native.close());
  }
}

export async function open(options: SlateDBOpenOptions): Promise<SlateDB> {
  const db = await call(() => native.openNative(options));
  return new SlateDB(db);
}

export async function openReader(options: SlateDBReaderOpenOptions): Promise<SlateDBReader> {
  const reader = await call(() => native.openReaderNative(options));
  return new SlateDBReader(reader);
}

export async function openAdmin(options: SlateDBAdminOpenOptions): Promise<SlateDBAdmin> {
  const admin = await call(() => native.openAdminNative(options));
  return new SlateDBAdmin(admin);
}
