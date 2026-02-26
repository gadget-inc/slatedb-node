#![deny(clippy::all)]
#![allow(clippy::too_many_arguments)]

use chrono::{DateTime, Utc};
use figment::{
  providers::Serialized,
  Figment,
};
use napi::{
  bindgen_prelude::*,
  Error as NapiError,
  Status,
};
use napi_derive::napi;
use slatedb::{
  admin::{
    load_object_store_from_env,
    Admin,
  },
  compactor::{
    CompactionSpec,
    SourceId,
  },
  config::{
    CheckpointOptions,
    CheckpointScope,
    DbReaderOptions,
    DurabilityLevel,
    FlushOptions,
    FlushType,
    GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions,
    MergeOptions,
    PutOptions,
    ReadOptions,
    ScanOptions,
    Settings,
    Ttl,
    WriteOptions,
  },
  object_store::ObjectStore,
  Db,
  DbIterator,
  DbReader,
  DbSnapshot,
  DbTransaction,
  Error as SlateError,
  ErrorKind,
  IsolationLevel,
  WriteBatch,
};
use std::{
  collections::BTreeMap,
  sync::Arc,
  time::Duration,
};
use tokio::sync::Mutex;
use ulid::Ulid;
use uuid::Uuid;

#[napi(object)]
pub struct OpenOptions {
  pub path: String,
  pub url: Option<String>,
  #[napi(js_name = "envFile")]
  pub env_file: Option<String>,
  #[napi(js_name = "settingsPath")]
  pub settings_path: Option<String>,
  pub settings: Option<serde_json::Value>,
}

#[napi(object)]
pub struct ReaderOpenOptions {
  pub path: String,
  pub url: Option<String>,
  #[napi(js_name = "envFile")]
  pub env_file: Option<String>,
  #[napi(js_name = "checkpointId")]
  pub checkpoint_id: Option<String>,
  #[napi(js_name = "manifestPollIntervalMs")]
  pub manifest_poll_interval_ms: Option<i64>,
  #[napi(js_name = "checkpointLifetimeMs")]
  pub checkpoint_lifetime_ms: Option<i64>,
  #[napi(js_name = "maxMemtableBytes")]
  pub max_memtable_bytes: Option<i64>,
  #[napi(js_name = "skipWalReplay")]
  pub skip_wal_replay: Option<bool>,
  pub options: Option<serde_json::Value>,
}

#[napi(object)]
pub struct AdminOpenOptions {
  pub path: String,
  pub url: Option<String>,
  #[napi(js_name = "envFile")]
  pub env_file: Option<String>,
  #[napi(js_name = "walUrl")]
  pub wal_url: Option<String>,
}

#[napi(object)]
pub struct ReadOptionsInput {
  #[napi(js_name = "durabilityFilter")]
  pub durability_filter: Option<String>,
  pub dirty: Option<bool>,
  #[napi(js_name = "cacheBlocks")]
  pub cache_blocks: Option<bool>,
}

#[napi(object)]
pub struct ScanOptionsInput {
  #[napi(js_name = "durabilityFilter")]
  pub durability_filter: Option<String>,
  pub dirty: Option<bool>,
  #[napi(js_name = "readAheadBytes")]
  pub read_ahead_bytes: Option<u32>,
  #[napi(js_name = "cacheBlocks")]
  pub cache_blocks: Option<bool>,
  #[napi(js_name = "maxFetchTasks")]
  pub max_fetch_tasks: Option<u32>,
}

#[napi(object)]
pub struct WriteOptionsInput {
  #[napi(js_name = "awaitDurable")]
  pub await_durable: Option<bool>,
}

#[napi(object)]
pub struct PutOptionsInput {
  #[napi(js_name = "ttlMs")]
  pub ttl_ms: Option<i64>,
  #[napi(js_name = "noExpiry")]
  pub no_expiry: Option<bool>,
}

#[napi(object)]
pub struct MergeOptionsInput {
  #[napi(js_name = "ttlMs")]
  pub ttl_ms: Option<i64>,
  #[napi(js_name = "noExpiry")]
  pub no_expiry: Option<bool>,
}

#[napi(object)]
pub struct CheckpointOptionsInput {
  #[napi(js_name = "lifetimeMs")]
  pub lifetime_ms: Option<i64>,
  pub source: Option<String>,
  pub name: Option<String>,
}

#[napi(object)]
pub struct GcDirectoryOptionsInput {
  #[napi(js_name = "intervalMs")]
  pub interval_ms: Option<i64>,
  #[napi(js_name = "minAgeMs")]
  pub min_age_ms: Option<i64>,
}

#[napi(object)]
pub struct GcOptionsInput {
  pub manifest: Option<GcDirectoryOptionsInput>,
  pub wal: Option<GcDirectoryOptionsInput>,
  pub compacted: Option<GcDirectoryOptionsInput>,
  pub compactions: Option<GcDirectoryOptionsInput>,
}

#[napi(object)]
pub struct CompactionSourceInput {
  #[napi(js_name = "sortedRun")]
  pub sorted_run: Option<u32>,
  pub sst: Option<String>,
}

#[napi(object)]
pub struct CompactionSpecInput {
  pub sources: Vec<CompactionSourceInput>,
  pub destination: u32,
}

#[napi(object)]
pub struct KeyValue {
  pub key: Buffer,
  pub value: Buffer,
}

#[napi(object)]
pub struct WriteHandleObject {
  pub seq: i64,
  #[napi(js_name = "createTs")]
  pub create_ts: i64,
}

#[napi(object)]
pub struct CheckpointCreateResultObject {
  pub id: String,
  #[napi(js_name = "manifestId")]
  pub manifest_id: i64,
}

#[napi(object)]
pub struct CheckpointObject {
  pub id: String,
  #[napi(js_name = "manifestId")]
  pub manifest_id: i64,
  #[napi(js_name = "expireTimeMs")]
  pub expire_time_ms: Option<i64>,
  #[napi(js_name = "createTimeMs")]
  pub create_time_ms: i64,
  pub name: Option<String>,
}

fn closed_error() -> NapiError {
  NapiError::new(Status::GenericFailure, "ClosedError: handle is closed".to_string())
}

fn invalid_error(message: impl Into<String>) -> NapiError {
  NapiError::new(Status::InvalidArg, format!("InvalidError: {}", message.into()))
}

fn internal_error(message: impl Into<String>) -> NapiError {
  NapiError::new(
    Status::GenericFailure,
    format!("InternalError: {}", message.into()),
  )
}

fn i64_to_u64(value: i64, field: &str) -> Result<u64> {
  u64::try_from(value).map_err(|_| invalid_error(format!("{field} must be >= 0")))
}

fn option_i64_to_u64(value: Option<i64>, field: &str) -> Result<Option<u64>> {
  value.map(|v| i64_to_u64(v, field)).transpose()
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
  i64::try_from(value).map_err(|_| internal_error(format!("{field} exceeds i64 range")))
}

fn map_box_error(err: impl std::fmt::Display) -> NapiError {
  invalid_error(err.to_string())
}

fn map_slate_error(err: SlateError) -> NapiError {
  let prefix = match err.kind() {
    ErrorKind::Transaction => "TransactionError",
    ErrorKind::Closed(_) => "ClosedError",
    ErrorKind::Unavailable => "UnavailableError",
    ErrorKind::Invalid => "InvalidError",
    ErrorKind::Data => "DataError",
    ErrorKind::Internal => "InternalError",
    _ => "InternalError",
  };
  NapiError::new(Status::GenericFailure, format!("{prefix}: {err}"))
}

fn parse_durability_filter(value: Option<&str>) -> Result<DurabilityLevel> {
  match value.map(|v| v.to_ascii_lowercase()) {
    None => Ok(DurabilityLevel::Memory),
    Some(v) if v == "memory" => Ok(DurabilityLevel::Memory),
    Some(v) if v == "remote" => Ok(DurabilityLevel::Remote),
    Some(v) => Err(invalid_error(format!(
      "invalid durabilityFilter '{v}', expected 'memory' or 'remote'"
    ))),
  }
}

fn parse_isolation(value: Option<&str>) -> Result<IsolationLevel> {
  match value.map(|v| v.to_ascii_lowercase()) {
    None => Ok(IsolationLevel::Snapshot),
    Some(v) if v == "si" || v == "snapshot" => Ok(IsolationLevel::Snapshot),
    Some(v)
      if v == "ssi" || v == "serializable" || v == "serializable_snapshot" =>
    {
      Ok(IsolationLevel::SerializableSnapshot)
    }
    Some(v) => Err(invalid_error(format!(
      "invalid isolation '{v}', expected 'si'/'snapshot' or 'ssi'/'serializable'"
    ))),
  }
}

fn parse_checkpoint_scope(value: Option<&str>) -> Result<CheckpointScope> {
  match value.map(|v| v.to_ascii_lowercase()) {
    None => Ok(CheckpointScope::All),
    Some(v) if v == "all" => Ok(CheckpointScope::All),
    Some(v) if v == "durable" => Ok(CheckpointScope::Durable),
    Some(v) => Err(invalid_error(format!(
      "invalid checkpoint scope '{v}', expected 'all' or 'durable'"
    ))),
  }
}

fn parse_flush_type(value: Option<&str>) -> Result<FlushType> {
  match value.map(|v| v.to_ascii_lowercase()) {
    None => Ok(FlushType::Wal),
    Some(v) if v == "wal" => Ok(FlushType::Wal),
    Some(v) if v == "memtable" => Ok(FlushType::MemTable),
    Some(v) => Err(invalid_error(format!(
      "invalid flush type '{v}', expected 'wal' or 'memtable'"
    ))),
  }
}

fn parse_ttl(ttl_ms: Option<i64>, no_expiry: Option<bool>) -> Result<Ttl> {
  if no_expiry.unwrap_or(false) {
    Ok(Ttl::NoExpiry)
  } else if let Some(ttl) = ttl_ms {
    Ok(Ttl::ExpireAfter(i64_to_u64(ttl, "ttlMs")?))
  } else {
    Ok(Ttl::Default)
  }
}

fn parse_read_options(input: Option<ReadOptionsInput>) -> Result<ReadOptions> {
  let mut options = ReadOptions::default();
  if let Some(input) = input {
    options.durability_filter = parse_durability_filter(input.durability_filter.as_deref())?;
    if let Some(dirty) = input.dirty {
      options.dirty = dirty;
    }
    if let Some(cache_blocks) = input.cache_blocks {
      options.cache_blocks = cache_blocks;
    }
  }
  Ok(options)
}

fn parse_scan_options(input: Option<ScanOptionsInput>) -> Result<ScanOptions> {
  let mut options = ScanOptions::default();
  if let Some(input) = input {
    options.durability_filter = parse_durability_filter(input.durability_filter.as_deref())?;
    if let Some(dirty) = input.dirty {
      options.dirty = dirty;
    }
    if let Some(read_ahead_bytes) = input.read_ahead_bytes {
      options.read_ahead_bytes = read_ahead_bytes as usize;
    }
    if let Some(cache_blocks) = input.cache_blocks {
      options.cache_blocks = cache_blocks;
    }
    if let Some(max_fetch_tasks) = input.max_fetch_tasks {
      options.max_fetch_tasks = max_fetch_tasks as usize;
    }
  }
  Ok(options)
}

fn parse_write_options(input: Option<WriteOptionsInput>) -> WriteOptions {
  let mut options = WriteOptions::default();
  if let Some(input) = input {
    if let Some(await_durable) = input.await_durable {
      options.await_durable = await_durable;
    }
  }
  options
}

fn parse_put_options(input: Option<PutOptionsInput>) -> Result<PutOptions> {
  let mut options = PutOptions::default();
  if let Some(input) = input {
    options.ttl = parse_ttl(input.ttl_ms, input.no_expiry)?;
  }
  Ok(options)
}

fn parse_merge_options(input: Option<MergeOptionsInput>) -> Result<MergeOptions> {
  let mut options = MergeOptions::default();
  if let Some(input) = input {
    options.ttl = parse_ttl(input.ttl_ms, input.no_expiry)?;
  }
  Ok(options)
}

fn parse_checkpoint_options(input: Option<CheckpointOptionsInput>) -> Result<CheckpointOptions> {
  let mut options = CheckpointOptions::default();
  if let Some(input) = input {
    options.lifetime = option_i64_to_u64(input.lifetime_ms, "lifetimeMs")?.map(Duration::from_millis);
    options.source = input
      .source
      .map(|source| Uuid::parse_str(&source).map_err(|e| invalid_error(format!("invalid source UUID: {e}"))))
      .transpose()?;
    options.name = input.name;
  }
  Ok(options)
}

fn parse_gc_directory_options(
  input: Option<GcDirectoryOptionsInput>,
  for_once: bool,
) -> Result<Option<GarbageCollectorDirectoryOptions>> {
  let Some(input) = input else {
    return Ok(None);
  };

  let Some(min_age_ms) = option_i64_to_u64(input.min_age_ms, "minAgeMs")? else {
    return Err(invalid_error(
      "GC directory options require minAgeMs when the section is provided",
    ));
  };

  Ok(Some(GarbageCollectorDirectoryOptions {
    interval: if for_once {
      None
    } else {
      option_i64_to_u64(input.interval_ms, "intervalMs")?.map(Duration::from_millis)
    },
    min_age: Duration::from_millis(min_age_ms),
  }))
}

fn parse_gc_options(input: Option<GcOptionsInput>, for_once: bool) -> Result<GarbageCollectorOptions> {
  let input = input.unwrap_or(GcOptionsInput {
    manifest: None,
    wal: None,
    compacted: None,
    compactions: None,
  });

  Ok(GarbageCollectorOptions {
    manifest_options: parse_gc_directory_options(input.manifest, for_once)?,
    wal_options: parse_gc_directory_options(input.wal, for_once)?,
    compacted_options: parse_gc_directory_options(input.compacted, for_once)?,
    compactions_options: parse_gc_directory_options(input.compactions, for_once)?,
  })
}

fn parse_compaction_spec(input: CompactionSpecInput) -> Result<CompactionSpec> {
  if input.sources.is_empty() {
    return Err(invalid_error("compaction spec requires at least one source"));
  }

  let mut sources = Vec::with_capacity(input.sources.len());
  for source in input.sources {
    match (source.sorted_run, source.sst) {
      (Some(sorted_run), None) => sources.push(SourceId::SortedRun(sorted_run)),
      (None, Some(sst)) => {
        let ulid = Ulid::from_string(&sst)
          .map_err(|e| invalid_error(format!("invalid SST ULID '{sst}': {e}")))?;
        sources.push(SourceId::Sst(ulid));
      }
      _ => {
        return Err(invalid_error(
          "each compaction source must provide exactly one of sortedRun or sst",
        ));
      }
    }
  }

  Ok(CompactionSpec::new(sources, input.destination))
}

fn load_settings(
  settings_path: Option<String>,
  settings_json: Option<serde_json::Value>,
) -> Result<Settings> {
  let settings = match settings_path {
    Some(path) => Settings::from_file(path).map_err(map_slate_error)?,
    None => Settings::load().map_err(map_slate_error)?,
  };

  if let Some(settings_json) = settings_json {
    Figment::from(settings)
      .merge(Serialized::defaults(settings_json))
      .extract::<Settings>()
      .map_err(|e| invalid_error(format!("invalid settings object: {e}")))
  } else {
    Ok(settings)
  }
}

fn load_reader_options(
  base_options: Option<serde_json::Value>,
  manifest_poll_interval_ms: Option<i64>,
  checkpoint_lifetime_ms: Option<i64>,
  max_memtable_bytes: Option<i64>,
  skip_wal_replay: Option<bool>,
) -> Result<DbReaderOptions> {
  let mut options = if let Some(base_options) = base_options {
    Figment::from(Serialized::defaults(DbReaderOptions::default()))
      .merge(Serialized::defaults(base_options))
      .extract::<DbReaderOptions>()
      .map_err(|e| invalid_error(format!("invalid reader options object: {e}")))?
  } else {
    DbReaderOptions::default()
  };

  if let Some(manifest_poll_interval_ms) = manifest_poll_interval_ms {
    options.manifest_poll_interval =
      Duration::from_millis(i64_to_u64(manifest_poll_interval_ms, "manifestPollIntervalMs")?);
  }
  if let Some(checkpoint_lifetime_ms) = checkpoint_lifetime_ms {
    options.checkpoint_lifetime =
      Duration::from_millis(i64_to_u64(checkpoint_lifetime_ms, "checkpointLifetimeMs")?);
  }
  if let Some(max_memtable_bytes) = max_memtable_bytes {
    options.max_memtable_bytes = i64_to_u64(max_memtable_bytes, "maxMemtableBytes")?;
  }
  if let Some(skip_wal_replay) = skip_wal_replay {
    options.skip_wal_replay = skip_wal_replay;
  }

  Ok(options)
}

fn resolve_object_store(url: Option<&str>, env_file: Option<String>) -> Result<Arc<dyn ObjectStore>> {
  if let Some(url) = url {
    return Db::resolve_object_store(url).map_err(map_slate_error);
  }

  load_object_store_from_env(env_file).map_err(map_box_error)
}

fn ensure_non_empty(name: &str, value: &[u8]) -> Result<()> {
  if value.is_empty() {
    return Err(invalid_error(format!("{name} cannot be empty")));
  }
  Ok(())
}

fn to_write_handle(handle: slatedb::WriteHandle) -> Result<WriteHandleObject> {
  Ok(WriteHandleObject {
    seq: u64_to_i64(handle.seqnum(), "seq")?,
    create_ts: handle.create_ts(),
  })
}

fn to_checkpoint_result(result: slatedb::CheckpointCreateResult) -> Result<CheckpointCreateResultObject> {
  Ok(CheckpointCreateResultObject {
    id: result.id.to_string(),
    manifest_id: u64_to_i64(result.manifest_id, "manifestId")?,
  })
}

fn checkpoint_to_object(checkpoint: slatedb::Checkpoint) -> Result<CheckpointObject> {
  Ok(CheckpointObject {
    id: checkpoint.id.to_string(),
    manifest_id: u64_to_i64(checkpoint.manifest_id, "manifestId")?,
    expire_time_ms: checkpoint.expire_time.map(|ts| ts.timestamp_millis()),
    create_time_ms: checkpoint.create_time.timestamp_millis(),
    name: checkpoint.name,
  })
}

fn key_value_to_object(key_value: slatedb::KeyValue) -> KeyValue {
  KeyValue {
    key: Buffer::from(key_value.key.to_vec()),
    value: Buffer::from(key_value.value.to_vec()),
  }
}

#[napi]
pub async fn open_native(options: OpenOptions) -> Result<NativeSlateDB> {
  let settings = load_settings(options.settings_path, options.settings)?;
  let object_store = resolve_object_store(options.url.as_deref(), options.env_file)?;
  let db = Db::builder(options.path, object_store)
    .with_settings(settings)
    .build()
    .await
    .map_err(map_slate_error)?;

  Ok(NativeSlateDB {
    inner: Arc::new(Mutex::new(Some(db))),
  })
}

#[napi]
pub async fn open_reader_native(options: ReaderOpenOptions) -> Result<NativeSlateDBReader> {
  let object_store = resolve_object_store(options.url.as_deref(), options.env_file)?;
  let reader_options = load_reader_options(
    options.options,
    options.manifest_poll_interval_ms,
    options.checkpoint_lifetime_ms,
    options.max_memtable_bytes,
    options.skip_wal_replay,
  )?;
  let checkpoint_id = options
    .checkpoint_id
    .map(|id| Uuid::parse_str(&id).map_err(|e| invalid_error(format!("invalid checkpointId: {e}"))))
    .transpose()?;

  let reader = DbReader::open(options.path, object_store, checkpoint_id, reader_options)
    .await
    .map_err(map_slate_error)?;

  Ok(NativeSlateDBReader {
    inner: Arc::new(Mutex::new(Some(reader))),
  })
}

#[napi]
pub async fn open_admin_native(options: AdminOpenOptions) -> Result<NativeSlateDBAdmin> {
  let object_store = resolve_object_store(options.url.as_deref(), options.env_file)?;
  let mut builder = Admin::builder(options.path, object_store);

  if let Some(wal_url) = options.wal_url {
    let wal_store = Db::resolve_object_store(&wal_url).map_err(map_slate_error)?;
    builder = builder.with_wal_object_store(wal_store);
  }

  Ok(NativeSlateDBAdmin {
    inner: Arc::new(builder.build()),
  })
}

#[napi(js_name = "NativeSlateDB")]
pub struct NativeSlateDB {
  inner: Arc<Mutex<Option<Db>>>,
}

#[napi]
impl NativeSlateDB {
  #[napi]
  pub async fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let value = db.get(key).await.map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi(js_name = "getWithOptions")]
  pub async fn get_with_options(
    &self,
    key: Buffer,
    options: Option<ReadOptionsInput>,
  ) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;
    let read_options = parse_read_options(options)?;

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let value = db
      .get_with_options(key, &read_options)
      .await
      .map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi]
  pub async fn put(&self, key: Buffer, value: Buffer) -> Result<WriteHandleObject> {
    ensure_non_empty("key", key.as_ref())?;
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db.put(key, value).await.map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi(js_name = "putWithOptions")]
  pub async fn put_with_options(
    &self,
    key: Buffer,
    value: Buffer,
    put_options: Option<PutOptionsInput>,
    write_options: Option<WriteOptionsInput>,
  ) -> Result<WriteHandleObject> {
    ensure_non_empty("key", key.as_ref())?;
    let put_options = parse_put_options(put_options)?;
    let write_options = parse_write_options(write_options);

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db
      .put_with_options(key, value, &put_options, &write_options)
      .await
      .map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi]
  pub async fn delete(&self, key: Buffer) -> Result<WriteHandleObject> {
    ensure_non_empty("key", key.as_ref())?;
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db.delete(key).await.map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi(js_name = "deleteWithOptions")]
  pub async fn delete_with_options(
    &self,
    key: Buffer,
    write_options: Option<WriteOptionsInput>,
  ) -> Result<WriteHandleObject> {
    ensure_non_empty("key", key.as_ref())?;
    let write_options = parse_write_options(write_options);

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db
      .delete_with_options(key, &write_options)
      .await
      .map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi]
  pub async fn merge(&self, key: Buffer, value: Buffer) -> Result<WriteHandleObject> {
    ensure_non_empty("key", key.as_ref())?;
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db.merge(key, value).await.map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi(js_name = "mergeWithOptions")]
  pub async fn merge_with_options(
    &self,
    key: Buffer,
    value: Buffer,
    merge_options: Option<MergeOptionsInput>,
    write_options: Option<WriteOptionsInput>,
  ) -> Result<WriteHandleObject> {
    ensure_non_empty("key", key.as_ref())?;
    let merge_options = parse_merge_options(merge_options)?;
    let write_options = parse_write_options(write_options);

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db
      .merge_with_options(key, value, &merge_options, &write_options)
      .await
      .map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi]
  pub async fn write(&self, batch: &NativeWriteBatch) -> Result<WriteHandleObject> {
    let batch = batch.clone_batch()?;

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db.write(batch).await.map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi(js_name = "writeWithOptions")]
  pub async fn write_with_options(
    &self,
    batch: &NativeWriteBatch,
    write_options: Option<WriteOptionsInput>,
  ) -> Result<WriteHandleObject> {
    let batch = batch.clone_batch()?;
    let write_options = parse_write_options(write_options);

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let handle = db
      .write_with_options(batch, &write_options)
      .await
      .map_err(map_slate_error)?;
    to_write_handle(handle)
  }

  #[napi]
  pub async fn scan(&self, start: Buffer, end: Option<Buffer>) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = match end {
      Some(end) => db
        .scan(start.as_ref()..end.as_ref())
        .await
        .map_err(map_slate_error)?,
      None => db.scan_prefix(start.as_ref()).await.map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanWithOptions")]
  pub async fn scan_with_options(
    &self,
    start: Buffer,
    end: Option<Buffer>,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let scan_options = parse_scan_options(options)?;

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = match end {
      Some(end) => db
        .scan_with_options(start.as_ref()..end.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
      None => db
        .scan_prefix_with_options(start.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefix")]
  pub async fn scan_prefix(&self, prefix: Buffer) -> Result<NativeSlateDBIterator> {
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = db
      .scan_prefix(prefix.as_ref())
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefixWithOptions")]
  pub async fn scan_prefix_with_options(
    &self,
    prefix: Buffer,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    let scan_options = parse_scan_options(options)?;

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = db
      .scan_prefix_with_options(prefix.as_ref(), &scan_options)
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi]
  pub async fn snapshot(&self) -> Result<NativeSlateDBSnapshot> {
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let snapshot = db.snapshot().await.map_err(map_slate_error)?;
    Ok(NativeSlateDBSnapshot {
      inner: Arc::new(Mutex::new(Some(snapshot))),
    })
  }

  #[napi]
  pub async fn begin(&self, isolation: Option<String>) -> Result<NativeSlateDBTransaction> {
    let isolation = parse_isolation(isolation.as_deref())?;

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let txn = db.begin(isolation).await.map_err(map_slate_error)?;
    Ok(NativeSlateDBTransaction {
      inner: Arc::new(Mutex::new(Some(txn))),
    })
  }

  #[napi]
  pub async fn flush(&self) -> Result<()> {
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    db.flush().await.map_err(map_slate_error)
  }

  #[napi(js_name = "flushWithOptions")]
  pub async fn flush_with_options(&self, flush_type: Option<String>) -> Result<()> {
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let flush_type = parse_flush_type(flush_type.as_deref())?;
    db
      .flush_with_options(FlushOptions { flush_type })
      .await
      .map_err(map_slate_error)
  }

  #[napi]
  pub async fn metrics(&self) -> Result<serde_json::Value> {
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let metrics = db.metrics();
    let names = metrics.names();
    let mut map = BTreeMap::new();
    for name in names {
      if let Some(stat) = metrics.lookup(name) {
        map.insert(name.to_string(), stat.get());
      }
    }

    serde_json::to_value(map).map_err(|e| internal_error(e.to_string()))
  }

  #[napi(js_name = "createCheckpoint")]
  pub async fn create_checkpoint(
    &self,
    scope: Option<String>,
    options: Option<CheckpointOptionsInput>,
  ) -> Result<CheckpointCreateResultObject> {
    let scope = parse_checkpoint_scope(scope.as_deref())?;
    let options = parse_checkpoint_options(options)?;

    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let result = db
      .create_checkpoint(scope, &options)
      .await
      .map_err(map_slate_error)?;

    to_checkpoint_result(result)
  }

  #[napi]
  pub async fn status(&self) -> Result<()> {
    let db = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    db.status().map_err(map_slate_error)
  }

  #[napi]
  pub async fn close(&self) -> Result<()> {
    let db = {
      let mut guard = self.inner.lock().await;
      guard.take().ok_or_else(closed_error)?
    };

    db.close().await.map_err(map_slate_error)
  }
}

#[napi(js_name = "NativeSlateDBSnapshot")]
pub struct NativeSlateDBSnapshot {
  inner: Arc<Mutex<Option<Arc<DbSnapshot>>>>,
}

#[napi]
impl NativeSlateDBSnapshot {
  #[napi]
  pub async fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;
    let snapshot = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let value = snapshot.get(key).await.map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi(js_name = "getWithOptions")]
  pub async fn get_with_options(
    &self,
    key: Buffer,
    options: Option<ReadOptionsInput>,
  ) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;
    let read_options = parse_read_options(options)?;

    let snapshot = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let value = snapshot
      .get_with_options(key, &read_options)
      .await
      .map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi]
  pub async fn scan(&self, start: Buffer, end: Option<Buffer>) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let snapshot = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = match end {
      Some(end) => snapshot
        .scan(start.as_ref()..end.as_ref())
        .await
        .map_err(map_slate_error)?,
      None => snapshot
        .scan_prefix(start.as_ref())
        .await
        .map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanWithOptions")]
  pub async fn scan_with_options(
    &self,
    start: Buffer,
    end: Option<Buffer>,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let scan_options = parse_scan_options(options)?;
    let snapshot = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = match end {
      Some(end) => snapshot
        .scan_with_options(start.as_ref()..end.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
      None => snapshot
        .scan_prefix_with_options(start.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefix")]
  pub async fn scan_prefix(&self, prefix: Buffer) -> Result<NativeSlateDBIterator> {
    let snapshot = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = snapshot
      .scan_prefix(prefix.as_ref())
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefixWithOptions")]
  pub async fn scan_prefix_with_options(
    &self,
    prefix: Buffer,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    let scan_options = parse_scan_options(options)?;
    let snapshot = {
      let guard = self.inner.lock().await;
      guard.clone().ok_or_else(closed_error)?
    };

    let iter = snapshot
      .scan_prefix_with_options(prefix.as_ref(), &scan_options)
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi]
  pub async fn close(&self) -> Result<()> {
    let mut guard = self.inner.lock().await;
    guard.take().ok_or_else(closed_error)?;
    Ok(())
  }
}

#[napi(js_name = "NativeSlateDBTransaction")]
pub struct NativeSlateDBTransaction {
  inner: Arc<Mutex<Option<DbTransaction>>>,
}

#[napi]
impl NativeSlateDBTransaction {
  #[napi]
  pub async fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;
    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;

    let value = txn.get(key).await.map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi(js_name = "getWithOptions")]
  pub async fn get_with_options(
    &self,
    key: Buffer,
    options: Option<ReadOptionsInput>,
  ) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;
    let read_options = parse_read_options(options)?;

    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;

    let value = txn
      .get_with_options(key, &read_options)
      .await
      .map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi]
  pub async fn scan(&self, start: Buffer, end: Option<Buffer>) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;

    let iter = match end {
      Some(end) => txn
        .scan(start.as_ref()..end.as_ref())
        .await
        .map_err(map_slate_error)?,
      None => txn
        .scan_prefix(start.as_ref())
        .await
        .map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanWithOptions")]
  pub async fn scan_with_options(
    &self,
    start: Buffer,
    end: Option<Buffer>,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let scan_options = parse_scan_options(options)?;

    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;

    let iter = match end {
      Some(end) => txn
        .scan_with_options(start.as_ref()..end.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
      None => txn
        .scan_prefix_with_options(start.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefix")]
  pub async fn scan_prefix(&self, prefix: Buffer) -> Result<NativeSlateDBIterator> {
    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;

    let iter = txn
      .scan_prefix(prefix.as_ref())
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefixWithOptions")]
  pub async fn scan_prefix_with_options(
    &self,
    prefix: Buffer,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    let scan_options = parse_scan_options(options)?;
    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;

    let iter = txn
      .scan_prefix_with_options(prefix.as_ref(), &scan_options)
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi]
  pub async fn put(&self, key: Buffer, value: Buffer) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;
    txn.put(key, value).map_err(map_slate_error)
  }

  #[napi(js_name = "putWithOptions")]
  pub async fn put_with_options(
    &self,
    key: Buffer,
    value: Buffer,
    options: Option<PutOptionsInput>,
  ) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let put_options = parse_put_options(options)?;
    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;
    txn
      .put_with_options(key, value, &put_options)
      .map_err(map_slate_error)
  }

  #[napi]
  pub async fn delete(&self, key: Buffer) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;
    txn.delete(key).map_err(map_slate_error)
  }

  #[napi]
  pub async fn merge(&self, key: Buffer, value: Buffer) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;
    txn.merge(key, value).map_err(map_slate_error)
  }

  #[napi(js_name = "mergeWithOptions")]
  pub async fn merge_with_options(
    &self,
    key: Buffer,
    value: Buffer,
    options: Option<MergeOptionsInput>,
  ) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let merge_options = parse_merge_options(options)?;

    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;
    txn
      .merge_with_options(key, value, &merge_options)
      .map_err(map_slate_error)
  }

  #[napi(js_name = "markRead")]
  pub async fn mark_read(&self, keys: Vec<Buffer>) -> Result<()> {
    if keys.is_empty() {
      return Ok(());
    }

    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;
    let refs = keys.iter().map(|key| key.as_ref());
    txn.mark_read(refs).map_err(map_slate_error)
  }

  #[napi(js_name = "unmarkWrite")]
  pub async fn unmark_write(&self, keys: Vec<Buffer>) -> Result<()> {
    if keys.is_empty() {
      return Ok(());
    }

    let guard = self.inner.lock().await;
    let txn = guard.as_ref().ok_or_else(closed_error)?;
    let refs = keys.iter().map(|key| key.as_ref());
    txn.unmark_write(refs).map_err(map_slate_error)
  }

  #[napi]
  pub async fn commit(&self, write_options: Option<WriteOptionsInput>) -> Result<Option<WriteHandleObject>> {
    let txn = {
      let mut guard = self.inner.lock().await;
      guard.take().ok_or_else(closed_error)?
    };

    let result = if let Some(write_options) = write_options {
      let write_options = parse_write_options(Some(write_options));
      txn
        .commit_with_options(&write_options)
        .await
        .map_err(map_slate_error)?
    } else {
      txn.commit().await.map_err(map_slate_error)?
    };

    result.map(to_write_handle).transpose()
  }

  #[napi]
  pub async fn rollback(&self) -> Result<()> {
    let txn = {
      let mut guard = self.inner.lock().await;
      guard.take().ok_or_else(closed_error)?
    };

    txn.rollback();
    Ok(())
  }

  #[napi]
  pub async fn close(&self) -> Result<()> {
    let mut guard = self.inner.lock().await;
    guard.take().ok_or_else(closed_error)?;
    Ok(())
  }
}

#[napi(js_name = "NativeSlateDBReader")]
pub struct NativeSlateDBReader {
  inner: Arc<Mutex<Option<DbReader>>>,
}

#[napi]
impl NativeSlateDBReader {
  #[napi]
  pub async fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;

    let mut guard = self.inner.lock().await;
    let reader = guard.as_mut().ok_or_else(closed_error)?;

    let value = reader.get(key).await.map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi(js_name = "getWithOptions")]
  pub async fn get_with_options(
    &self,
    key: Buffer,
    options: Option<ReadOptionsInput>,
  ) -> Result<Option<Buffer>> {
    ensure_non_empty("key", key.as_ref())?;
    let read_options = parse_read_options(options)?;

    let mut guard = self.inner.lock().await;
    let reader = guard.as_mut().ok_or_else(closed_error)?;

    let value = reader
      .get_with_options(key, &read_options)
      .await
      .map_err(map_slate_error)?;
    Ok(value.map(|value| Buffer::from(value.to_vec())))
  }

  #[napi]
  pub async fn scan(&self, start: Buffer, end: Option<Buffer>) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let mut guard = self.inner.lock().await;
    let reader = guard.as_mut().ok_or_else(closed_error)?;

    let iter = match end {
      Some(end) => reader
        .scan(start.as_ref()..end.as_ref())
        .await
        .map_err(map_slate_error)?,
      None => reader
        .scan_prefix(start.as_ref())
        .await
        .map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanWithOptions")]
  pub async fn scan_with_options(
    &self,
    start: Buffer,
    end: Option<Buffer>,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    if start.is_empty() {
      return Err(invalid_error("start cannot be empty"));
    }

    let scan_options = parse_scan_options(options)?;
    let mut guard = self.inner.lock().await;
    let reader = guard.as_mut().ok_or_else(closed_error)?;

    let iter = match end {
      Some(end) => reader
        .scan_with_options(start.as_ref()..end.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
      None => reader
        .scan_prefix_with_options(start.as_ref(), &scan_options)
        .await
        .map_err(map_slate_error)?,
    };

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefix")]
  pub async fn scan_prefix(&self, prefix: Buffer) -> Result<NativeSlateDBIterator> {
    let mut guard = self.inner.lock().await;
    let reader = guard.as_mut().ok_or_else(closed_error)?;

    let iter = reader
      .scan_prefix(prefix.as_ref())
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi(js_name = "scanPrefixWithOptions")]
  pub async fn scan_prefix_with_options(
    &self,
    prefix: Buffer,
    options: Option<ScanOptionsInput>,
  ) -> Result<NativeSlateDBIterator> {
    let scan_options = parse_scan_options(options)?;
    let mut guard = self.inner.lock().await;
    let reader = guard.as_mut().ok_or_else(closed_error)?;

    let iter = reader
      .scan_prefix_with_options(prefix.as_ref(), &scan_options)
      .await
      .map_err(map_slate_error)?;

    Ok(NativeSlateDBIterator {
      inner: Arc::new(Mutex::new(Some(iter))),
    })
  }

  #[napi]
  pub async fn close(&self) -> Result<()> {
    let reader = {
      let mut guard = self.inner.lock().await;
      guard.take().ok_or_else(closed_error)?
    };

    reader.close().await.map_err(map_slate_error)
  }
}

#[napi(js_name = "NativeSlateDBIterator")]
pub struct NativeSlateDBIterator {
  inner: Arc<Mutex<Option<DbIterator>>>,
}

#[napi]
impl NativeSlateDBIterator {
  #[napi]
  pub async fn next(&self) -> Result<Option<KeyValue>> {
    let mut guard = self.inner.lock().await;
    let iter = guard.as_mut().ok_or_else(closed_error)?;

    let value = iter.next().await.map_err(map_slate_error)?;
    Ok(value.map(key_value_to_object))
  }

  #[napi]
  pub async fn seek(&self, key: Buffer) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;

    let mut guard = self.inner.lock().await;
    let iter = guard.as_mut().ok_or_else(closed_error)?;

    iter.seek(key).await.map_err(map_slate_error)
  }

  #[napi]
  pub async fn close(&self) -> Result<()> {
    let mut guard = self.inner.lock().await;
    guard.take().ok_or_else(closed_error)?;
    Ok(())
  }
}

#[napi(js_name = "NativeWriteBatch")]
pub struct NativeWriteBatch {
  inner: Arc<std::sync::Mutex<WriteBatch>>,
}

impl NativeWriteBatch {
  fn clone_batch(&self) -> Result<WriteBatch> {
    let guard = self
      .inner
      .lock()
      .map_err(|_| internal_error("write batch lock poisoned"))?;
    Ok(guard.clone())
  }
}

#[napi]
impl NativeWriteBatch {
  #[napi(constructor)]
  pub fn new() -> Self {
    Self {
      inner: Arc::new(std::sync::Mutex::new(WriteBatch::new())),
    }
  }

  #[napi]
  pub fn put(&self, key: Buffer, value: Buffer) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let mut guard = self
      .inner
      .lock()
      .map_err(|_| internal_error("write batch lock poisoned"))?;
    guard.put(key, value);
    Ok(())
  }

  #[napi(js_name = "putWithOptions")]
  pub fn put_with_options(
    &self,
    key: Buffer,
    value: Buffer,
    options: Option<PutOptionsInput>,
  ) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let put_options = parse_put_options(options)?;
    let mut guard = self
      .inner
      .lock()
      .map_err(|_| internal_error("write batch lock poisoned"))?;
    guard.put_with_options(key, value, &put_options);
    Ok(())
  }

  #[napi]
  pub fn delete(&self, key: Buffer) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let mut guard = self
      .inner
      .lock()
      .map_err(|_| internal_error("write batch lock poisoned"))?;
    guard.delete(key);
    Ok(())
  }

  #[napi]
  pub fn merge(&self, key: Buffer, value: Buffer) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let mut guard = self
      .inner
      .lock()
      .map_err(|_| internal_error("write batch lock poisoned"))?;
    guard.merge(key, value);
    Ok(())
  }

  #[napi(js_name = "mergeWithOptions")]
  pub fn merge_with_options(
    &self,
    key: Buffer,
    value: Buffer,
    options: Option<MergeOptionsInput>,
  ) -> Result<()> {
    ensure_non_empty("key", key.as_ref())?;
    let merge_options = parse_merge_options(options)?;
    let mut guard = self
      .inner
      .lock()
      .map_err(|_| internal_error("write batch lock poisoned"))?;
    guard.merge_with_options(key, value, &merge_options);
    Ok(())
  }
}

#[napi(js_name = "NativeSlateDBAdmin")]
pub struct NativeSlateDBAdmin {
  inner: Arc<Admin>,
}

#[napi]
impl NativeSlateDBAdmin {
  #[napi(js_name = "readManifest")]
  pub async fn read_manifest(&self, id: Option<i64>) -> Result<Option<String>> {
    let id = option_i64_to_u64(id, "id")?;
    self
      .inner
      .read_manifest(id)
      .await
      .map_err(map_box_error)
  }

  #[napi(js_name = "listManifests")]
  pub async fn list_manifests(&self, start: Option<i64>, end: Option<i64>) -> Result<String> {
    let start = option_i64_to_u64(start, "start")?.unwrap_or(u64::MIN);
    let end = option_i64_to_u64(end, "end")?.unwrap_or(u64::MAX);
    self
      .inner
      .list_manifests(start..end)
      .await
      .map_err(map_box_error)
  }

  #[napi(js_name = "readCompactions")]
  pub async fn read_compactions(&self, id: Option<i64>) -> Result<Option<String>> {
    let id = option_i64_to_u64(id, "id")?;
    self
      .inner
      .read_compactions(id)
      .await
      .map_err(map_box_error)
  }

  #[napi(js_name = "listCompactions")]
  pub async fn list_compactions(&self, start: Option<i64>, end: Option<i64>) -> Result<String> {
    let start = option_i64_to_u64(start, "start")?.unwrap_or(u64::MIN);
    let end = option_i64_to_u64(end, "end")?.unwrap_or(u64::MAX);
    self
      .inner
      .list_compactions(start..end)
      .await
      .map_err(map_box_error)
  }

  #[napi(js_name = "readCompaction")]
  pub async fn read_compaction(
    &self,
    id: String,
    compactions_id: Option<i64>,
  ) -> Result<Option<String>> {
    let compaction_id =
      Ulid::from_string(&id).map_err(|e| invalid_error(format!("invalid compaction ULID: {e}")))?;
    let compaction = self
      .inner
      .read_compaction(
        compaction_id,
        option_i64_to_u64(compactions_id, "compactionsId")?,
      )
      .await
      .map_err(map_box_error)?;

    match compaction {
      Some(compaction) => {
        let value = serde_json::to_string(&compaction)
          .map_err(|e| invalid_error(format!("failed to encode compaction: {e}")))?;
        Ok(Some(value))
      }
      None => Ok(None),
    }
  }

  #[napi(js_name = "submitCompaction")]
  pub async fn submit_compaction(&self, spec: CompactionSpecInput) -> Result<String> {
    let spec = parse_compaction_spec(spec)?;
    let compaction = self
      .inner
      .submit_compaction(spec)
      .await
      .map_err(map_box_error)?;

    serde_json::to_string(&compaction)
      .map_err(|e| invalid_error(format!("failed to encode compaction: {e}")))
  }

  #[napi(js_name = "createCheckpoint")]
  pub async fn create_checkpoint(
    &self,
    options: Option<CheckpointOptionsInput>,
  ) -> Result<CheckpointCreateResultObject> {
    let options = parse_checkpoint_options(options)?;
    let result = self
      .inner
      .create_detached_checkpoint(&options)
      .await
      .map_err(map_slate_error)?;
    to_checkpoint_result(result)
  }

  #[napi(js_name = "listCheckpoints")]
  pub async fn list_checkpoints(&self, name: Option<String>) -> Result<Vec<CheckpointObject>> {
    let checkpoints = self
      .inner
      .list_checkpoints(name.as_deref())
      .await
      .map_err(map_box_error)?;

    checkpoints.into_iter().map(checkpoint_to_object).collect()
  }

  #[napi(js_name = "refreshCheckpoint")]
  pub async fn refresh_checkpoint(
    &self,
    id: String,
    lifetime_ms: Option<i64>,
  ) -> Result<()> {
    let checkpoint_id =
      Uuid::parse_str(&id).map_err(|e| invalid_error(format!("invalid checkpoint UUID: {e}")))?;
    self
      .inner
      .refresh_checkpoint(
        checkpoint_id,
        option_i64_to_u64(lifetime_ms, "lifetimeMs")?.map(Duration::from_millis),
      )
      .await
      .map_err(map_slate_error)
  }

  #[napi(js_name = "deleteCheckpoint")]
  pub async fn delete_checkpoint(&self, id: String) -> Result<()> {
    let checkpoint_id =
      Uuid::parse_str(&id).map_err(|e| invalid_error(format!("invalid checkpoint UUID: {e}")))?;
    self
      .inner
      .delete_checkpoint(checkpoint_id)
      .await
      .map_err(map_slate_error)
  }

  #[napi(js_name = "getTimestampForSequence")]
  pub async fn get_timestamp_for_sequence(
    &self,
    seq: i64,
    round_up: Option<bool>,
  ) -> Result<Option<i64>> {
    let seq = i64_to_u64(seq, "seq")?;
    let timestamp = self
      .inner
      .get_timestamp_for_sequence(seq, round_up.unwrap_or(false))
      .await
      .map_err(map_slate_error)?;
    Ok(timestamp.map(|ts| ts.timestamp_millis()))
  }

  #[napi(js_name = "getSequenceForTimestamp")]
  pub async fn get_sequence_for_timestamp(
    &self,
    timestamp_ms: i64,
    round_up: Option<bool>,
  ) -> Result<Option<i64>> {
    let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
      .ok_or_else(|| invalid_error("timestampMs out of range"))?;

    let seq = self
      .inner
      .get_sequence_for_timestamp(timestamp, round_up.unwrap_or(false))
      .await
      .map_err(map_slate_error)?;
    seq.map(|value| u64_to_i64(value, "sequence")).transpose()
  }

  #[napi(js_name = "createClone")]
  pub async fn create_clone(
    &self,
    parent_path: String,
    parent_checkpoint: Option<String>,
  ) -> Result<()> {
    let parent_checkpoint = parent_checkpoint
      .map(|checkpoint| {
        Uuid::parse_str(&checkpoint)
          .map_err(|e| invalid_error(format!("invalid parent checkpoint UUID: {e}")))
      })
      .transpose()?;

    self
      .inner
      .create_clone(parent_path, parent_checkpoint)
      .await
      .map_err(map_box_error)
  }

  #[napi(js_name = "runGcOnce")]
  pub async fn run_gc_once(&self, options: Option<GcOptionsInput>) -> Result<()> {
    let options = parse_gc_options(options, true)?;
    self.inner.run_gc_once(options).await.map_err(map_box_error)
  }

  #[napi(js_name = "runGc")]
  pub async fn run_gc(&self, options: Option<GcOptionsInput>) -> Result<()> {
    let options = parse_gc_options(options, false)?;
    self.inner.run_gc(options).await.map_err(map_slate_error)
  }
}
