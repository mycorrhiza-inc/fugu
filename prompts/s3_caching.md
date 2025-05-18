# S3 Caching Architecture

## 1. Overview

We need a transparent local cache layer for S3-based file operations. The goals are:

- Minimize repeated downloads of the same S3 object.
- Validate freshness based on time-to-live (TTL) or ETag.
- Support a "null cache" (no caching) mode for local development.
- Provide deterministic mapping between an `S3Location` and a local file path.
- Store cache metadata (e.g., last‐modified timestamp, ETag, access time).
- Allow pluggable cache policies (TTL, ETag-based, manual invalidation).

## 2. Core Components

1. **CacheManager** (singleton)
   - Manages the global cache directory and policies.
   - Interface:
     ```rust
     struct CacheManager {
         base_dir: PathBuf,
         policy: Box<dyn CachePolicy + Send + Sync>,
     }

     impl CacheManager {
         fn instance() -> &'static Self;
         async fn get_or_fetch(s3_loc: &S3Location) -> LocalPath;
         async fn invalidate(s3_loc: &S3Location);
     }
     ```

2. **CachePolicy** (trait)
   - Encapsulates cache‐validation logic.
   - Methods:
     ```rust
     trait CachePolicy {
         fn is_fresh(meta: &CacheMeta) -> bool;
         fn when_to_refresh(meta: &CacheMeta) -> Instant;
     }
     ```
   - Built‐in implementations:
     - `TtlPolicy { duration: Duration }`
     - `EtagPolicy` (compare remote ETag header)
     - `NullPolicy` (always stale)

3. **CacheMeta**
   - Metadata stored alongside each cached file:
     ```rust
     struct CacheMeta {
         local_path: PathBuf,
         etag: Option<String>,
         last_modified: DateTime<Utc>,
         last_checked: DateTime<Utc>,
     }
     ```

4. **Metadata Store**
   - Two options:
     - Per‐file JSON sidecar (`file.ext.meta.json`).
     - Single SQLite DB at `base_dir/cache.db`.
   - JSON is simple to start; SQLite scales better.

5. **Path Mapping**
   - Base directory: configurable via env or default (`~/.cache/fugu_s3/`).
   - For each `S3Location { bucket, key }`:
     - Sanitize `key` segments to valid file names.
     - Local file path: `base_dir/bucket/<key>`.
     - Ensure parent directories exist.

## 3. Fetch Workflow

1. Call `CacheManager::get_or_fetch(&s3_loc)`.
2. Map to local file path and metadata path.
3. Load or create `CacheMeta`.
4. If `policy.is_fresh(&meta) == true`:
   - Return `meta.local_path` directly.
5. Otherwise:
   - Download from S3 (using our existing client).
   - Write to a temp file, then atomically rename.
   - Update `CacheMeta` (update timestamps, ETag).
   - Persist metadata.
   - Return the new path.

## 4. Development / Null Cache Mode

- Provide a `NullPolicy` that always treats caches as stale.
- Or skip all caching logic by swapping in a `NoCacheManager` that directly fetches from S3 to a temp file each time.

## 5. Upload Workflow

- Mirror: Map a `LocalPath` to an `S3Location` via `CacheManager.upload(loc)`.
- Optionally invalidate the local cache entry after a successful upload.

## 6. Error Handling & Concurrency

- Concurrent fetches should synchronize on a file lock (e.g., `flock`) to prevent duplicate downloads.
- Metadata writes should be atomic.
- Provide clear error propagation if S3 download fails.

## 7. Configuration & Extensibility

- Environment variables:
  - `S3_CACHE_DIR`
  - `S3_CACHE_POLICY` (e.g., `ttl=3600`, `etag`)
- Allow custom policies via code.

---

`prompts/s3_caching.md` outlines the design and can be iterated as we implement the cache layer.