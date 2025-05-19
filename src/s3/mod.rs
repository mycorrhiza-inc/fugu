use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::SystemTime;

use anyhow::anyhow;
use rkyv::{Archive, Deserialize, Serialize, rancor};

#[derive(Serialize, Deserialize, Archive, Debug, Clone)]
pub struct S3Location {
    pub key: String,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
}
// https://examplebucket.sfo3.digitaloceanspaces.com/this/is/the/file/key
//
// For this example the
// bucket: examplebucket
// region: sfo3
// endpoint: https://sfo3.digitaloceanspaces.com
// key: this/is/the/file/key
impl From<S3Location> for String {
    fn from(location: S3Location) -> String {
        // strip off the "https://" prefix on the endpoint to get e.g. "sfo3.digitaloceanspaces.com"
        let host_part = location
            .endpoint
            .strip_prefix("https://")
            .unwrap_or(&location.endpoint);

        // make sure we don’t end up with duplicate or missing slashes
        let key_part = location.key.trim_start_matches('/');

        // build "https://{bucket}.{host_part}/{key…}"
        let mut url = format!("https://{}.{}", location.bucket, host_part);
        if !key_part.is_empty() {
            url.push('/');
            url.push_str(key_part);
        }
        url
    }
}

//
// Try to parse the URL back into its components.
// Expects URLs of the form
//   "https://{bucket}.{region}.{rest…}/{key…}"
//
impl TryFrom<String> for S3Location {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        // 1) strip the scheme
        let without_scheme = value
            .strip_prefix("https://")
            .ok_or(anyhow!("Did not begin with https://"))?;

        // 2) split into host vs. path
        //    e.g. "examplebucket.sfo3.digitaloceanspaces.com/this/is/…"
        let mut split = without_scheme.splitn(2, '/');
        let host = split.next().unwrap();
        let key = split.next().unwrap_or("").to_string();

        // 3) break host into bucket, region, and the rest of the domain
        //    host_parts = ["examplebucket", "sfo3", "digitaloceanspaces.com"]
        let host_parts: Vec<&str> = host.split('.').collect();
        if host_parts.len() < 3 {
            return Err(anyhow!("Invalid s3 location"));
        }
        let bucket = host_parts[0].to_string();
        let region = host_parts[1].to_string();
        let domain_rest = host_parts[2..].join(".");

        // 4) rebuild the endpoint (scheme + region + rest-of-domain)
        let endpoint = format!("https://{}.{}", region, domain_rest);

        Ok(S3Location {
            key,
            bucket,
            region,
            endpoint,
        })
    }
}

// HEADER: S3 credentials and configuration
use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Credentials;
use tokio::fs;
// Build a Region, Credentials and (if provided) custom Endpoint
pub struct S3ConfigParams {
    pub endpoint: String,
    pub region: String,
    pub default_bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

impl Default for S3ConfigParams {
    fn default() -> Self {
        const BUCKET_ENV: &str = "S3_FUGU_BUCKET";
        const ENDPOINT_ENV: &str = "S3_ENDPOINT";
        const REGION_ENV: &str = "S3_REGION";
        const DEFAULT_BUCKET: &str = "fugudocs";
        const DEFAULT_ENDPOINT: &str = "https://sfo3.digitaloceanspaces.com";
        const DEFAULT_REGION: &str = "sfo3";
        const ACCESS_ENV: &str = "S3_ACCESS_KEY";
        const SECRET_ENV: &str = "S3_SECRET_KEY";

        S3ConfigParams {
            endpoint: std::env::var(ENDPOINT_ENV).unwrap_or_else(|_err| {
                println!("{ENDPOINT_ENV} not defined, defaulting to {DEFAULT_ENDPOINT}");
                DEFAULT_ENDPOINT.into()
            }),
            region: std::env::var(REGION_ENV).unwrap_or_else(|_err| {
                println!("{REGION_ENV} not defined, defaulting to {DEFAULT_REGION}");
                DEFAULT_REGION.into()
            }),
            default_bucket: std::env::var(BUCKET_ENV).unwrap_or_else(|_err| {
                println!("{ENDPOINT_ENV} not defined, defaulting to {DEFAULT_ENDPOINT}");
                DEFAULT_BUCKET.into()
            }),

            access_key: std::env::var(ACCESS_ENV)
                .unwrap_or_else(|_| panic!("{ACCESS_ENV} Not Set")),
            secret_key: std::env::var(SECRET_ENV)
                .unwrap_or_else(|_| panic!("{SECRET_ENV} Not Set")),
        }
    }
}

pub static DEFAULT_S3_CONFIG: LazyLock<S3ConfigParams> =
    LazyLock::new(|| S3ConfigParams::default());

pub async fn make_s3_client(s3_config: &S3ConfigParams, s3_loc: &S3Location) -> S3Client {
    let region = Region::new(s3_loc.region.clone());
    let creds = Credentials::new(
        &s3_config.access_key,
        &s3_config.secret_key,
        None, // no session token
        None, // no expiration
        "manual",
    );

    // Start from the env-loader so we still pick up other settings (timeouts, retry, etc)
    let cfg_loader = aws_config::defaults(BehaviorVersion::latest())
        .region(region.clone())
        .credentials_provider(creds)
        .endpoint_url(&s3_loc.endpoint);

    let sdk_config = cfg_loader.load().await;
    S3Client::new(&sdk_config)
}

// Making the path type a String since non utf8 paths are cringe. Also rkyv doesnt implement its
// serialization on path types.
pub type LocalPath = String;

trait RemoteFileLocation: Sized {
    fn raw_filepath(&self) -> LocalPath;
    fn mdata_filepath(&self) -> LocalPath;
    async fn raw_fetch(&self) -> anyhow::Result<Vec<u8>>;
    async fn raw_upload(&self, bytes: &[u8]) -> anyhow::Result<()>;
}
impl RemoteFileLocation for LocalPath {
    async fn raw_fetch(&self) -> anyhow::Result<Vec<u8>> {
        Ok(fs::read(self).await?)
    }

    async fn raw_upload(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let path: PathBuf = self.clone().into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, bytes).await?;
        Ok(())
    }
    fn raw_filepath(&self) -> LocalPath {
        self.clone()
    }
    fn mdata_filepath(&self) -> LocalPath {
        panic!(
            "This method shouldnt be used, metadata for these files should always be computed diretly."
        )
    }
}

impl RemoteFileLocation for S3Location {
    async fn raw_fetch(&self) -> anyhow::Result<Vec<u8>> {
        let bucket = &self.bucket;
        let key = &self.key;
        let client = make_s3_client(&DEFAULT_S3_CONFIG, self).await;
        // Download object
        let resp = client.get_object().bucket(bucket).key(key).send().await?;
        // Read body
        let data = resp
            .body
            .collect()
            .await
            .map_err(|_| anyhow!("invalid file location"))?;
        let bytes = data.to_vec();
        // Determine local path and write file
        let full_path_str = self.raw_filepath();
        let full_path: PathBuf = full_path_str.clone().into();
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&full_path, &bytes).await?;
        Ok(bytes)
    }
    async fn raw_upload(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let client = make_s3_client(&DEFAULT_S3_CONFIG, self).await;
        client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(bytes.to_vec().into())
            .send()
            .await?;
        Ok(())
    }
    fn raw_filepath(&self) -> LocalPath {
        CACHE_DIR.to_string() + "/data/" + &self.bucket + "/" + &self.key
    }
    fn mdata_filepath(&self) -> LocalPath {
        CACHE_DIR.to_string() + "/mdata/" + &self.bucket + "/" + &self.key
    }
}

const CACHE_DIR: &str = "./cache";

type HashValue = u64;

fn hash_from_bytes(bytes: &[u8]) -> HashValue {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

// Caching Policies:
#[derive(Clone, Copy, Serialize, Deserialize, Archive, Default)]
struct CacheMeta {
    hash: HashValue,
    last_checked_unixtime_seconds: u64,
}

impl CacheMeta {
    fn calc_from_bytes(bytes: &[u8]) -> Self {
        Self {
            hash: hash_from_bytes(bytes),
            last_checked_unixtime_seconds: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }
}

trait CachePolicyImpl {
    fn is_fresh_from_mdata(meta: &ArchivedCacheMeta) -> bool;
    async fn is_fresh(loc: &impl RemoteFileLocation) -> bool {
        let mdata_path = loc.mdata_filepath();

        // Try to read and parse metadata
        let bytes = match tokio::fs::read(&mdata_path).await {
            Ok(b) => b,
            Err(_) => return false,
        };

        // Zero-copy deserialization using rkyv
        let archived = match rkyv::access::<ArchivedCacheMeta, rancor::Error>(&bytes) {
            Ok(a) => a,
            Err(_) => return false,
        };

        Self::is_fresh_from_mdata(archived)
    }

    async fn update_cache(loc: &impl RemoteFileLocation, bytes: &[u8]) {
        let mdata_path = loc.mdata_filepath();

        let meta = CacheMeta::calc_from_bytes(bytes);

        // Serialize and write metadata
        let bytes = match rkyv::to_bytes::<rancor::Error>(&meta) {
            Ok(b) => b,
            Err(_) => return,
        };
        let path: PathBuf = mdata_path.into();

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }

        let _ = tokio::fs::write(path, bytes).await;
    }
}

#[derive(Clone, Copy)]
struct NoCache;
impl CachePolicyImpl for NoCache {
    fn is_fresh_from_mdata(_: &ArchivedCacheMeta) -> bool {
        false
    }
    async fn is_fresh(_: &impl RemoteFileLocation) -> bool {
        false
    }
    async fn update_cache(_: &impl RemoteFileLocation, _: &[u8]) {}
}

struct DefaultCacheConfig;
impl CachePolicyImpl for DefaultCacheConfig {
    fn is_fresh_from_mdata(meta: &ArchivedCacheMeta) -> bool {
        const DEFAULT_CACHE_TIME: u64 = 30;
        let current_unixtime_seconds =
            match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(dur) => dur.as_secs(),
                Err(_) => {
                    // If leap seconds happen invalidate cache.
                    return false;
                }
            };
        meta.last_checked_unixtime_seconds + DEFAULT_CACHE_TIME > current_unixtime_seconds
    }
}

trait FileSystemLocation: RemoteFileLocation {
    type CachePolicy: CachePolicyImpl;

    async fn fetch(&self) -> anyhow::Result<Vec<u8>> {
        let is_fresh = Self::CachePolicy::is_fresh(self).await;
        let bytes = match is_fresh {
            true => self.raw_filepath().raw_fetch().await?,
            false => self.raw_fetch().await?,
        };
        Ok(bytes)
    }

    async fn upload(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let _ = Self::CachePolicy::update_cache(self, bytes).await;
        self.raw_upload(bytes).await?;
        Ok(())
    }
}

// To disable cache just flip this so that the cache policy is NoCache
impl FileSystemLocation for S3Location {
    type CachePolicy = DefaultCacheConfig;
}

// No cache is necessary for local files.
impl FileSystemLocation for LocalPath {
    type CachePolicy = NoCache;
}
