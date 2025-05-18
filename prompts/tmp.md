This is a rust file that implements some s3 configuration for files and stuff.

```rs

use anyhow::anyhow;
use rkyv::{Archive, Deserialize, Serialize};

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

// Making the path type a String since non utf8 paths are cringe. 
// Also rkyv doesnt implement its serialization on path types.
pub type LocalPath = String;

// What is the best way to do caching of files locally??
//
// Requirements.
// 1. Maybe for dev mode it should be possible to not even use s3, and just do everything on the local
// file system.
// 2. When in S3 mode the following mode should occur in terms of caching and stuff.
// - Every file for s3 should have a matching filepath on the system that can be deterministically
// generated from the bucket and key.
// - This should get stored in some kind of way to know when the file was last cached from s3. And
// then redownload or otherwise validate the file if the file hasnt been updated in a while. It
// should also have a field it could use to store a u64 that comes from a hashing algorithm. ( for
// now it should probably just use the default hash algorithm, )
// - This data is only used when using s3 as a cache, so it might be a good idea to just store it on the drive
// directly
// So in addition to using the local file system for caching on an s3 system. The
// local file system should.
//
//
// So what would a file access system look like for s3 without using any cache?
//
// It would Look up file on s3. -> Download to local file. -> Read from that local file to produce
// the object
//
// With a cache module it would look like
//
// Look up file from s3 -> Check local cache to see if file exists -> If valid and exists, return local file
// location, else refetch from s3, and return the file.
//
// Although since s3 always needs to download the file locally before using it, you can role the no
// cache case by just giving it a cache and setting it so that it always returns an expired hash
// result. (One could call this the null cache or something)
//
//
// So idea. Store the s3 cache as a lazy static, along with all the configuration needed for doing
// uploads. And given the following traits:

trait RemoteFileLocation {
    fn fetch(&self) -> LocalPath;
    fn upload(loc: &LocalPath) -> Self;
}
impl RemoteFileLocation for LocalPath {
    fn fetch(&self) -> LocalPath {
        self.clone()
    }
    fn upload(loc: &LocalPath) -> Self {
        loc.clone()
    }
}

impl RemoteFileLocation for S3Location {
    fn fetch(&self) -> LocalPath {
        todo!()
    }
    fn upload(loc: &LocalPath) -> Self {
        todo!()
    }
}
```


How could you implement the caching for all the s3 fetching that is documented in the middle comment?

For now just document your thoughts on the architecture. Once you are done you can write your thoughts to prompts/s3_caching.md

