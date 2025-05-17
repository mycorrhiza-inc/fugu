use anyhow::anyhow;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Archive, Debug, Clone)]
pub enum FileLocation {
    S3Location(S3Location),
    LocalPath(LocalPath),
}
// Making the path type a String since non utf8 paths are cringe. Also rkyv doesnt implement its
// serialization on path types.
pub type LocalPath = String;

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
        let bucket_env = "S3_FUGU_BUCKET";
        let endpoint_env = "S3_ENDPOINT";
        let region_env = "S3_REGION";
        let default_bucket = "fugudocs";
        let default_endpoint = "https://sfo3.digitaloceanspaces.com";
        let default_region = "sfo3";

        let access_env = "S3_ACCESS_KEY";
        let secret_env = "S3_SECRET_KEY";
        S3ConfigParams {
            endpoint: std::env::var(endpoint_env).unwrap_or_else(|_err| {
                println!("{endpoint_env} not defined, defaulting to {default_endpoint}");
                default_endpoint.into()
            }),
            region: std::env::var(region_env).unwrap_or_else(|_err| {
                println!("{region_env} not defined, defaulting to {default_region}");
                default_region.into()
            }),
            default_bucket: std::env::var(bucket_env).unwrap_or_else(|_err| {
                println!("{endpoint_env} not defined, defaulting to {default_endpoint}");
                default_bucket.into()
            }),

            access_key: std::env::var(access_env)
                .unwrap_or_else(|_| panic!("{access_env} Not Set")),
            secret_key: std::env::var(secret_env)
                .unwrap_or_else(|_| panic!("{secret_env} Not Set")),
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
