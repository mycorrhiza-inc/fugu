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
