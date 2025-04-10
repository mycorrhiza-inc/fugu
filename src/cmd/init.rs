use std::fs;
use std::io;
use std::path::PathBuf;

use super::commands::InitCommand;

#[derive(Debug)]
struct FuguPaths {
    root: PathBuf,
    filecache: PathBuf,
    tmp: PathBuf,
    config: PathBuf,
}

impl FuguPaths {
    fn new() -> io::Result<Self> {
        let home_dir = std::env::var("HOME")
            .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "HOME directory not found"))?;

        let root = PathBuf::from(home_dir).join(".fugu");

        Ok(Self {
            filecache: root.join("tmp/cache"),
            tmp: root.join("tmp"),
            config: root.join("config.yml"),
            root,
        })
    }

    fn create_directories(&self, force: bool) -> io::Result<()> {
        if force {
            // Force overwrite directories
            if self.root.exists() {
                fs::remove_dir_all(&self.root)?;
            }
            fs::create_dir_all(&self.root)?;
            fs::create_dir_all(&self.filecache)?;
            fs::create_dir_all(&self.tmp)?;
        } else {
            // Only create if they don't exist
            if !self.root.exists() {
                fs::create_dir_all(&self.root)?;
            }
            if !self.filecache.exists() {
                fs::create_dir_all(&self.filecache)?;
            }
            if !self.tmp.exists() {
                fs::create_dir_all(&self.tmp)?;
            }
        }

        if !self.config.exists() {
            fs::write(
                &self.config,
                "# Fugu configuration file\n\n# Add your configuration here\n",
            )?;
        }
        Ok(())
    }

    fn verify_path(&self, path: &PathBuf, expected_type: PathType) -> Vec<String> {
        let mut issues = Vec::new();

        if !path.exists() {
            issues.push(format!("Missing {}: {}", expected_type, path.display()));
            return issues;
        }

        match expected_type {
            PathType::Directory if !path.is_dir() => {
                issues.push(format!(
                    "{} exists but is not a directory: {}",
                    expected_type,
                    path.display()
                ));
            }
            PathType::File if !path.is_file() => {
                issues.push(format!(
                    "{} exists but is not a file: {}",
                    expected_type,
                    path.display()
                ));
            }
            _ => {}
        }

        if let Err(e) = self.verify_permissions(path) {
            issues.push(format!(
                "Permission error for {}: {} ({})",
                expected_type,
                path.display(),
                e
            ));
        }

        issues
    }

    fn verify_permissions(&self, path: &PathBuf) -> io::Result<()> {
        let metadata = fs::metadata(path)?;
        if metadata.permissions().readonly() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Path is read-only",
            ));
        }
        Ok(())
    }

    fn verify_structure(&self) -> Vec<String> {
        let mut issues = Vec::new();

        issues.extend(self.verify_path(&self.root, PathType::Directory));
        issues.extend(self.verify_path(&self.filecache, PathType::Directory));
        issues.extend(self.verify_path(&self.tmp, PathType::Directory));
        issues.extend(self.verify_path(&self.config, PathType::File));

        issues
    }
}

#[derive(Debug)]
enum PathType {
    Directory,
    File,
}

impl std::fmt::Display for PathType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathType::Directory => write!(f, "directory"),
            PathType::File => write!(f, "file"),
        }
    }
}

pub async fn run(args: InitCommand) -> Result<(), Box<dyn std::error::Error>> {
    let paths = FuguPaths::new()?;
    if !args.force {
        let issues = paths.verify_structure();
        if !issues.is_empty() {
            println!("Found issues with fugu directory structure:");
            for issue in issues {
                println!("  - {}", issue);
            }
            return Err("Fugu directory structure is incomplete or has permission issues".into());
        }
        println!("Successfully verified fugu directory structure at ~/.fugu");
    }
    paths.create_directories(args.force)?;
    println!("Successfully created fugu directory structure at ~/.fugu");
    Ok(())
}
