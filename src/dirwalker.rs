use crate::progress::ProgressIndicator;
use log::*;
use std::fs::Metadata;
use std::io;
use std::path::{Component, Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

pub struct DataFile {
    pub entry: DirEntry,
    pub path_components: Vec<String>,
    pub metadata: Metadata,
}

pub struct WalkedDir {
    pub canonical_path: PathBuf,
    pub prefix: PathBuf,
    pub files: Vec<DataFile>,
}

impl WalkedDir {
    pub fn walk<P>(
        base: P,
        progress: &mut ProgressIndicator,
    ) -> io::Result<WalkedDir>
    where
        P: AsRef<Path>,
    {
        let canonical_path = base.as_ref().canonicalize().unwrap();
        let prefix = canonical_path.parent().expect("Cannot be a root folder");
        let mut files = vec![];

        // Walk folder tree
        progress.scan_begin();
        let dir_iter = WalkDir::new(canonical_path.clone())
            .follow_links(true)
            .into_iter()
            .filter_entry(|entry| {
                entry.depth() == 0
                    || !entry
                        .file_name()
                        .to_str()
                        .map(|s| s.starts_with('.'))
                        .unwrap_or(false)
            });
        for entry in dir_iter {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let partial_path = entry.path().strip_prefix(prefix).unwrap();
            debug!("File: {}", partial_path.display());
            progress.scan_progress(partial_path.to_str().unwrap());

            // Split path components
            let mut comp: Vec<String> = vec![];
            for c in partial_path.components() {
                match c {
                    Component::Normal(os_str) => comp.push(
                        os_str
                            .to_str()
                            .expect("Cannot encode path, this is not good")
                            .to_string(),
                    ),
                    _ => {
                        unreachable!("Invalid path: {}", partial_path.display())
                    }
                }
            }

            files.push(DataFile {
                metadata: entry.metadata()?,
                entry,
                path_components: comp,
            });
        }
        progress.scan_end();

        // Sort files according to the file tree in bt v2 spec.
        files.sort_by_cached_key(|val| {
            val.path_components
                .iter()
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>()
        });
        info!("File list sorted.");

        Ok(WalkedDir {
            prefix: prefix.to_path_buf(),
            canonical_path,
            files,
        })
    }
}
