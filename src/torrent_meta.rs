use crate::bencode::BencodeValue;
use crate::progress::ProgressIndicator;

use log::*;
use memmap2::MmapOptions;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::fs::{File, Metadata};
use std::io;
use std::path::{Component, Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

struct Bep3Hasher {
    piece_size: u64,
    hashes: Vec<Vec<u8>>,
    hash_context: Sha1,
    // # of bytes of this piece that has been hashed into context
    hashed_size: u64,
}

impl Bep3Hasher {
    fn new(piece_size: u64) -> Bep3Hasher {
        Bep3Hasher {
            piece_size,
            hashes: vec![],
            hash_context: Sha1::new(),
            hashed_size: 0,
        }
    }

    fn visit_file(&mut self, data: &[u8], progress: &mut ProgressIndicator) {
        let mut offset = 0usize;
        let mut file_left = data.len();
        let mut piece_left = (self.piece_size - self.hashed_size) as usize;
        loop {
            let consume = std::cmp::min(piece_left, file_left) as usize;
            self.hash_context.update(&data[offset..offset + consume]);
            progress.hash_progress(consume as u64);
            offset += consume;
            piece_left -= consume;
            file_left -= consume;
            if piece_left == 0 {
                // finish this chunk
                let raw_hash = self.hash_context.finalize_reset().to_vec();
                assert_eq!(20, raw_hash.len());
                self.hashes.push(raw_hash);
                piece_left = self.piece_size as usize;
            }
            if file_left == 0 {
                // return
                self.hashed_size = self.piece_size - piece_left as u64;
                return;
            }
        }
    }

    fn visit_end(&mut self) {
        if self.hashed_size > 0 {
            let raw_hash = self.hash_context.finalize_reset().to_vec();
            assert_eq!(20, raw_hash.len());
            self.hashes.push(raw_hash);
            self.hashed_size = 0;
        }
    }
}

struct FileMetadata {
    path: PathBuf,
    path_components: Vec<String>,
    metadata: Metadata,
}

impl FileMetadata {
    // Prefix will be stripped away from p to form the "path" list.
    pub fn new(
        entry: &DirEntry,
        prefix: &Path,
        progress: &mut ProgressIndicator,
    ) -> io::Result<FileMetadata> {
        assert!(entry.file_type().is_file());
        let path = entry.path();
        let partial_path = path.strip_prefix(prefix).unwrap();
        debug!("File: {}", partial_path.to_str().unwrap());
        let mut comp: Vec<String> = vec![];
        for c in partial_path.components() {
            match c {
                Component::Prefix(_) => unimplemented!("Not supported"),
                Component::RootDir => unimplemented!("Not supported"),
                Component::CurDir => unreachable!("Should not occurs"),
                Component::ParentDir => unreachable!("Why .. in path?"),
                Component::Normal(os_str) => comp.push(
                    os_str
                        .to_str()
                        .expect("Cannot encode path, this is not good")
                        .to_string(),
                ),
            }
        }
        progress.scan_progress(partial_path.to_str().unwrap());
        Ok(FileMetadata {
            path: path.to_path_buf(),
            path_components: comp,
            metadata: path.metadata()?,
        })
    }
}

pub struct TorrentMetadata {
    files: Vec<FileMetadata>,
    announces: Vec<Vec<String>>,
    piece_size: u64,
    private: bool,
    nodes: Vec<(String, u16)>,
    webseeds: Vec<String>,
}

impl TorrentMetadata {
    pub fn new(
        announces: Vec<Vec<String>>,
        nodes: Vec<(String, u16)>,
        private: bool,
        user_piece_size: Option<u64>,
        webseeds: Vec<String>,
    ) -> Self {
        if let Some(x) = user_piece_size {
            assert!((x & (x - 1)) == 0);
            assert!(x >= 16 * 1024);
            assert!(x <= 2 * 1024 * 1024);
        }
        assert!(!announces.is_empty() || !nodes.is_empty());

        TorrentMetadata {
            files: vec![],
            announces,
            piece_size: user_piece_size.unwrap_or(0),
            private,
            nodes,
            webseeds,
        }
    }

    pub fn scan<P: AsRef<Path>>(
        &mut self,
        base: P,
        progress: &mut ProgressIndicator,
    ) -> io::Result<()> {
        let canonical_path = base.as_ref().canonicalize()?;
        let prefix = canonical_path.parent().expect("Cannot be a root folder");

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
            let entry = entry?;
            if entry.file_type().is_file() {
                self.files
                    .push(FileMetadata::new(&entry, prefix, progress)?);
            }
        }
        progress.scan_end();

        // Sort files according to the file tree in bt v2 spec.
        self.files.sort_by_cached_key(|val| {
            val.path_components
                .iter()
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>()
        });
        info!("File list sorted.");

        Ok(())
    }

    pub fn hash(&mut self, progress: &mut ProgressIndicator) -> io::Result<BencodeValue> {
        // Determine piece size
        let total_size = self.files.iter().map(|f| f.metadata.len()).sum();
        self.piece_size = if self.piece_size != 0 {
            // Don't change if piece size is pre-set
            self.piece_size
        } else if total_size < 50 * 1024 * 1024 * 1024 {
            // Use 256KB for < 50GB
            256 * 1024
        } else {
            // Use 1MB for >= 50GB
            1024 * 1024
        };

        // Compute piece hashes
        progress.hash_begin(total_size);
        let mut hasher = Bep3Hasher::new(self.piece_size);
        for file_meta in &self.files {
            if file_meta.metadata.len() == 0 {
                hasher.visit_file(b"", progress);
            } else {
                debug!("Hashing {}...", file_meta.path.display());
                let f = File::open(&file_meta.path)?;
                let mmap = unsafe { MmapOptions::new().map(&f)? };
                hasher.visit_file(mmap.as_ref(), progress);
            }
        }
        hasher.visit_end();
        progress.hash_end();

        // Assemble info struct
        let mut info = BTreeMap::new();
        let name = self.files[0].path_components[0].clone();
        info.insert(b"name".to_vec(), BencodeValue::from(name.as_ref()));
        info.insert(
            b"piece length".to_vec(),
            BencodeValue::from(self.piece_size as i64),
        );
        if self.private {
            // BEP 27
            info.insert(b"private".to_vec(), BencodeValue::from(1));
        }
        info.insert(
            b"pieces".to_vec(),
            BencodeValue::Bytes(hasher.hashes.concat()),
        );
        if self.files.len() == 1 && self.files[0].path_components.len() == 1 {
            // Single file mode
            let file = &self.files[0];
            info.insert(
                b"length".to_vec(),
                BencodeValue::from(file.metadata.len() as i64),
            );
        } else {
            // Multi file mode
            let mut files = vec![];
            for f in &self.files {
                let mut file = BTreeMap::new();
                file.insert(
                    b"length".to_vec(),
                    BencodeValue::from(f.metadata.len() as i64),
                );
                assert!(f.path_components.len() > 1);
                assert_eq!(name, f.path_components[0]);
                let mut path_vec = vec![];
                for idx in 1..f.path_components.len() {
                    path_vec.push(BencodeValue::from(f.path_components[idx].as_ref()));
                }
                file.insert(b"path".to_vec(), BencodeValue::List(path_vec));
                files.push(BencodeValue::Map(file));
            }
            info.insert(b"files".to_vec(), BencodeValue::List(files));
        }

        // Assemble the torrent file structure
        let mut ret = BTreeMap::<Vec<u8>, BencodeValue>::new();
        if !self.announces.is_empty() {
            ret.insert(
                b"announce".to_vec(),
                BencodeValue::from(self.announces[0][0].as_str()),
            );
        }
        if self.announces.len() > 1 || self.announces[0].len() > 1 {
            // BEP 12
            ret.insert(
                b"announce-list".to_vec(),
                BencodeValue::List(
                    self.announces
                        .iter()
                        .map(|tier| {
                            BencodeValue::List(
                                tier.iter()
                                    .map(|a| BencodeValue::from(a.as_str()))
                                    .collect(),
                            )
                        })
                        .collect(),
                ),
            );
        }
        if !self.nodes.is_empty() {
            // BEP 5
            ret.insert(
                b"nodes".to_vec(),
                BencodeValue::List(
                    self.nodes
                        .iter()
                        .map(|(host, port)| {
                            BencodeValue::List(vec![
                                BencodeValue::from(host.as_str()),
                                BencodeValue::from(*port as i64),
                            ])
                        })
                        .collect(),
                ),
            );
        }
        if !self.webseeds.is_empty() {
            // BEP 19
            ret.insert(
                b"url-list".to_vec(),
                if self.webseeds.len() == 1 {
                    BencodeValue::from(self.webseeds[0].as_str())
                } else {
                    BencodeValue::List(
                        self.webseeds
                            .iter()
                            .map(|s| BencodeValue::from(s.as_str()))
                            .collect(),
                    )
                },
            );
        }
        ret.insert(b"info".to_vec(), BencodeValue::Map(info));
        Ok(BencodeValue::Map(ret))
    }
}
