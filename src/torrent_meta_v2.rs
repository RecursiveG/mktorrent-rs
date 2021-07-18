use crate::bencode::BencodeValue;
use crate::dirwalker::*;
use crate::progress::ProgressIndicator;

use crossbeam::queue::SegQueue;
use crossbeam::scope;
use indicatif::HumanDuration;
use log::*;
use memmap2::MmapOptions;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

struct HashJob<'a> {
    file: &'a Path,
    // starting offset bytes, aligned to v1 boundary
    offset: u64,
    // actual length, not aligned to v1 nor v2 piece size
    data_len: u64,
    // arbitrary #
    v1_pieces: u64,
    v1_piece_size: u64,
    v1_hash: &'a mut [u8],
    v1_last_hash_zero_fill: bool,
    // v1_piece*piece_factor except last job of the file.
    v2_pieces: u64,
    v2_hash: &'a mut [u8],
}

impl Debug for HashJob<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "HashJob[\
            path={}, offset={}, data_len={}, v1_pieces={}, \
            v1_piece_size={}, v1_hash.len={}, v2_piece={}, \
            v2_hash.len={}]",
            self.file.display(),
            self.offset,
            self.data_len,
            self.v1_pieces,
            self.v1_piece_size,
            self.v1_hash.len(),
            self.v2_pieces,
            self.v2_hash.len()
        )
    }
}

struct FileMetadata {
    file: DataFile,
    // How many leaf pieces in the merkle tree
    merkle_piece_count: u64,
    // merkle_tree[0] is the hash of 16KiB blocks
    // merkle_tree[1] is the layer above, etc.
    // Length of each layer is a multiple of 32 (size of SHA256).
    // Hashs that don't cover actual data won't be stored.
    merkle_tree: Vec<Vec<u8>>,
    // How many piece for the sha1 pieces
    hash_v1_piece_count: u64,
    // Hash computed using BEP3 method.
    // Hashs that don't cover actual data won't be stored.
    hash_v1: Vec<u8>,
    // padding bytes after this file. BEP47
    padding: u64,
    is_last_data_file: bool,
}

impl FileMetadata {
    fn new(f: DataFile, piece_size: u64) -> Self {
        let l = f.metadata.len();
        if l == 0 {
            // Empty file is treated differently.
            FileMetadata {
                file: f,
                merkle_piece_count: 0,
                merkle_tree: vec![],
                hash_v1_piece_count: 0,
                hash_v1: vec![],
                padding: 0,
                is_last_data_file: false,
            }
        } else {
            let merkle_piece_count = (l - 1) / (16 * 1024) + 1;
            let hash_v1_piece_count = (l - 1) / piece_size + 1;

            FileMetadata {
                file: f,
                merkle_piece_count,
                merkle_tree: vec![vec![0; (merkle_piece_count * 32) as usize]],
                hash_v1_piece_count,
                hash_v1: vec![0; (hash_v1_piece_count * 20) as usize],
                padding: if l % piece_size == 0 {
                    0
                } else {
                    piece_size - (l % piece_size)
                },
                is_last_data_file: false,
            }
        }
    }
}

pub struct TorrentMetadata {
    files: Vec<FileMetadata>,
    total_bytes: u64,
    announces: Vec<Vec<String>>,
    // bytes of a piece for v1
    piece_size: u64,
    // how many 16KiB pieces in piece_size
    piece_factor: u64,
    // piece_factor = 2^piece_level
    piece_level: u64,
    private: bool,
    nodes: Vec<(String, u16)>,
    webseeds: Vec<String>,
    // meta version
}

impl TorrentMetadata {
    pub fn new(
        announces: Vec<Vec<String>>,
        nodes: Vec<(String, u16)>,
        private: bool,
        user_piece_size: Option<u64>,
        webseeds: Vec<String>,
        walked_dir: WalkedDir,
    ) -> Self {
        if let Some(x) = user_piece_size {
            assert!((x & (x - 1)) == 0);
            assert!(x >= 16 * 1024);
            assert!(x <= 2 * 1024 * 1024);
        }
        if walked_dir.files.is_empty() {
            panic!("No file selected");
        }
        let total_bytes =
            walked_dir.files.iter().map(|e| e.metadata.len()).sum();
        // TODO auto piece size selection
        let piece_size = user_piece_size.unwrap_or(256 * 1024);
        let mut piece_level = 0u64;
        let mut tmp = 16 * 1024;
        for level in 0.. {
            if tmp == piece_size {
                piece_level = level;
                break;
            } else if tmp < piece_size {
                tmp *= 2;
            } else {
                panic!("incorrect piece size");
            }
        }
        assert!(!announces.is_empty() || !nodes.is_empty());

        let mut ret = TorrentMetadata {
            files: walked_dir
                .files
                .into_iter()
                .map(|e| FileMetadata::new(e, piece_size))
                .collect(),
            total_bytes,
            announces,
            piece_size,
            piece_factor: 2u64.checked_pow(piece_level as u32).unwrap(),
            piece_level,
            private,
            nodes,
            webseeds,
        };
        for i in ret.files.len() - 1..=0 {
            if ret.files[i].file.metadata.len() > 0 {
                ret.files[i].is_last_data_file = true;
                ret.files[i].padding = 0;
            }
        }
        ret
    }

    pub fn hash(
        &mut self,
        progress: &mut ProgressIndicator,
        thread_num: u32,
        write_v1: bool,
        write_v2: bool,
    ) -> io::Result<BencodeValue> {
        assert!(write_v1 || write_v2);
        let tasks: SegQueue<HashJob> = SegQueue::new();

        // Fill task queue
        const MAX_JOB_BYTES: u64 = 1024 * 1024 * 1024; // 1GiB
        const MERKLE_PIECE_SIZE: u64 = 16 * 1024; // 16KiB
        for f in &mut self.files {
            if f.file.metadata.len() == 0 {
                continue;
            }
            let num_tasks = (f.file.metadata.len() - 1) / MAX_JOB_BYTES + 1;
            // # of piece for each task
            let mut task_pieces = vec![];
            for _ in 0..f.hash_v1_piece_count % num_tasks {
                task_pieces.push(f.hash_v1_piece_count / num_tasks + 1);
            }
            for _ in (f.hash_v1_piece_count % num_tasks)..num_tasks {
                task_pieces.push(f.hash_v1_piece_count / num_tasks);
            }

            let mut piece_offset = 0;
            let mut v1_hash = f.hash_v1.as_mut_slice();
            let mut v2_hash = f.merkle_tree.get_mut(0).unwrap().as_mut_slice();
            let mut left_merkle_piece = f.merkle_piece_count;
            for (idx, &this_v1_pieces) in task_pieces.iter().enumerate() {
                if left_merkle_piece == 0 {
                    panic!(
                        "left_merkle_piece==0 {}",
                        f.file.entry.path().display()
                    );
                }
                let this_v2_pieces = std::cmp::min(
                    this_v1_pieces * self.piece_factor,
                    left_merkle_piece,
                );
                let right_boundary = std::cmp::min(
                    (piece_offset + this_v1_pieces) * self.piece_size,
                    f.file.metadata.len(),
                );
                let data_len = right_boundary - piece_offset * self.piece_size;

                let (this_v1_hash, rem_v1_hash) =
                    v1_hash.split_at_mut((this_v1_pieces * 20) as usize);
                let (this_v2_hash, rem_v2_hash) =
                    v2_hash.split_at_mut((this_v2_pieces * 32) as usize);
                v1_hash = rem_v1_hash;
                v2_hash = rem_v2_hash;

                let job = HashJob {
                    file: f.file.entry.path(),
                    offset: piece_offset * self.piece_size,
                    data_len,
                    v1_pieces: this_v1_pieces,
                    v1_piece_size: self.piece_size,
                    v1_hash: this_v1_hash,
                    v1_last_hash_zero_fill: !((idx == task_pieces.len() - 1)
                        && f.is_last_data_file),
                    v2_pieces: this_v2_pieces,
                    v2_hash: this_v2_hash,
                };
                debug!("{:?}", job);
                tasks.push(job);

                piece_offset += this_v1_pieces;
                left_merkle_piece -= this_v2_pieces;
            }
        }

        progress.hash_begin(self.total_bytes);
        let piece_factor = self.piece_factor;
        scope(|s| {
            let (progress_notify, progress_rx) = mpsc::channel();

            // UI thread
            s.spawn(|_| {
                // Rusty magic, moves rx inside without moving progress.
                let progress_rx = progress_rx;
                loop {
                    if let Ok(b) = progress_rx.recv() {
                        trace!(
                            "{:?} Received hash_progress: {}",
                            thread::current().id(),
                            b
                        );
                        progress.hash_progress(b);
                    } else {
                        return;
                    }
                }
            });

            // Worker threads
            for _ in 0..thread_num {
                let progress = progress_notify.clone();
                s.spawn(|_| {
                    let mut byte_count = 0u64;
                    // move progress into lambda
                    let progress = progress;
                    loop {
                        let job = tasks.pop();
                        if job.is_none() {
                            break;
                        }
                        let job = job.unwrap();
                        debug!(
                            "{:?} takes job {:?}",
                            thread::current().id(),
                            job
                        );

                        // mmap file
                        let f = File::open(job.file).unwrap();
                        let mmap =
                            unsafe { MmapOptions::new().map(&f).unwrap() };
                        let data = mmap.as_ref();

                        // local vars
                        // |----|----|----|----| one v1, four v2 pieces
                        // |-----------|         actual data
                        // |------------0000000| covered by one v1 hash
                        // |----|----|-|         covered by 3 v2 hashes
                        // ^-job.offset        ^-hash_rbound
                        let hash_rbound =
                            job.offset + job.v1_pieces * job.v1_piece_size;
                        let data_rbound = job.offset + job.data_len;
                        let mut cursor = job.offset;

                        let mut v1_hasher = Sha1::new();
                        let mut v2_hasher = Sha256::new();
                        let mut finished_v1_pieces = 0u64;
                        let mut finished_v2_pieces = 0u64;

                        // loop, in each iter, we either read a full 16kb block
                        // or reaches the end of the file.
                        while cursor < data_rbound {
                            trace!(
                                "{:?} cursor={} file={}",
                                thread::current().id(),
                                cursor,
                                job.file.display()
                            );
                            let bytes =
                                if cursor + MERKLE_PIECE_SIZE > data_rbound {
                                    data_rbound - cursor
                                } else {
                                    MERKLE_PIECE_SIZE
                                };
                            v1_hasher.update(
                                &data[cursor as usize
                                    ..(cursor + bytes) as usize],
                            );
                            v2_hasher.update(
                                &data[cursor as usize
                                    ..(cursor + bytes) as usize],
                            );
                            let _ = progress.send(bytes);
                            byte_count += bytes;

                            if bytes == MERKLE_PIECE_SIZE {
                                // Finished a v2 piece
                                let l = (finished_v2_pieces * 32) as usize;
                                let r =
                                    ((finished_v2_pieces + 1) * 32) as usize;
                                job.v2_hash[l..r].copy_from_slice(
                                    v2_hasher.finalize_reset().as_slice(),
                                );
                                finished_v2_pieces += 1;

                                // Also finished a v1 piece
                                if (finished_v2_pieces % piece_factor) == 0 {
                                    let l = (finished_v1_pieces * 20) as usize;
                                    let r = ((finished_v1_pieces + 1) * 20)
                                        as usize;
                                    job.v1_hash[l..r].copy_from_slice(
                                        v1_hasher.finalize_reset().as_slice(),
                                    );
                                    finished_v1_pieces += 1;
                                }
                            }

                            cursor += bytes;
                        }
                        assert!(cursor <= hash_rbound);

                        if cursor % MERKLE_PIECE_SIZE != 0 {
                            // file end is not aligned to 16kb
                            // write the last sha256
                            let l = (finished_v2_pieces * 32) as usize;
                            let r = ((finished_v2_pieces + 1) * 32) as usize;
                            job.v2_hash[l..r].copy_from_slice(
                                v2_hasher.finalize_reset().as_slice(),
                            );
                            finished_v2_pieces += 1;
                        }

                        if cursor != hash_rbound {
                            // file end is not aligned to v1 hash boundary
                            // zero fill the remaining v1 hash for gap file
                            // and write the last sha1
                            if job.v1_last_hash_zero_fill {
                                v1_hasher.update(vec![
                                    0;
                                    (hash_rbound - cursor)
                                        as usize
                                ]);
                            }
                            let l = (finished_v1_pieces * 20) as usize;
                            let r = ((finished_v1_pieces + 1) * 20) as usize;
                            job.v1_hash[l..r].copy_from_slice(
                                v1_hasher.finalize_reset().as_slice(),
                            );
                            finished_v1_pieces += 1;
                        }

                        assert_eq!(finished_v1_pieces, job.v1_pieces);
                        assert_eq!(finished_v2_pieces, job.v2_pieces);
                    }
                    debug!(
                        "{:?} processed {} bytes",
                        thread::current().id(),
                        byte_count
                    );
                });
            }
            drop(progress_notify);
        })
        .unwrap();
        drop(tasks);
        progress.hash_end();

        // Complete the merkle trees
        let start = Instant::now();
        let mut filler_sha256 = vec![vec![0; 32]];
        for i in 1..50 {
            // Covers up to 16 EiB. Really?!
            let mut hasher = Sha256::new();
            hasher.update(filler_sha256[i - 1].as_slice());
            hasher.update(filler_sha256[i - 1].as_slice());
            filler_sha256.push(hasher.finalize().to_vec());
        }

        for f in &mut self.files {
            if f.file.metadata.len() == 0 || f.merkle_tree[0].len() == 32 {
                continue;
            }
            for level in 0.. {
                let src = f.merkle_tree.get(level).unwrap().as_slice();
                assert!(src.len() != 32);
                assert!(src.len() % 32 == 0);
                let mut dst = vec![];
                for i in 0..src.len() / 64 {
                    dst.extend_from_slice(
                        Sha256::digest(&src[i * 64..(i + 1) * 64]).as_slice(),
                    );
                }
                if src.len() > 32 && src.len() % 64 != 0 {
                    let mut hasher = Sha256::new();
                    hasher.update(&src[(src.len() / 64) * 64..]);
                    hasher.update(filler_sha256.get(level).unwrap().as_slice());
                    dst.extend_from_slice(hasher.finalize().as_slice());
                }
                let dst_len = dst.len();
                f.merkle_tree.push(dst);
                if dst_len == 32 {
                    break;
                }
            }
        }
        info!("Merkle tree built in {}", HumanDuration(start.elapsed()));

        // Assemble info struct
        let mut ret = BTreeMap::<Vec<u8>, BencodeValue>::new();
        let mut info = BTreeMap::new();
        let name = self.files[0].file.path_components[0].clone();
        info.insert(b"name".to_vec(), BencodeValue::from(name.as_ref()));
        info.insert(
            b"piece length".to_vec(),
            BencodeValue::from(self.piece_size as i64),
        );
        if self.private {
            // BEP 27
            info.insert(b"private".to_vec(), BencodeValue::from(1));
        }

        // BEP3 pieces + "length"/"files"
        if write_v1 {
            info.insert(
                b"pieces".to_vec(),
                BencodeValue::Bytes(
                    self.files
                        .iter()
                        .map(|e| e.hash_v1.as_slice())
                        .collect::<Vec<_>>()
                        .concat(),
                ),
            );
            if self.files.len() == 1
                && self.files[0].file.path_components.len() == 1
            {
                // Single file mode
                let file = &self.files[0];
                info.insert(
                    b"length".to_vec(),
                    BencodeValue::from(file.file.metadata.len() as i64),
                );
            } else {
                // Multi file mode
                let mut files = vec![];
                for f in &self.files {
                    let mut file = BTreeMap::new();
                    file.insert(
                        b"length".to_vec(),
                        BencodeValue::from(f.file.metadata.len() as i64),
                    );
                    // assert!(f.file.path_components.len() > 1);
                    assert_eq!(name, f.file.path_components[0]);
                    let mut path_vec = vec![];
                    for idx in 1..f.file.path_components.len() {
                        path_vec.push(BencodeValue::from(
                            f.file.path_components[idx].as_ref(),
                        ));
                    }
                    file.insert(b"path".to_vec(), BencodeValue::List(path_vec));
                    files.push(BencodeValue::Map(file));

                    if f.padding > 0 {
                        let mut pad_file = BTreeMap::new();
                        pad_file
                            .insert(b"attr".to_vec(), BencodeValue::from("p"));
                        pad_file.insert(
                            b"length".to_vec(),
                            BencodeValue::from(f.padding as i64),
                        );
                        pad_file.insert(
                            b"path".to_vec(),
                            BencodeValue::List(vec![
                                BencodeValue::from(".pad"),
                                BencodeValue::from(
                                    format!("{}", f.padding).as_str(),
                                ),
                            ]),
                        );
                        files.push(BencodeValue::Map(pad_file));
                    }
                }
                info.insert(b"files".to_vec(), BencodeValue::List(files));
            }
        }

        // BEP 52:
        // rootless - like bep3;
        // multi/single file mode - duplicate "name" in file tree
        if write_v2 {
            info.insert(b"meta version".to_vec(), BencodeValue::Integer(2));
            let mut file_tree: BTreeMap<Vec<u8>, BencodeValue> =
                BTreeMap::new();
            let can_strip = self
                .files
                .iter()
                .filter(|e| e.file.path_components.len() <= 1)
                .count()
                == 0;
            let rootless = true; // TODO
            let strip = if can_strip && rootless { 1 } else { 0 };
            for f in &self.files {
                let mut t = &mut file_tree;
                for p in f.file.path_components.iter().skip(strip) {
                    let e = t
                        .entry(p.as_bytes().to_vec())
                        .or_insert_with(|| BencodeValue::Map(BTreeMap::new()));
                    if let BencodeValue::Map(m) = e {
                        t = m;
                    } else {
                        panic!("bug");
                    }
                }
                let mut inner = BTreeMap::new();
                inner.insert(
                    b"length".to_vec(),
                    BencodeValue::from(f.file.metadata.len() as i64),
                );
                if f.file.metadata.len() != 0 {
                    assert!(f.merkle_tree.last().unwrap().len() == 32);
                    inner.insert(
                        b"pieces root".to_vec(),
                        BencodeValue::Bytes(
                            f.merkle_tree.last().unwrap().clone(),
                        ),
                    );
                }
                t.insert(vec![], BencodeValue::Map(inner));
            }
            info.insert(b"file tree".to_vec(), BencodeValue::Map(file_tree));

            let mut piece_layers = BTreeMap::new();
            for f in &self.files {
                if f.file.metadata.len() <= self.piece_size {
                    continue;
                }
                let key = f.merkle_tree.last().unwrap();
                let val = f.merkle_tree.get(self.piece_level as usize).unwrap();
                assert!(key.len() == 32);
                assert!(val.len() % 32 == 0 && val.len() > 32);
                piece_layers
                    .insert(key.clone(), BencodeValue::Bytes(val.clone()));
            }
            ret.insert(
                b"piece layers".to_vec(),
                BencodeValue::Map(piece_layers),
            );
        }
        // TODO debug info hash

        // Assemble torrent file structure
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
