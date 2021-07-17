// mktorrent-rs creates the BitTorrent metadata file for file sharing.
// Copyright (C) 2021  Recursive G

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

mod bencode;
mod dirwalker;
mod progress;
mod torrent_meta;

use dirwalker::WalkedDir;
use progress::ProgressIndicator;
use torrent_meta::TorrentMetadata as TorrentMetadataV1;

use clap::Clap;
use log::*;
use std::fs::OpenOptions;
use std::io::Write;

fn split_host_port(s: &str) -> Option<(String, u16)> {
    let (l, r) = s.rsplit_once(':')?;
    let port = match r.parse::<u16>() {
        Ok(p) => p,
        Err(_) => return None,
    };
    let host = if l.starts_with('[') && l.ends_with(']') {
        l[1..l.len() - 1].to_string()
    } else {
        l.to_string()
    };
    Some((host, port))
}

/// Creates BitTorrent metadata file.
#[derive(Clap, Debug)]
#[clap(name = "mktorrent-rs", version = "0.1.0", author = "Recursive G")]
struct CliOptions {
    /// Input file or folder.
    input: String,
    /// Output torrent file.
    #[clap(short, long)]
    output: String,
    /// Bytes of each piece. Must be a power of 2. 16KB minimal. Leave unset for auto.
    #[clap(short, long)]
    piece_size: Option<u64>,
    /// Specify tracker URLs. Use this option multiple times to specify
    /// mutiple tiers. Use comma to split trackers in the same tier.
    #[clap(short, long)]
    announce: Vec<String>,
    /// List of DHT nodes to include in the file.
    /// Do NOT add http(s):// prefixes. Add square brackets to IPv6 addresses.
    /// Use this option multiple times to include multiple nodes.
    #[clap(short, long)]
    node: Vec<String>,
    /// Create file in BEP-52 (BitTorrent v2) format.
    #[clap(long)]
    bep52: bool,
    /// Mark torrent as private.
    #[clap(long)]
    private: bool,
    /// A level of verbosity, and can be used multiple times.
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// (debug) stop after dir walk.
    #[clap(long)]
    stop_after_dirwalk: bool,
    /// (debug) stop after hash.
    #[clap(long)]
    stop_after_hash: bool,
    /// WebSeed(BEP19) URLs. Use this option can be used multiple times.
    #[clap(long)]
    webseed: Vec<String>,
}

fn main() {
    // CLI option checks
    let opts = CliOptions::parse();
    stderrlog::new()
        .module(module_path!())
        .verbosity(opts.verbose as usize + 2)
        .init()
        .unwrap();

    if opts.bep52 {
        unimplemented!("BitTorrent v2 format is not implemented.");
    }
    if opts.announce.is_empty() && opts.node.is_empty() {
        error!("Please specify tracker/node URL using --announce/--node");
        return;
    }

    if let Some(x) = opts.piece_size {
        if x < 16 * 1024 {
            error!("Piece size too small");
            return;
        }
        if x > 2 * 1024 * 1024 {
            error!("Piece size too large");
            return;
        }
        if (x & (x - 1)) != 0 {
            error!("Piece size is not a power of 2");
            return;
        }
    }

    // Argument parsing
    let tiered_announces = opts
        .announce
        .iter()
        .map(|s| s.split(',').map(str::to_string).collect())
        .collect();
    debug!("Tiered announce URLs:\n{:#?}", tiered_announces);
    let mut nodes = vec![];
    for host_port in opts.node {
        match split_host_port(host_port.as_str()) {
            Some(x) => {
                debug!("Node host={} port={}", x.0, x.1);
                nodes.push(x)
            }
            None => {
                error!("Invalid node: {}", host_port);
                return;
            }
        }
    }

    // Directory walk
    let mut progress = ProgressIndicator::new(opts.verbose > 0);
    let walked_dir = WalkedDir::walk(opts.input, &mut progress).unwrap();
    if opts.stop_after_dirwalk {
        return;
    }

    //
    let mut torrent_meta = TorrentMetadataV1::new(
        tiered_announces,
        nodes,
        opts.private,
        opts.piece_size,
        opts.webseed,
        walked_dir,
    );

    // Hash and build BencodeValue
    let meta = if !opts.bep52 {
        torrent_meta.hash(&mut progress).unwrap()
    } else {
        unreachable!()
    };
    if opts.stop_after_hash {
        return;
    }

    // Write file
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(opts.output)
        .unwrap();
    file.write_all(meta.serialize().as_slice()).unwrap();

    // let mut total_size: u64 = 0;
    // for file_meta in &torrent_meta.files {
    //     println!("{}", file_meta.path.to_str().unwrap());
    //     for comp in &file_meta.path_components {
    //         println!("  ==> {}", comp);
    //     }
    //     println!("  size: {}", file_meta.fsize);
    //     println!("  sha1: {}", file_meta.sha1);
    //     total_size += file_meta.fsize;
    // }
    // println!("Total size: {}", total_size);
}
