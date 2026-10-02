// Gupax
//
// Copyright (c) 2024-2025 Cyrix126
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! Syncs the payout history: removes the payouts of orphaned blocks.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::StreamExt;
use log::*;
use serde::Deserialize;

use self::rpc::{NoAnswer, NodeClient, NodeRpc, block_header, synced_node};
use crate::{
    constants::{P2POOL_PAYOUT_CHECK_DEPTH, SECOND},
    disk::{errors::TomlError, gupax_p2pool_api::GupaxP2poolApi},
    xmr::PayoutOrd,
};

mod rpc;

// Time without data after which a request fails, so a stalled sync ends.
// The largest requests of a sync started answering within 0.2 s from a remote node.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

// Connections to the node, leaving P2Pool one of the 3 a public node accepts per IP.
const NODE_MAX_CONNECTIONS: usize = 2;

// Sources of the payout sync.
struct SyncSources {
    // RPC of the nodes of P2Pool, in the order it connects to them.
    nodes: Vec<NodeRpc>,
    found_blocks: PathBuf,
}

impl SyncSources {
    fn new(api: &GupaxP2poolApi) -> Self {
        Self {
            nodes: api.nodes.hosts.iter().map(NodeRpc::new).collect(),
            found_blocks: api.found_blocks.clone(),
        }
    }
}

// Starts a sync in a new thread, unless one is running.
// Returns whether it started.
pub fn start(api: &mut GupaxP2poolApi, gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>) -> bool {
    if api.syncing {
        return false;
    }
    let sources = SyncSources::new(api);
    api.syncing = true;
    api.stop_sync = false;
    api.sync.clear();
    let gupax_p2pool_api = Arc::clone(gupax_p2pool_api);
    std::thread::spawn(move || sync(&gupax_p2pool_api, &sources));
    true
}

#[tokio::main]
async fn sync(gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>, sources: &SyncSources) {
    let mut results = Vec::new();
    tokio::select! {
        () = run(gupax_p2pool_api, sources, &mut results) => (),
        () = stopped(gupax_p2pool_api) => results.push("Stopped".to_string()),
    }
    let mut api = gupax_p2pool_api.lock().unwrap();
    api.sync = results.join(". ");
    info!("Payout sync | {}", api.sync);
    api.syncing = false;
}

async fn stopped(gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>) {
    while !gupax_p2pool_api.lock().unwrap().stop_sync {
        tokio::time::sleep(SECOND).await;
    }
}

// Syncs the payouts with [sources] and describes the result in [results].
async fn run(
    gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>,
    sources: &SyncSources,
    results: &mut Vec<String>,
) {
    let (node, height) = match synced_node(&sources.nodes).await {
        Ok(synced) => synced,
        Err(e) => {
            results.push(format!("Node error: {e:#}"));
            return;
        }
    };
    let checked = async {
        let found_blocks = read_found_blocks(&sources.found_blocks)?;
        let last = last_final_block(height);
        remove_orphaned_payouts(gupax_p2pool_api, &node, &found_blocks, last, results).await
    }
    .await;
    if let Err(e) = checked {
        results.push(format!("Orphan check error: {e:#}"));
    }
}

// Last block of a chain of [height] blocks that can no longer be orphaned.
fn last_final_block(height: u64) -> u64 {
    height.saturating_sub(1 + P2POOL_PAYOUT_CHECK_DEPTH)
}

fn write_error(e: TomlError) -> anyhow::Error {
    anyhow::anyhow!("payout history write error: {e}")
}

//---------------------------------------------------------------------------------------------------- Orphaned blocks
// Block found by the pool in the P2Pool data API, with the fields the sync uses.
#[derive(Deserialize)]
struct FoundBlock {
    height: u64,
    hash: String,
}

// Removes the payouts of the final blocks found by the pool that were orphaned, and describes the
// result. [last] is the last final block. Fails with the error of the first block left unchecked.
async fn remove_orphaned_payouts(
    gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>,
    node: &NodeClient<'_>,
    found_blocks: &[FoundBlock],
    last: u64,
    results: &mut Vec<String>,
) -> anyhow::Result<()> {
    let (paid, found_blocks) = {
        let api = gupax_p2pool_api.lock().unwrap();
        let paid: HashSet<u64> = api
            .log
            .lines()
            .filter_map(PayoutOrd::payout_height)
            .collect();
        // A block is checked once final.
        let found_blocks: Vec<&FoundBlock> = found_blocks
            .iter()
            .filter(|block| block.height <= last && !api.final_checked.contains(&block.height))
            .collect();
        (paid, found_blocks)
    };
    let paying: Vec<&FoundBlock> = found_blocks
        .iter()
        .copied()
        .filter(|block| paid.contains(&block.height))
        .collect();
    let mut headers = futures::stream::iter(&paying)
        .map(|block| block_header(node, block.height))
        .buffered(NODE_MAX_CONNECTIONS);
    let mut orphaned = Vec::new();
    let mut kept = HashSet::new();
    let mut unchecked = HashSet::new();
    let mut error = None;
    for (checked, block) in paying.iter().enumerate() {
        let header = match headers.next().await {
            Some(Ok(header)) => header,
            // A node without answer leaves the blocks unchecked, with their payouts kept.
            Some(Err(e)) if e.is::<NoAnswer>() => {
                unchecked.extend(paying[checked..].iter().map(|block| block.height));
                error.get_or_insert(e);
                break;
            }
            Some(Err(e)) => {
                unchecked.insert(block.height);
                error.get_or_insert(e);
                continue;
            }
            None => break,
        };
        if header.hash == block.hash {
            kept.insert(block.height);
        } else {
            info!("Payout sync | Block {} was orphaned", block.height);
            orphaned.push(block.height);
        }
    }
    // Another block of the pool may have replaced the orphaned one.
    orphaned.retain(|height| !kept.contains(height) && !unchecked.contains(height));
    let removed =
        GupaxP2poolApi::remove_payouts(gupax_p2pool_api, &orphaned).map_err(write_error)?;
    let checked = found_blocks.iter().map(|block| block.height);
    gupax_p2pool_api
        .lock()
        .unwrap()
        .final_checked
        .extend(checked.filter(|height| !unchecked.contains(height)));
    results.push(format!("Orphaned payouts removed: {removed}"));
    error.map_or(Ok(()), Err)
}

// Blocks found by the pool, listed by P2Pool in the file [path].
fn read_found_blocks(path: &Path) -> anyhow::Result<Vec<FoundBlock>> {
    match std::fs::read_to_string(path) {
        Ok(found_blocks) => Ok(serde_json::from_str(&found_blocks)?),
        // P2Pool writes it once the pool found a block.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}
