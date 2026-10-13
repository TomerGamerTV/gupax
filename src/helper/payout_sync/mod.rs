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

//! Syncs the payout history: adds the payouts that Gupax did not see in the P2Pool output
//! and removes the payouts of orphaned blocks.

use std::{
    collections::{HashMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use log::*;
use monero::{
    Address, AddressType, PrivateKey, PublicKey, Transaction, ViewPair,
    blockdata::transaction::SubField, cryptonote::onetime_key::KeyGenerator,
};
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;

use self::observer::{observer_last_found, observer_payouts, observer_url};
use self::rpc::{
    BlockHeader, NoAnswer, NodeClient, NodeError, NodeRpc, block_coinbase, block_header,
    block_headers, coinbases, synced_node, transaction,
};
use super::MONERO_BLOCK_TIME_IN_SECONDS;
use crate::{
    components::update::APP_USER_AGENT,
    constants::{P2POOL_PAYOUT_CHECK_DEPTH, SECOND},
    disk::{errors::TomlError, gupax_p2pool_api::GupaxP2poolApi, state::P2poolChain},
    miscs::client_with,
    xmr::{AtomicUnit, PayoutOrd},
};

mod observer;
mod rpc;

// Time without data after which a request fails, so a stalled sync ends and the automatic syncs
// go on. The largest requests of a sync started answering within 0.2 s from a remote node.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

// (date, atomic_unit, height) of each payout found.
type Payouts = Vec<(String, AtomicUnit, u64)>;

// Blocks of the last 30 days, scanned by the first scan of an address.
const FIRST_SCAN_BLOCKS: u64 = 30 * 24 * 60 * 60 / MONERO_BLOCK_TIME_IN_SECONDS;
// Maximum headers and transactions returned by a restricted node per request.
const NODE_MAX_HEADERS: u64 = 1000;
const NODE_MAX_TRANSACTIONS: usize = 100;
// Interval of the automatic syncs, the time for a block to become final.
pub const SYNC_INTERVAL: Duration =
    Duration::from_secs(P2POOL_PAYOUT_CHECK_DEPTH * MONERO_BLOCK_TIME_IN_SECONDS);
// Interval of the automatic syncs after a failed one.
pub const RETRY_INTERVAL: Duration = Duration::from_secs(10 * 60);
// Bytes read from a private view key file, which holds a few lines.
const VIEW_KEY_FILE_MAX: u64 = 64 * 1024;
// Source of the view key scans in the scan file.
const VIEW_KEY_SOURCE: &str = "view-key";

// The missing payouts are found with the view key or the observer.
struct SyncSources {
    view_key: String,
    scan_from: String,
    observer: String,
    chain: P2poolChain,
    address: String,
    // RPC of the nodes of P2Pool, in the order it connects to them.
    nodes: Vec<NodeRpc>,
    found_blocks: PathBuf,
}

impl SyncSources {
    fn new(api: &GupaxP2poolApi) -> Self {
        // An observer left empty is the public one of its chain.
        let observer = api
            .observers
            .iter()
            .find(|observer| observer.chain.as_ref() == Some(&api.chain))
            .map_or(String::new(), |observer| match observer.url.trim() {
                "" => api.chain.public_observer().to_string(),
                url => observer_url(url),
            });
        Self {
            view_key: api.view_key.trim().to_string(),
            scan_from: api.scan_from.trim().to_string(),
            observer,
            chain: api.chain.clone(),
            address: api.address.clone(),
            nodes: (api.nodes.hosts.iter())
                .map(|host| NodeRpc::new(host, &api.nodes.socks5))
                .collect(),
            found_blocks: api.found_blocks.clone(),
        }
    }
}

// Address P2Pool pays: [subaddress] if valid, else [wallet].
pub fn payout_address(wallet: &str, subaddress: &str) -> String {
    if Address::from_str(subaddress).is_ok_and(|a| a.addr_type == AddressType::SubAddress) {
        subaddress.to_string()
    } else {
        wallet.to_string()
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
    api.next_sync = Some(Instant::now() + SYNC_INTERVAL);
    let gupax_p2pool_api = Arc::clone(gupax_p2pool_api);
    std::thread::spawn(move || sync(&gupax_p2pool_api, &sources));
    true
}

#[tokio::main]
async fn sync(gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>, sources: &SyncSources) {
    let mut results = Vec::new();
    let failed = tokio::select! {
        failed = run(gupax_p2pool_api, sources, &mut results) => failed,
        () = stopped(gupax_p2pool_api) => {
            results.push("Stopped".to_string());
            false
        }
    };
    let mut api = gupax_p2pool_api.lock().unwrap();
    api.sync = results.join(". ");
    if failed {
        api.next_sync = Some(Instant::now() + RETRY_INTERVAL);
    }
    info!("Payout sync | {}", api.sync);
    api.syncing = false;
}

async fn stopped(gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>) {
    while !gupax_p2pool_api.lock().unwrap().stop_sync {
        tokio::time::sleep(SECOND).await;
    }
}

// Syncs the payouts with [sources], describes the result in [results] and returns whether it
// failed.
async fn run(
    gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>,
    sources: &SyncSources,
    results: &mut Vec<String>,
) -> bool {
    let mut failed = false;
    let (node, height) = match synced_node(&sources.nodes).await {
        Ok(synced) => synced,
        Err(e) => {
            results.push(format!("Node error: {e:#}"));
            return true;
        }
    };
    let found_blocks = read_found_blocks(&sources.found_blocks);
    // The observer lists the payouts when the view key does not belong to the address.
    let pair = if sources.view_key.is_empty() {
        None
    } else {
        view_pair(&sources.address, &sources.view_key)
            .inspect_err(|e| {
                results.push(format!("Sync error: {e:#}"));
                failed = true;
            })
            .ok()
    };
    let mut main = MainChain {
        last: last_final_block(height),
        hashes: HashMap::new(),
        payouts: Payouts::new(),
    };
    if let Some(pair) = &pair {
        let found = found_blocks
            .iter()
            .flatten()
            .map(|block| block.height)
            .collect();
        let scanned = scan(
            gupax_p2pool_api,
            &node,
            sources,
            height,
            pair,
            &found,
            results,
        )
        .await;
        match scanned {
            Ok(scanned) => (main.hashes, main.payouts) = scanned,
            Err(e) => {
                results.push(format!("Sync error: {e:#}"));
                failed = true;
            }
        }
    } else if !sources.observer.is_empty() {
        let oldest_found = found_blocks.as_ref().ok().and_then(|found_blocks| {
            let api = gupax_p2pool_api.lock().unwrap();
            let unchecked = unchecked_blocks(&api, found_blocks, main.last);
            unchecked.iter().map(|block| block.height).min()
        });
        match observer_sync(gupax_p2pool_api, sources, oldest_found, height).await {
            Ok((added, payouts)) => {
                results.push(format!("Missing payouts added: {added}"));
                main.payouts = payouts;
            }
            Err(e) => {
                results.push(format!("Sync error: {e:#}"));
                failed = true;
            }
        }
    }
    let checked = async {
        let found_blocks = found_blocks?;
        let pair = pair.as_ref();
        remove_orphaned_payouts(gupax_p2pool_api, &node, &found_blocks, pair, &main, results).await
    }
    .await;
    if let Err(e) = checked {
        results.push(format!("Orphan check error: {e:#}"));
        failed = true;
    }
    failed
}

// Last block of a chain of [height] blocks that can no longer be orphaned.
fn last_final_block(height: u64) -> u64 {
    height.saturating_sub(1 + P2POOL_PAYOUT_CHECK_DEPTH)
}

fn client() -> anyhow::Result<ClientWithMiddleware> {
    let client = reqwest::Client::builder()
        .read_timeout(READ_TIMEOUT)
        .user_agent(APP_USER_AGENT)
        .build()?;
    Ok(client_with(client))
}

fn write_error(e: TomlError) -> anyhow::Error {
    anyhow::anyhow!("payout history write error: {e}")
}

//---------------------------------------------------------------------------------------------------- Observer
// Adds the payouts of the address of [sources] listed by its observer since the last sync, and
// returns how many were added with the payouts listed. [oldest_found] is the oldest block found by
// the pool left to check for orphans and [height] the height of the chain.
async fn observer_sync(
    gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>,
    sources: &SyncSources,
    oldest_found: Option<u64>,
    height: u64,
) -> anyhow::Result<(u64, Payouts)> {
    let source = sources.chain.to_string().to_lowercase();
    let address = &sources.address;
    let path_scan = gupax_p2pool_api.lock().unwrap().path_scan.clone();
    let position = GupaxP2poolApi::read_scan(&path_scan, &source, address);
    // The payouts before [position] are in the history, the orphan check needs the ones
    // of the blocks it checks.
    let from_height =
        position.map(|position| oldest_found.map_or(position, |found| found.min(position)));
    let client = client()?;
    // The observer lists the payouts up to the last block found by the pool it knows.
    let last = last_final_block(height).min(observer_last_found(&client, &sources.observer).await?);
    let Some(payouts) =
        observer_payouts(&client, &sources.observer, address, from_height, last).await?
    else {
        return Ok((0, Payouts::new()));
    };
    let added = GupaxP2poolApi::merge_payouts(gupax_p2pool_api, &payouts).map_err(write_error)?;
    if position.is_none_or(|position| last + 1 > position) {
        GupaxP2poolApi::write_scan(&path_scan, last + 1, &source, address, position.is_some())
            .map_err(write_error)?;
    }
    Ok((added, payouts))
}

//---------------------------------------------------------------------------------------------------- Orphaned blocks
// Block found by the pool in the P2Pool data API, with the fields the sync uses.
#[derive(Deserialize)]
struct FoundBlock {
    height: u64,
    hash: String,
}

// Main chain known by the sync before the orphan check.
struct MainChain {
    // Last final block.
    last: u64,
    // Hash of the main chain block at the height of each block found by the pool the scan saw.
    hashes: HashMap<u64, String>,
    // Payouts of the address listed by the observer or found by the scan.
    payouts: Payouts,
}

// Removes the payouts of the final blocks found by the pool that were orphaned, except the payouts
// of the main chain blocks of [main] or found with [pair], and describes the result.
// Fails with the error of the first block left unchecked.
async fn remove_orphaned_payouts(
    gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>,
    node: &NodeClient<'_>,
    found_blocks: &[FoundBlock],
    pair: Option<&ViewPair>,
    main: &MainChain,
    results: &mut Vec<String>,
) -> anyhow::Result<()> {
    let (paid, found_blocks) = {
        let api = gupax_p2pool_api.lock().unwrap();
        let paid: HashSet<u64> = api
            .log
            .lines()
            .filter_map(PayoutOrd::payout_height)
            .collect();
        (paid, unchecked_blocks(&api, found_blocks, main.last))
    };
    let paying: Vec<&FoundBlock> = found_blocks
        .iter()
        .copied()
        .filter(|block| paid.contains(&block.height))
        .collect();
    // The hashes of the blocks the scan saw come from the scan.
    let mut headers = futures::stream::iter(
        paying
            .iter()
            .filter(|block| !main.hashes.contains_key(&block.height)),
    )
    .map(|block| block_header(node, block.height))
    .buffered(node.connections());
    // (height, amount) of the payouts of main chain blocks.
    let mut main_chain: Vec<(u64, u64)> = main
        .payouts
        .iter()
        .map(|(_, atomic_unit, height)| (*height, atomic_unit.to_u64()))
        .collect();
    let mut orphaned = Vec::new();
    let mut kept = HashSet::new();
    let mut unchecked = HashSet::new();
    let mut error = None;
    let mut answered = true;
    for &block in &paying {
        let seen = main.hashes.get(&block.height);
        let header = match seen {
            Some(_) => None,
            // A node without answer leaves the blocks unchecked, with their payouts kept.
            None if !answered => {
                unchecked.insert(block.height);
                continue;
            }
            None => match headers.next().await {
                Some(Ok(header)) => Some(header),
                Some(Err(e)) => {
                    answered = !e.is::<NoAnswer>();
                    unchecked.insert(block.height);
                    error.get_or_insert(e);
                    continue;
                }
                None => break,
            },
        };
        if seen.or(header.as_ref().map(|header| &header.hash)) == Some(&block.hash) {
            kept.insert(block.height);
            continue;
        }
        info!("Payout sync | Block {} was orphaned", block.height);
        orphaned.push(block);
        // The block that replaced it may pay the user too.
        if let (Some(pair), Some(header)) = (pair, header) {
            let amount = coinbases(node, std::slice::from_ref(&header))
                .await
                .and_then(|txs| {
                    txs.iter()
                        .map(|tx| Ok(coinbase_payout(&transaction(tx)?, pair)))
                        .sum()
                });
            match amount {
                Ok(0) => (),
                Ok(amount) => main_chain.push((block.height, amount)),
                Err(e) => {
                    unchecked.insert(block.height);
                    error.get_or_insert(e);
                }
            }
        }
    }
    let mut heights = Vec::new();
    // (height, amount) of the payouts of the orphaned blocks.
    let mut orphaned_payouts = Vec::new();
    for block in orphaned {
        if unchecked.contains(&block.height) {
            continue;
        }
        if !kept.contains(&block.height) {
            heights.push(block.height);
            continue;
        }
        // Another block of the pool replaced the orphaned one: the payout of the orphaned block is
        // told apart with the view key, from the orphaned block the node keeps.
        let Some(pair) = pair else {
            continue;
        };
        match block_coinbase(node, &block.hash).await {
            Ok(tx) => match coinbase_payout(&tx, pair) {
                0 => (),
                amount => orphaned_payouts.push((block.height, amount)),
            },
            // The node no longer keeps the orphaned block.
            Err(e) if e.is::<NodeError>() => (),
            Err(e) => {
                unchecked.insert(block.height);
                error.get_or_insert(e);
            }
        }
    }
    let removed =
        GupaxP2poolApi::remove_payouts(gupax_p2pool_api, &heights, &main_chain, &orphaned_payouts)
            .map_err(write_error)?;
    let checked = found_blocks.iter().map(|block| block.height);
    gupax_p2pool_api
        .lock()
        .unwrap()
        .final_checked
        .extend(checked.filter(|height| !unchecked.contains(height)));
    results.push(format!("Orphaned payouts removed: {removed}"));
    error.map_or(Ok(()), Err)
}

// Blocks of [found_blocks] final up to the block [last] that the orphan check did not check yet.
fn unchecked_blocks<'a>(
    api: &GupaxP2poolApi,
    found_blocks: &'a [FoundBlock],
    last: u64,
) -> Vec<&'a FoundBlock> {
    found_blocks
        .iter()
        .filter(|block| block.height <= last && !api.final_checked.contains(&block.height))
        .collect()
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

//---------------------------------------------------------------------------------------------------- Private view key
// Scans the final blocks since the last scan of the address of [sources], or since its date if
// set, and adds the payouts to [pair] found after each range of blocks. Returns the hash of the
// blocks at the heights [found] with the payouts found.
async fn scan(
    gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>,
    node: &NodeClient<'_>,
    sources: &SyncSources,
    height: u64,
    pair: &ViewPair,
    found: &HashSet<u64>,
    results: &mut Vec<String>,
) -> anyhow::Result<(HashMap<u64, String>, Payouts)> {
    let address = &sources.address;
    let tip = height.saturating_sub(1);
    let path_scan = gupax_p2pool_api.lock().unwrap().path_scan.clone();
    let position = GupaxP2poolApi::read_scan(&path_scan, VIEW_KEY_SOURCE, address);
    let start = if sources.scan_from.is_empty() {
        position.unwrap_or(tip.saturating_sub(FIRST_SCAN_BLOCKS))
    } else {
        let date = &sources.scan_from;
        gupax_p2pool_api.lock().unwrap().sync = format!("Looking for the first block of {date}");
        let start = first_block_since(node, height, day_start(date)?).await?;
        GupaxP2poolApi::write_scan(&path_scan, start, VIEW_KEY_SOURCE, address, false)
            .map_err(write_error)?;
        // The next scans resume from the block of the date.
        let mut api = gupax_p2pool_api.lock().unwrap();
        if api.scan_from.trim() == date {
            api.scan_from.clear();
        }
        start
    };
    let mut saved = position.is_some() || !sources.scan_from.is_empty();
    let end = last_final_block(height);
    // The headers of a range download with the coinbases of the previous one.
    let chunks = futures::stream::iter((start..=end).step_by(NODE_MAX_HEADERS as usize))
        .then(|first| async move {
            gupax_p2pool_api.lock().unwrap().sync = format!("Scanning block {first} of {end}");
            let last = (first + NODE_MAX_HEADERS - 1).min(end);
            let headers = block_headers(node, first, last).await?;
            let chunks: Vec<anyhow::Result<Vec<BlockHeader>>> = headers
                .chunks(NODE_MAX_TRANSACTIONS)
                .map(|chunk| Ok(chunk.to_vec()))
                .collect();
            anyhow::Ok(futures::stream::iter(chunks))
        })
        .try_flatten()
        .map_ok(|headers| async move {
            let txs = coinbases(node, &headers).await?;
            anyhow::Ok((headers, txs))
        })
        .try_buffered(node.connections());
    let mut chunks = std::pin::pin!(chunks);
    let mut hashes = HashMap::new();
    let mut payouts = Payouts::new();
    // Payouts of the range not added yet.
    let mut range = Payouts::new();
    let mut added = 0;
    results.push(format!("Missing payouts added: {added}"));
    while let Some((headers, txs)) = chunks.try_next().await? {
        for tx in txs {
            let amount = coinbase_payout(&transaction(&tx)?, pair);
            if amount > 0 {
                range.push((
                    utc_date(tx.block_timestamp),
                    AtomicUnit::from_u64(amount),
                    tx.block_height,
                ));
            }
        }
        let Some(last) = headers.last().map(|header| header.height) else {
            continue;
        };
        for header in headers {
            if found.contains(&header.height) {
                hashes.insert(header.height, header.hash);
            }
        }
        // A range ends every [NODE_MAX_HEADERS] blocks from [start], and at [end].
        if !(last + 1 - start).is_multiple_of(NODE_MAX_HEADERS) && last != end {
            continue;
        }
        added += GupaxP2poolApi::merge_payouts(gupax_p2pool_api, &range).map_err(write_error)?;
        let written =
            GupaxP2poolApi::write_scan(&path_scan, last + 1, VIEW_KEY_SOURCE, address, saved)
                .map_err(write_error)?;
        anyhow::ensure!(written, "the payout history was reset");
        saved = true;
        if let Some(result) = results.last_mut() {
            *result = format!("Missing payouts added: {added}");
        }
        payouts.append(&mut range);
    }
    Ok((hashes, payouts))
}

// Keys to find the outputs paid to [address].
fn view_pair(address: &str, view_key: &str) -> anyhow::Result<ViewPair> {
    let address = Address::from_str(address)?;
    let view = PrivateKey::from_str(view_key.trim())?;
    // The public view key of a subaddress derives from its public spend key.
    let public_view = match address.addr_type {
        AddressType::SubAddress => view * &address.public_spend,
        _ => PublicKey::from_private_key(&view),
    };
    anyhow::ensure!(
        public_view == address.public_view,
        "the private view key does not belong to the P2Pool address"
    );
    Ok(ViewPair {
        view,
        spend: address.public_spend,
    })
}

// The first 64 hexadecimal characters of the file at [path], as in the output of
// monero-wallet-cli where the private key comes first.
pub fn read_view_key(path: &Path) -> Result<String, String> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .and_then(|file| file.take(VIEW_KEY_FILE_MAX).read_to_end(&mut bytes))
        .map_err(|e| format!("View key file read error: {e}"))?;
    bytes
        .split(|byte| !byte.is_ascii_hexdigit())
        .find(|word| word.len() == 64)
        .map(|word| word.iter().copied().map(char::from).collect())
        .ok_or_else(|| "View key file: no key found".to_string())
}

// First block of a chain of [height] blocks mined at or after [timestamp].
async fn first_block_since(
    node: &NodeClient<'_>,
    height: u64,
    timestamp: i64,
) -> anyhow::Result<u64> {
    let (mut low, mut high) = (0, height);
    while low < high {
        let middle = low + (high - low) / 2;
        if block_header(node, middle).await?.timestamp < timestamp {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(low)
}

// Amount paid to [pair] by the coinbase transaction [tx], if it is the one of a P2Pool block,
// which holds a merge mining tag.
fn coinbase_payout(tx: &Transaction, pair: &ViewPair) -> u64 {
    let extra = tx.prefix.extra.try_parse();
    // The merge mining tag of the older P2Pool blocks holds only the 32 bytes of the side chain
    // id, at the end of the extra field.
    let raw = &tx.prefix.extra.0;
    let p2pool = extra
        .0
        .iter()
        .any(|field| matches!(field, SubField::MergeMining(..)))
        || raw.len() >= 34 && raw[raw.len() - 34..raw.len() - 32] == [0x03, 32];
    let Some(tx_pubkey) = extra.tx_pubkey().filter(|_| p2pool) else {
        return 0;
    };
    // The shared key is derived once, the view tags rule out most outputs.
    let keygen = KeyGenerator::from_key(pair, tx_pubkey);
    tx.prefix
        .outputs
        .iter()
        .enumerate()
        .filter(|(index, output)| {
            output.target.check_view_tag(keygen.rv, *index)
                && output
                    .target
                    .as_one_time_key()
                    .is_some_and(|key| keygen.check(*index, key))
        })
        .map(|(_, output)| output.amount.0)
        .sum()
}

//---------------------------------------------------------------------------------------------------- Common
// Formats a timestamp like the dates of the P2Pool output.
fn utc_date(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0).map_or_else(
        || "????-??-?? ??:??:??.????".to_string(),
        |date| date.format("%Y-%m-%d %H:%M:%S.0000").to_string(),
    )
}

// Timestamp of the start of the UTC day [date], written YYYY-MM-DD.
fn day_start(date: &str) -> anyhow::Result<i64> {
    let date = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .with_context(|| format!("the start date {date} is not written YYYY-MM-DD"))?;
    Ok(date.and_time(chrono::NaiveTime::MIN).and_utc().timestamp())
}

//---------------------------------------------------------------------------------------------------- Tests
#[cfg(test)]
mod tests {
    // Responses recorded from node2.monerodevs.org:18089, mini.p2pool.observer and
    // nano.p2pool.observer on 2026-10-01, 2026-10-02, 2026-10-04 and 2026-10-13.
    use super::rpc::Transactions;
    use super::*;
    use crate::constants::P2POOL_API_PATH_BLOCKS;
    use crate::disk::state::Observer;
    use crate::helper::p2pool::P2poolNodes;
    use mockito::Matcher;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Monero General Fund, which publishes its private view key.
    const ADDRESS: &str = "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGv7SqSsaBYBb98uNbr2VBBEt7f2wfn3RVGQBEP3A";
    const VIEW_KEY: &str = "f359631075708155cc3d92a32b75a7d02a5dcf27756707b47a2b31b21c389501";
    const HASH_3654400: &str = "ccdc3dada00723a548338fb86588999eb03a9ab0d6fb9bb74de1e9ec594e6808";
    const HASH_3654401: &str = "5313b6628b27c6d48420393a86b941b4416f0cfa8b83af47b53edf46d6a11ed0";
    // Block of the pool missing from the main chain.
    const HASH_ORPHANED: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    // Last final block of the node of [mock_node], at height 3,774,988.
    const LAST_FINAL: u64 = 3774927;

    fn payout_3654401() -> (String, AtomicUnit, u64) {
        (
            "2026-04-17 11:01:06.0000".to_string(),
            AtomicUnit::from_u64(274561854),
            3654401,
        )
    }

    fn mock_observer(
        server: &mut mockito::ServerGuard,
        address: &str,
        status: usize,
        body: &str,
    ) -> mockito::Mock {
        server
            .mock("GET", format!("/api/payouts/{address}").as_str())
            .match_query(Matcher::Exact("limit=0".to_string()))
            .match_header("user-agent", APP_USER_AGENT)
            .with_status(status)
            .with_body(body)
            .create()
    }

    // Last block found by the pool known by the observer, at height 3,782,741.
    const OBSERVER_FOUND_BLOCKS: &str =
        include_str!("../../../tests/fixtures/payouts/observer_found_blocks_last.json");

    fn mock_observer_found_blocks(server: &mut mockito::ServerGuard, body: &str) -> mockito::Mock {
        server
            .mock("GET", "/api/found_blocks")
            .match_query(Matcher::Exact("limit=1".to_string()))
            .match_header("user-agent", APP_USER_AGENT)
            .with_body(body)
            .create()
    }

    fn mock_rpc(
        server: &mut mockito::ServerGuard,
        method: &str,
        params: serde_json::Value,
        body: &str,
    ) -> mockito::Mock {
        server
            .mock("POST", "/json_rpc")
            .match_header("content-type", "application/json")
            .match_body(Matcher::PartialJson(
                json!({"method": method, "params": params}),
            ))
            .with_body(body)
            .create()
    }

    // Synced node whose main chain holds the blocks 3,654,400 and 3,654,401, below 99,999,999.
    fn mock_node(server: &mut mockito::ServerGuard) {
        let info = include_str!("../../../tests/fixtures/payouts/node_info.json");
        mock_rpc(server, "get_info", json!({}), info);
        for (height, body) in [
            (
                3654400,
                include_str!("../../../tests/fixtures/payouts/node_block_header_3654400.json"),
            ),
            (
                3654401,
                include_str!("../../../tests/fixtures/payouts/node_block_header_3654401.json"),
            ),
            (
                99999999,
                include_str!("../../../tests/fixtures/payouts/node_block_header_99999999.json"),
            ),
        ] {
            mock_rpc(
                server,
                "get_block_header_by_height",
                json!({"height": height}),
                body,
            );
        }
    }

    // Writes the blocks found by the pool in the P2Pool data API directory [dir].
    fn write_found_blocks(dir: &Path, blocks: serde_json::Value) -> PathBuf {
        let found_blocks = dir.join(P2POOL_API_PATH_BLOCKS);
        std::fs::create_dir_all(found_blocks.parent().unwrap()).unwrap();
        std::fs::write(&found_blocks, blocks.to_string()).unwrap();
        found_blocks
    }

    // Nodes of P2Pool at the addresses [hosts], written ip:port.
    fn nodes_at(hosts: &[String]) -> P2poolNodes {
        let args: Vec<String> = hosts
            .iter()
            .flat_map(|host| {
                let (ip, rpc) = host.split_once(':').unwrap();
                ["--host", ip, "--rpc-port", rpc].map(String::from)
            })
            .collect();
        P2poolNodes::from_args(&args)
    }

    // Checks the blocks of the file [found_blocks] with the node [server].
    fn check_orphans(
        api: &Arc<Mutex<GupaxP2poolApi>>,
        server: &mockito::ServerGuard,
        found_blocks: &Path,
        pair: Option<&ViewPair>,
        listed: &Payouts,
    ) -> (anyhow::Result<()>, Vec<String>) {
        let main = MainChain {
            last: LAST_FINAL,
            hashes: HashMap::new(),
            payouts: listed.clone(),
        };
        api.lock().unwrap().nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api.lock().unwrap());
        let mut results = Vec::new();
        let checked = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(remove_orphaned_payouts(
                api,
                &sources.nodes[0].client().unwrap(),
                &read_found_blocks(found_blocks).unwrap(),
                pair,
                &main,
                &mut results,
            ));
        (checked, results)
    }

    // Observer of the default chain at [url].
    fn observer_at(url: &str) -> Vec<Observer> {
        vec![Observer {
            chain: Some(P2poolChain::default()),
            url: url.to_string(),
        }]
    }

    // A subaddress of the General Fund.
    fn subaddress() -> Address {
        let address = Address::from_str(ADDRESS).unwrap();
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();
        let index = monero::cryptonote::subaddress::Index { major: 0, minor: 1 };
        let spend = monero::cryptonote::subaddress::get_spend_public_key(&pair, index);
        Address::subaddress(address.network, spend, pair.view * &spend)
    }

    #[test]
    fn address_paid_by_p2pool() {
        assert_eq!(payout_address("4A", ""), "4A");
        let subaddress = subaddress().to_string();
        assert_eq!(payout_address("4A", &subaddress), subaddress);
        assert_eq!(payout_address("4A", "8B"), "4A");
    }

    #[test]
    fn view_key_of_the_address() {
        let address = Address::from_str(ADDRESS).unwrap();
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();
        assert_eq!(pair.spend, address.public_spend);
        let subaddress = subaddress();
        let pair = view_pair(&subaddress.to_string(), VIEW_KEY).unwrap();
        assert_eq!(pair.spend, subaddress.public_spend);
        let error = view_pair(ADDRESS, &format!("01{}", "00".repeat(31))).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the private view key does not belong to the P2Pool address"
        );
    }

    #[test]
    fn removes_the_payouts_of_orphaned_blocks() {
        let mut server = mockito::Server::new();
        mock_node(&mut server);
        let error = json!({
            "error": {"code": -5, "message": "Internal error: can't get block by height. Height = 3654402."},
            "id": "0",
            "jsonrpc": "2.0",
        });
        let height = json!({"height": 3654402});
        mock_rpc(
            &mut server,
            "get_block_header_by_height",
            height,
            &error.to_string(),
        );
        let dir = tempfile::tempdir().unwrap();
        let api = GupaxP2poolApi::temporary(dir.path());
        let found_blocks = write_found_blocks(
            dir.path(),
            json!([
                {"height": 99999999, "hash": HASH_ORPHANED},
                {"height": 3654402, "hash": HASH_ORPHANED},
                {"height": 3654401, "hash": HASH_3654401},
                {"height": 3654400, "hash": HASH_ORPHANED},
            ]),
        );
        let payout = |date: &str, atomic_unit: u64, height: u64| {
            (date.to_string(), AtomicUnit::from_u64(atomic_unit), height)
        };
        let payouts = [
            payout("2026-04-17 10:59:38.0000", 1, 3654400),
            payout_3654401(),
            payout("2026-04-17 11:03:00.0000", 2, 3654402),
            // Found while P2Pool was stopped.
            payout("2026-04-17 13:00:00.0000", 4, 3654500),
            // Not final yet.
            payout("2026-04-17 14:00:00.0000", 8, 99999999),
        ];
        let api = Arc::new(Mutex::new(api));
        GupaxP2poolApi::merge_payouts(&api, &payouts).unwrap();

        // The other blocks are checked after the block the node failed to give.
        let (checked, results) = check_orphans(&api, &server, &found_blocks, None, &Payouts::new());
        assert_eq!(results, ["Orphaned payouts removed: 1"]);
        let error = checked.unwrap_err().to_string();
        assert!(error.contains("can't get block by height"));
        // P2Pool did not find a block yet.
        let missing = dir.path().join("missing");
        let (checked, results) = check_orphans(&api, &server, &missing, None, &Payouts::new());
        checked.unwrap();
        assert_eq!(results, ["Orphaned payouts removed: 0"]);

        let api = api.lock().unwrap();
        assert_eq!(api.payout_u64, 4);
        assert_eq!(api.xmr.to_u64(), 274561868);
        assert!(!api.has_payout(3654400));
    }

    #[test]
    fn checks_a_final_block_once() {
        let mut server = mockito::Server::new();
        let info = include_str!("../../../tests/fixtures/payouts/node_info.json");
        mock_rpc(&mut server, "get_info", json!({}), info);
        let header = include_str!("../../../tests/fixtures/payouts/node_block_header_3654401.json");
        let height = json!({"height": 3654401});
        let checked = mock_rpc(&mut server, "get_block_header_by_height", height, header).expect(1);
        let dir = tempfile::tempdir().unwrap();
        let api = GupaxP2poolApi::temporary(dir.path());
        let found_blocks = write_found_blocks(
            dir.path(),
            json!([{"height": 3654401, "hash": HASH_3654401}]),
        );
        let api = Arc::new(Mutex::new(api));
        GupaxP2poolApi::merge_payouts(&api, &[payout_3654401()]).unwrap();

        for _ in 0..2 {
            let (result, results) =
                check_orphans(&api, &server, &found_blocks, None, &Payouts::new());
            result.unwrap();
            assert_eq!(results, ["Orphaned payouts removed: 0"]);
        }
        checked.assert();
    }

    #[test]
    fn keeps_a_payout_paid_again_at_the_same_height() {
        let mut server = mockito::Server::new();
        mock_node(&mut server);
        let dir = tempfile::tempdir().unwrap();
        let api = GupaxP2poolApi::temporary(dir.path());
        let found_blocks = write_found_blocks(
            dir.path(),
            json!([
                {"height": 3654400, "hash": HASH_ORPHANED},
                {"height": 3654400, "hash": HASH_3654400},
            ]),
        );
        let payout = (
            "2026-04-17 10:59:38.0000".to_string(),
            AtomicUnit::from_u64(1),
            3654400,
        );
        let api = Arc::new(Mutex::new(api));
        GupaxP2poolApi::merge_payouts(&api, &[payout]).unwrap();

        let (checked, results) = check_orphans(&api, &server, &found_blocks, None, &Payouts::new());
        checked.unwrap();
        assert_eq!(results, ["Orphaned payouts removed: 0"]);
        assert!(api.lock().unwrap().has_payout(3654400));
    }

    #[test]
    fn stops_at_a_node_without_answer() {
        // This node gives an invalid response.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let requests = Arc::new(AtomicUsize::new(0));
        std::thread::spawn({
            let requests = Arc::clone(&requests);
            move || {
                for connection in listener.incoming() {
                    requests.fetch_add(1, Ordering::Relaxed);
                    use std::io::Write;
                    let mut connection = connection.unwrap();
                    // A node answers once the request arrives.
                    connection.peek(&mut [0]).ok();
                    connection.write_all(b"invalid\r\n\r\n").ok();
                    // A connection closed with the request unread is reset, and the client
                    // retries a reset request.
                    connection.shutdown(std::net::Shutdown::Write).ok();
                    std::io::copy(&mut connection, &mut std::io::sink()).ok();
                }
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.nodes = nodes_at(&[address]);
        let heights = [3654400, 3654401, 3654402];
        let found = heights.map(|height| json!({"height": height, "hash": HASH_ORPHANED}));
        let found_blocks = write_found_blocks(dir.path(), json!(found));
        let payouts = heights.map(|height| {
            let date = "2026-04-17 10:59:38.0000".to_string();
            (date, AtomicUnit::from_u64(1), height)
        });
        let api = Arc::new(Mutex::new(api));
        GupaxP2poolApi::merge_payouts(&api, &payouts).unwrap();
        let main = MainChain {
            last: LAST_FINAL,
            hashes: HashMap::new(),
            payouts: Payouts::new(),
        };
        let sources = SyncSources::new(&api.lock().unwrap());
        let node = &sources.nodes[0];

        let mut results = Vec::new();
        let checked = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(remove_orphaned_payouts(
                &api,
                &node.client().unwrap(),
                &read_found_blocks(&found_blocks).unwrap(),
                None,
                &main,
                &mut results,
            ));
        assert!(checked.is_err());
        assert_eq!(results, ["Orphaned payouts removed: 0"]);
        assert_eq!(api.lock().unwrap().payout_u64, 3);
        // The last block is left unchecked.
        assert!(requests.load(Ordering::Relaxed) < heights.len());
    }

    #[test]
    fn syncs_with_the_first_synced_node() {
        let mut unsynced = mockito::Server::new();
        let mut info: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/payouts/node_info.json"
        ))
        .unwrap();
        info["result"]["synchronized"] = json!(false);
        mock_rpc(&mut unsynced, "get_info", json!({}), &info.to_string());
        let mut server = mockito::Server::new();
        mock_node(&mut server);
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.nodes = nodes_at(&[unsynced.host_with_port(), server.host_with_port()]);
        api.found_blocks = write_found_blocks(
            dir.path(),
            json!([{"height": 3654400, "hash": HASH_ORPHANED}]),
        );
        let orphaned = (
            "2026-04-17 10:59:38.0000".to_string(),
            AtomicUnit::from_u64(1),
            3654400,
        );
        let api = Arc::new(Mutex::new(api));
        GupaxP2poolApi::merge_payouts(&api, &[orphaned]).unwrap();
        let sources = SyncSources::new(&api.lock().unwrap());

        // The second node shows the block orphaned.
        sync(&api, &sources);
        assert_eq!(api.lock().unwrap().sync, "Orphaned payouts removed: 1");
    }

    #[test]
    fn keeps_the_payout_of_the_block_replacing_an_orphaned_one() {
        let mut server = mockito::Server::new();
        mock_node(&mut server);
        // Coinbase transaction of the main chain block 3,654,401.
        let mut coinbase: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/payouts/node_coinbases_3654400_3654402.json"
        ))
        .unwrap();
        coinbase["txs"]
            .as_array_mut()
            .unwrap()
            .retain(|tx| tx["block_height"] == 3654401);
        server
            .mock("POST", "/get_transactions")
            .with_body(coinbase.to_string())
            .create();
        let orphaned = (
            "2026-04-17 11:00:00.1234".to_string(),
            AtomicUnit::from_u64(7),
            3654401,
        );
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();

        // The main chain block is found with the view key, or listed by the observer.
        for (pair, listed) in [(Some(&pair), vec![]), (None, vec![payout_3654401()])] {
            let dir = tempfile::tempdir().unwrap();
            let api = GupaxP2poolApi::temporary(dir.path());
            let found_blocks = write_found_blocks(
                dir.path(),
                json!([{"height": 3654401, "hash": HASH_ORPHANED}]),
            );
            let api = Arc::new(Mutex::new(api));
            GupaxP2poolApi::merge_payouts(&api, &[orphaned.clone(), payout_3654401()]).unwrap();
            let (checked, results) = check_orphans(&api, &server, &found_blocks, pair, &listed);
            checked.unwrap();
            assert_eq!(results, ["Orphaned payouts removed: 1"]);
            assert_eq!(
                api.lock().unwrap().log,
                "2026-04-17 11:01:06.0000 | 0.000274561854 XMR | Block 3,654,401\n"
            );
        }
    }

    #[test]
    fn removes_the_payout_of_an_orphaned_block_replaced_by_the_pool() {
        let mut server = mockito::Server::new();
        mock_node(&mut server);
        // Coinbase transaction of the main chain block 3,654,401.
        let mut coinbase: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/payouts/node_coinbases_3654400_3654402.json"
        ))
        .unwrap();
        coinbase["txs"]
            .as_array_mut()
            .unwrap()
            .retain(|tx| tx["block_height"] == 3654401);
        server
            .mock("POST", "/get_transactions")
            .with_body(coinbase.to_string())
            .create();
        // The orphaned block holds the coinbase transaction of an older block.
        let older: Transactions = serde_json::from_str(include_str!(
            "../../../tests/fixtures/payouts/node_coinbase_2587708.json"
        ))
        .unwrap();
        let orphaned_block = monero::Block {
            header: monero::BlockHeader {
                major_version: monero::VarInt(16),
                minor_version: monero::VarInt(16),
                timestamp: monero::VarInt(0),
                prev_id: monero::Hash::null(),
                nonce: 0,
            },
            miner_tx: transaction(&older.txs[0]).unwrap(),
            tx_hashes: Vec::new(),
        };
        let blob = hex::encode(monero::consensus::serialize(&orphaned_block));
        let block = json!({"id": "0", "result": {"blob": blob, "status": "OK"}});
        let missing = json!({
            "error": {"code": -5, "message": "Internal error: can't get block by hash."},
            "id": "0",
        });
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();
        let payout = |atomic_unit: u64| {
            let date = "2026-04-17 11:00:00.1234".to_string();
            (date, AtomicUnit::from_u64(atomic_unit), 3654401)
        };

        // The orphaned block is told apart while the node keeps it.
        for (orphaned_block, removed) in [(block, 1), (missing, 0)] {
            let hash = json!({"hash": HASH_ORPHANED});
            let mock = mock_rpc(&mut server, "get_block", hash, &orphaned_block.to_string());
            let dir = tempfile::tempdir().unwrap();
            let api = GupaxP2poolApi::temporary(dir.path());
            let found_blocks = write_found_blocks(
                dir.path(),
                json!([
                    {"height": 3654401, "hash": HASH_ORPHANED},
                    {"height": 3654401, "hash": HASH_3654401},
                ]),
            );
            // The payouts of both blocks, and of another address in the main chain block.
            let api = Arc::new(Mutex::new(api));
            GupaxP2poolApi::merge_payouts(&api, &[payout(312415032), payout_3654401(), payout(7)])
                .unwrap();
            let (checked, results) =
                check_orphans(&api, &server, &found_blocks, Some(&pair), &Payouts::new());
            checked.unwrap();
            assert_eq!(results, [format!("Orphaned payouts removed: {removed}")]);
            assert_eq!(api.lock().unwrap().payout_u64, 3 - removed);
            mock.remove();
        }
    }

    #[test]
    fn checks_the_blocks_the_scan_saw() {
        // This node fails every request.
        let server = mockito::Server::new();
        let dir = tempfile::tempdir().unwrap();
        let api = GupaxP2poolApi::temporary(dir.path());
        let found_blocks = write_found_blocks(
            dir.path(),
            json!([
                {"height": 3654401, "hash": HASH_3654401},
                {"height": 3654400, "hash": HASH_ORPHANED},
            ]),
        );
        let orphaned = (
            "2026-04-17 10:59:38.0000".to_string(),
            AtomicUnit::from_u64(1),
            3654400,
        );
        let api = Arc::new(Mutex::new(api));
        GupaxP2poolApi::merge_payouts(&api, &[orphaned, payout_3654401()]).unwrap();
        let main = MainChain {
            last: LAST_FINAL,
            hashes: HashMap::from([
                (3654400, HASH_3654400.to_string()),
                (3654401, HASH_3654401.to_string()),
            ]),
            payouts: vec![payout_3654401()],
        };
        api.lock().unwrap().nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api.lock().unwrap());

        let mut results = Vec::new();
        let checked = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(remove_orphaned_payouts(
                &api,
                &sources.nodes[0].client().unwrap(),
                &read_found_blocks(&found_blocks).unwrap(),
                None,
                &main,
                &mut results,
            ));
        checked.unwrap();
        assert_eq!(results, ["Orphaned payouts removed: 1"]);
        assert!(!api.lock().unwrap().has_payout(3654400));
    }

    #[test]
    fn counts_the_payouts_of_p2pool_blocks() {
        let coinbases: Transactions = serde_json::from_str(include_str!(
            "../../../tests/fixtures/payouts/node_coinbases_3654400_3654402.json"
        ))
        .unwrap();
        let tx = coinbases
            .txs
            .iter()
            .find(|tx| tx.block_height == 3654401)
            .unwrap();
        let mut tx = transaction(tx).unwrap();
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();
        assert_eq!(coinbase_payout(&tx, &pair), 274561854);

        // The same outputs paid by a block without merge mining tag, which P2Pool adds.
        let mut extra = tx.prefix.extra.try_parse();
        extra
            .0
            .retain(|field| !matches!(field, SubField::MergeMining(..)));
        tx.prefix.extra = extra.into();
        assert_eq!(coinbase_payout(&tx, &pair), 0);

        // A block of an older P2Pool.
        let coinbase: Transactions = serde_json::from_str(include_str!(
            "../../../tests/fixtures/payouts/node_coinbase_2587708.json"
        ))
        .unwrap();
        let tx = transaction(&coinbase.txs[0]).unwrap();
        assert_eq!(coinbase_payout(&tx, &pair), 312415032);
    }

    #[test]
    fn observer_payouts_of_an_address() {
        let mut server = mockito::Server::new();
        let general_fund =
            include_str!("../../../tests/fixtures/payouts/observer_payouts_general_fund.json");
        let unknown = include_str!("../../../tests/fixtures/payouts/observer_payouts_unknown.json");
        mock_observer(&mut server, ADDRESS, 200, general_fund);
        mock_observer(&mut server, "4AAAA", 404, unknown);
        mock_observer(&mut server, "4BBBB", 404, "404 page not found");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let client = client().unwrap();

        let payouts = runtime
            .block_on(observer_payouts(
                &client,
                &server.url(),
                ADDRESS,
                None,
                u64::MAX,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(payouts.len(), 21);
        assert_eq!(payouts[0], payout_3654401());
        let total: u64 = payouts.iter().map(|payout| payout.1.to_u64()).sum();
        assert_eq!(total, 12828731759);

        // The observer knows no share of this address.
        let payouts = runtime
            .block_on(observer_payouts(
                &client,
                &server.url(),
                "4AAAA",
                None,
                u64::MAX,
            ))
            .unwrap();
        assert_eq!(payouts, None);
        let error = runtime
            .block_on(observer_payouts(
                &client,
                &server.url(),
                "4BBBB",
                None,
                u64::MAX,
            ))
            .unwrap_err();
        assert!(format!("{error:#}").contains("404 Not Found"));
    }

    #[test]
    fn view_key_scan_resumes_after_the_last_block() {
        let mut server = mockito::Server::new();
        let headers = mock_rpc(
            &mut server,
            "get_block_headers_range",
            json!({"start_height": 3654400, "end_height": 3654402}),
            include_str!("../../../tests/fixtures/payouts/node_block_headers_3654400_3654402.json"),
        )
        .expect(1);
        server
            .mock("POST", "/get_transactions")
            .match_header("content-type", "application/json")
            .match_body(Matcher::PartialJson(json!({
                "txs_hashes": [
                    "a8c5318ba8a1b8531ff24bb4b79e2d4b7904269f04419543c6db93b773f6da54",
                    "9a9adc7a5d78b3a0e96d61fcd035d0fe82ba7df85532000e3b9536cbf6134750",
                    "6f79bacb782f43cdbe011074e5179c3aacf4ceaee5f7abb7e4d0c5fd6ebefc1d"
                ],
                "prune": true
            })))
            .with_body(include_str!(
                "../../../tests/fixtures/payouts/node_coinbases_3654400_3654402.json"
            ))
            .create();
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        let path_scan = api.path_scan.clone();
        for (height, source, address) in [
            (3700000, VIEW_KEY_SOURCE, "4AAAA"),
            (3700000, "nano", ADDRESS),
            (3654400, VIEW_KEY_SOURCE, ADDRESS),
        ] {
            GupaxP2poolApi::write_scan(&path_scan, height, source, address, false).unwrap();
        }
        api.nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api);
        let api = Arc::new(Mutex::new(api));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let node = sources.nodes[0].client().unwrap();
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();
        // A node at height 3,654,463, so the block 3,654,402 is final.
        let height = 3654463;
        let found = HashSet::from([3654401, 3700000]);

        let mut results = Vec::new();
        let scanned = scan(&api, &node, &sources, height, &pair, &found, &mut results);
        let (hashes, payouts) = runtime.block_on(scanned).unwrap();
        assert_eq!(results, ["Missing payouts added: 1"]);
        assert_eq!(
            api.lock().unwrap().log,
            "2026-04-17 11:01:06.0000 | 0.000274561854 XMR | Block 3,654,401\n"
        );
        // The orphan check takes them from the scan.
        assert_eq!(hashes, HashMap::from([(3654401, HASH_3654401.to_string())]));
        assert_eq!(payouts, [payout_3654401()]);
        let read_scan = |source, address| GupaxP2poolApi::read_scan(&path_scan, source, address);
        assert_eq!(read_scan(VIEW_KEY_SOURCE, ADDRESS), Some(3654403));
        assert_eq!(read_scan(VIEW_KEY_SOURCE, "4AAAA"), Some(3700000));
        assert_eq!(read_scan("nano", ADDRESS), Some(3700000));

        // The blocks are scanned once.
        let mut results = Vec::new();
        let scanned = scan(&api, &node, &sources, height, &pair, &found, &mut results);
        runtime.block_on(scanned).unwrap();
        assert_eq!(results, ["Missing payouts added: 0"]);
        headers.assert();
    }

    #[test]
    fn view_key_scan_from_a_date() {
        let mut server = mockito::Server::new();
        // Blocks 2 minutes apart, the block 3,654,400 being the first one of 2026-04-17.
        let day = day_start("2026-04-17").unwrap();
        server
            .mock("POST", "/json_rpc")
            .match_body(Matcher::PartialJson(
                json!({"method": "get_block_header_by_height"}),
            ))
            .with_body_from_request(move |request| {
                let body: serde_json::Value =
                    serde_json::from_slice(request.body().unwrap()).unwrap();
                let height = body["params"]["height"].as_i64().unwrap();
                let header = json!({
                    "height": height,
                    "hash": "",
                    "miner_tx_hash": "",
                    "timestamp": day + (height - 3654400) * 120,
                });
                json!({"id": "0", "result": {"block_header": header, "status": "OK"}})
                    .to_string()
                    .into()
            })
            .create();
        let headers = mock_rpc(
            &mut server,
            "get_block_headers_range",
            json!({"start_height": 3654400, "end_height": 3654402}),
            include_str!("../../../tests/fixtures/payouts/node_block_headers_3654400_3654402.json"),
        )
        .expect(1);
        server
            .mock("POST", "/get_transactions")
            .with_body(include_str!(
                "../../../tests/fixtures/payouts/node_coinbases_3654400_3654402.json"
            ))
            .create();
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        let path_scan = api.path_scan.clone();
        // The blocks were scanned before the date was set.
        GupaxP2poolApi::write_scan(&path_scan, 3654403, VIEW_KEY_SOURCE, ADDRESS, false).unwrap();
        api.scan_from = "2026-04-17".to_string();
        api.nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api);
        let api = Arc::new(Mutex::new(api));
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();

        let node = sources.nodes[0].client().unwrap();
        let found = HashSet::new();

        // A node at height 3,654,463, so the block 3,654,402 is final.
        let mut results = Vec::new();
        let scanned = scan(&api, &node, &sources, 3654463, &pair, &found, &mut results);
        let (_, payouts) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(scanned)
            .unwrap();
        assert_eq!(payouts, [payout_3654401()]);
        headers.assert();
        // The next scans resume from the last block scanned.
        assert_eq!(api.lock().unwrap().scan_from, "");
        assert_eq!(
            GupaxP2poolApi::read_scan(&path_scan, VIEW_KEY_SOURCE, ADDRESS),
            Some(3654403)
        );
        assert!(day_start("17/04/2026").is_err());
    }

    #[test]
    fn view_key_scan_stops_at_a_reset_of_the_payouts() {
        let mut server = mockito::Server::new();
        mock_rpc(
            &mut server,
            "get_block_headers_range",
            json!({"start_height": 3654400, "end_height": 3654402}),
            include_str!("../../../tests/fixtures/payouts/node_block_headers_3654400_3654402.json"),
        );
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        let path_scan = api.path_scan.clone();
        GupaxP2poolApi::write_scan(&path_scan, 3654400, VIEW_KEY_SOURCE, ADDRESS, false).unwrap();
        // The payouts are reset while the scan waits for the coinbases.
        let reset = path_scan.clone();
        server
            .mock("POST", "/get_transactions")
            .with_body_from_request(move |_| {
                std::fs::write(&reset, "").unwrap();
                include_str!("../../../tests/fixtures/payouts/node_coinbases_3654400_3654402.json")
                    .into()
            })
            .create();
        api.nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api);
        let api = Arc::new(Mutex::new(api));
        let pair = view_pair(ADDRESS, VIEW_KEY).unwrap();

        let mut results = Vec::new();
        let node = sources.nodes[0].client().unwrap();
        let found = HashSet::new();
        let scanned = scan(&api, &node, &sources, 3654463, &pair, &found, &mut results);
        let error = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(scanned)
            .unwrap_err();
        assert_eq!(error.to_string(), "the payout history was reset");
        // The next scan starts over.
        assert_eq!(
            GupaxP2poolApi::read_scan(&path_scan, VIEW_KEY_SOURCE, ADDRESS),
            None
        );
    }

    #[test]
    fn observer_sync_resumes_after_the_final_blocks() {
        let mut server = mockito::Server::new();
        let general_fund =
            include_str!("../../../tests/fixtures/payouts/observer_payouts_general_fund.json");
        let full = mock_observer(&mut server, ADDRESS, 200, general_fund).expect(1);
        let mut resumed = |from_height: u64, payouts: &str| {
            server
                .mock("GET", format!("/api/payouts/{ADDRESS}").as_str())
                .match_query(Matcher::Exact(format!("limit=0&from_height={from_height}")))
                .with_body(payouts)
                .expect(1)
                .create()
        };
        let after_final = resumed(LAST_FINAL + 1, "[]");
        let mut recent: Vec<serde_json::Value> = serde_json::from_str(general_fund).unwrap();
        recent.retain(|payout| payout["main_height"].as_u64() >= Some(3654400));
        let after_found = resumed(3654400, &serde_json::to_string(&recent).unwrap());
        mock_observer_found_blocks(&mut server, OBSERVER_FOUND_BLOCKS);
        mock_node(&mut server);
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        api.observers = observer_at(&server.url());
        api.found_blocks = write_found_blocks(
            dir.path(),
            json!([{"height": 3654400, "hash": HASH_3654400}]),
        );
        api.nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api);
        let api = Arc::new(Mutex::new(api));

        sync(&api, &sources);
        assert_eq!(
            api.lock().unwrap().sync,
            "Missing payouts added: 21. Orphaned payouts removed: 0"
        );
        let path_scan = api.lock().unwrap().path_scan.clone();
        assert_eq!(
            GupaxP2poolApi::read_scan(&path_scan, "nano", ADDRESS),
            Some(LAST_FINAL + 1)
        );
        // The orphan check is done with the block found by the pool.
        sync(&api, &sources);
        // Gupax restarted: the block found by the pool comes before the final blocks of the first
        // sync.
        api.lock().unwrap().final_checked.clear();
        sync(&api, &sources);
        assert_eq!(
            api.lock().unwrap().sync,
            "Missing payouts added: 0. Orphaned payouts removed: 0"
        );
        full.assert();
        after_final.assert();
        after_found.assert();
    }

    #[test]
    fn observer_unaware_of_the_address() {
        let mut server = mockito::Server::new();
        let unknown = include_str!("../../../tests/fixtures/payouts/observer_payouts_unknown.json");
        mock_observer(&mut server, ADDRESS, 404, unknown);
        mock_observer_found_blocks(&mut server, OBSERVER_FOUND_BLOCKS);
        mock_node(&mut server);
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        api.observers = observer_at(&server.url());
        api.nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api);
        let api = Arc::new(Mutex::new(api));

        sync(&api, &sources);
        let api = api.lock().unwrap();
        assert_eq!(
            api.sync,
            "Missing payouts added: 0. Orphaned payouts removed: 0"
        );
        // The whole history is requested once the observer knows the address.
        assert_eq!(
            GupaxP2poolApi::read_scan(&api.path_scan, "nano", ADDRESS),
            None
        );
    }

    #[test]
    fn observer_sync_up_to_the_blocks_the_observer_knows() {
        let mut server = mockito::Server::new();
        let general_fund =
            include_str!("../../../tests/fixtures/payouts/observer_payouts_general_fund.json");
        mock_observer(&mut server, ADDRESS, 200, general_fund);
        // The observer did not see the blocks found by the pool after the block 3,652,571.
        let found_blocks = json!([{"main_block": {"height": 3652571}}]).to_string();
        mock_observer_found_blocks(&mut server, &found_blocks);
        mock_node(&mut server);
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        api.observers = observer_at(&server.url());
        api.nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api);
        let api = Arc::new(Mutex::new(api));

        sync(&api, &sources);
        let api = api.lock().unwrap();
        assert_eq!(
            api.sync,
            "Missing payouts added: 20. Orphaned payouts removed: 0"
        );
        assert_eq!(
            GupaxP2poolApi::read_scan(&api.path_scan, "nano", ADDRESS),
            Some(3652572)
        );
    }

    #[test]
    fn observer_of_the_chain() {
        let mut api = GupaxP2poolApi::new();
        let observer = |chain, url: &str| Observer {
            chain,
            url: url.to_string(),
        };
        api.observers = vec![
            observer(Some(P2poolChain::Mini), ""),
            // A pasted URL may end with a line break.
            observer(Some(P2poolChain::Nano), "127.0.0.1:8080\n"),
            observer(None, "p2pool.observer"),
        ];
        api.chain = P2poolChain::Nano;
        assert_eq!(SyncSources::new(&api).observer, "http://127.0.0.1:8080");
        api.chain = P2poolChain::Mini;
        assert_eq!(
            SyncSources::new(&api).observer,
            "https://mini.p2pool.observer"
        );
        // An observer without chain is left out.
        api.chain = P2poolChain::Main;
        assert_eq!(SyncSources::new(&api).observer, "");

        // A self-hosted observer is reached over plain HTTP unless its URL names a scheme.
        assert_eq!(observer_url("localhost:8080"), "http://localhost:8080");
        assert_eq!(observer_url("[::1]:8080"), "http://[::1]:8080");
        assert_eq!(observer_url("https://192.168.1.10"), "https://192.168.1.10");
        // A pasted URL may end with a slash.
        assert_eq!(observer_url("p2pool.example/"), "https://p2pool.example");
    }

    #[test]
    fn sync_sources_priority() {
        let mut server = mockito::Server::new();
        let general_fund =
            include_str!("../../../tests/fixtures/payouts/observer_payouts_general_fund.json");
        let observer = mock_observer(&mut server, ADDRESS, 200, general_fund).expect(1);
        let resumed = server
            .mock("GET", format!("/api/payouts/{ADDRESS}").as_str())
            .match_query(Matcher::Exact(format!(
                "limit=0&from_height={}",
                LAST_FINAL + 1
            )))
            .with_body("[]")
            .expect(1)
            .create();
        mock_observer_found_blocks(&mut server, OBSERVER_FOUND_BLOCKS);
        mock_node(&mut server);
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        api.found_blocks = write_found_blocks(
            dir.path(),
            json!([{"height": 3654400, "hash": HASH_ORPHANED}]),
        );
        let orphaned = (
            "2026-04-17 10:59:38.0000".to_string(),
            AtomicUnit::from_u64(1),
            3654400,
        );
        let api = Arc::new(Mutex::new(api));
        GupaxP2poolApi::merge_payouts(&api, &[orphaned]).unwrap();
        api.lock().unwrap().nodes = nodes_at(&[server.host_with_port()]);
        let sources = |view_key: &str, observers: Vec<Observer>| {
            let mut api = api.lock().unwrap();
            api.view_key = view_key.to_string();
            api.observers = observers;
            api.next_sync = None;
            SyncSources::new(&api)
        };

        // The observer adds the missing payouts, then the orphaned one is removed.
        sync(&api, &sources("", observer_at(&server.url())));
        assert_eq!(
            api.lock().unwrap().sync,
            "Missing payouts added: 21. Orphaned payouts removed: 1"
        );
        assert_eq!(api.lock().unwrap().payout_u64, 21);
        assert!(api.lock().unwrap().next_sync.is_none());

        // The view key of another address.
        let view_key = format!("01{}", "00".repeat(31));
        sync(&api, &sources(&view_key, observer_at(&server.url())));
        assert_eq!(
            api.lock().unwrap().sync,
            "Sync error: the private view key does not belong to the P2Pool address. Missing payouts added: 0. Orphaned payouts removed: 0"
        );
        // The failed sync is retried.
        assert!(api.lock().unwrap().next_sync.is_some());
        observer.assert();
        resumed.assert();

        // Without view key and observer, only the orphaned payouts are checked.
        sync(&api, &sources("", Vec::new()));
        assert_eq!(api.lock().unwrap().sync, "Orphaned payouts removed: 0");
        assert!(api.lock().unwrap().next_sync.is_none());
    }

    #[test]
    fn stop_a_sync_waiting_for_the_node() {
        // This node accepts connections and never answers.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        api.address = ADDRESS.to_string();
        api.view_key = VIEW_KEY.to_string();
        api.nodes = nodes_at(&[listener.local_addr().unwrap().to_string()]);
        let api = Arc::new(Mutex::new(api));

        assert!(start(&mut api.lock().unwrap(), &api));
        // Let the request reach the node.
        std::thread::sleep(Duration::from_millis(200));
        api.lock().unwrap().stop_sync = true;
        let deadline = Instant::now() + Duration::from_secs(10);
        while api.lock().unwrap().syncing {
            assert!(Instant::now() < deadline, "the sync did not stop");
            std::thread::sleep(Duration::from_millis(10));
        }

        let api = api.lock().unwrap();
        assert_eq!(api.sync, "Stopped");
    }

    #[test]
    fn view_key_in_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("view_key");
        // Output of the viewkey command of monero-wallet-cli.
        let public = "a".repeat(64);
        std::fs::write(&file, format!("secret: {VIEW_KEY}\npublic: {public}\n")).unwrap();
        assert_eq!(read_view_key(&file).as_deref(), Ok(VIEW_KEY));

        std::fs::write(&file, "secret: 1234").unwrap();
        assert_eq!(
            read_view_key(&file),
            Err("View key file: no key found".to_string())
        );
        std::fs::remove_file(&file).unwrap();
        assert!(
            read_view_key(&file)
                .unwrap_err()
                .starts_with("View key file read error")
        );
    }
}
