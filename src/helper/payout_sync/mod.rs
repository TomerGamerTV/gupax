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
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::StreamExt;
use log::*;
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;

use self::observer::{observer_last_found, observer_payouts, observer_url};
use self::rpc::{NoAnswer, NodeClient, NodeRpc, block_header, synced_node};
use crate::{
    components::update::APP_USER_AGENT,
    constants::{P2POOL_PAYOUT_CHECK_DEPTH, SECOND},
    disk::{errors::TomlError, gupax_p2pool_api::GupaxP2poolApi, state::P2poolChain},
    miscs::client_with,
    xmr::{AtomicUnit, PayoutOrd},
};

mod observer;
mod rpc;

// Time without data after which a request fails, so a stalled sync ends.
// The largest requests of a sync started answering within 0.2 s from a remote node.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

// (date, atomic_unit, height) of each payout found.
type Payouts = Vec<(String, AtomicUnit, u64)>;

// Connections to the node, leaving P2Pool one of the 3 a public node accepts per IP.
const NODE_MAX_CONNECTIONS: usize = 2;

// The missing payouts are listed by the observer of the chain if set.
struct SyncSources {
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
            observer,
            chain: api.chain.clone(),
            address: api.address.clone(),
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
    let found_blocks = read_found_blocks(&sources.found_blocks);
    let last = last_final_block(height);
    // Final payouts of the address listed by the observer.
    let mut listed = Payouts::new();
    if !sources.observer.is_empty() {
        let oldest_found = found_blocks.as_ref().ok().and_then(|found_blocks| {
            let api = gupax_p2pool_api.lock().unwrap();
            let unchecked = unchecked_blocks(&api, found_blocks, last);
            unchecked.iter().map(|block| block.height).min()
        });
        match observer_sync(gupax_p2pool_api, sources, oldest_found, height).await {
            Ok((added, payouts)) => {
                results.push(format!("Missing payouts added: {added}"));
                listed = payouts;
            }
            Err(e) => results.push(format!("Sync error: {e:#}")),
        }
    }
    let checked = async {
        let found_blocks = found_blocks?;
        remove_orphaned_payouts(
            gupax_p2pool_api,
            &node,
            &found_blocks,
            last,
            &listed,
            results,
        )
        .await
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

// Removes the payouts of the final blocks found by the pool that were orphaned, except the payouts
// of the main chain blocks [listed], and describes the result. [last] is the last final block.
// Fails with the error of the first block left unchecked.
async fn remove_orphaned_payouts(
    gupax_p2pool_api: &Arc<Mutex<GupaxP2poolApi>>,
    node: &NodeClient<'_>,
    found_blocks: &[FoundBlock],
    last: u64,
    listed: &Payouts,
    results: &mut Vec<String>,
) -> anyhow::Result<()> {
    let (paid, found_blocks) = {
        let api = gupax_p2pool_api.lock().unwrap();
        let paid: HashSet<u64> = api
            .log
            .lines()
            .filter_map(PayoutOrd::payout_height)
            .collect();
        (paid, unchecked_blocks(&api, found_blocks, last))
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
    // (height, amount) of the payouts of main chain blocks.
    let main_chain: Vec<(u64, u64)> = listed
        .iter()
        .map(|(_, atomic_unit, height)| (*height, atomic_unit.to_u64()))
        .collect();
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
    let removed = GupaxP2poolApi::remove_payouts(gupax_p2pool_api, &orphaned, &main_chain)
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

//---------------------------------------------------------------------------------------------------- Common
// Formats a timestamp like the dates of the P2Pool output.
fn utc_date(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0).map_or_else(
        || "????-??-?? ??:??:??.????".to_string(),
        |date| date.format("%Y-%m-%d %H:%M:%S.0000").to_string(),
    )
}

//---------------------------------------------------------------------------------------------------- Tests
#[cfg(test)]
mod tests {
    // Responses recorded from node2.monerodevs.org:18089, mini.p2pool.observer and
    // nano.p2pool.observer on 2026-10-01, 2026-10-02, 2026-10-04 and 2026-10-13.
    use super::*;
    use crate::constants::P2POOL_API_PATH_BLOCKS;
    use crate::disk::state::Observer;
    use crate::helper::p2pool::P2poolNodes;
    use mockito::Matcher;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Monero General Fund, which publishes its private view key.
    const ADDRESS: &str = "44AFFq5kSiGBoZ4NMDwYtN18obc8AemS33DBLWs3H7otXft3XjrpDtQGv7SqSsaBYBb98uNbr2VBBEt7f2wfn3RVGQBEP3A";
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
        listed: &Payouts,
    ) -> (anyhow::Result<()>, Vec<String>) {
        api.lock().unwrap().nodes = nodes_at(&[server.host_with_port()]);
        let sources = SyncSources::new(&api.lock().unwrap());
        let mut results = Vec::new();
        let checked = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(remove_orphaned_payouts(
                api,
                &sources.nodes[0].client().unwrap(),
                &read_found_blocks(found_blocks).unwrap(),
                LAST_FINAL,
                listed,
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
        let (checked, results) = check_orphans(&api, &server, &found_blocks, &Payouts::new());
        assert_eq!(results, ["Orphaned payouts removed: 1"]);
        let error = checked.unwrap_err().to_string();
        assert!(error.contains("can't get block by height"));
        // P2Pool did not find a block yet.
        let missing = dir.path().join("missing");
        let (checked, results) = check_orphans(&api, &server, &missing, &Payouts::new());
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
            let (result, results) = check_orphans(&api, &server, &found_blocks, &Payouts::new());
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

        let (checked, results) = check_orphans(&api, &server, &found_blocks, &Payouts::new());
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
        let sources = SyncSources::new(&api.lock().unwrap());
        let node = &sources.nodes[0];

        let mut results = Vec::new();
        let checked = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(remove_orphaned_payouts(
                &api,
                &node.client().unwrap(),
                &read_found_blocks(&found_blocks).unwrap(),
                LAST_FINAL,
                &Payouts::new(),
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
}
