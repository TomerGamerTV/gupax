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

//! RPC of the Monero node the payout sync reads.

use std::time::Duration;

use anyhow::Context;
use log::*;
use monero::{Transaction, consensus::deserialize};
use reqwest::header::CONTENT_TYPE;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::json;

use super::READ_TIMEOUT;
use crate::{helper::p2pool::P2poolHost, miscs::client_with};

// Time for a node to answer the first request, after which the sync tries the next one.
const NODE_TIMEOUT: Duration = Duration::from_secs(15);

// RPC of a node.
pub(super) struct NodeRpc {
    url: String,
    timeout: Duration,
}

impl NodeRpc {
    pub(super) fn new(host: &P2poolHost) -> Self {
        let node = &host.node;
        let ip = if node.ip.contains(':') {
            format!("[{}]", node.ip)
        } else {
            node.ip.clone()
        };
        Self {
            url: format!("http://{ip}:{}", node.rpc),
            timeout: NODE_TIMEOUT,
        }
    }

    // Client of the RPC.
    pub(super) fn client(&self) -> anyhow::Result<NodeClient<'_>> {
        let client = reqwest::Client::builder()
            .read_timeout(READ_TIMEOUT)
            .build()?;
        Ok(NodeClient {
            rpc: self,
            http: client_with(client),
        })
    }
}

pub(super) struct NodeClient<'a> {
    rpc: &'a NodeRpc,
    http: ClientWithMiddleware,
}

// Result of [get_block_header_by_height], with the fields the sync uses.
#[derive(Deserialize)]
struct BlockHeaderResult {
    block_header: BlockHeader,
    status: String,
}

// Result of [get_block], with the fields the sync uses.
#[derive(Deserialize)]
struct BlockResult {
    blob: String,
    status: String,
}

// Block header of the Monero node, with the fields the sync uses.
#[derive(Clone, Deserialize)]
pub(super) struct BlockHeader {
    pub(super) height: u64,
    pub(super) hash: String,
    miner_tx_hash: String,
}

// Header of the main chain block at [height].
pub(super) async fn block_header(
    node: &NodeClient<'_>,
    height: u64,
) -> anyhow::Result<BlockHeader> {
    let params = json!({"height": height});
    let result = rpc::<BlockHeaderResult>(node, "get_block_header_by_height", params).await?;
    ensure_ok(&result.status)?;
    Ok(result.block_header)
}

// Responses of the Monero node, with the fields the sync uses.
#[derive(Deserialize)]
struct BlockHeaders {
    headers: Vec<BlockHeader>,
    status: String,
}

#[derive(Deserialize)]
pub(super) struct Transactions {
    #[serde(default)]
    pub(super) txs: Vec<Tx>,
    status: String,
}

#[derive(Deserialize)]
pub(super) struct Tx {
    pruned_as_hex: String,
    pub(super) block_height: u64,
    pub(super) block_timestamp: i64,
}

pub(super) async fn block_headers(
    node: &NodeClient<'_>,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<BlockHeader>> {
    let params = json!({"start_height": start, "end_height": end});
    let headers = rpc::<BlockHeaders>(node, "get_block_headers_range", params).await?;
    ensure_ok(&headers.status)?;
    Ok(headers.headers)
}

pub(super) async fn coinbases(
    node: &NodeClient<'_>,
    headers: &[BlockHeader],
) -> anyhow::Result<Vec<Tx>> {
    let hashes: Vec<&str> = headers.iter().map(|h| h.miner_tx_hash.as_str()).collect();
    let body = json!({"txs_hashes": hashes, "prune": true});
    let response = post(node, "/get_transactions", &body.to_string())
        .await?
        .error_for_status()?;
    let txs = json::<Transactions>(response).await?;
    ensure_ok(&txs.status)?;
    anyhow::ensure!(
        txs.txs.len() == hashes.len(),
        "missing coinbase transactions"
    );
    Ok(txs.txs)
}

// Coinbase transaction of the block [hash], in the main chain or orphaned.
pub(super) async fn block_coinbase(
    node: &NodeClient<'_>,
    hash: &str,
) -> anyhow::Result<Transaction> {
    let result = rpc::<BlockResult>(node, "get_block", json!({"hash": hash})).await?;
    ensure_ok(&result.status)?;
    let block: monero::Block = deserialize(&hex::decode(&result.blob)?)?;
    Ok(block.miner_tx)
}

pub(super) fn transaction(tx: &Tx) -> anyhow::Result<Transaction> {
    Ok(deserialize(&hex::decode(&tx.pruned_as_hex)?)?)
}

// This matches a JSON-RPC response of the Monero node.
#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    message: String,
}

// Result of [get_info], with the fields the sync uses.
#[derive(Deserialize)]
struct Info {
    height: u64,
    synchronized: bool,
    status: String,
}

// First node of [nodes] synced to the network, with its client and the height of its chain.
// Fails with the error of the first node.
pub(super) async fn synced_node(nodes: &[NodeRpc]) -> anyhow::Result<(NodeClient<'_>, u64)> {
    let mut first_error = None;
    for node in nodes {
        let synced = async {
            let client = node.client()?;
            let info = rpc::<Info>(&client, "get_info", json!({})).await?;
            ensure_ok(&info.status)?;
            anyhow::ensure!(info.synchronized, "the node is not synced");
            anyhow::Ok((client, info.height))
        };
        match tokio::time::timeout(node.timeout, synced)
            .await
            .context(NoAnswer)
            .flatten()
        {
            Ok(synced) => return Ok(synced),
            Err(e) => {
                info!("Payout sync | Node {} skipped: {e:#}", node.url);
                first_error.get_or_insert(e);
            }
        }
    }
    Err(first_error.unwrap_or_else(|| anyhow::anyhow!("P2Pool has no node")))
}

async fn rpc<T: DeserializeOwned>(
    node: &NodeClient<'_>,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<T> {
    let body = json!({"jsonrpc": "2.0", "id": "0", "method": method, "params": params});
    let response = post(node, "/json_rpc", &body.to_string())
        .await?
        .error_for_status()?;
    let response = json::<RpcResponse<T>>(response).await?;
    if let Some(error) = response.error {
        return Err(NodeError(error.message).into());
    }
    response
        .result
        .ok_or_else(|| anyhow::anyhow!("node error: no result"))
}

// Posts the JSON [body] to [path] of [node].
async fn post(node: &NodeClient<'_>, path: &str, body: &str) -> anyhow::Result<reqwest::Response> {
    node.http
        .post(format!("{}{path}", node.rpc.url))
        .header(CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .context(NoAnswer)
}

async fn json<T: DeserializeOwned>(response: reqwest::Response) -> anyhow::Result<T> {
    let body = response.bytes().await.context(NoAnswer)?;
    Ok(serde_json::from_slice(&body)?)
}

// Context of the error of a request that the node did not answer.
#[derive(Debug)]
pub(super) struct NoAnswer;

impl std::fmt::Display for NoAnswer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("no answer from the node")
    }
}

// Error the node answered to a request.
#[derive(Debug)]
pub(super) struct NodeError(String);

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "node error: {}", self.0)
    }
}

impl std::error::Error for NodeError {}

fn ensure_ok(status: &str) -> anyhow::Result<()> {
    anyhow::ensure!(status == "OK", "node status: {status}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn rpc_at(url: String) -> NodeRpc {
        NodeRpc {
            url,
            timeout: NODE_TIMEOUT,
        }
    }

    // Node at height 3,774,988 answering get_info.
    fn mock_info(synchronized: bool) -> mockito::ServerGuard {
        let mut server = mockito::Server::new();
        let info = json!({"height": 3774988, "synchronized": synchronized, "status": "OK"});
        server
            .mock("POST", "/json_rpc")
            .with_body(json!({"id": "0", "result": info}).to_string())
            .create();
        server
    }

    #[test]
    fn uses_the_first_synced_node() {
        let mut busy = mockito::Server::new();
        busy.mock("POST", "/json_rpc")
            .with_body(json!({"id": "0", "error": {"code": -1, "message": "Busy"}}).to_string())
            .create();
        let servers = [mock_info(false), busy, mock_info(true)];
        let nodes = servers.each_ref().map(|server| rpc_at(server.url()));
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let (node, height) = runtime.block_on(synced_node(&nodes)).unwrap();
        assert_eq!(node.rpc.url, servers[2].url());
        assert_eq!(height, 3774988);
        // Without synced node, the error of the first node.
        let error = runtime.block_on(synced_node(&nodes[..2])).err().unwrap();
        assert_eq!(error.to_string(), "the node is not synced");
    }

    #[test]
    fn skips_an_unreachable_node() {
        let server = mock_info(true);
        // An address reserved for documentation, which no host answers.
        let mut unreachable = rpc_at("http://192.0.2.1:18081".to_string());
        unreachable.timeout = Duration::from_millis(100);
        let nodes = [unreachable, rpc_at(server.url())];
        let start = Instant::now();

        let (node, _) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(synced_node(&nodes))
            .unwrap();
        assert_eq!(node.rpc.url, server.url());
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
