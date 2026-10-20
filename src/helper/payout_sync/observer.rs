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

//! API of the P2Pool observers the payout sync reads.

use std::net::IpAddr;

use anyhow::Context;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, de::DeserializeOwned};

use super::{Payouts, client, utc_date};
use crate::xmr::AtomicUnit;

// Payout returned by the API of a P2Pool observer, with the fields the sync uses.
#[derive(Deserialize)]
struct ObserverPayout {
    main_height: u64,
    timestamp: i64,
    coinbase_reward: u64,
}

// Block found by the pool returned by the API of a P2Pool observer, with the fields the sync uses.
#[derive(Deserialize)]
struct ObserverFoundBlock {
    main_block: ObserverMainBlock,
}

#[derive(Deserialize)]
struct ObserverMainBlock {
    height: u64,
}

// Output of the coinbase transaction of a block found by the pool returned by the API of a P2Pool
// observer, with the fields the sync uses.
#[derive(Deserialize)]
struct ObserverCoinbaseOutput {
    miner_address: String,
    // Subaddress the output pays, if the miner has one.
    miner_payout_address: Option<String>,
    value: u64,
}

// This matches an error returned by the API of a P2Pool observer.
#[derive(Deserialize)]
struct ObserverError {
    error: String,
}

// URL of the observer [url], over plain HTTP when [url] names no scheme and an IP address or
// localhost, as for a self-hosted observer.
pub(super) fn observer_url(url: &str) -> String {
    let url = url.trim_end_matches('/');
    if url.contains("://") {
        return url.to_string();
    }
    let host = url.split([':', '/']).next().unwrap_or_default();
    let local = url.starts_with('[') || host == "localhost" || host.parse::<IpAddr>().is_ok();
    let scheme = if local { "http" } else { "https" };
    format!("{scheme}://{url}")
}

// Height of the last block found by the pool that [observer] knows.
pub(super) async fn observer_last_found(
    client: &ClientWithMiddleware,
    observer: &str,
) -> anyhow::Result<u64> {
    let blocks = client
        .get(format!("{observer}/api/found_blocks?limit=1"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<ObserverFoundBlock>>()
        .await?;
    let block = blocks
        .first()
        .context("the observer knows no block found by the pool")?;
    Ok(block.main_block.height)
}

// Lists the payouts of [address] known by [observer], from the block [from_height] if set
// to the block [last], or none if [observer] knows no share of [address].
pub(super) async fn observer_payouts(
    client: &ClientWithMiddleware,
    observer: &str,
    address: &str,
    from_height: Option<u64>,
    last: u64,
) -> anyhow::Result<Option<Payouts>> {
    let from = from_height.map_or_else(String::new, |height| format!("&from_height={height}"));
    let url = format!("{observer}/api/payouts/{address}?limit=0{from}");
    let Some(payouts) = observer_get::<Vec<ObserverPayout>>(client, url).await? else {
        return Ok(None);
    };
    Ok(Some(
        payouts
            .into_iter()
            .filter(|payout| payout.main_height <= last)
            .map(|payout| {
                (
                    utc_date(payout.timestamp),
                    AtomicUnit::from_u64(payout.coinbase_reward),
                    payout.main_height,
                )
            })
            .collect(),
    ))
}

// Payout to [address] of the block [hash] found by the pool, which [observer] computes from the
// shares of the pool, or none if [observer] does not know the block.
pub(super) async fn observer_block_payout(
    observer: &str,
    hash: &str,
    address: &str,
) -> anyhow::Result<Option<u64>> {
    let url = format!("{observer}/api/block_by_id/{hash}/coinbase");
    let outputs = observer_get::<Vec<ObserverCoinbaseOutput>>(&client()?, url).await?;
    Ok(outputs.map(|outputs| {
        outputs
            .iter()
            .filter(|output| {
                output
                    .miner_payout_address
                    .as_deref()
                    .unwrap_or(&output.miner_address)
                    == address
            })
            .map(|output| output.value)
            .sum()
    }))
}

// Response of the API of a P2Pool observer at [url], or none if the observer does not know what
// [url] names.
async fn observer_get<T: DeserializeOwned>(
    client: &ClientWithMiddleware,
    url: String,
) -> anyhow::Result<Option<T>> {
    let response = client.get(url).send().await?;
    if let Some(e) = response.error_for_status_ref().err() {
        let error = response.json::<ObserverError>().await;
        if error.is_ok_and(|error| error.error == "not_found") {
            return Ok(None);
        }
        return Err(e.into());
    }
    Ok(Some(response.json().await?))
}
