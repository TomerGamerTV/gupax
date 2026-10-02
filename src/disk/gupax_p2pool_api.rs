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

use super::*;
use crate::helper::p2pool::P2poolNodes;
use crate::utils::regex::{P2POOL_REGEX, contains_no_payout, p2pool_block_height};
use std::collections::{HashMap, HashSet};
//---------------------------------------------------------------------------------------------------- Gupax-P2Pool API
#[derive(Clone, Debug)]
pub struct GupaxP2poolApi {
    pub log: String,           // Log file only containing full payout lines
    pub log_rev: String,       // Same as above but reversed based off lines
    pub payout: HumanNumber,   // Human-friendly display of payout count
    pub payout_u64: u64,       // [u64] version of above
    pub payout_ord: PayoutOrd, // Ordered Vec of payouts, see [PayoutOrd]
    pub payout_low: String, // A pre-allocated/computed [String] of the above Vec from low payout to high
    pub payout_high: String, // Same as above but high -> low
    pub xmr: AtomicUnit,    // XMR stored as atomic units
    pub path_log: PathBuf,  // Path to [log]
    pub sync: String,       // Result of the last payouts sync
    pub syncing: bool,
    pub stop_sync: bool,
    pub found_blocks: PathBuf, // Blocks found by the pool, in the data API of the running P2Pool
    pub final_checked: HashSet<u64>, // Heights of the final blocks of the pool checked for orphans
    pub nodes: P2poolNodes,    // Monero nodes of the running P2Pool
    announced: HashMap<u64, u64>, // Amount of the payout P2Pool announced at each height
    pending: Vec<String>,      // Payout lines of [log] missing from its file
    replaced: Vec<(u64, u64)>, // (height, amount) of the replaced payouts left in the file of [log]
}

impl Default for GupaxP2poolApi {
    fn default() -> Self {
        Self::new()
    }
}

impl GupaxP2poolApi {
    //---------------------------------------------------------------------------------------------------- Init, these pretty much only get called once
    pub fn new() -> Self {
        Self {
            log: String::new(),
            log_rev: String::new(),
            payout: HumanNumber::unknown(),
            payout_u64: 0,
            payout_ord: PayoutOrd::new(),
            payout_low: String::new(),
            payout_high: String::new(),
            xmr: AtomicUnit::new(),
            path_log: PathBuf::new(),
            sync: String::new(),
            syncing: false,
            stop_sync: false,
            found_blocks: PathBuf::new(),
            final_checked: HashSet::new(),
            nodes: P2poolNodes::default(),
            announced: HashMap::new(),
            pending: Vec::new(),
            replaced: Vec::new(),
        }
    }

    #[cfg(test)]
    // Creates the files in the directory [dir].
    pub fn temporary(dir: &Path) -> Self {
        Self::create_all_files(dir).unwrap();
        let mut api = Self::new();
        api.fill_paths(dir);
        api.read_all_files_and_update().unwrap();
        api
    }

    pub fn fill_paths(&mut self, gupax_p2pool_dir: &Path) {
        let mut path_log = gupax_p2pool_dir.to_path_buf();
        path_log.push(GUPAX_P2POOL_API_LOG);
        *self = Self {
            path_log,
            ..std::mem::take(self)
        };
    }

    pub fn create_all_files(gupax_p2pool_dir: &Path) -> Result<(), TomlError> {
        for file in GUPAX_P2POOL_API_FILE_ARRAY {
            let mut path = gupax_p2pool_dir.to_path_buf();
            path.push(file);
            // A link to a missing file is kept.
            if path.exists() || path.is_symlink() {
                info!(
                    "GupaxP2poolApi | [{}] already exists, skipping...",
                    path.display()
                );
                continue;
            }
            match std::fs::File::create(&path) {
                Ok(_) => {
                    info!("GupaxP2poolApi | [{}] create ... OK", path.display());
                }
                Err(e) => {
                    warn!(
                        "GupaxP2poolApi | [{}] create ... FAIL: {}",
                        path.display(),
                        e
                    );
                    return Err(TomlError::Io(e));
                }
            }
        }
        Ok(())
    }

    pub fn read_all_files_and_update(&mut self) -> Result<(), TomlError> {
        let log = Self::read_file(&self.path_log)?;
        self.update(log);
        Ok(())
    }

    // Replaces the history with [log], and the totals with its totals.
    fn update(&mut self, mut log: String) {
        // A history edited by hand may end without a line break.
        if !log.is_empty() && !log.ends_with('\n') {
            log.push('\n');
        }
        self.payout_ord.update_from_payout_log(&log);
        self.log = log;
        self.refresh();
    }

    // Updates the totals, the sorted payouts and [log_rev] from the history.
    fn refresh(&mut self) {
        self.update_payout_strings();
        (self.payout_u64, self.xmr) = self.payout_ord.total();
        self.payout = HumanNumber::from_u64(self.payout_u64);
        self.update_log_rev();
    }

    // Completely delete the [p2pool] folder and create defaults.
    pub fn create_new(path: &PathBuf) -> Result<(), TomlError> {
        info!(
            "GupaxP2poolApi | Deleting old folder at [{}]...",
            path.display()
        );
        std::fs::remove_dir_all(path)?;
        info!(
            "GupaxP2poolApi | Creating new default folder at [{}]...",
            path.display()
        );
        create_gupax_p2pool_dir(path)?;
        Self::create_all_files(path)?;
        Ok(())
    }

    //---------------------------------------------------------------------------------------------------- Live, functions that actually update/write live stats
    pub fn update_log_rev(&mut self) {
        let mut log_rev = String::with_capacity(self.log.len());
        for line in self.log.lines().rev() {
            log_rev.push_str(line);
            log_rev.push('\n');
        }
        self.log_rev = log_rev;
    }

    pub fn format_payout(date: &str, atomic_unit: &AtomicUnit, block: &HumanNumber) -> String {
        format!("{date} | {atomic_unit} XMR | Block {block}")
    }

    pub fn update_payout_low(&mut self) {
        self.payout_ord.sort_payout_low_to_high();
        self.payout_low = self.payout_ord.to_string();
    }

    pub fn update_payout_high(&mut self) {
        self.payout_ord.sort_payout_high_to_low();
        self.payout_high = self.payout_ord.to_string();
    }

    pub fn update_payout_strings(&mut self) {
        self.update_payout_low();
        self.update_payout_high();
    }

    // Records the payout that P2Pool announces in [line], in place of the payout it announced before
    // at the same height: another block of the pool replaced that block.
    pub fn record_found_block(&mut self, line: &str) -> Result<(), TomlError> {
        let payout = P2POOL_REGEX
            .payout
            .is_match(line)
            .then(|| PayoutOrd::parse_raw_payout_line(line));
        let height = match &payout {
            Some((_, _, height)) => *height,
            None => contains_no_payout(line)
                .then(|| p2pool_block_height(line))
                .flatten(),
        };
        let amount = payout
            .as_ref()
            .map(|(_, atomic_unit, _)| atomic_unit.to_u64());
        if let Some(height) = height
            && let Some(&announced) = self.announced.get(&height)
            && amount != Some(announced)
        {
            self.announced.remove(&height);
            self.replaced.push((height, announced));
        }
        let Some((date, atomic_unit, Some(height))) = payout else {
            return self.flush();
        };
        self.announced.insert(height, atomic_unit.to_u64());
        let block = HumanNumber::from_u64(height);
        self.add(Self::format_payout(&date, &atomic_unit, &block))
    }

    // Removes the payouts received in the blocks at [heights] and updates [api] and the files.
    // Returns the number of payouts removed.
    pub fn remove_payouts(api: &Mutex<Self>, heights: &[u64]) -> Result<u64, TomlError> {
        if heights.is_empty() {
            return Ok(0);
        }
        Self::edit_unlocked(api, |lines| {
            let count = lines.len();
            lines.retain(|line| {
                PayoutOrd::payout_height(line).is_none_or(|height| !heights.contains(&height))
            });
            (count - lines.len()) as u64
        })
    }

    // Whether the amounts [a] and [b] of a payout are the same: an amount of the history may be
    // truncated, 1 atomic unit lower.
    fn same_amount(a: u64, b: u64) -> bool {
        a.abs_diff(b) <= 1
    }

    fn same_payout(line: &str, height: u64, amount: u64) -> bool {
        PayoutOrd::payout_height(line) == Some(height)
            && Self::same_amount(
                PayoutOrd::parse_formatted_payout_line(line).1.to_u64(),
                amount,
            )
    }

    fn lists<'a>(lines: impl IntoIterator<Item = &'a str>, line: &str) -> bool {
        let Some(height) = PayoutOrd::payout_height(line) else {
            return false;
        };
        let amount = PayoutOrd::parse_formatted_payout_line(line).1.to_u64();
        lines
            .into_iter()
            .any(|other| Self::same_payout(other, height, amount))
    }

    // Adds the payout [line] to [self], then to the file unless another Gupax sharing it wrote it.
    fn add(&mut self, line: String) -> Result<(), TomlError> {
        if !Self::lists(self.log.lines(), &line) {
            self.push(&line);
            self.pending.push(line);
        }
        self.flush()
    }

    fn push(&mut self, line: &str) {
        let (date, atomic_unit, block) = PayoutOrd::parse_formatted_payout_line(line);
        self.payout_ord.push(date, atomic_unit, block);
        self.log.push_str(line);
        self.log.push('\n');
        self.refresh();
    }

    // Writes the changes of [self] missing from the file.
    fn flush(&mut self) -> Result<(), TomlError> {
        if self.pending.is_empty() && self.replaced.is_empty() {
            return Ok(());
        }
        self.edit(|_| ())
    }

    // Applies [edit] to the lines of the file with the changes of [self] it misses, then writes
    // them and updates [self]. Returns the result of [edit].
    fn edit<T>(&mut self, edit: impl FnOnce(&mut Vec<String>) -> T) -> Result<T, TomlError> {
        // Another Gupax sharing the file may have changed it.
        let (log, edited) = Self::edit_file(&self.path_log, |file| {
            let (log, edited) = Self::edit_lines(file, &self.pending, &self.replaced, edit);
            (log.clone(), (log, edited))
        })?;
        self.pending.clear();
        self.replaced.clear();
        if log != self.log {
            self.update(log);
        }
        Ok(edited)
    }

    // Applies [edit] like [Self::edit], locking [api] only to read its changes and to swap in the
    // history rebuilt from the file.
    fn edit_unlocked<T>(
        api: &Mutex<Self>,
        edit: impl FnOnce(&mut Vec<String>) -> T,
    ) -> Result<T, TomlError> {
        let (path_log, log, pending, replaced) = {
            let api = api.lock().unwrap();
            (
                api.path_log.clone(),
                api.log.clone(),
                api.pending.clone(),
                api.replaced.clone(),
            )
        };
        let (edited_log, edited) = Self::edit_file(&path_log, |file| {
            let (log, edited) = Self::edit_lines(file, &pending, &replaced, edit);
            (log.clone(), (log, edited))
        })?;
        let mut history = (edited_log != log).then(|| {
            let mut history = Self::new();
            history.update(edited_log);
            history
        });
        let mut api = api.lock().unwrap();
        api.pending.retain(|line| !pending.contains(line));
        api.replaced.retain(|payout| !replaced.contains(payout));
        match &mut history {
            Some(history) if api.log == log => api.swap_history(history),
            // Another thread changed the history meanwhile.
            Some(_) => api.edit(|_| ())?,
            None => (),
        }
        // The history swapped out is freed once [api] is unlocked.
        drop(api);
        Ok(edited)
    }

    // Removes the [replaced] payouts from the lines of [file], adds the [pending] payouts it misses,
    // then applies [edit]. Returns the edited content with the result of [edit].
    fn edit_lines<T>(
        file: &str,
        pending: &[String],
        replaced: &[(u64, u64)],
        edit: impl FnOnce(&mut Vec<String>) -> T,
    ) -> (String, T) {
        let mut lines: Vec<String> = file.lines().map(String::from).collect();
        for &(height, amount) in replaced {
            if let Some(replaced) = lines
                .iter()
                .position(|line| Self::same_payout(line, height, amount))
            {
                lines.remove(replaced);
            }
        }
        let missing: Vec<String> = pending
            .iter()
            .filter(|line| !Self::lists(lines.iter().map(String::as_str), line))
            .cloned()
            .collect();
        lines.extend(missing);
        let edited = edit(&mut lines);
        let log = lines.iter().map(|line| format!("{line}\n")).collect();
        (log, edited)
    }

    // Swaps the history of [self] and its totals with the ones of [other].
    fn swap_history(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.log, &mut other.log);
        std::mem::swap(&mut self.log_rev, &mut other.log_rev);
        std::mem::swap(&mut self.payout, &mut other.payout);
        std::mem::swap(&mut self.payout_u64, &mut other.payout_u64);
        std::mem::swap(&mut self.payout_ord, &mut other.payout_ord);
        std::mem::swap(&mut self.payout_low, &mut other.payout_low);
        std::mem::swap(&mut self.payout_high, &mut other.payout_high);
        std::mem::swap(&mut self.xmr, &mut other.xmr);
    }

    // Content of the file at [path], empty if it is missing.
    fn read_file(path: &Path) -> std::io::Result<String> {
        match fs::File::open(path) {
            Ok(file) => {
                // A Gupax sharing the file may be writing it.
                if let Err(e) = file.lock_shared() {
                    warn!("GupaxP2poolApi | Lock [{}] ... FAIL: {e}", path.display());
                }
                std::io::read_to_string(&file)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(e) => Err(e),
        }
    }

    // Replaces the content of the file at [path] with the content [edit] returns from it, locked
    // for the other Gupax sharing the file. Returns the value [edit] returns. Fails on a link to a
    // missing file.
    fn edit_file<T>(path: &Path, edit: impl FnOnce(&str) -> (String, T)) -> std::io::Result<T> {
        use std::io::{Seek, Write};
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(!path.is_symlink())
            .open(path)?;
        if let Err(e) = file.lock() {
            warn!("GupaxP2poolApi | Lock [{}] ... FAIL: {e}", path.display());
        }
        let content = std::io::read_to_string(&file)?;
        let (edited, value) = edit(&content);
        // Lines added at the end are appended.
        if let Some(added) = edited.strip_prefix(content.as_str()) {
            file.write_all(added.as_bytes())?;
        } else {
            file.rewind()?;
            file.write_all(edited.as_bytes())?;
            file.set_len(edited.len() as u64)?;
        }
        Ok(value)
    }
}
