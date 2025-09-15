// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator as _;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, map_bound};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();

        if let Some(handle) = self.flush_thread.lock().take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("Failed to join flush thread"))?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();

        // make sure the database directory exists
        std::fs::create_dir_all(path)?;

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // 1. memtable
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // 2. imm_memtabls
        for table in snapshot.imm_memtables.iter() {
            if let Some(value) = table.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // SSTables
        let keep_table = |key: &[u8], table: &SsTable| -> bool {
            if !key_within(key, table.first_key().raw_ref(), table.last_key().raw_ref()) {
                return false;
            }
            if let Some(bloom) = &table.bloom {
                if !bloom.may_contain(farmhash::fingerprint32(key)) {
                    return false;
                }
            }

            true
        };

        // 3. l0_sstables
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            if !keep_table(key, &snapshot.sstables[table_id]) {
                continue;
            }
            let table = snapshot.sstables[table_id].clone();
            l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                table,
                KeySlice::from_slice(key),
            )?));
        }

        let l0_iter = MergeIterator::create(l0_iters);

        // 4. l1_sstables
        let mut l1_ssts = Vec::with_capacity(snapshot.levels[0].1.len());
        for table_id in snapshot.levels[0].1.iter() {
            if !keep_table(key, &snapshot.sstables[table_id]) {
                continue;
            }
            l1_ssts.push(snapshot.sstables[table_id].clone());
        }

        let l1_iter =
            SstConcatIterator::create_and_seek_to_key(l1_ssts, KeySlice::from_slice(key))?;

        let table_iter = TwoMergeIterator::create(l0_iter, l1_iter)?;
        if table_iter.is_valid()
            && table_iter.key().raw_ref() == key
            && !table_iter.value().is_empty()
        {
            return Ok(Some(Bytes::copy_from_slice(table_iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(key, value)?;
            size = guard.memtable.approximate_size();
        }

        self.try_freeze(size)?;

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, b"")
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));

        let mut state = self.state.write();
        let snapshot = Arc::make_mut(&mut state);
        let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);
        snapshot.imm_memtables.insert(0, old_memtable);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        // Select a memtable to flush.
        let imm_memtable = {
            let guard = self.state.read();
            guard
                .imm_memtables
                .last()
                .expect("imm_memtables is empty")
                .clone()
        };

        // Create an SST file corresponding to a memtable.
        let mut builder = SsTableBuilder::new(self.options.block_size);
        imm_memtable.flush(&mut builder)?;

        let sstable = builder.build(
            imm_memtable.id(),
            Some(self.block_cache.clone()),
            self.path_of_sst(imm_memtable.id()),
        )?;

        // Remove the memtable from the immutable memtable list and add the SST file to L0 SSTs.
        {
            let mut guard = self.state.write();
            let state = Arc::make_mut(&mut guard);
            state.imm_memtables.pop();
            state.l0_sstables.insert(0, sstable.sst_id());
            state.sstables.insert(sstable.sst_id(), Arc::new(sstable));
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        // merge memtable and immutable memtables
        // memtable is the first one, which is the latest one
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        // l0_sstables

        // skip the SSTs that is impossible to contain the key/key range.
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sstable_id in snapshot.l0_sstables.iter() {
            let sstable = snapshot.sstables[sstable_id].clone();
            if !range_overlap(
                sstable.first_key().raw_ref(),
                sstable.last_key().raw_ref(),
                lower,
                upper,
            ) {
                continue;
            }
            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(sstable, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sstable,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sstable)?,
            };
            l0_iters.push(Box::new(iter));
        }

        let l0_iter = MergeIterator::create(l0_iters);

        // l1_sstables
        let mut l1_ssts = Vec::with_capacity(snapshot.levels[0].1.len());
        for sstable_id in snapshot.levels[0].1.iter() {
            let sstable = snapshot.sstables[sstable_id].clone();
            if range_overlap(sstable.first_key().raw_ref(), sstable.last_key().raw_ref(), lower, upper) {
                l1_ssts.push(sstable);
            }
        }

        let l1_iter = match lower {
            Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(l1_ssts, KeySlice::from_slice(key))?,
            Bound::Excluded(key) => {
                let mut iter = SstConcatIterator::create_and_seek_to_key(l1_ssts, KeySlice::from_slice(key))?;
                if iter.is_valid() && iter.key().raw_ref() == key {
                    iter.next()?;
                }
                iter
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(l1_ssts)?,
        };


        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        let iter = TwoMergeIterator::create(iter, l1_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}

fn range_overlap(
    first_key: &[u8],
    last_key: &[u8],
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> bool {
    match lower {
        Bound::Included(bound) if last_key < bound => return false,
        Bound::Excluded(bound) if last_key <= bound => return false,
        _ => {}
    };

    match upper {
        Bound::Included(bound) if first_key > bound => return false,
        Bound::Excluded(bound) if first_key >= bound => return false,
        _ => {}
    };

    true
}

fn key_within(key: &[u8], first_key: &[u8], last_key: &[u8]) -> bool {
    first_key <= key && key <= last_key
}
