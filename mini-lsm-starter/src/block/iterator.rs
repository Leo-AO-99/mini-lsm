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

use std::sync::Arc;

use bytes::Buf;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };

        let (key, _) = iter.kv_at_idx(0);
        iter.first_key = key.to_key_vec();

        iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let (key, value_range) = self.kv_at_idx(idx);

        self.key = key.to_key_vec();
        self.value_range = value_range;
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    pub fn prev(&mut self) {
        if self.idx == 0 {
            return;
        }
        self.seek_to_idx(self.idx - 1);
    }

    fn kv_at_idx(&self, idx: usize) -> (KeySlice, (usize, usize)) {
        let offset = self.block.offsets[idx] as usize;
        let mut entry = &self.block.data[offset..];
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];

        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_begin = offset + SIZEOF_U16 + key_len + SIZEOF_U16;
        (
            KeySlice::from_slice(key),
            (value_begin, value_begin + value_len),
        )
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            let (key_at_idx, _) = self.kv_at_idx(mid);
            match key_at_idx.cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    self.seek_to_idx(mid);
                    return;
                }
            }
        }
        self.seek_to_idx(low);
    }
}
