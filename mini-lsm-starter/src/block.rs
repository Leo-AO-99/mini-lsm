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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Block {{")?;
        for (idx, offset) in self.offsets.iter().enumerate() {
            let mut entry = &self.data[*offset as usize..];
            let key_len = entry.get_u16() as usize;
            let key = &entry[..key_len];
            entry.advance(key_len);
            let value_len = entry.get_u16() as usize;
            let value = &entry[..value_len];
            writeln!(
                f,
                "  [{idx}] key: {:?}, value: {:?}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value)
            )?;
        }
        write!(f, "}}")
    }
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_of_elements = self.offsets.len();
        let mut buf =
            BytesMut::with_capacity(self.data.len() + num_of_elements * SIZEOF_U16 + SIZEOF_U16);

        buf.put_slice(&self.data);

        for offset in &self.offsets {
            buf.put_u16(*offset);
        }

        buf.put_u16(num_of_elements as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - 2 - num_of_elements * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // get offset array
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // retrieve data
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
