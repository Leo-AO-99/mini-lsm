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

use anyhow::Result;

use super::StorageIterator;

enum CurrentIterator {
    A,
    B,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    current: CurrentIterator,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            current: CurrentIterator::A,
        };
        iter.refresh_current();

        Ok(iter)
    }

    fn refresh_current(&mut self) {
        let current = if self.a.is_valid() {
            if self.b.is_valid() {
                if self.a.key() <= self.b.key() {
                    CurrentIterator::A
                } else {
                    CurrentIterator::B
                }
            } else {
                CurrentIterator::A
            }
        } else {
            if self.b.is_valid() {
                CurrentIterator::B
            } else {
                // although it is a, it is also invalid
                CurrentIterator::A
            }
        };

        self.current = current;
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.current {
            CurrentIterator::A => self.a.key(),
            CurrentIterator::B => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.current {
            CurrentIterator::A => self.a.value(),
            CurrentIterator::B => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.current {
            CurrentIterator::A => self.a.is_valid(),
            CurrentIterator::B => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self.current {
            CurrentIterator::A => {
                if self.a.is_valid() && self.b.is_valid() {
                    if self.a.key() == self.b.key() {
                        self.b.next()?;
                    }
                }
                self.a.next()?
            }
            CurrentIterator::B => {
                if self.a.is_valid() && self.b.is_valid() {
                    if self.a.key() == self.b.key() {
                        self.a.next()?;
                    }
                }
                self.b.next()?
            }
        }

        self.refresh_current();

        Ok(())
    }
}
