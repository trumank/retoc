use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::{borrow::Cow, io::Read};
use strum::{Display, FromRepr};
use tracing::instrument;

use crate::{read_array, read_string_data, ser::*};

const FNAME_HASH_ALGORITHM_ID: u64 = 0xC164_0000;

fn name_hash(name: &str) -> u64 {
    let lower = name.to_ascii_lowercase();
    if lower.is_ascii() {
        cityhasher::hash(lower.as_bytes())
    } else {
        cityhasher::hash(lower.encode_utf16().flat_map(|s| s.to_le_bytes()).collect::<Vec<u8>>())
    }
}

fn name_header(name: &str) -> [u8; 2] {
    let len = if name.is_ascii() { name.len() as i16 } else { name.encode_utf16().count() as i16 + i16::MIN };
    len.to_be_bytes()
}

/// Breaks down a combined FName string into a base name and a number. Number is 0 if there is no number
pub(crate) fn break_down_name_string<'a>(name: &'a str) -> (&'a str, i32) {
    let mut name_without_number: &'a str = name;
    let mut name_number: i32 = 0; // 0 means no number

    // Attempt to break down the composite name into the name part and the number part
    if let Some((left, right)) = name.rsplit_once('_') {
        // Right part needs to be parsed as a valid signed integer that is >= 0 and converts back to the same string
        // Last part is important for not touching names like: Rocket_04 - 04 should stay a part of the name, not a number, otherwise we would actually get Rocket_4 when deserializing!
        if let Ok(parsed_number) = right.parse::<i32>()
            && parsed_number >= 0
            && parsed_number.to_string() == right
        {
            name_without_number = left;
            name_number = parsed_number + 1; // stored as 1 more than the actual number
        }
    }
    (name_without_number, name_number)
}

pub fn read_name_batch<S: Read>(s: &mut S) -> Result<Vec<String>> {
    let num: u32 = s.de()?;
    if num == 0 {
        return Ok(vec![]);
    }
    let _num_string_bytes: u32 = s.de()?;
    let hash_version: u64 = s.de()?;
    assert_eq!(hash_version, FNAME_HASH_ALGORITHM_ID);

    let _hash_bytes: Vec<u8> = s.de_ctx(num as usize * 8)?;
    let lengths = read_array(num as usize, s, |s| Ok(i16::from_be_bytes(s.de()?)))?;
    let names: Vec<_> = lengths
        .iter()
        .map(|&l| {
            let l = if l < 0 { i16::MIN - l } else { l };
            read_string_data(l as i32, s)
        })
        .collect::<Result<_>>()?;
    Ok(names)
}

pub fn write_name_batch<S: Write, T: AsRef<str>>(s: &mut S, names: &[T]) -> Result<()> {
    fn name_byte_size(name: &str) -> u32 {
        if name.is_ascii() { name.len() as u32 } else { name.encode_utf16().count() as u32 * 2 }
    }

    s.ser(&(names.len() as u32))?;
    if names.is_empty() {
        return Ok(());
    }

    s.ser(&names.iter().map(|s| name_byte_size(s.as_ref())).sum::<u32>())?;
    s.ser(&FNAME_HASH_ALGORITHM_ID)?;

    for name in names {
        s.ser(&name_hash(name.as_ref()))?;
    }

    for name in names {
        s.ser(&name_header(name.as_ref()))?;
    }

    for name in names {
        let name = name.as_ref();
        if name.is_ascii() {
            s.write_all(name.as_bytes())?;
        } else {
            for c in name.encode_utf16() {
                s.ser(&c)?;
            }
        }
    }
    Ok(())
}

pub fn read_name_batch_parts(names_buffer: &[u8]) -> Result<Vec<String>> {
    let mut names = vec![];
    let mut s = Cursor::new(names_buffer);
    while s.position() < names_buffer.len() as u64 {
        let l = i16::from_be_bytes(s.de()?);
        let l = if l < 0 { i16::MIN - l } else { l };
        if l < 0 && s.position() & 1 != 0 {
            // UTF16 strings aligned to 2 bytes so read one byte to reach alignment
            s.de::<u8>()?;
        }
        names.push(read_string_data(l as i32, &mut s)?);
    }
    Ok(names)
}

pub fn write_name_batch_parts<T: AsRef<str>>(names: &[T]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut cur_names = Cursor::new(vec![]);
    let mut cur_hashes = Cursor::new(vec![]);

    cur_hashes.ser(&FNAME_HASH_ALGORITHM_ID)?;

    for name in names {
        let name = name.as_ref();
        cur_names.ser(&name_header(name))?;
        if name.is_ascii() {
            cur_names.write_all(name.as_bytes())?;
        } else {
            if cur_names.position() & 1 != 0 {
                cur_names.ser(&0u8)?;
            }
            for c in name.encode_utf16() {
                cur_names.ser(&c)?;
            }
        }
        cur_hashes.ser(&name_hash(name))?;
    }

    Ok((cur_names.into_inner(), cur_hashes.into_inner()))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FNameMap {
    kind: EMappedNameType,
    names: Vec<String>,
    name_lookup: HashMap<String, usize>,
}
impl FNameMap {
    #[instrument(skip_all, "FNameMap")]
    pub fn deserialize<S: Read>(s: &mut S, kind: EMappedNameType) -> Result<Self> {
        let names: Vec<String> = read_name_batch(s)?;
        Ok(Self::create_from_names(kind, names))
    }
    #[instrument(skip_all, "FNameMap")]
    pub fn serialize<S: Write>(&self, s: &mut S) -> Result<()> {
        write_name_batch(s, &self.names)
    }
}

impl FNameMap {
    pub fn create(kind: EMappedNameType) -> Self {
        Self { kind, names: Vec::new(), name_lookup: HashMap::new() }
    }
    pub fn create_from_names(kind: EMappedNameType, names: Vec<String>) -> Self {
        let mut name_lookup: HashMap<String, usize> = HashMap::with_capacity(names.len());
        for (name_index, name) in names.iter().cloned().enumerate() {
            name_lookup.insert(name, name_index);
        }
        Self { kind, names, name_lookup }
    }
    pub fn get(&self, name: FMappedName) -> Cow<'_, str> {
        assert_eq!(name.kind(), self.kind, "Attempt to map name of the different kind in this name map Name Kind is {}, but name map kind is {}", name.kind(), self.kind);
        let n = &self.names[name.index() as usize];
        if name.number != 0 { format!("{n}_{}", name.number - 1).into() } else { n.into() }
    }

    pub fn store(&mut self, name: &str) -> FMappedName {
        let (name_without_number, name_number) = break_down_name_string(name);

        // Attempt to resolve the existing name through lookup
        if let Some(existing_index) = self.name_lookup.get(name_without_number) {
            return FMappedName::create((*existing_index) as u32, self.kind, name_number as u32);
        }

        // Create a new name and add it to the names list and to the name lookup
        let new_name_index = self.names.len();
        self.name_lookup.insert(name_without_number.to_string(), new_name_index);
        self.names.push(name_without_number.to_string());
        FMappedName::create(new_name_index as u32, self.kind, name_number as u32)
    }

    pub fn copy_raw_names(&self) -> Vec<String> {
        self.names.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Display, FromRepr, Serialize, Deserialize)]
#[repr(u32)]
pub enum EMappedNameType {
    #[default]
    Package = 0,
    Container = 1,
    Global = 2,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FMappedName {
    index_and_type: u32,
    pub number: u32,
}
impl FMappedName {
    const INDEX_BITS: u32 = 30;
    const INDEX_MASK: u32 = (1 << Self::INDEX_BITS) - 1;
    const TYPE_MASK: u32 = !Self::INDEX_MASK;
    const TYPE_SHIFT: u32 = Self::INDEX_BITS;
    pub fn create(index: u32, kind: EMappedNameType, number: u32) -> Self {
        let shifted_type: u32 = (kind as u32) << Self::TYPE_SHIFT;
        let index_and_type: u32 = (index & Self::INDEX_MASK) | (shifted_type & Self::TYPE_MASK);
        FMappedName { index_and_type, number }
    }
    pub fn index(self) -> u32 {
        self.index_and_type & Self::INDEX_MASK
    }
    pub fn kind(self) -> EMappedNameType {
        let kind: u32 = (self.index_and_type & Self::TYPE_MASK) >> Self::TYPE_SHIFT;
        EMappedNameType::from_repr(kind).unwrap()
    }
}
impl Readable for FMappedName {
    #[instrument(skip_all, name = "FMappedName")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self { index_and_type: s.de()?, number: s.de()? })
    }
}
impl Writeable for FMappedName {
    #[instrument(skip_all, name = "FMinimalName")]
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.index_and_type)?;
        stream.ser(&self.number)?;
        Ok(())
    }
}
