use anyhow::Result;
use byteorder::{WriteBytesExt, BE};
use std::collections::HashMap;
use std::io::Write;
use std::{borrow::Cow, io::Read};
use strum::{Display, FromRepr};
use tracing::instrument;

use crate::{break_down_name_string, read_array, read_string, ser::*};

const FNAME_HASH_ALGORITHM_ID: u64 = 0xC1640000;

pub(crate) fn read_name_batch<S: Read>(s: &mut S) -> Result<Vec<String>> {
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
            read_string(l as i32, s)
        })
        .collect::<Result<_>>()?;
    Ok(names)
}

pub(crate) fn write_name_batch<S: Write>(s: &mut S, names: &[String]) -> Result<()> {
    fn name_byte_size(name: &String) -> u32 {
        if name.is_ascii() {
            name.bytes().len() as u32
        } else {
            name.encode_utf16().count() as u32 * 2
        }
    }

    s.ser(&(names.len() as u32))?;
    if names.is_empty() {
        return Ok(());
    }

    s.ser(&names.iter().map(name_byte_size).sum::<u32>())?;
    s.ser(&FNAME_HASH_ALGORITHM_ID)?;

    for name in names {
        let lower = name.to_ascii_lowercase();
        let hash: u64 = if lower.is_ascii() {
            cityhasher::hash(lower.as_bytes())
        } else {
            cityhasher::hash(
                lower
                    .encode_utf16()
                    .flat_map(|s| s.to_le_bytes())
                    .collect::<Vec<u8>>(),
            )
        };
        s.ser(&hash)?;
    }

    for name in names {
        let len = if name.is_ascii() {
            name.as_bytes().len() as i16
        } else {
            name.encode_utf16().count() as i16 + i16::MIN
        };
        s.write_i16::<BE>(len)?;
    }

    for name in names {
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

#[derive(Debug, Clone, Default)]
pub(crate) struct FNameMap {
    kind: EMappedNameType,
    names: Vec<String>,
    name_lookup: HashMap<String, usize>,
}
impl FNameMap {
    #[instrument(skip_all, "FNameMap")]
    pub(crate) fn deserialize<S: Read>(s: &mut S, kind: EMappedNameType) -> Result<Self> {
        let names: Vec<String> = read_name_batch(s)?;
        Ok(Self::create_from_names(kind, names))
    }
    #[instrument(skip_all, "FNameMap")]
    pub(crate) fn serialize<S: Write>(&self, s: &mut S) -> Result<()> {
        write_name_batch(s, &self.names)
    }
}

impl FNameMap {
    pub(crate) fn create(kind: EMappedNameType) -> Self {
        Self {
            kind,
            names: Vec::new(),
            name_lookup: HashMap::new(),
        }
    }
    pub(crate) fn create_from_names(kind: EMappedNameType, names: Vec<String>) -> Self {
        let mut name_lookup: HashMap<String, usize> = HashMap::with_capacity(names.len());
        for name_index in 0..names.len() {
            name_lookup.insert(names[name_index].clone(), name_index);
        }
        Self {
            kind,
            names,
            name_lookup,
        }
    }
    pub(crate) fn get(&self, name: FMappedName) -> Cow<'_, str> {
        assert_eq!(name.kind(), self.kind, "Attempt to map name of the different kind in this name map Name Kind is {}, but name map kind is {}", name.kind(), self.kind);
        let n = &self.names[name.index() as usize];
        if name.number != 0 {
            format!("{n}_{}", name.number - 1).into()
        } else {
            n.into()
        }
    }

    pub(crate) fn store(&mut self, name: &str) -> FMappedName {
        let (name_without_number, name_number) = break_down_name_string(name);

        // Attempt to resolve the existing name through lookup
        if let Some(existing_index) = self.name_lookup.get(name_without_number) {
            return FMappedName::create((*existing_index) as u32, self.kind, name_number as u32);
        }

        // Create a new name and add it to the names list and to the name lookup
        let new_name_index = self.names.len();
        self.name_lookup
            .insert(name_without_number.to_string(), new_name_index);
        self.names.push(name_without_number.to_string());
        FMappedName::create(new_name_index as u32, self.kind, name_number as u32)
    }

    pub(crate) fn copy_raw_names(&self) -> Vec<String> {
        self.names.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Display, FromRepr)]
#[repr(u32)]
pub(crate) enum EMappedNameType {
    #[default]
    Package = 0,
    Container = 1,
    Global = 2,
}
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FMappedName {
    index_and_type: u32,
    pub(crate) number: u32,
}
impl FMappedName {
    const INDEX_BITS: u32 = 30;
    const INDEX_MASK: u32 = (1 << Self::INDEX_BITS) - 1;
    const TYPE_MASK: u32 = !Self::INDEX_MASK;
    const TYPE_SHIFT: u32 = Self::INDEX_BITS;
    pub(crate) fn create(index: u32, kind: EMappedNameType, number: u32) -> Self {
        let shifted_type: u32 = (kind as u32) << Self::TYPE_SHIFT;
        let index_and_type: u32 = (index & Self::INDEX_MASK) | (shifted_type & Self::TYPE_MASK);
        FMappedName {
            index_and_type,
            number,
        }
    }
    pub(crate) fn index(self) -> u32 {
        self.index_and_type & Self::INDEX_MASK
    }
    pub(crate) fn kind(self) -> EMappedNameType {
        let kind: u32 = (self.index_and_type & Self::TYPE_MASK) >> Self::TYPE_SHIFT;
        EMappedNameType::from_repr(kind).unwrap()
    }
}
impl Readable for FMappedName {
    #[instrument(skip_all, name = "FMappedName")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            index_and_type: s.de()?,
            number: s.de()?,
        })
    }
}
impl Writeable for FMappedName {
    #[instrument(skip_all, name = "FMinimalName")]
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.index_and_type)?;
        stream.ser(&self.number)?;
        Ok({})
    }
}
