use std::{borrow::Cow, io::Read};
use std::io::Write;
use anyhow::Result;
use strum::{Display, FromRepr};
use tracing::instrument;

use crate::{read_array, read_string, ser::*};

pub(crate) fn read_name_batch<S: Read>(s: &mut S) -> Result<Vec<String>> {
    let num: u32 = s.de()?;
    if num == 0 {
        return Ok(vec![]);
    }
    let _num_string_bytes: u32 = s.de()?;
    let _hash_version: u64 = s.de()?;

    let _use_saved_hashes = false; // TODO check CanUseSavedHashes(HashVersion)

    let _hash_bytes: Vec<u8> = s.de_ctx(num as usize * 8)?;
    let lengths = read_array(num as usize, s, |s| Ok(u16::from_be_bytes(s.de()?)))?;
    let names: Vec<_> = lengths
        .iter()
        .map(|&l| {
            let utf16 = l & 0x8000 != 0; // check high bit
            let l = (l & !0x8000) as i32; // reset high bit
            read_string(if utf16 { -l } else { l }, s)
        })
        .collect::<Result<_>>()?;
    Ok(names)
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FNameMap {
    names: Vec<String>,
}
impl Readable for FNameMap {
    #[instrument(skip_all, "FNameMap")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let names: Vec<String> = read_name_batch(s)?;
        Ok(Self { names })
    }
}
impl FNameMap {
    pub(crate) fn get(&self, name: FMappedName) -> Cow<'_, str> {
        let n = &self.names[name.index() as usize];
        if name.number != 0 {
            format!("{n}_{}", name.number - 1).into()
        } else {
            n.into()
        }
    }
    pub(crate) fn copy_raw_names(&self) -> Vec<String> { self.names.clone() }
}

#[derive(Debug, Clone, Copy, PartialEq, Display, FromRepr)]
#[repr(u32)]
enum EMappedNameType {
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
        FMappedName{index_and_type, number}
    }
    pub(crate) fn index(self) -> u32 { self.index_and_type & Self::INDEX_MASK }
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
impl Writeable for FMappedName
{
    #[instrument(skip_all, name = "FMinimalName")]
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.index_and_type)?;
        stream.ser(&self.number)?;
        Ok({})
    }
}
