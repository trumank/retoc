use std::{borrow::Cow, io::Read};

use anyhow::Result;
use tracing::instrument;

use crate::{read_array, read_string, ReadExt, Readable};

#[derive(Debug)]
pub(crate) struct FNameMap {
    names: Vec<String>,
}
impl Readable for FNameMap {
    #[instrument(skip_all, "FNameMap")]
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        let num: u32 = s.ser()?;
        let _num_string_bytes: u32 = s.ser()?;
        let _hash_version: u64 = s.ser()?;

        let _use_saved_hashes = false; // TODO check CanUseSavedHashes(HashVersion)

        let _hash_bytes: Vec<u8> = s.ser_ctx(num as usize * 8)?;
        let lengths = read_array(num as usize, s, |s| Ok(u16::from_be_bytes(s.ser()?)))?;
        let names: Vec<_> = lengths
            .iter()
            .map(|&l| {
                let utf16 = l & 0x8000 != 0; // check high bit
                let l = (l & !0x8000) as i32; // reset high bit
                read_string(if utf16 { -l } else { l }, s)
            })
            .collect::<Result<_>>()?;

        Ok(Self { names })
    }
}
impl FNameMap {
    pub(crate) fn get(&self, name: FMinimalName) -> Cow<'_, str> {
        let n = &self.names[name.index.value as usize & 0xff_ffff];
        if name.number != 0 {
            format!("{n}_{}", name.number - 1).into()
        } else {
            n.into()
        }
    }
}

#[derive(Debug)]
pub(crate) struct FMappedName {
    index: u32,
    number: u32,
}

impl Readable for FMappedName {
    #[instrument(skip_all, name = "FMappedName")]
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            index: s.ser()?,
            number: s.ser()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FMinimalName {
    index: FNameEntryId,
    number: i32,
}
impl Readable for FMinimalName {
    #[instrument(skip_all, name = "FMinimalName")]
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            index: s.ser()?,
            number: s.ser()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct FNameEntryId {
    value: u32,
}
impl Readable for FNameEntryId {
    #[instrument(skip_all, name = "FNameEntryId")]
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self { value: s.ser()? })
    }
}
