use std::io::Read;

use anyhow::Result;
use byteorder::{ReadBytesExt, LE};
use tracing::instrument;

use crate::read_array;

#[instrument(skip_all)]
fn read_script_objects<S: Read>(mut stream: S) -> Result<()> {
    let global_name_map = read_name_batch(&mut stream)?;
    let num_script_objects = stream.read_u32::<LE>()?;

    let script_objects = read_array(
        num_script_objects as usize,
        &mut stream,
        FScriptObjectEntry::read,
    )?;

    println!("{:#?}", script_objects);

    Ok(())
}

#[instrument(skip_all)]
fn read_name_batch<S: Read>(mut stream: S) -> Result<FNameBatch> {
    let num = stream.read_u32::<LE>()?;
    let num_string_bytes = stream.read_u32::<LE>()?;
    let hash_version = stream.read_u64::<LE>()?;

    let use_saved_hashes = false; // TODO check CanUseSavedHashes(HashVersion)

    let num_hash_bytes = num * 8;
    let num_header_bytes = num * 2;

    let mut hash_bytes = vec![0; num_hash_bytes as usize];
    stream.read_exact(&mut hash_bytes)?;

    let mut header_bytes = vec![0; num_header_bytes as usize];
    stream.read_exact(&mut header_bytes)?;

    let mut string_bytes = vec![0; num_string_bytes as usize];
    stream.read_exact(&mut string_bytes)?;

    Ok(FNameBatch {
        num,
        hash_bytes,
        header_bytes,
        string_bytes,
    })
}

#[derive(Debug)]
struct FScriptObjectEntry {
    //mapped: FMappedName,
    object_name: FMinimalName,
    global_index: FPackageObjectIndex,
    outer_index: FPackageObjectIndex,
    cdo_class_index: FPackageObjectIndex,
}
impl FScriptObjectEntry {
    #[instrument(skip_all, name = "FScriptObjectEntry")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            //mapped: FMappedName::read(s)?,
            object_name: FMinimalName::read(s)?,
            global_index: FPackageObjectIndex::read(s)?,
            outer_index: FPackageObjectIndex::read(s)?,
            cdo_class_index: FPackageObjectIndex::read(s)?,
        })
    }
}

#[derive(Debug)]
struct FPackageObjectIndex {
    type_and_id: u64,
}
impl FPackageObjectIndex {
    #[instrument(skip_all, name = "FPackageObjectIndex")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            type_and_id: s.read_u64::<LE>()?,
        })
    }
}

#[derive(Debug)]
struct FMappedName {
    index: u32,
    number: u32,
}
impl FMappedName {
    #[instrument(skip_all, name = "FMappedName")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            index: s.read_u32::<LE>()?,
            number: s.read_u32::<LE>()?,
        })
    }
}

#[derive(Debug)]
struct FMinimalName {
    index: FNameEntryId,
    number: i32,
}
impl FMinimalName {
    #[instrument(skip_all, name = "FMinimalName")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            index: FNameEntryId::read(s)?,
            number: s.read_i32::<LE>()?,
        })
    }
}

#[derive(Debug)]
struct FNameEntryId {
    value: u32,
}
impl FNameEntryId {
    #[instrument(skip_all, "FNameEntryId")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            value: s.read_u32::<LE>()?,
        })
    }
}

#[derive(Debug)]
struct FNameBatch {
    num: u32,
    hash_bytes: Vec<u8>,
    header_bytes: Vec<u8>,
    string_bytes: Vec<u8>,
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::BufReader};

    use super::*;
    #[test]
    fn test_read_script_objects() -> Result<()> {
        let mut stream = BufReader::new(File::open("giga.bin")?);

        ser_hex::read("trace.json", &mut stream, |s| read_script_objects(s))?;

        Ok(())
    }
}
