use std::io::{Read, Seek};

use anyhow::Result;
use strum::FromRepr;
use tracing::instrument;

use crate::{
    name_map::{FMinimalName, FNameMap},
    read_array,
    ser::*,
};

#[instrument(skip_all)]
fn read_script_objects<S: Read>(s: &mut S) -> Result<()> {
    let global_name_map: FNameMap = s.de()?;
    let num_script_objects: u32 = s.de()?;

    let script_objects = read_array(num_script_objects as usize, s, FScriptObjectEntry::read)?;

    for s in script_objects {
        //println!(
        //    "{}:",
        //    global_name_map.header_bytes[s.object_name.index.value as usize & 0xffffff]
        //);
        //println!("{}:", s.object_name.index.value as usize & 0xffffff);
        println!("{}:", global_name_map.get(s.object_name));
        println!("  global_index:    {:?}", s.global_index.value());
        println!("  outer_index:     {:?}", s.outer_index.value());
        println!("  cdo_class_index: {:?}", s.cdo_class_index.value());
    }

    //println!("{:#?}", script_objects);

    Ok(())
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
            object_name: s.de()?,
            global_index: s.de()?,
            outer_index: s.de()?,
            cdo_class_index: s.de()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)] // Needed for sizeof to determine number of entries in package header
pub(crate) struct FPackageObjectIndex {
    type_and_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
pub(crate) enum FPackageObjectIndexType {
    Export,
    ScriptImport,
    PackageImport,
    Null
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct FPackageImportReference {
    imported_package_index: u32,
    imported_public_export_hash_index: u32
}

impl FPackageObjectIndex {
    const INDEX_BITS: u64 = 62;
    const INDEX_MASK: u64 = (1 << FPackageObjectIndex::INDEX_BITS) - 1;
    const TYPE_SHIFT: u64 = FPackageObjectIndex::INDEX_BITS;
    const INVALID_ID: u64 = !0;

    fn invalid() -> FPackageObjectIndex {
        FPackageObjectIndex{type_and_id: FPackageObjectIndex::INVALID_ID }
    }
    fn new(kind: FPackageObjectIndexType, value: u64) -> FPackageObjectIndex
    {
        FPackageObjectIndex{type_and_id: ((kind as u64) << FPackageObjectIndex::TYPE_SHIFT) | value}
    }
    fn kind(self) -> FPackageObjectIndexType {
        FPackageObjectIndexType::from_repr((self.type_and_id >> Self::TYPE_SHIFT) as usize).unwrap()
    }
    fn value(self) -> Option<u64> {
        (self.kind() != FPackageObjectIndexType::Null).then_some(self.type_and_id)
    }
    fn export(self) -> Option<u32> {
        (self.kind() == FPackageObjectIndexType::Export).then_some(self.type_and_id as u32)
    }
    fn package_import(self) -> Option<FPackageImportReference> {
        (self.kind() == FPackageObjectIndexType::PackageImport).then_some(FPackageImportReference{
            imported_package_index: ((self.type_and_id & FPackageObjectIndex::INDEX_MASK) >> 32) as u32,
            imported_public_export_hash_index: (self.type_and_id as u32)
        })
    }
}
impl Readable for FPackageObjectIndex {
    #[instrument(skip_all, name = "FPackageObjectIndex")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            type_and_id: s.de()?,
        })
    }
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
