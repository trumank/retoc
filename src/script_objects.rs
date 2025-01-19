use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::io::Seek;

use anyhow::Result;
use serde::Serializer;
use strum::FromRepr;
use tracing::instrument;

use crate::{
    name_map::{FMappedName, FNameMap},
    read_array,
    ser::*,
};
use crate::name_map::EMappedNameType;

#[derive(Debug, Clone, Default)]
pub(crate) struct ZenScriptObjects {
    pub(crate) global_name_map: FNameMap,
    pub(crate) script_objects: Vec<FScriptObjectEntry>,
    pub(crate) script_object_lookup: HashMap<FPackageObjectIndex, FScriptObjectEntry>,
}
impl Readable for ZenScriptObjects {
    #[instrument(skip_all, name = "ZenScriptObjects")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let global_name_map: FNameMap = FNameMap::deserialize(s, EMappedNameType::Global)?;
        let num_script_objects: u32 = s.de()?;
        let script_objects = read_array(num_script_objects as usize, s, FScriptObjectEntry::read)?;

        // Build lookup by package object index for fast access
        let mut script_object_lookup: HashMap<FPackageObjectIndex, FScriptObjectEntry> = HashMap::with_capacity(num_script_objects as usize);
        script_objects.iter().for_each(|script_object| {
            script_object_lookup.insert(script_object.global_index, *script_object);
        });
        Ok(Self{ global_name_map, script_objects, script_object_lookup })
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct FScriptObjectEntry {
    pub(crate) object_name: FMappedName,
    pub(crate) global_index: FPackageObjectIndex,
    pub(crate) outer_index: FPackageObjectIndex,
    pub(crate) cdo_class_index: FPackageObjectIndex,
}
impl FScriptObjectEntry {
    #[instrument(skip_all, name = "FScriptObjectEntry")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            object_name: s.de()?,
            global_index: s.de()?,
            outer_index: s.de()?,
            cdo_class_index: s.de()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
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
    pub(crate) imported_package_index: u32,
    pub(crate) imported_public_export_hash_index: u32
}

impl FPackageObjectIndex {
    const INDEX_BITS: u64 = 62;
    const INDEX_MASK: u64 = (1 << FPackageObjectIndex::INDEX_BITS) - 1;
    const TYPE_SHIFT: u64 = FPackageObjectIndex::INDEX_BITS;
    const INVALID_ID: u64 = !0;

    pub(crate) fn invalid() -> FPackageObjectIndex {
        FPackageObjectIndex{type_and_id: FPackageObjectIndex::INVALID_ID }
    }
    pub(crate) fn null() -> FPackageObjectIndex { Self::invalid() }
    pub(crate) fn new(kind: FPackageObjectIndexType, value: u64) -> FPackageObjectIndex
    {
        FPackageObjectIndex{type_and_id: ((kind as u64) << FPackageObjectIndex::TYPE_SHIFT) | value}
    }
    pub(crate) fn kind(self) -> FPackageObjectIndexType {
        FPackageObjectIndexType::from_repr((self.type_and_id >> Self::TYPE_SHIFT) as usize).unwrap()
    }
    pub(crate) fn value(self) -> Option<u64> {
        (self.kind() != FPackageObjectIndexType::Null).then_some(self.type_and_id)
    }
    pub(crate) fn export(self) -> Option<u32> {
        (self.kind() == FPackageObjectIndexType::Export).then_some(self.type_and_id as u32)
    }
    pub(crate) fn package_import(self) -> Option<FPackageImportReference> {
        (self.kind() == FPackageObjectIndexType::PackageImport).then_some(FPackageImportReference{
            imported_package_index: ((self.type_and_id & FPackageObjectIndex::INDEX_MASK) >> 32) as u32,
            imported_public_export_hash_index: (self.type_and_id as u32)
        })
    }
    pub(crate) fn is_null(self) -> bool { self.kind() == FPackageObjectIndexType::Null }

    pub(crate) fn generate_import_hash_from_object_path(object_path: &str) -> u64 {
        let lower_slash_path = object_path
            .chars()
            .map(|c| match c {
                ':' | '.' => '/',
                c => c.to_ascii_lowercase(),
            })
            .collect::<String>();
        let mut hash: u64 = cityhasher::hash(
            &lower_slash_path
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<u8>>(),
        );
        hash &= !(3 << 62);
        hash
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
impl Display for FPackageObjectIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.serialize_u64(self.type_and_id)
    }
}
impl Writeable for FPackageObjectIndex {
    #[instrument(skip_all, name = "FPackageObjectIndex")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.type_and_id)?;
        Ok({})
    }
}

#[cfg(test)]
mod test {
    use fs_err as fs;
    use std::io::BufReader;

    use super::*;
    #[test]
    fn test_read_script_objects() -> Result<()> {
        let mut stream = BufReader::new(fs::File::open("giga.bin")?);

        ser_hex::read("trace.json", &mut stream, |s| -> Result<()> {
            let script_objects: ZenScriptObjects = s.de()?;

            for s in script_objects.script_objects {
                println!("{}:", script_objects.global_name_map.get(s.object_name));
                println!("  global_index:    {:?}", s.global_index.value());
                println!("  outer_index:     {:?}", s.outer_index.value());
                println!("  cdo_class_index: {:?}", s.cdo_class_index.value());
            }
            return Ok({})
        })?;

        Ok(())
    }
}
