use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};

use anyhow::Result;
use serde::Serializer;
use strum::FromRepr;
use tracing::instrument;

use crate::name_map::{read_name_batch_parts, EMappedNameType};
use crate::{
    name_map::{FMappedName, FNameMap},
    ser::*,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct ZenScriptObjects {
    pub(crate) global_name_map: FNameMap,
    pub(crate) script_objects: Vec<FScriptObjectEntry>,
    pub(crate) script_object_lookup: HashMap<FPackageObjectIndex, FScriptObjectEntry>,
}
impl ZenScriptObjects {
    #[instrument(skip_all, name = "ZenScriptObjects")]
    pub(crate) fn deserialize_new<S: Read>(s: &mut S) -> Result<Self> {
        let global_name_map: FNameMap = FNameMap::deserialize(s, EMappedNameType::Global)?;
        Ok(Self::new(s.de()?, global_name_map))
    }
    #[instrument(skip_all, name = "ZenScriptObjects")]
    pub(crate) fn deserialize_old<S: Read>(s: &mut S, names: &[u8]) -> Result<Self> {
        let global_name_map: FNameMap =
            FNameMap::create_from_names(EMappedNameType::Global, read_name_batch_parts(names)?);
        Ok(Self::new(s.de()?, global_name_map))
    }
    fn new(script_objects: Vec<FScriptObjectEntry>, global_name_map: FNameMap) -> Self {
        // Build lookup by package object index for fast access
        let mut script_object_lookup: HashMap<FPackageObjectIndex, FScriptObjectEntry> =
            HashMap::with_capacity(script_objects.len());
        script_objects.iter().for_each(|script_object| {
            script_object_lookup.insert(script_object.global_index, *script_object);
        });
        Self {
            global_name_map,
            script_objects,
            script_object_lookup,
        }
    }
    pub(crate) fn print(&self) {
        for s in &self.script_objects {
            println!("{}:", self.global_name_map.get(s.object_name));
            println!("  global_index:    {:?}", s.global_index.value());
            println!("  outer_index:     {:?}", s.outer_index.value());
            println!("  cdo_class_index: {:?}", s.cdo_class_index.value());
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct FScriptObjectEntry {
    pub(crate) object_name: FMappedName,
    pub(crate) global_index: FPackageObjectIndex,
    pub(crate) outer_index: FPackageObjectIndex,
    pub(crate) cdo_class_index: FPackageObjectIndex,
}
impl Readable for FScriptObjectEntry {
    #[instrument(skip_all, name = "FScriptObjectEntry")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            object_name: s.de()?,
            global_index: s.de()?,
            outer_index: s.de()?,
            cdo_class_index: s.de()?,
        })
    }
}
impl Writeable for FScriptObjectEntry {
    #[instrument(skip_all, name = "FScriptObjectEntry")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.object_name)?;
        s.ser(&self.global_index)?;
        s.ser(&self.outer_index)?;
        s.ser(&self.cdo_class_index)?;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default, Hash)]
#[repr(C)] // Needed for sizeof to determine number of entries in package header
pub(crate) struct FPackageObjectIndex {
    type_and_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
pub(crate) enum FPackageObjectIndexType {
    Export,
    ScriptImport,
    PackageImport,
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct FPackageImportReference {
    pub(crate) imported_package_index: u32,
    pub(crate) imported_public_export_hash_index: u32,
}

impl FPackageObjectIndex {
    const INDEX_BITS: u64 = 62;
    const INDEX_MASK: u64 = (1 << Self::INDEX_BITS) - 1;
    const TYPE_SHIFT: u64 = Self::INDEX_BITS;
    const INVALID_ID: u64 = !0;

    pub(crate) fn create_from_raw(raw: u64) -> Self {
        Self { type_and_id: raw }
    }
    pub(crate) fn create(kind: FPackageObjectIndexType, value: u64) -> Self {
        Self {
            type_and_id: ((kind as u64) << Self::TYPE_SHIFT) | value,
        }
    }
    pub(crate) fn create_null() -> Self {
        Self::create(FPackageObjectIndexType::Null, Self::INVALID_ID)
    }
    pub(crate) fn create_export(export_index: u32) -> Self {
        Self::create(FPackageObjectIndexType::Export, export_index as u64)
    }
    pub(crate) fn create_script_import(object_path: &str) -> Self {
        let import_hash = Self::generate_import_hash_from_object_path(object_path);
        Self::create(FPackageObjectIndexType::ScriptImport, import_hash)
    }
    pub(crate) fn create_package_import(import_ref: FPackageImportReference) -> Self {
        let import_value = import_ref.imported_public_export_hash_index as u64
            | ((import_ref.imported_package_index as u64) << 32);
        Self::create(FPackageObjectIndexType::PackageImport, import_value)
    }
    // Function to create a legacy UE4 zen package import from the full, lower-case name of the imported/exported object using / as a separator
    pub(crate) fn create_legacy_package_import_from_path(object_path: &str) -> Self {
        let import_hash = Self::generate_import_hash_from_object_path(object_path);
        Self::create(FPackageObjectIndexType::PackageImport, import_hash)
    }
    pub(crate) fn raw_index(self) -> u64 {
        self.type_and_id & Self::INDEX_MASK
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
        (self.kind() == FPackageObjectIndexType::PackageImport).then_some(FPackageImportReference {
            imported_package_index: ((self.type_and_id & FPackageObjectIndex::INDEX_MASK) >> 32)
                as u32,
            imported_public_export_hash_index: (self.type_and_id as u32),
        })
    }
    pub(crate) fn is_null(self) -> bool {
        self.kind() == FPackageObjectIndexType::Null
    }
    pub(crate) fn to_raw(self) -> u64 {
        self.type_and_id
    }

    fn generate_import_hash_from_object_path(object_path: &str) -> u64 {
        let lower_slash_path = object_path
            .chars()
            .map(|c| match c {
                ':' | '.' => '/',
                c => c.to_ascii_lowercase(),
            })
            .collect::<String>();
        let mut hash: u64 = cityhasher::hash(
            lower_slash_path
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
        Ok(())
    }
}
impl std::fmt::Debug for FPackageObjectIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            FPackageObjectIndexType::Export => write!(
                f,
                "FPackageObjectIndex::Export({:X?})",
                self.export().unwrap()
            ),
            FPackageObjectIndexType::ScriptImport => write!(
                f,
                "FPackageObjectIndex::ScriptImport({:X?})",
                self.raw_index()
            ),
            FPackageObjectIndexType::PackageImport => write!(
                f,
                "FPackageObjectIndex::PackageImport({:X?})",
                self.package_import().unwrap()
            ),
            FPackageObjectIndexType::Null => write!(f, "FPackageObjectIndex::Null"),
        }
    }
}

#[cfg(test)]
mod test {
    use fs_err as fs;
    use std::io::BufReader;
    use ue_reflection::ObjectType;

    use crate::{
        name_map::write_name_batch_parts, EIoChunkType, EngineVersion, FIoChunkId, IoStoreWriter,
        UEPathBuf,
    };

    use super::*;
    #[test]
    fn test_read_script_objects_new() -> Result<()> {
        let mut stream = BufReader::new(fs::File::open("tests/UE5.3/ScriptObjects.bin")?);

        let script_objects = ZenScriptObjects::deserialize_new(&mut stream)?;
        script_objects.print();

        Ok(())
    }

    #[test]
    fn test_read_script_objects_old() -> Result<()> {
        let names = fs::read("tests/UE4.27/LoaderGlobalNames_1.bin")?;
        let mut meta = BufReader::new(fs::File::open("tests/UE4.27/LoaderInitialLoadMeta_1.bin")?);

        let script_objects = ZenScriptObjects::deserialize_old(&mut meta, &names)?;
        script_objects.print();

        Ok(())
    }

    /// generate script objects from refelection dump generated by https://github.com/trumank/meatloaf/tree/master/dumper
    //#[test]
    fn test_gen_script_objects() -> Result<()> {
        let dump: HashMap<String, ue_reflection::ObjectType> =
            serde_json::from_reader(std::io::BufReader::new(fs::File::open("fsd.json")?))?;

        // map CDO => Class
        let mut cdos = HashMap::<&str, &str>::new();

        let mut names = FNameMap::create(EMappedNameType::Global);
        let mut script_objects = vec![];
        for (path, object) in &dump {
            if path.starts_with("/Script/") {
                if let ObjectType::Class(class) = &object {
                    if let Some(cdo) = &class.class_default_object {
                        cdos.insert(&cdo, &path);
                    }
                }
            }
        }
        for (path, object) in &dump {
            if path.starts_with("/Script/") {
                if let ObjectType::Class(class) = &object {
                    if let Some(cdo) = &class.class_default_object {
                        cdos.insert(&cdo, &path);
                    }
                }
                let mut components = path.rsplit(['/', '.', ':']);
                let name = components.next().unwrap();
                let count = components.count();
                let name = if count == 2 {
                    // special case for /Script/ package objects
                    path
                } else {
                    name
                };
                let outer = &match object {
                    ObjectType::ScriptStruct(obj) => &obj.r#struct.object,
                    ObjectType::Class(obj) => &obj.r#struct.object,
                    ObjectType::Function(obj) => &obj.r#struct.object,
                    ObjectType::Enum(obj) => &obj.object,
                    ObjectType::Object(object) => object,
                    ObjectType::Package(obj) => &obj.object,
                }
                .outer;

                let outer_index = outer
                    .as_ref()
                    .map_or(FPackageObjectIndex::create_null(), |outer| {
                        FPackageObjectIndex::create_script_import(&outer)
                    });
                let cdo_class_index = cdos
                    .get(path.as_str())
                    .map_or(FPackageObjectIndex::create_null(), |outer| {
                        FPackageObjectIndex::create_script_import(&outer)
                    });
                script_objects.push(FScriptObjectEntry {
                    object_name: names.store(name),
                    global_index: FPackageObjectIndex::create_script_import(&path),
                    outer_index,
                    cdo_class_index,
                });
            }
        }
        for s in &script_objects {
            println!("{}:", names.get(s.object_name));
            println!("  global_index:    {:?}", s.global_index);
            println!("  outer_index:     {:?}", s.outer_index);
            println!("  cdo_class_index: {:?}", s.cdo_class_index);
        }

        let ue_version = EngineVersion::UE4_27;
        let mut writer = IoStoreWriter::new(
            "global.utoc",
            ue_version.toc_version(),
            None,
            UEPathBuf::new(),
        )?;

        let (buf_names, buf_name_hashes) = write_name_batch_parts(&names.copy_raw_names())?;
        let buf_script_objects = {
            let mut buf = vec![];
            WriteExt::ser(&mut buf, &script_objects)?;
            buf
        };

        writer.write_chunk(
            FIoChunkId::create(0, 0, EIoChunkType::LoaderGlobalNames),
            None,
            &buf_names,
        )?;
        writer.write_chunk(
            FIoChunkId::create(0, 0, EIoChunkType::LoaderGlobalNameHashes),
            None,
            &buf_name_hashes,
        )?;
        writer.write_chunk(
            FIoChunkId::create(0, 0, EIoChunkType::LoaderInitialLoadMeta),
            None,
            &buf_script_objects,
        )?;

        writer.finalize()?;

        //let names = fs::read("tests/UE4.27/LoaderGlobalNames_1.bin")?;
        //let mut meta = BufReader::new(fs::File::open("tests/UE4.27/LoaderInitialLoadMeta_1.bin")?);

        //let script_objects = ZenScriptObjects::deserialize_old(&mut meta, &names)?;

        //for s in script_objects.script_objects {
        //    println!("{}:", script_objects.global_name_map.get(s.object_name));
        //    println!("  global_index:    {:?}", s.global_index);
        //    println!("  outer_index:     {:?}", s.outer_index);
        //    println!("  cdo_class_index: {:?}", s.cdo_class_index);
        //}

        Ok(())
    }
}
