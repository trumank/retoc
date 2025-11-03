use anyhow::{Context, Result, anyhow};
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use indexmap::IndexSet;
use std::io::{Read, Seek, Write};
use strum::FromRepr;

use crate::crc::generate_name_hash;
use crate::name_map::{break_down_name_string, read_name_batch, write_name_batch};
use crate::{FGuid, ser::*};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct FAssetRegistryHeader {
    filter_editor_only_data: bool,
}

impl Readable for FAssetRegistryHeader {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { filter_editor_only_data: stream.de()? })
    }
}

impl Writeable for FAssetRegistryHeader {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.filter_editor_only_data)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, FromRepr)]
#[repr(u32)]
pub enum FAssetRegistryVersion {
    /// From before file versioning was implemented
    #[default]
    PreVersioning = 0,
    /// The first version of the runtime asset registry to include file versioning
    HardSoftDependencies,
    /// Added FAssetRegistryState and support for piecemeal serialization
    AddAssetRegistryState,
    /// AssetData serialization format changed, versions before this are not readable
    ChangedAssetData,
    /// Removed MD5 hash from package data
    RemovedMD5Hash,
    /// Added hard/soft manage references
    AddedHardManage,
    /// Added MD5 hash of cooked package to package data
    AddedCookedMD5Hash,
    /// Added UE::AssetRegistry::EDependencyProperty to each dependency
    AddedDependencyFlags,
    /// Major tag format change that replaces USE_COMPACT_ASSET_REGISTRY:
    /// * Target tag INI settings cooked into tag data
    /// * Instead of FString values are stored directly as one of:
    ///     - Narrow / wide string
    ///     - [Numberless] FName
    ///     - [Numberless] export path
    ///     - Localized string
    /// * All value types are deduplicated
    /// * All key-value maps are cooked into a single contiguous range
    /// * Switched from FName table to seek-free and more optimized FName batch loading
    /// * Removed global tag storage, a tag map reference-counts one store per asset registry
    /// * All configs can mix fixed and loose tag maps
    FixedTags,
    /// Added Version information to AssetPackageData
    WorkspaceDomain,
    /// Added ImportedClasses to AssetPackageData
    PackageImportedClasses,
    /// A new version number of UE5 was added to FPackageFileSummary
    PackageFileSummaryVersionChange,
    /// Change to linker export/import resource serialization
    ObjectResourceOptionalVersionChange,
    /// Added FIoHash for each FIoChunkId in the package to the AssetPackageData
    AddedChunkHashes,
    /// Classes are serialized as path names rather than short object names, e.g. /Script/Engine.StaticMesh
    ClassPaths,
    /// Asset bundles are serialized as FTopLevelAssetPath instead of FSoftObjectPath, deprecated FAssetData::ObjectPath
    RemoveAssetPathFNames,
    /// Added header with bFilterEditorOnlyData flag
    AddedHeader,
    /// Added Extension to AssetPackageData
    AssetPackageDataHasExtension,
    /// Added PackageLocation to AssetPackageData
    AssetPackageDataHasPackageLocation,
    /// Replaced 2 byte wide string with UTF8 String
    MarshalledTextAsUTF8String,
    /// Replaced FAssetPackageData::PackageGuid with PackageSavedHash
    PackageSavedHash,
    /// FPackageDependencyData::LoadDependenciesFromPackageHeader changed how it calculates PackageDependencies
    ExternalActorToWorldIsEditorOnly,
}

impl Readable for FAssetRegistryVersion {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        let value: u32 = stream.de()?;
        Self::from_repr(value).with_context(|| format!("invalid FAssetRegistryVersion: {value}"))
    }
}

impl Writeable for FAssetRegistryVersion {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&(*self as u32))
    }
}

fn read_fname<S: Read>(stream: &mut S, names: &[String]) -> Result<String> {
    let n = stream.read_u32::<LE>()?;
    let index = (n & 0x7FFFFFFF) as usize;
    let name = names.get(index).ok_or_else(|| anyhow!("Invalid name index: {}", index))?;

    if n & 0x80000000 != 0 {
        let number = stream.read_u32::<LE>()?;
        Ok(format!("{}_{}", name, number - 1))
    } else {
        Ok(name.clone())
    }
}

fn read_fname_pre_fixed_tags<S: Read>(stream: &mut S, names: &[String]) -> Result<String> {
    let index = stream.read_i32::<LE>()?;
    let number = stream.read_i32::<LE>()?;

    if index < 0 || index as usize >= names.len() {
        anyhow::bail!("Invalid name index: {} (total names: {})", index, names.len());
    }

    let name = &names[index as usize];

    if number > 0 { Ok(format!("{}_{}", name, number - 1)) } else { Ok(name.clone()) }
}

fn write_fname<S: Write>(stream: &mut S, value: &str, name_map: &IndexSet<&str>) -> Result<()> {
    let (base_name, number) = break_down_name_string(value);

    let index = name_map.get_index_of(base_name).expect("name should already be in name map") as u32;

    if number != 0 {
        stream.write_u32::<LE>(index | 0x80000000)?;
        stream.write_i32::<LE>(number)?;
    } else {
        stream.write_u32::<LE>(index)?;
    }
    Ok(())
}

/// FTopLevelAssetPath structure
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FTopLevelAssetPath {
    pub package_name: String, // e.g., "/Script/Engine"
    pub asset_name: String,   // e.g., "StaticMesh"
}

impl FTopLevelAssetPath {
    fn read_fname<S: Read>(stream: &mut S, names: &[String]) -> Result<Self> {
        Ok(Self {
            package_name: read_fname(stream, names)?,
            asset_name: read_fname(stream, names)?,
        })
    }

    fn write_fname<S: Write>(&self, stream: &mut S, name_map: &IndexSet<&str>) -> Result<()> {
        write_fname(stream, &self.package_name, name_map)?;
        write_fname(stream, &self.asset_name, name_map)?;
        Ok(())
    }
}

/// FAssetBundleEntry structure
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FAssetBundleEntry {
    pub bundle_name: String,
    pub asset_paths: Vec<FTopLevelAssetPath>,
}

impl FAssetBundleEntry {
    fn read_fname<S: Read>(stream: &mut S, names: &[String]) -> Result<Self> {
        let bundle_name = read_fname(stream, names)?;
        let num_paths: i32 = stream.de()?;
        let asset_paths = read_array(num_paths as usize, stream, |s| {
            // serialized as FSoftObjectPath
            let path = FTopLevelAssetPath::read_fname(s, names)?;
            let _sub_path: String = s.de()?;
            Ok(path)
        })?;

        Ok(Self { bundle_name, asset_paths })
    }

    fn write_fname<S: Write>(&self, stream: &mut S, name_map: &IndexSet<&str>) -> Result<()> {
        write_fname(stream, &self.bundle_name, name_map)?;
        stream.ser(&(self.asset_paths.len() as i32))?;
        for path in &self.asset_paths {
            // serialized as FSoftObjectPath
            path.write_fname(stream, name_map)?;
            stream.ser(&String::new())?;
        }
        Ok(())
    }
}

/// Asset class path - can be either modern FTopLevelAssetPath or legacy short name
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetClassPath {
    /// Modern format (UE 5.1+): package and asset name
    TopLevelAssetPath(FTopLevelAssetPath),
    /// Legacy format (pre-ClassPaths): short class name
    LegacyClassName(String), // e.g., "StaticMesh"
}

impl AssetClassPath {
    /// Convert to full path string like "/Script/Engine.StaticMesh"
    pub fn to_path_string(&self) -> String {
        match self {
            AssetClassPath::TopLevelAssetPath(path) => {
                if path.package_name.is_empty() && path.asset_name.is_empty() {
                    String::new()
                } else {
                    format!("{}.{}", path.package_name, path.asset_name)
                }
            }
            AssetClassPath::LegacyClassName(name) => name.clone(),
        }
    }

    /// Get package name (only available for TopLevelAssetPath)
    pub fn package_name(&self) -> &str {
        match self {
            AssetClassPath::TopLevelAssetPath(path) => &path.package_name,
            AssetClassPath::LegacyClassName(_) => "",
        }
    }

    /// Get asset name
    pub fn asset_name(&self) -> &str {
        match self {
            AssetClassPath::TopLevelAssetPath(path) => &path.asset_name,
            AssetClassPath::LegacyClassName(name) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportPath {
    pub object_path: String,
    pub package_path: String,
    pub asset_class: AssetClassPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, FromRepr)]
#[repr(u32)]
pub enum TagType {
    AnsiString = 0,
    WideString = 1,
    NumberlessName = 2,
    Name = 3,
    NumberlessExportPath = 4,
    ExportPath = 5,
    LocalizedText = 6,
}

impl Readable for TagType {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        let value: u32 = stream.de()?;
        Self::from_repr(value).with_context(|| format!("invalid AssetRegistry TagType: {value}"))
    }
}

impl Writeable for TagType {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&(*self as u32))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair {
    pub name: String,
    pub type_: TagType,
    pub index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapHandle {
    pub has_numberless_keys: bool,
    pub num: u16,
    pub pair_begin: u32,
}

impl Readable for MapHandle {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        let packed = stream.read_u64::<LE>()?;
        Ok(Self {
            has_numberless_keys: (packed >> 63) != 0,
            num: (packed >> 32) as u16,
            pair_begin: packed as u32,
        })
    }
}

impl Writeable for MapHandle {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        let packed = ((self.has_numberless_keys as u64) << 63) | ((self.num as u64) << 32) | (self.pair_begin as u64);
        stream.write_u64::<LE>(packed)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetData {
    pub object_path: String,
    pub package_path: String,
    pub asset_class: String,
    pub package_name: String,
    pub asset_name: String,
    pub tags: MapHandle,
    pub legacy_tags: Vec<(String, String)>,
    pub bundles: Vec<FAssetBundleEntry>,
    pub chunk_ids: Vec<u32>,
    pub flags: u32,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Dependencies {
    pub dependencies_size: u64,
    pub dependencies: Vec<u32>,
    pub package_data_buffer_size: u32,
}

impl Readable for Dependencies {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Dependencies {
            dependencies_size: stream.de()?,
            dependencies: stream.de()?,
            package_data_buffer_size: stream.de()?,
        })
    }
}

impl Writeable for Dependencies {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.dependencies_size)?;
        stream.ser(&self.dependencies)?;
        stream.ser(&self.package_data_buffer_size)?;
        Ok(())
    }
}

const MAGIC_START: u32 = 0x12345679;
const MAGIC_END: u32 = 0x87654321;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Store {
    pub pair_count: u32,
    pub texts: Vec<String>,
    pub nbl_names: Vec<String>,
    pub names: Vec<String>,
    pub nbl_export_paths: Vec<ExportPath>,
    pub export_paths: Vec<ExportPath>,
    pub ansi_strings: Vec<String>,
    pub wide_strings: Vec<String>,
    pub pairs: Vec<Pair>,
}

impl Store {
    fn read_with_names<S: Read>(stream: &mut S, names: &[String], registry_version: FAssetRegistryVersion) -> Result<Self> {
        let magic = stream.read_u32::<LE>()?;
        anyhow::ensure!(magic == MAGIC_START, "Invalid Store magic: expected {MAGIC_START:#x}, got {magic:#x}");

        let nbl_names_count: u32 = stream.de()?;
        let names_count: u32 = stream.de()?;
        let nbl_export_path_count: u32 = stream.de()?;
        let export_path_count: u32 = stream.de()?;
        let texts_count: u32 = stream.de()?;
        let ansi_strings_count: u32 = stream.de()?;
        let wide_strings_count: u32 = stream.de()?;
        let _ansi_string_bytes: u32 = stream.de()?;
        let _wide_string_bytes: u32 = stream.de()?;
        let nbl_pair_count: u32 = stream.de()?;
        let pair_count: u32 = stream.de()?;

        let _text_bytes: u32 = stream.de()?;
        let texts = if registry_version >= FAssetRegistryVersion::MarshalledTextAsUTF8String {
            read_array(texts_count as usize, stream, |s| -> Result<String> {
                let len = s.read_u32::<LE>()? as usize;
                let mut chars = vec![0u8; len];
                s.read_exact(&mut chars)?;
                Ok(String::from_utf8(chars)?)
            })?
        } else {
            read_array(texts_count as usize, stream, |s| -> Result<String> {
                let len = s.read_u32::<LE>()? as usize;
                let mut chars = vec![0u8; len.saturating_sub(1)];
                s.read_exact(&mut chars)?;
                s.read_u8()?;
                Ok(String::from_utf8(chars)?)
            })?
        };

        // Read name arrays
        let nbl_names: Vec<String> = read_array(nbl_names_count as usize, stream, |s| read_fname(s, names))?;
        let store_names: Vec<String> = read_array(names_count as usize, stream, |s| read_fname(s, names))?;

        let nbl_export_paths: Vec<ExportPath> = if registry_version >= FAssetRegistryVersion::ClassPaths {
            read_array(nbl_export_path_count as usize, stream, |s| {
                Ok(ExportPath {
                    asset_class: AssetClassPath::TopLevelAssetPath(FTopLevelAssetPath::read_fname(s, names)?),
                    object_path: read_fname(s, names)?,
                    package_path: read_fname(s, names)?,
                })
            })?
        } else {
            read_array(nbl_export_path_count as usize, stream, |s| {
                Ok(ExportPath {
                    asset_class: AssetClassPath::LegacyClassName(read_fname(s, names)?),
                    object_path: read_fname(s, names)?,
                    package_path: read_fname(s, names)?,
                })
            })?
        };

        let export_paths: Vec<ExportPath> = if registry_version >= FAssetRegistryVersion::ClassPaths {
            read_array(export_path_count as usize, stream, |s| {
                Ok(ExportPath {
                    asset_class: AssetClassPath::TopLevelAssetPath(FTopLevelAssetPath::read_fname(s, names)?),
                    object_path: read_fname(s, names)?,
                    package_path: read_fname(s, names)?,
                })
            })?
        } else {
            read_array(export_path_count as usize, stream, |s| {
                Ok(ExportPath {
                    asset_class: AssetClassPath::LegacyClassName(read_fname(s, names)?),
                    object_path: read_fname(s, names)?,
                    package_path: read_fname(s, names)?,
                })
            })?
        };

        // Read string offset tables (not used for reading, but required for file format)
        let _ansi_string_offsets: Vec<u32> = u32::de_vec(ansi_strings_count as usize, stream)?;
        let _wide_string_offsets: Vec<u32> = u32::de_vec(wide_strings_count as usize, stream)?;

        // Read null-terminated ANSI strings
        let ansi_strings = read_array(ansi_strings_count as usize, stream, |s| -> Result<String> {
            let mut chars = vec![];
            loop {
                let byte = s.read_u8()?;
                if byte == 0 {
                    break;
                }
                chars.push(byte);
            }
            Ok(String::from_utf8(chars)?)
        })?;

        // Read null-terminated wide strings
        let wide_strings = read_array(wide_strings_count as usize, stream, |s| -> Result<String> {
            let mut chars = vec![];
            loop {
                let word = s.read_u16::<LE>()?;
                if word == 0 {
                    break;
                }
                chars.push(word);
            }
            Ok(String::from_utf16(&chars)?)
        })?;

        // Read pair array
        let pairs: Vec<Pair> = read_array(nbl_pair_count as usize, stream, |s| {
            let name_idx = s.read_u32::<LE>()? as usize;
            let name = names.get(name_idx).ok_or_else(|| anyhow!("Invalid pair name index: {}", name_idx))?.clone();
            let packed = s.read_u32::<LE>()?;
            let type_ = TagType::from_repr(packed & 0x7).with_context(|| format!("Invalid pair TagType: {}", packed & 0x7))?;
            let index = packed >> 3;
            Ok(Pair { name, type_, index })
        })?;

        let magic_end = stream.read_u32::<LE>()?;
        anyhow::ensure!(magic_end == MAGIC_END, "Invalid Store end magic: expected {MAGIC_END:#x}, got {magic_end:#x}");

        Ok(Self {
            pair_count,
            texts,
            nbl_names,
            names: store_names,
            nbl_export_paths,
            export_paths,
            ansi_strings,
            wide_strings,
            pairs,
        })
    }

    fn write_with_name_map<S: Write>(&self, stream: &mut S, name_map: &IndexSet<&str>, registry_version: FAssetRegistryVersion) -> Result<()> {
        stream.write_u32::<LE>(MAGIC_START)?;

        // Write counts
        stream.ser(&(self.nbl_names.len() as u32))?;
        stream.ser(&(self.names.len() as u32))?;
        stream.ser(&(self.nbl_export_paths.len() as u32))?;
        stream.ser(&(self.export_paths.len() as u32))?;
        stream.ser(&(self.texts.len() as u32))?;
        stream.ser(&(self.ansi_strings.len() as u32))?;
        stream.ser(&(self.wide_strings.len() as u32))?;

        // Write byte sizes
        let ansi_bytes: u32 = self.ansi_strings.iter().map(|s| s.len() as u32 + 1).sum();
        let wide_bytes: u32 = self.wide_strings.iter().map(|s| s.chars().count() as u32 + 1).sum();
        stream.ser(&ansi_bytes)?;
        stream.ser(&wide_bytes)?;

        stream.ser(&(self.pairs.len() as u32))?;
        stream.ser(&self.pair_count)?;

        if registry_version >= FAssetRegistryVersion::MarshalledTextAsUTF8String {
            let text_bytes: u32 = self.texts.iter().map(|s| s.len() as u32 + 4).sum();
            stream.ser(&text_bytes)?;
            for text in &self.texts {
                stream.ser(&(text.len() as u32))?;
                stream.write_all(text.as_bytes())?;
            }
        } else {
            let text_bytes: u32 = self.texts.iter().map(|s| s.len() as u32 + 1 + 4).sum();
            stream.ser(&text_bytes)?;
            for text in &self.texts {
                stream.ser(&(text.len() as u32 + 1))?;
                stream.write_all(text.as_bytes())?;
                stream.write_u8(0)?;
            }
        }

        // Write name arrays
        for name in &self.nbl_names {
            write_fname(stream, name, name_map)?;
        }
        for name in &self.names {
            write_fname(stream, name, name_map)?;
        }

        // Write export path arrays
        if registry_version >= FAssetRegistryVersion::ClassPaths {
            for path in &self.nbl_export_paths {
                match &path.asset_class {
                    AssetClassPath::TopLevelAssetPath(top_level) => {
                        top_level.write_fname(stream, name_map)?;
                    }
                    AssetClassPath::LegacyClassName(_) => {
                        unreachable!();
                    }
                }
                write_fname(stream, &path.object_path, name_map)?;
                write_fname(stream, &path.package_path, name_map)?;
            }
            for path in &self.export_paths {
                match &path.asset_class {
                    AssetClassPath::TopLevelAssetPath(top_level) => {
                        top_level.write_fname(stream, name_map)?;
                    }
                    AssetClassPath::LegacyClassName(_) => {
                        unreachable!();
                    }
                }
                write_fname(stream, &path.object_path, name_map)?;
                write_fname(stream, &path.package_path, name_map)?;
            }
        } else {
            for path in &self.nbl_export_paths {
                match &path.asset_class {
                    AssetClassPath::LegacyClassName(class) => {
                        write_fname(stream, class, name_map)?;
                    }
                    AssetClassPath::TopLevelAssetPath(_) => {
                        unreachable!();
                    }
                }
                write_fname(stream, &path.object_path, name_map)?;
                write_fname(stream, &path.package_path, name_map)?;
            }
            for path in &self.export_paths {
                match &path.asset_class {
                    AssetClassPath::LegacyClassName(class) => {
                        write_fname(stream, class, name_map)?;
                    }
                    AssetClassPath::TopLevelAssetPath(_) => {
                        unreachable!();
                    }
                }
                write_fname(stream, &path.object_path, name_map)?;
                write_fname(stream, &path.package_path, name_map)?;
            }
        }

        // Write string offset tables
        let mut offset = 0u32;
        for s in &self.ansi_strings {
            stream.ser(&offset)?;
            offset += s.len() as u32 + 1;
        }

        let mut offset = 0u32;
        for s in &self.wide_strings {
            stream.ser(&offset)?;
            offset += s.chars().count() as u32 + 1;
        }

        // Write ANSI strings
        for s in &self.ansi_strings {
            stream.write_all(s.as_bytes())?;
            stream.write_u8(0)?;
        }

        // Write wide strings
        for s in &self.wide_strings {
            for c in s.chars() {
                stream.write_u16::<LE>(c as u16)?;
            }
            stream.write_u16::<LE>(0)?;
        }

        // Write pairs
        for pair in &self.pairs {
            let name_idx = name_map.get_index_of(&*pair.name).expect("name should already be in name map") as u32;
            stream.ser(&name_idx)?;
            stream.write_u32::<LE>((pair.type_ as u32) | (pair.index << 3))?;
        }

        stream.write_u32::<LE>(MAGIC_END)?;
        Ok(())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AssetRegistry {
    pub version: FGuid,
    pub registry_version: FAssetRegistryVersion,
    pub header: FAssetRegistryHeader,
    pub store: Store,
    pub asset_data: Vec<AssetData>,
    pub dependencies: Dependencies,
}

impl AssetRegistry {
    pub fn new(registry_version: FAssetRegistryVersion) -> Self {
        Self { registry_version, ..Default::default() }
    }

    pub fn add_asset(&mut self, asset_data: AssetData) {
        self.asset_data.push(asset_data);
    }

    pub fn deserialize<S: Read + Seek>(stream: &mut S) -> Result<Self> {
        let version: FGuid = stream.de()?;
        let registry_version: FAssetRegistryVersion = stream.de()?;

        let header = if registry_version >= FAssetRegistryVersion::AddedHeader { stream.de()? } else { FAssetRegistryHeader::default() };

        if registry_version < FAssetRegistryVersion::FixedTags {
            return Self::de_pre_fixed_tags(stream, version, registry_version, header);
        }

        let names = read_name_batch(stream)?;
        let store = Store::read_with_names(stream, &names, registry_version)?;

        let asset_count: u32 = stream.de()?;
        let asset_data = read_array(asset_count as usize, stream, |s| {
            let object_path = read_fname(s, &names)?;
            let package_path = read_fname(s, &names)?;
            let asset_class = read_fname(s, &names)?;
            let package_name = read_fname(s, &names)?;
            let asset_name = read_fname(s, &names)?;
            let tags: MapHandle = s.de()?;
            let bundle_count: u32 = s.de()?;

            // Read bundle entries
            let bundles = read_array(bundle_count as usize, s, |s| FAssetBundleEntry::read_fname(s, &names))?;

            let chunk_ids: Vec<u32> = s.de()?;
            let flags: u32 = s.de()?;

            Ok(AssetData {
                object_path,
                package_path,
                asset_class,
                package_name,
                asset_name,
                tags,
                legacy_tags: Vec::new(),
                bundles,
                chunk_ids,
                flags,
            })
        })?;

        let dependencies: Dependencies = stream.de()?;

        Ok(AssetRegistry {
            version,
            registry_version,
            header,
            store,
            asset_data,
            dependencies,
        })
    }

    pub fn serialize<S: Write + Seek>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.version)?;
        stream.ser(&self.registry_version)?;

        if self.registry_version >= FAssetRegistryVersion::AddedHeader {
            stream.ser(&self.header)?;
        }

        if self.registry_version < FAssetRegistryVersion::FixedTags {
            return Self::ser_pre_fixed_tags(self, stream);
        }

        let mut name_map = IndexSet::new();

        fn extract_base(name: &str) -> &str {
            break_down_name_string(name).0
        }

        for asset in &self.asset_data {
            name_map.insert(extract_base(&asset.object_path));
            name_map.insert(extract_base(&asset.package_path));
            name_map.insert(extract_base(&asset.asset_class));
            name_map.insert(extract_base(&asset.package_name));
            name_map.insert(extract_base(&asset.asset_name));

            // Collect bundle names
            for bundle in &asset.bundles {
                name_map.insert(extract_base(&bundle.bundle_name));
                for path in &bundle.asset_paths {
                    name_map.insert(extract_base(&path.package_name));
                    name_map.insert(extract_base(&path.asset_name));
                }
            }
        }

        for name in &self.store.nbl_names {
            name_map.insert(extract_base(name));
        }
        for name in &self.store.names {
            name_map.insert(extract_base(name));
        }
        // Collect export path names - version dependent
        if self.registry_version >= FAssetRegistryVersion::ClassPaths {
            for path in &self.store.nbl_export_paths {
                name_map.insert(extract_base(path.asset_class.package_name()));
                name_map.insert(extract_base(path.asset_class.asset_name()));
                name_map.insert(extract_base(&path.object_path));
                name_map.insert(extract_base(&path.package_path));
            }
            for path in &self.store.export_paths {
                name_map.insert(extract_base(path.asset_class.package_name()));
                name_map.insert(extract_base(path.asset_class.asset_name()));
                name_map.insert(extract_base(&path.object_path));
                name_map.insert(extract_base(&path.package_path));
            }
        } else {
            for path in &self.store.nbl_export_paths {
                match &path.asset_class {
                    AssetClassPath::LegacyClassName(class) => {
                        name_map.insert(extract_base(class));
                    }
                    AssetClassPath::TopLevelAssetPath(top_level) => {
                        name_map.insert(extract_base(&top_level.package_name));
                        name_map.insert(extract_base(&top_level.asset_name));
                    }
                }
                name_map.insert(extract_base(&path.object_path));
                name_map.insert(extract_base(&path.package_path));
            }
            for path in &self.store.export_paths {
                match &path.asset_class {
                    AssetClassPath::LegacyClassName(class) => {
                        name_map.insert(extract_base(class));
                    }
                    AssetClassPath::TopLevelAssetPath(top_level) => {
                        name_map.insert(extract_base(&top_level.package_name));
                        name_map.insert(extract_base(&top_level.asset_name));
                    }
                }
                name_map.insert(extract_base(&path.object_path));
                name_map.insert(extract_base(&path.package_path));
            }
        }
        for pair in &self.store.pairs {
            name_map.insert(&*pair.name);
        }

        let names: Vec<&str> = name_map.iter().cloned().collect();
        write_name_batch(stream, &names)?;

        self.store.write_with_name_map(stream, &name_map, self.registry_version)?;

        stream.ser(&(self.asset_data.len() as u32))?;
        for asset in &self.asset_data {
            write_fname(stream, &asset.object_path, &name_map)?;
            write_fname(stream, &asset.package_path, &name_map)?;
            write_fname(stream, &asset.asset_class, &name_map)?;
            write_fname(stream, &asset.package_name, &name_map)?;
            write_fname(stream, &asset.asset_name, &name_map)?;
            stream.ser(&asset.tags)?;
            stream.ser(&(asset.bundles.len() as u32))?;
            for bundle in &asset.bundles {
                bundle.write_fname(stream, &name_map)?;
            }
            stream.ser(&asset.chunk_ids)?;
            stream.ser(&asset.flags)?;
        }

        stream.ser(&self.dependencies)?;

        Ok(())
    }

    fn de_pre_fixed_tags<S: Read + Seek>(stream: &mut S, version: FGuid, registry_version: FAssetRegistryVersion, header: FAssetRegistryHeader) -> Result<Self> {
        use std::io::SeekFrom;

        let name_offset = stream.read_i64::<LE>()?;

        let data_start = stream.stream_position()?;

        let names = if name_offset > 0 && data_start > 0 {
            stream.seek(SeekFrom::Start(name_offset as u64))?;

            let name_count = stream.read_i32::<LE>()?;
            anyhow::ensure!(name_count >= 0, "Invalid name count: {}", name_count);

            let mut names = Vec::with_capacity(name_count as usize);
            for _ in 0..name_count {
                names.push(stream.de()?);
                let _hash_a = stream.read_u16::<LE>()?;
                let _hash_b = stream.read_u16::<LE>()?;
            }

            stream.seek(SeekFrom::Start(data_start))?;
            names
        } else {
            Vec::new()
        };

        let asset_count: u32 = stream.de()?;
        let asset_data = read_array(asset_count as usize, stream, |s| {
            Ok(AssetData {
                object_path: read_fname_pre_fixed_tags(s, &names)?,
                package_path: read_fname_pre_fixed_tags(s, &names)?,
                asset_class: read_fname_pre_fixed_tags(s, &names)?,
                package_name: read_fname_pre_fixed_tags(s, &names)?,
                asset_name: read_fname_pre_fixed_tags(s, &names)?,
                tags: MapHandle { has_numberless_keys: false, num: 0, pair_begin: 0 },
                legacy_tags: Self::read_simple_tags(s, &names)?,
                bundles: Vec::new(),
                chunk_ids: s.de()?,
                flags: s.de()?,
            })
        })?;

        Ok(AssetRegistry {
            version,
            registry_version,
            header,
            store: Store::default(),
            asset_data,
            dependencies: Dependencies::default(),
        })
    }

    fn read_simple_tags<S: Read + Seek>(stream: &mut S, names: &[String]) -> Result<Vec<(String, String)>> {
        let tag_count = stream.read_i32::<LE>()?;
        let mut tags = Vec::with_capacity(tag_count as usize);

        for _ in 0..tag_count {
            let key = read_fname_pre_fixed_tags(stream, names)?;
            let value: String = stream.de()?;
            tags.push((key, value));
        }

        Ok(tags)
    }

    fn ser_pre_fixed_tags<S: Write + Seek>(&self, stream: &mut S) -> Result<()> {
        use std::io::SeekFrom;

        let mut name_map = IndexSet::new();
        for asset in &self.asset_data {
            name_map.insert(break_down_name_string(&asset.object_path).0);
            name_map.insert(break_down_name_string(&asset.package_path).0);
            name_map.insert(break_down_name_string(&asset.asset_class).0);
            name_map.insert(break_down_name_string(&asset.package_name).0);
            name_map.insert(break_down_name_string(&asset.asset_name).0);
            // Include tag keys in name map
            for (key, _) in &asset.legacy_tags {
                name_map.insert(break_down_name_string(key).0);
            }
        }

        let name_offset_pos = stream.stream_position()?;
        stream.write_i64::<LE>(0)?; // Placeholder

        // Write asset data
        stream.ser(&(self.asset_data.len() as u32))?;
        for asset in &self.asset_data {
            Self::write_fname_pre_fixed_tags(stream, &asset.object_path, &name_map)?;
            Self::write_fname_pre_fixed_tags(stream, &asset.package_path, &name_map)?;
            Self::write_fname_pre_fixed_tags(stream, &asset.asset_class, &name_map)?;
            Self::write_fname_pre_fixed_tags(stream, &asset.package_name, &name_map)?;
            Self::write_fname_pre_fixed_tags(stream, &asset.asset_name, &name_map)?;

            // Write tags
            stream.write_i32::<LE>(asset.legacy_tags.len() as i32)?;
            for (key, value) in &asset.legacy_tags {
                Self::write_fname_pre_fixed_tags(stream, key, &name_map)?;
                stream.ser(value)?; // FString
            }

            // Write chunk_ids
            stream.ser(&asset.chunk_ids)?;

            // Write flags
            stream.ser(&asset.flags)?;
        }

        // TODO some kind of dependencies?
        stream.write_u64::<LE>(0)?;

        // Write name table at current position
        let name_table_offset = stream.stream_position()?;
        stream.write_i32::<LE>(name_map.len() as i32)?;

        for name in &name_map {
            stream.ser(&name.to_string())?;
            let (a, b) = generate_name_hash(name);
            stream.write_u16::<LE>(a)?;
            stream.write_u16::<LE>(b)?;
        }

        // Go back and write the actual name table offset
        let end_pos = stream.stream_position()?;
        stream.seek(SeekFrom::Start(name_offset_pos))?;
        stream.write_i64::<LE>(name_table_offset as i64)?;
        stream.seek(SeekFrom::Start(end_pos))?;

        Ok(())
    }

    fn write_fname_pre_fixed_tags<S: Write>(stream: &mut S, value: &str, name_map: &IndexSet<&str>) -> Result<()> {
        let (base_name, number) = break_down_name_string(value);
        let index = name_map.get_index_of(base_name).expect("name should already be in name map") as i32;

        stream.write_i32::<LE>(index)?;
        stream.write_i32::<LE>(number)?;
        Ok(())
    }
}

pub mod dbg {
    use super::*;
    use std::fmt::{Debug, Formatter, Result};

    #[derive(PartialEq, Eq)]
    pub struct Dbg<'reg, 'data, D> {
        reg: &'reg AssetRegistry,
        data: &'data D,
    }

    impl<'reg, 'data, D> Dbg<'reg, 'data, D> {
        pub fn new(reg: &'reg AssetRegistry, data: &'data D) -> Self {
            Self { reg, data }
        }
    }

    impl Debug for Dbg<'_, '_, AssetData> {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            f.debug_struct("AssetData")
                .field("object_path", &self.data.object_path)
                .field("package_path", &self.data.package_path)
                .field("asset_class", &self.data.asset_class)
                .field("package_name", &self.data.package_name)
                .field("asset_name", &self.data.asset_name)
                .field("tags", &Dbg::new(self.reg, &self.data.tags))
                .field("legacy_tags", &self.data.legacy_tags)
                .field("bundles", &self.data.bundles)
                .field("chunk_ids", &self.data.chunk_ids)
                .field("flags", &self.data.flags)
                .finish()
        }
    }

    impl Debug for Dbg<'_, '_, MapHandle> {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            let mut list = f.debug_list();
            let start = self.data.pair_begin as usize;
            let end = start + self.data.num as usize;
            for i in start..end {
                if let Some(pair) = self.reg.store.pairs.get(i) {
                    list.entry(&Dbg::new(self.reg, pair));
                }
            }
            list.finish()
        }
    }

    impl Debug for Dbg<'_, '_, Pair> {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            let mut dbg = f.debug_struct("Pair");
            dbg.field("name", &self.data.name);
            dbg.field("type", &self.data.type_);

            let store = &self.reg.store;
            let idx = self.data.index as usize;

            match self.data.type_ {
                TagType::AnsiString => {
                    if let Some(value) = store.ansi_strings.get(idx) {
                        dbg.field("value", value);
                    }
                }
                TagType::WideString => {
                    if let Some(value) = store.wide_strings.get(idx) {
                        dbg.field("value", value);
                    }
                }
                TagType::NumberlessName => {
                    if let Some(name) = store.nbl_names.get(idx) {
                        dbg.field("value", name);
                    }
                }
                TagType::Name => {
                    if let Some(name) = store.names.get(idx) {
                        dbg.field("value", name);
                    }
                }
                TagType::NumberlessExportPath => {
                    if let Some(export_path) = store.nbl_export_paths.get(idx) {
                        dbg.field("value", export_path);
                    }
                }
                TagType::ExportPath => {
                    if let Some(export_path) = store.export_paths.get(idx) {
                        dbg.field("value", export_path);
                    }
                }
                TagType::LocalizedText => {
                    if let Some(text) = store.texts.get(idx) {
                        dbg.field("value", text);
                    }
                }
            }

            dbg.finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_export_path_roundtrip() {
        let mut registry = AssetRegistry::new(FAssetRegistryVersion::ClassPaths);
        registry.store.nbl_export_paths.push(ExportPath {
            object_path: "/Game/Test".to_string(),
            package_path: "/Game".to_string(),
            asset_class: AssetClassPath::TopLevelAssetPath(FTopLevelAssetPath {
                package_name: "/Script/Engine".to_string(),
                asset_name: "Blueprint".to_string(),
            }),
        });

        let mut buf = std::io::Cursor::new(Vec::new());
        registry.serialize(&mut buf).unwrap();

        buf.set_position(0);
        let decoded = AssetRegistry::deserialize(&mut buf).unwrap();

        assert_eq!(registry, decoded);
    }

    #[test]
    fn test_asset_data_roundtrip() {
        let mut registry = AssetRegistry::new(FAssetRegistryVersion::FixedTags);
        registry.asset_data.push(AssetData {
            object_path: "/Game/MyAsset.MyAsset".to_string(),
            package_path: "/Game".to_string(),
            asset_class: "Blueprint".to_string(),
            package_name: "/Game/MyAsset".to_string(),
            asset_name: "MyAsset".to_string(),
            tags: MapHandle { has_numberless_keys: true, num: 0, pair_begin: 0 },
            legacy_tags: Vec::new(),
            bundles: Vec::new(),
            chunk_ids: vec![1, 2, 3],
            flags: 0x42,
        });

        let mut buf = std::io::Cursor::new(Vec::new());
        registry.serialize(&mut buf).unwrap();

        buf.set_position(0);
        let decoded = AssetRegistry::deserialize(&mut buf).unwrap();

        assert_eq!(registry, decoded);
    }

    #[test]
    fn test_fname_with_number() {
        let mut registry = AssetRegistry::new(FAssetRegistryVersion::FixedTags);
        registry.asset_data.push(AssetData {
            object_path: "/Game/Test_0".to_string(),
            package_path: "/Game".to_string(),
            asset_class: "StaticMesh_5".to_string(),
            package_name: "/Game/Test".to_string(),
            asset_name: "Test_0".to_string(),
            tags: MapHandle { has_numberless_keys: true, num: 0, pair_begin: 0 },
            legacy_tags: Vec::new(),
            bundles: Vec::new(),
            chunk_ids: vec![],
            flags: 0,
        });

        let mut buf = std::io::Cursor::new(Vec::new());
        registry.serialize(&mut buf).unwrap();

        buf.set_position(0);
        let decoded = AssetRegistry::deserialize(&mut buf).unwrap();

        assert_eq!(registry.asset_data[0].object_path, decoded.asset_data[0].object_path);
        assert_eq!(registry.asset_data[0].asset_class, decoded.asset_data[0].asset_class);
        assert_eq!(registry.asset_data[0].asset_name, decoded.asset_data[0].asset_name);
    }

    fn test_asset_registry_roundtrip(path: &str) {
        let input = std::fs::read(path).unwrap();

        let registry = AssetRegistry::deserialize(&mut Cursor::new(&input)).unwrap();

        assert!(!registry.asset_data.is_empty(), "Asset data should not be empty");

        let mut output = vec![];
        registry.serialize(&mut Cursor::new(&mut output)).unwrap();

        let registry2 = AssetRegistry::deserialize(&mut Cursor::new(&output)).unwrap();

        assert_eq!(registry.version, registry2.version);
        assert_eq!(registry.registry_version, registry2.registry_version);
        assert_eq!(registry.asset_data.len(), registry2.asset_data.len());

        for (a, b) in registry.asset_data.iter().zip(registry2.asset_data.iter()) {
            assert_eq!(a, b);
        }

        pretty_assertions::assert_eq!(registry, registry2);

        // std::fs::write("input.bin", &input).unwrap();
        // std::fs::write("output.bin", &output).unwrap();

        assert_eq!(&input, &output);
    }

    #[test]
    fn test_parse_ue422_asset_registry() {
        test_asset_registry_roundtrip("tests/UE4.22/AssetRegistry.bin");
    }

    #[test]
    fn test_parse_ue427_asset_registry() {
        test_asset_registry_roundtrip("tests/UE4.27/AssetRegistry.bin");
    }

    #[test]
    fn test_parse_ue55_asset_registry() {
        test_asset_registry_roundtrip("tests/UE5.5/AssetRegistry.bin");
    }

    #[test]
    fn test_parse_ue56_asset_registry() {
        test_asset_registry_roundtrip("tests/UE5.6/AssetRegistry.bin");
    }

    #[test]
    fn test_asset_registry_field_access() {
        let data = std::fs::read("tests/UE4.27/AssetRegistry.bin").unwrap();
        let mut cursor = std::io::Cursor::new(&data);
        let registry = AssetRegistry::deserialize(&mut cursor).unwrap();

        // Test that we can access string fields directly
        for asset in &registry.asset_data {
            assert!(!asset.object_path.is_empty());
            assert!(!asset.package_path.is_empty());
            assert!(!asset.asset_class.is_empty());
        }
    }
}
