use crate::zen::{EUnrealEngineObjectUE4Version, EUnrealEngineObjectUE5Version, FCustomVersion, FPackageFileVersion, FPackageIndex, FZenPackageVersioningInfo};
use crate::{iostore::IoStoreTrait, ser::*, zen::FZenPackageHeader, EIoChunkType, FGuid, FIoChunkId, FPackageId};
use anyhow::{anyhow, bail, Result};
use byteorder::{WriteBytesExt as _, LE};
use std::borrow::Cow;
use std::io::{Read, Seek, SeekFrom, Write};
use std::{io::Cursor, path::Path};
use std::collections::HashMap;
use std::rc::Rc;
use std::collections::HashSet;
use tracing::instrument;
use crate::script_objects::{FPackageObjectIndex, FPackageObjectIndexType};

#[derive(Debug, Copy, Clone, Default)]
struct FMinimalName {
    index: u32,
    number: i32,
}
impl Readable for FMinimalName {
    #[instrument(skip_all, name = "FMinimalName")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            index: s.de()?,
            number: s.de()?,
        })
    }
}
impl Writeable for FMinimalName
{
    #[instrument(skip_all, name = "FMinimalName")]
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(self.index)?;
        stream.ser(self.number)?;
        Ok({})
    }
}

#[derive(Debug, Copy, Clone, Default)]
struct FCountOffsetPair {
    count: i32,
    offset: i32,
}
impl Readable for FCountOffsetPair {
    #[instrument(skip_all, name = "FCountOffsetPair")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            count: s.de()?,
            offset: s.de()?
        })
    }
}
impl Writeable for FCountOffsetPair {
    #[instrument(skip_all, name = "FCountOffsetPair")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(self.count)?;
        s.ser(self.offset)?;
        Ok({})
    }
}

#[derive(Debug, Default)]
struct FGenerationInfo {
    export_count: i32,
    name_count: i32,
}
impl Readable for FGenerationInfo {
    #[instrument(skip_all, name = "FGenerationInfo")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            export_count: s.de()?,
            name_count: s.de()?
        })
    }
}
impl Writeable for FGenerationInfo {
    #[instrument(skip_all, name = "FGenerationInfo")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(self.export_count)?;
        s.ser(self.name_count)?;
        Ok({})
    }
}

#[derive(Debug, Clone, Default)]
struct FEngineVersion {
    engine_major: u16,
    engine_minor: u16,
    engine_patch: u16,
    changelist: u32,
    branch: String,
}
impl Readable for FEngineVersion {
    #[instrument(skip_all, name = "FEngineVersion")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            engine_major: s.de()?,
            engine_minor: s.de()?,
            engine_patch: s.de()?,
            changelist: s.de()?,
            branch: s.de()?
        })
    }
}
impl Writeable for FEngineVersion {
    #[instrument(skip_all, name = "FEngineVersion")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(self.engine_major)?;
        s.ser(self.engine_minor)?;
        s.ser(self.engine_patch)?;
        s.ser(self.changelist)?;
        s.ser(self.branch.clone())?;
        Ok({})
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FLegacyPackageVersioningInfo
{
    legacy_file_version: i32,
    package_file_version: FPackageFileVersion,
    licensee_version: i32,
    custom_versions: Vec<FCustomVersion>,
    is_unversioned: bool,
}
impl FLegacyPackageVersioningInfo {
    pub(crate) const LEGACY_FILE_VERSION_UE5: i32 = -8;
    pub(crate) const LEGACY_FILE_VERSION_UE4: i32 = -7;
    pub(crate) const VER_UE4_LATEST: i32 = 522;
}
impl Readable for FLegacyPackageVersioningInfo {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        // We can only read latest UE4 packages (4.26+) and UE5 packages, bail out if the package is too old
        let legacy_file_version: i32 = s.de()?;
        if legacy_file_version != FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE4 && legacy_file_version != FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE5 {
            bail!("Package file version too old: {} (Supported versions are {} for UE4 and {} for UE5)", legacy_file_version, FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE4, FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE5);
        }

        // There should never be a UE3 version written here
        let legacy_ue3_version: i32 = s.de()?;
        if legacy_ue3_version != 0 {
            bail!("Expected to find zero UE3 version, got {}", legacy_ue3_version);
        }

        // Read raw file version for UE4 and UE5 (if package is UE5)
        let raw_file_version_ue4: i32 = s.de()?;
        let raw_file_version_ue5: i32 = if legacy_file_version == FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE5 { s.de()? } else { 0 };
        let package_file_version = FPackageFileVersion{file_version_ue4: raw_file_version_ue4, file_version_ue5: raw_file_version_ue5};

        let licensee_version: i32 = s.de()?;
        let custom_versions: Vec<FCustomVersion> = s.de()?;
        let is_unversioned = raw_file_version_ue4 == 0 && raw_file_version_ue5 == 0 && licensee_version == 0 && custom_versions.is_empty();

        Ok(Self{legacy_file_version, package_file_version, licensee_version, custom_versions, is_unversioned})
    }
}
impl Writeable for FLegacyPackageVersioningInfo {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {

        // We need to at the very least have UE4 file version to write legacy package versioning info
        if self.package_file_version.file_version_ue4 == 0 {
            bail!("Cannot serialize package versioning info without UE4 file version");
        }

        // Derive legacy file version from the presence of UE5 file version
        let legacy_file_version: i32 = if self.package_file_version.file_version_ue5 != 0 { FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE5 } else { FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE4 };
        s.ser(legacy_file_version)?;

        // There should never be a UE3 version written
        let legacy_ue3_version: i32 = 0;
        s.ser(legacy_ue3_version)?;

        // Write raw file version for UE4 and UE5 (if package is UE5)
        // Note that we should not write any versions if this package was loaded as unversioned, since our own version is only used internally and the game should still assume latest
        let raw_file_version_ue4: i32 = if self.is_unversioned { 0 } else { self.package_file_version.file_version_ue4 };
        s.ser(raw_file_version_ue4)?;
        if legacy_file_version == FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE5 {
            let raw_file_version_ue5: i32 = if self.is_unversioned { 0 } else { self.package_file_version.file_version_ue5 };
            s.ser(raw_file_version_ue5)?;
        }

        let licensee_version = if self.is_unversioned { 0 } else { self.licensee_version };
        s.ser(licensee_version)?;
        s.ser(self.custom_versions.clone())?;
        Ok({})
    }
}

#[derive(Debug, PartialEq)]
#[repr(u32)]
pub(crate) enum EPackageFlags {
    Cooked = 0x00000200,
    FilterEditorOnly = 0x80000000,
    UsesUnversionedProperties = 0x00002000,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FPackageFileSummary
{
    versioning_info: FLegacyPackageVersioningInfo,
    total_header_size: i32,
    package_name: String,
    package_flags: u32,
    names: FCountOffsetPair,
    // never written for cooked packages
    soft_object_paths: FCountOffsetPair,
    exports: FCountOffsetPair,
    imports: FCountOffsetPair,
    // empty placeholder for cooked packages
    depends_offset: i32,
    package_guid: FGuid,
    package_source: u32,
    world_tile_info_data_offset: i32,
    chunk_ids: Vec<i32>,
    preload_dependencies: FCountOffsetPair,
    names_referenced_from_export_data_count: i32,
    data_resource_offset: i32,
    // empty placeholder for cooked packages
    asset_registry_data_offset: i32,
    // meaningless for cooked packages
    bulk_data_start_offset: i32,
}
impl FPackageFileSummary {
    const PACKAGE_FILE_TAG: u32 = 0x9E2A83C1;
    pub(crate) fn has_package_flags(&self, package_flags: EPackageFlags) -> bool { (self.package_flags & package_flags as u32) != 0 }
    pub(crate) fn is_filter_editor_only(&self) -> bool { self.has_package_flags(EPackageFlags::FilterEditorOnly) }
    pub(crate) fn uses_unversioned_property_serialization(&self) -> bool { self.has_package_flags(EPackageFlags::UsesUnversionedProperties) }
}
impl FPackageFileSummary {
    #[instrument(skip_all, name = "FPackageFileSummary")]
    fn deserialize<S: Read>(s: &mut S, package_version_fallback: Option<FPackageFileVersion>) -> Result<Self> {

        // Check asset magic first
        let asset_magic_tag: u32 = s.de()?;
        if asset_magic_tag != FPackageFileSummary::PACKAGE_FILE_TAG {
            bail!("Package file magic mismatch: {} (expected {})", asset_magic_tag, FPackageFileSummary::PACKAGE_FILE_TAG);
        }

        let mut versioning_info: FLegacyPackageVersioningInfo = s.de()?;
        // We need a valid package file version to deserialize this package, so we rely on having a fallback if the package is unversioned
        if versioning_info.is_unversioned {
            if !package_version_fallback.is_some() {
                bail!("Cannot deserialize an unversioned package without a fallback package file version");
            }
            versioning_info.package_file_version = package_version_fallback.unwrap();
        }
        // Make sure we are not attempting to read versions before UE4 NonOuterPackageImport. Our export/import serialization does not support such old versions
        if versioning_info.package_file_version.file_version_ue4 < EUnrealEngineObjectUE4Version::NonOuterPackageImport as i32 {
            bail!("Encountered UE4 package file version {}, which is below minimum supported version {}", versioning_info.package_file_version.file_version_ue4, EUnrealEngineObjectUE4Version::NonOuterPackageImport as i32);
        }

        let total_header_size: i32 = s.de()?;
        let package_name: String = s.de()?;
        let package_flags: u32 = s.de()?;

        let is_filter_editor_only = (package_flags & EPackageFlags::FilterEditorOnly as u32) != 0;

        let names: FCountOffsetPair = s.de()?;
        let soft_object_paths: FCountOffsetPair = if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::AddSoftObjectPathList as i32 { s.de()? } else { FCountOffsetPair::default() };

        // Not written when editor only data is filtered out
        let _localization_id: String = if !is_filter_editor_only { s.de()? } else { "".to_string() };
        // Not written when cooking or filtering editor only data
        let _gatherable_text_data: FCountOffsetPair = s.de()?;

        let exports: FCountOffsetPair = s.de()?;
        let imports: FCountOffsetPair = s.de()?;
        // Serialized for cooked packages, but is always an empty array for each export. We need it to calculate the size of the exports though
        let depends_offset: i32 = s.de()?;

        // Cooked packages never have soft package references or searchable names
        let _soft_package_references: FCountOffsetPair = s.de()?;
        let _searchable_names_offset: i32 = s.de()?;
        // Cooked packages do not have thumbnails ever, no point in saving this
        let _thumbnail_table_offset: i32 = s.de()?;

        let package_guid : FGuid = s.de()?;

        // Package generations are always 0,0 for modern packages, persistent package GUID is never written for cooked packages
        let _persistent_package_guid: FGuid = if !is_filter_editor_only { s.de()? } else { FGuid::default() };
        let _package_generations: Vec<FGenerationInfo> = s.de()?;

        // These are always empty for cooked packages, so no point in saving them
        let _saved_by_engine_version: FEngineVersion = s.de()?; // saved_by_engine_version
        let _compatible_with_engine_version: FEngineVersion = s.de()?; // compatible_with_engine_version

        // Unused, always 0 for modern packages
        let compression_flags: u32 = s.de()?;
        if compression_flags != 0 {
           bail!("Expected 0 legacy compression flags when reading a package, got {}", compression_flags);
        }
        // This is not supported by the UE itself, so no point in trying to read full TArray<FCompressedChunk>
        // FCompressedChunk definition for reference: uncompressed_offset: i32, uncompressed_size: i32, compressed_offset: i32, compressed_size: i32
        let num_compressed_chunks: i32 = s.de()?;
        if num_compressed_chunks != 0 {
            bail!("Per-chunk package file compression is not supported by modern UE versions");
        }

        // UE CRC32 hash of the normalized package filename, not used by the engine, but we should preserve it
        let package_source: u32 = s.de()?;

        // No longer used, always empty
        let _additional_packages_to_cook: Vec<String> = s.de()?;
        // Serialized for packages with filtered editor only data as 1 integer (0x0), not read in runtime
        let asset_registry_data_offset: i32 = s.de()?;
        // Written as an offset, but is never read for cooked packages
        let bulk_data_start_offset: i32 = s.de()?;

        // Legacy world composition data, but can very much be written on UE4 games
        let world_tile_info_data_offset: i32 = s.de()?;

        let chunk_ids: Vec<i32> = s.de()?;
        let preload_dependencies: FCountOffsetPair = s.de()?;

        // Assume all names are referenced if this is an old package
        let names_referenced_from_export_data_count: i32 = if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::NamesReferencedFromExportData as i32 { s.de()? } else { names.count };

        // Package trailers should never be written for cooked packages, they are only used for saving EditorBulkData in editor domain with package virtualization
        let _payload_toc_offset: i32 = if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::PayloadTOC as i32 { s.de()? } else { -1 };

        // Data resource offset is only written with new bulk data save format, otherwise bulk data meta is simply saved inline
        let data_resource_offset: i32 = if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32 { s.de()? } else { -1 };

        Ok(FPackageFileSummary{
            versioning_info,
            total_header_size,
            package_name,
            package_flags,
            names,
            soft_object_paths,
            exports,
            imports,
            depends_offset,
            package_guid,
            package_source,
            world_tile_info_data_offset,
            chunk_ids,
            preload_dependencies,
            names_referenced_from_export_data_count,
            data_resource_offset,
            asset_registry_data_offset,
            bulk_data_start_offset,
        })
    }

    // Deserializes all the information that can be safely deserialized without knowing the package version
    #[instrument(skip_all, name = "FPackageFileSummary - Minimal")]
    fn deserialize_summary_minimal_version_independent<S: Read>(s: &mut S) -> Result<(FLegacyPackageVersioningInfo, FCountOffsetPair, String, i32, u32)> {

        // Check asset magic first
        let asset_magic_tag: u32 = s.de()?;
        if asset_magic_tag != FPackageFileSummary::PACKAGE_FILE_TAG {
            bail!("Package file magic mismatch: {} (expected {})", asset_magic_tag, FPackageFileSummary::PACKAGE_FILE_TAG);
        }

        let versioning_info: FLegacyPackageVersioningInfo = s.de()?;
        let total_header_size: i32 = s.de()?;
        let package_name: String = s.de()?;
        let package_flags: u32 = s.de()?;

        let names: FCountOffsetPair = s.de()?;
        Ok((versioning_info, names, package_name, total_header_size, package_flags))
    }

    #[instrument(skip_all, name = "FPackageFileSummary")]
    fn serialize<S: Write>(&self, s: &mut S) -> Result<()> {

        let asset_magic_tag: u32 = FPackageFileSummary::PACKAGE_FILE_TAG;
        s.ser(asset_magic_tag)?;

        // Make sure we are not attempting to write versions before UE4 NonOuterPackageImport. Our export/import serialization does not support such old versions
        if self.versioning_info.package_file_version.file_version_ue4 < EUnrealEngineObjectUE4Version::NonOuterPackageImport as i32 {
            bail!("Attempt to write UE4 package file version {}, which is below minimum supported version {}", self.versioning_info.package_file_version.file_version_ue4, EUnrealEngineObjectUE4Version::NonOuterPackageImport as i32);
        }

        s.ser(self.versioning_info.clone())?;
        s.ser(self.total_header_size)?;
        s.ser(self.package_name.clone())?;
        s.ser(self.package_flags)?;

        s.ser(self.names)?;
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::AddSoftObjectPathList as i32 {
            s.ser(self.soft_object_paths)?;
        }

        // Not written when editor only data is filtered out
        if !self.is_filter_editor_only() {
            let localization_id: String = "".to_string();
            s.ser(localization_id)?;
        }
        // Not written when cooking or filtering editor only data
        let gatherable_text_data = FCountOffsetPair{count: 0, offset: 0};
        s.ser(gatherable_text_data)?;

        s.ser(self.exports)?;
        s.ser(self.imports)?;

        // Serialized for cooked packages, but is always an empty array for each export
        // This is an actual offset somewhere in the package header, but it is not used by anything and the game can handle 0 there
        // So we will write 0 here, but if we wanted to preserve binary equality, we would track this offset when writing the asset header
        let depends_offset: i32 = 0;
        s.ser(depends_offset)?;

        // Cooked packages never have soft package references or searchable names
        let soft_package_references = FCountOffsetPair{count: 0, offset: 0};
        s.ser(soft_package_references)?;
        let searchable_names_offset: i32 = 0;
        s.ser(searchable_names_offset)?;
        // Cooked packages do not have thumbnails
        let thumbnails_table_offset: i32 = 0;
        s.ser(thumbnails_table_offset)?;

        s.ser(self.package_guid)?;

        // Package generations are always saved as one (0,0) entry for modern packages
        // Note that the FLinkerLoad expects there to still be a single generation, it will crash if there is none
        let package_generations: Vec<FGenerationInfo> = vec![FGenerationInfo{export_count: 0, name_count: 0}];
        s.ser(package_generations)?;
        // Persistent package GUID is never written for cooked packages
        if !self.is_filter_editor_only() {
            let persistent_package_guid = FGuid{a: 0, b: 0, c: 0, d: 0};
            s.ser(persistent_package_guid)?;
        }

        // Saved and compatible engine versions are always empty for cooked packages
        let saved_by_engine_version = FEngineVersion{engine_major: 0, engine_minor: 0, engine_patch: 0, changelist: 0, branch: "".to_string()};
        let compatible_with_engine_version = FEngineVersion{engine_major: 0, engine_minor: 0, engine_patch: 0, changelist: 0, branch: "".to_string()};
        s.ser(saved_by_engine_version)?;
        s.ser(compatible_with_engine_version)?;

        // Unused, always 0 for modern packages
        let compression_flags: u32 = 0;
        s.ser(compression_flags)?;
        // Unused, always empty array for modern UE packages, UE will refuse to load packages where this is not an empty array
        let num_compressed_chunks: i32 = 0;
        s.ser(num_compressed_chunks)?;

        s.ser(self.package_source)?;

        // No longer used, always empty
        let additional_packages_to_cook: Vec<String> = vec![];
        s.ser(additional_packages_to_cook)?;

        // Serialized for packages with filtered editor only data as 1 integer (0x0), not read in runtime
        s.ser(self.asset_registry_data_offset)?;
        // Written as an offset, but is never read for cooked packages as the data is never written in the header file
        s.ser(self.bulk_data_start_offset)?;
        // Legacy world composition data, but can very much be written on UE4 games
        s.ser(self.world_tile_info_data_offset)?;

        s.ser(self.chunk_ids.clone())?;
        s.ser(self.preload_dependencies)?;

        // Only write number of referenced names if this is a UE5 package
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::NamesReferencedFromExportData as i32 { s.ser(self.names_referenced_from_export_data_count)?; }

        // Package trailers should never be written for cooked packages, they are only used for saving EditorBulkData in editor domain with package virtualization
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::PayloadTOC as i32 {
            let payload_toc_offset: i64 = -1;
            s.ser(payload_toc_offset)?;
        }

        // Data resource offset is only written with new bulk data save format, otherwise bulk data meta is simply saved inline
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32 { s.ser(self.data_resource_offset)?; }

        Ok({})
    }
}

#[derive(Debug, Clone, Default)]
struct FPackageNameMap {
    names: Vec<String>
}
impl FPackageNameMap {
    #[instrument(skip_all, name = "FPackageNameMap")]
    fn read<S: Read + Seek>(stream: &mut S, summary: &FPackageFileSummary) -> Result<FPackageNameMap> {

        stream.seek(SeekFrom::Start(summary.names.offset as u64))?;

        let mut names: Vec<String> = Vec::with_capacity(summary.names.count as usize);
        for _ in 0..summary.names.count {

            let name_string: String = stream.de()?;
            let _non_case_preserving_hash: u16 = stream.de()?;
            let _case_preserving_hash: u16 = stream.de()?;
            names.push(name_string);
        }
        Ok(Self{names})
    }

    #[instrument(skip_all, name = "FPackageNameMap")]
    fn write<S: Write + Seek>(&self, stream: &mut S, summary: &mut FPackageFileSummary, package_summary_offset: u64) -> Result<()> {

        // Tell the summary where the names start and how many there are
        summary.names.offset = (stream.stream_position()? - package_summary_offset) as i32;
        summary.names.count = self.names.len() as i32;

        for i in 0..self.names.len() {

            // Write the name string
            stream.ser(self.names[i].clone())?;

            // Write 0 for case preserving and non-case preserving hashes. They are not used by the game
            let non_case_preserving_hash: u16 = 0;
            let case_preserving_hash: u16 = 0;
            stream.ser(non_case_preserving_hash)?;
            stream.ser(case_preserving_hash)?;
        }
        Ok({})
    }

    fn get(&self, name: FMinimalName) -> Cow<'_, str> {
        let bare_name = &self.names[name.index as usize];
        if name.number != 0 {
            format!("{bare_name}_{}", name.number - 1).into()
        } else {
            bare_name.into()
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FObjectImport {
    class_package: FMinimalName,
    class_name: FMinimalName,
    outer_index: FPackageIndex,
    object_name: FMinimalName,
    is_optional: bool,
}
impl FObjectImport
{
    #[instrument(skip_all, name = "FObjectImport")]
    fn deserialize<S: Read>(s: &mut S, summary: &FPackageFileSummary) -> Result<Self> {

        let class_package: FMinimalName = s.de()?;
        let class_name: FMinimalName = s.de()?;
        let outer_index: FPackageIndex = s.de()?;
        let object_name: FMinimalName = s.de()?;

        // Used to support imports that live in their own packages for One File Per Actor in UE5
        // Such imports cannot exist in cooked data, and as such, we should never encounter them
        if !summary.is_filter_editor_only() {
            let _package_name: FMinimalName = s.de()?;
        }

        let should_serialize_optional = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::OptionalResources as i32;
        let is_optional: bool = if should_serialize_optional { s.de()? } else { false };

        Ok(FObjectImport{class_package, class_name, outer_index, object_name, is_optional})
    }

    #[instrument(skip_all, name = "FObjectImport")]
    fn serialize<S: Write>(&self, s: &mut S, summary: &FPackageFileSummary) -> Result<()> {

        s.ser(self.class_package)?;
        s.ser(self.class_name)?;
        s.ser(self.outer_index)?;
        s.ser(self.object_name)?;

        // We should never be serializing uncooked packages, might be worth to assert here instead of writing an empty name
        if !summary.is_filter_editor_only() {
            let package_name: FMinimalName = FMinimalName::default();
            s.ser(package_name)?;
        }

        let should_serialize_optional = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::OptionalResources as i32;
        if should_serialize_optional {
            s.ser(self.is_optional)?;
        }
        Ok({})
    }
}

#[derive(Debug, Clone, Default)]
struct FObjectExport {
    class_index: FPackageIndex,
    super_index: FPackageIndex,
    template_index: FPackageIndex,
    outer_index: FPackageIndex,
    object_name: FMinimalName,
    object_flags: u32,
    serial_size: i64,
    serial_offset: i64,
    is_not_for_client: bool,
    is_not_for_server: bool,
    is_inherited_instance: bool,
    // if false, the object must be kept for editor builds running client/server builds even if it is to be stripped based on not for server/client
    is_not_always_loaded_for_editor_game: bool,
    is_asset: bool,
    generate_public_hash: bool,
    first_export_dependency_index: i32,
    serialize_before_serialize_dependencies: i32,
    create_before_serialize_dependencies: i32,
    serialize_before_create_dependencies: i32,
    create_before_create_dependencies: i32,
    script_serialization_start_offset: i64,
    script_serialization_end_offset: i64,
}
impl FObjectExport
{
    #[instrument(skip_all, name = "FObjectExport")]
    fn deserialize<S: Read>(s: &mut S, summary: &FPackageFileSummary) -> Result<Self> {

        let class_index: FPackageIndex = s.de()?;
        let super_index: FPackageIndex = s.de()?;
        let template_index: FPackageIndex = s.de()?;
        let outer_index: FPackageIndex = s.de()?;
        let object_name: FMinimalName = s.de()?;
        let object_flags: u32 = s.de()?;
        let serial_size: i64 = s.de()?;
        let serial_offset: i64 = s.de()?;

        // Forced exports as a concept do not exist in modern engine versions, this property is always false
        let _is_forced_export: bool = s.de()?;

        let is_not_for_client: bool = s.de()?;
        let is_not_for_server: bool = s.de()?;

        // Package GUID serialization of exports has been removed in UE5
        let should_serialize_package_guid = summary.versioning_info.package_file_version.file_version_ue5 < EUnrealEngineObjectUE5Version::RemoveObjectExportPackageGUID as i32;
        if should_serialize_package_guid {
            let _package_guid: FGuid = s.de()?;
        }

        // Added in UE5. Default to false for old assets
        let should_serialize_inherited_instance = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::TrackObjectExportIsInherited as i32;
        let is_inherited_instance: bool = if should_serialize_inherited_instance { s.de()? } else { false };

        // Package flags are only relevant for forced exports, which do not exist as a concept anymore, so this value is always 0
        let _package_flags: u32 = s.de()?;

        let is_not_always_loaded_for_editor_game: bool = s.de()?;
        let is_asset: bool = s.de()?;

        // Assume public hash to be generated for assets before UE5
        let should_serialize_generate_public_hash = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::OptionalResources as i32;
        let generate_public_hash: bool = if should_serialize_generate_public_hash { s.de()? } else { true };

        let first_export_dependency_index: i32 = s.de()?;
        let serialize_before_serialize_dependencies: i32 = s.de()?;
        let create_before_serialize_dependencies: i32 = s.de()?;
        let serialize_before_create_dependencies: i32 = s.de()?;
        let create_before_create_dependencies: i32 = s.de()?;

        let should_serialize_script_props = !summary.uses_unversioned_property_serialization() && summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::ScriptSerializationOffset as i32;
        let script_serialization_start_offset: i64 = if should_serialize_script_props { s.de()? } else { 0 };
        let script_serialization_end_offset: i64 = if should_serialize_script_props { s.de()? } else { 0 };

        Ok(FObjectExport{
            class_index, super_index, template_index, outer_index, object_name, object_flags, serial_size, serial_offset,
            is_not_for_client, is_not_for_server, is_inherited_instance, is_not_always_loaded_for_editor_game,
            is_asset, generate_public_hash, first_export_dependency_index, serialize_before_serialize_dependencies,
            create_before_serialize_dependencies, serialize_before_create_dependencies, create_before_create_dependencies,
            script_serialization_start_offset, script_serialization_end_offset
        })
    }

    #[instrument(skip_all, name = "FObjectExport")]
    fn serialize<S: Write>(&self, s: &mut S, summary: &FPackageFileSummary) -> Result<()> {

        s.ser(self.class_index)?;
        s.ser(self.super_index)?;
        s.ser(self.template_index)?;
        s.ser(self.outer_index)?;
        s.ser(self.object_name)?;
        s.ser(self.object_flags)?;
        s.ser(self.serial_size)?;
        s.ser(self.serial_offset)?;

        // Forced exports as a concept do not exist in modern engine versions, this property is always false
        let is_forced_export: bool = false;
        s.ser(is_forced_export)?;

        s.ser(self.is_not_for_client)?;
        s.ser(self.is_not_for_server)?;

        // Package GUID serialization of exports has been removed in UE5. Before then, we serialize an empty GUID
        let should_serialize_package_guid = summary.versioning_info.package_file_version.file_version_ue5 < EUnrealEngineObjectUE5Version::RemoveObjectExportPackageGUID as i32;
        if should_serialize_package_guid {
            let package_guid: FGuid = FGuid{a: 0, b: 0, c: 0, d: 0};
            s.ser(package_guid)?;
        }

        // Added in UE5. Default to false for old assets
        let should_serialize_inherited_instance = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::TrackObjectExportIsInherited as i32;
        if should_serialize_inherited_instance {
            s.ser(self.is_inherited_instance)?;
        }

        // Package flags are only relevant for forced exports, which do not exist as a concept anymore, so this value is always 0
        let package_flags: u32 = 0;
        s.ser(package_flags)?;

        s.ser(self.is_not_always_loaded_for_editor_game)?;
        s.ser(self.is_asset)?;

        // Assume public hash to be generated for assets before UE5
        let should_serialize_generate_public_hash = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::OptionalResources as i32;
        if should_serialize_generate_public_hash {
            s.ser(self.generate_public_hash)?;
        }

        s.ser(self.first_export_dependency_index)?;
        s.ser(self.serialize_before_serialize_dependencies)?;
        s.ser(self.create_before_serialize_dependencies)?;
        s.ser(self.serialize_before_create_dependencies)?;
        s.ser(self.create_before_create_dependencies)?;

        let should_serialize_script_props = !summary.uses_unversioned_property_serialization() && summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::ScriptSerializationOffset as i32;
        if should_serialize_script_props {
            s.ser(self.script_serialization_start_offset)?;
            s.ser(self.script_serialization_end_offset)?;
        }
        Ok({})
    }
}

#[derive(Debug, Copy, Clone, Default)]
struct FObjectDataResource {
    flags: u32,
    serial_offset: i64,
    duplicate_serial_offset: i64,
    serial_size: i64,
    raw_size: i64,
    outer_index: FPackageIndex,
    legacy_bulk_data_flags: u32,
}
impl Readable for FObjectDataResource {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let flags: u32 = s.de()?;
        let serial_offset: i64 = s.de()?;
        let duplicate_serial_offset: i64 = s.de()?;
        let serial_size: i64 = s.de()?;
        let raw_size: i64 = s.de()?;
        let outer_index: FPackageIndex = s.de()?;
        let legacy_bulk_data_flags: u32 = s.de()?;

        Ok(FObjectDataResource{flags, serial_offset, duplicate_serial_offset, serial_size, raw_size, outer_index, legacy_bulk_data_flags})
    }
}
impl Writeable for FObjectDataResource {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(self.flags)?;
        s.ser(self.serial_offset)?;
        s.ser(self.duplicate_serial_offset)?;
        s.ser(self.serial_size)?;
        s.ser(self.raw_size)?;
        s.ser(self.outer_index)?;
        s.ser(self.legacy_bulk_data_flags)?;
        Ok({})
    }
}

#[derive(Debug, Clone, Default)]
struct FLegacyPackageHeader {
    summary: FPackageFileSummary,
    name_map: FPackageNameMap,
    imports: Vec<FObjectImport>,
    exports: Vec<FObjectExport>,
    preload_dependencies: Vec<FPackageIndex>,
    data_resources: Vec<FObjectDataResource>,
}
impl FLegacyPackageHeader {
    fn deserialize<S: Read + Seek>(s: &mut S, package_version_fallback: Option<FPackageFileVersion>) -> Result<FLegacyPackageHeader> {

        // Determine the package version first. We need package version to parse the summary and the rest of the header
        let package_file_version = try_derive_package_file_version_from_package(s, package_version_fallback)?;

        // Deserialize package summary
        let package_summary_offset: u64 = s.stream_position()?;
        let package_summary: FPackageFileSummary = FPackageFileSummary::deserialize(s, Some(package_file_version))?;

        // Deserialize name map
        let name_map: FPackageNameMap = FPackageNameMap::read(s, &package_summary)?;

        // Deserialize import map
        let imports_start_offset = package_summary_offset + package_summary.imports.offset as u64;
        s.seek(SeekFrom::Start(imports_start_offset))?;

        let mut imports: Vec<FObjectImport> = Vec::with_capacity(package_summary.imports.count as usize);
        for _ in 0..package_summary.imports.count {
            let object_import: FObjectImport = FObjectImport::deserialize(s, &package_summary)?;
            imports.push(object_import);
        }

        // Deserialize export map
        let exports_start_offset = package_summary_offset + package_summary.exports.offset as u64;
        s.seek(SeekFrom::Start(exports_start_offset))?;

        let mut exports: Vec<FObjectExport> = Vec::with_capacity(package_summary.exports.count as usize);
        for _ in 0..package_summary.exports.count {
            let object_export: FObjectExport = FObjectExport::deserialize(s, &package_summary)?;
            exports.push(object_export);
        }

        // Deserialize preload dependencies
        let preload_dependencies_start_offset = package_summary_offset + package_summary.preload_dependencies.offset as u64;
        s.seek(SeekFrom::Start(preload_dependencies_start_offset))?;
        let preload_dependencies: Vec<FPackageIndex> = s.de_ctx(package_summary.preload_dependencies.count as usize)?;

        // Data resources are absent on packages below UE 5.2
        let mut data_resources: Vec<FObjectDataResource> = Vec::new();
        if package_summary.data_resource_offset != 0 {

            let data_resource_start_offset = package_summary_offset + package_summary.data_resource_offset as u64;
            s.seek(SeekFrom::Start(data_resource_start_offset))?;

            // Might be worth moving into the enum once UE adds more data resource versions
            let data_resource_version: u32 = s.de()?;
            if data_resource_version != 1 {
                bail!("Unknown data resource version {}. Only EVersion::Initial (1) is supported", data_resource_version);
            }

            let data_resource_count: i32 = s.de()?;
            data_resources = s.de_ctx(data_resource_count as usize)?;
        }
        Ok(FLegacyPackageHeader{summary: package_summary, name_map, imports, exports, preload_dependencies, data_resources})
    }
    fn serialize<S: Write + Seek>(&self, s: &mut S) -> Result<()> {

        let package_summary_offset: u64 = s.stream_position()?;
        let mut package_summary: FPackageFileSummary = self.summary.clone();

        // Write initial package summary. We will overwrite it again once we have the offsets of the relevant data members
        FPackageFileSummary::serialize(&package_summary, s)?;

        // Write name map. It directly follows the package summary
        FPackageNameMap::write(&self.name_map, s, &mut package_summary, package_summary_offset)?;

        // Write soft object paths offset. We do not actually write any soft object paths because they must be serialized inline for cooked assets,
        // because zen header cannot preserve object paths that are not serialized inline
        let soft_object_paths_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.soft_object_paths = FCountOffsetPair{count: 0, offset: soft_object_paths_offset};

        // Serialize import map
        let imports_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.imports = FCountOffsetPair{count: self.imports.len() as i32, offset: imports_start_offset};
        for object_import in &self.imports {
            FObjectImport::serialize(&object_import, s, &package_summary)?;
        }

        // Serialize export map
        let exports_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.exports = FCountOffsetPair{count: self.exports.len() as i32, offset: exports_start_offset};
        for object_export in &self.exports {
            FObjectExport::serialize(&object_export, s, &package_summary)?;
        }

        // Serialize depends map. This is just an empty placeholder for cooked assets
        let depends_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.depends_offset = depends_start_offset;
        let empty_depends_list: Vec<FPackageIndex> = Vec::new();
        for _ in 0..self.exports.len() {
            s.ser(empty_depends_list.clone())?;
        }

        // Serialize asset registry data. This is just an empty placeholder for cooked assets
        let asset_registry_data_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.asset_registry_data_offset = asset_registry_data_start_offset;
        let asset_object_data_count: i32 = 0;
        s.ser(asset_object_data_count)?;

        // World composition data from the package summary is not used in runtime and is only written for legacy world composition assets in 4.27, so write 0
        package_summary.world_tile_info_data_offset = 0;

        // Serialize preload dependencies
        let preload_dependencies_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.preload_dependencies = FCountOffsetPair{count: self.preload_dependencies.len() as i32, offset: preload_dependencies_start_offset};
        for preload_dependency in &self.preload_dependencies {
            s.ser(preload_dependency.clone())?;
        }

        // Serialize data resources if they are present. Write -1 if there are no data resources
        package_summary.data_resource_offset = -1;
        if !self.data_resources.is_empty() {

            let data_resources_start_offset = (s.stream_position()? - package_summary_offset) as i32;
            package_summary.data_resource_offset = data_resources_start_offset;

            // Might be worth moving into the enum once UE adds more data resource versions
            let data_resource_version: u32 = 1;
            s.ser(data_resource_version)?;

            let data_resource_count: i32 = self.data_resources.len() as i32;
            s.ser(data_resource_count)?;
            for data_resource in &self.data_resources {
                s.ser(data_resource.clone())?;
            }
        }

        // Set total size of the serialized header. The rest of the data is not considered the part of it
        let total_header_size = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.total_header_size = total_header_size;
        let position_after_writing_header = s.stream_position()?;

        // This would be written after export blobs, but since this value is meaningless in cooked games, write 0
        package_summary.bulk_data_start_offset = 0;

        // Go back to the initial package summary and overwrite it with a patched-up version
        s.seek(SeekFrom::Start(package_summary_offset))?;
        FPackageFileSummary::serialize(&package_summary, s)?;

        // Seek back to the position after the header
        s.seek(SeekFrom::Start(position_after_writing_header))?;
        Ok({})
    }
}

// Cache that stores the packages that were retrieved for the purpose of dependency resolution, to avoid loading and parsing them multiple times
#[derive(Debug)]
pub(crate) struct FZenPackageHeaderCache {
    package_headers_cache: HashMap<FPackageId, Rc<FZenPackageHeader>>,
    packages_failed_load: HashSet<FPackageId>,
}
impl FZenPackageHeaderCache {
    pub(crate) fn create() -> Self { Self{ package_headers_cache: HashMap::new(), packages_failed_load: HashSet::new() } }
    fn lookup(&mut self, store_access: &dyn IoStoreTrait, package_id: FPackageId) -> Result<Rc<FZenPackageHeader>> {

        // If we already have a package in the cache, return it
        if let Some(existing_package) = self.package_headers_cache.get(&package_id) {
            return Ok(existing_package.clone());
        }
        // If we have previously attempted to load a package and failed, return that
        if self.packages_failed_load.contains(&package_id) {
            return Err(anyhow!("Package has failed loading previously"));
        }

        // Optional package segments cannot be imported from other packages, and optional segments are also not supported outside of editor. So ChunkIndex is always 0
        let package_chunk_id = FIoChunkId::from_package_id(package_id, 0, EIoChunkType::ExportBundleData);
        let package_data = store_access.read(package_chunk_id);

        // Mark the package as failed load if it's chunk failed to load
        if let Err(read_error) = package_data {
            self.packages_failed_load.insert(package_id);
            return Err(read_error)
        }

        let mut zen_package_buffer = Cursor::new(package_data?);
        let zen_package_header = FZenPackageHeader::deserialize(&mut zen_package_buffer);

        // Mark the package as failed if we failed to parse the header
        if zen_package_header.is_err() {
            self.packages_failed_load.insert(package_id);
            return Err(zen_package_header.unwrap_err());
        }

        // Move the package header into a shared pointer and store it into the map
        let shared_package_header: Rc<FZenPackageHeader> = Rc::new(zen_package_header?);
        self.package_headers_cache.insert(package_id, shared_package_header.clone());
        Ok(shared_package_header)
    }
}

struct ResolvedZenImport {
    class_package: String,
    class_name: String,
    outer: Option<Box<ResolvedZenImport>>,
    index: FPackageObjectIndex,
}

pub(crate) fn build_legacy<P: AsRef<Path>>(
    iostore: &dyn IoStoreTrait,
    chunk_id: FIoChunkId,
    out_path: P,
    package_header_cache: &mut FZenPackageHeaderCache,
    fallback_package_file_version: Option<FPackageFileVersion>,
) -> Result<()> {
    let mut zen_data = Cursor::new(iostore.read(chunk_id)?);

    let mut out_buffer = vec![];
    let mut cur = Cursor::new(&mut out_buffer);

    let zen_summary: FZenPackageHeader = FZenPackageHeader::deserialize(&mut zen_data)?;

    // Populate package summary with basic data
    let mut legacy_package_summary = FPackageFileSummary::default();
    legacy_package_summary.package_name = zen_summary.name_map.get(zen_summary.summary.name).to_string();
    legacy_package_summary.package_flags = zen_summary.summary.package_flags;
    legacy_package_summary.package_guid = FGuid{a: 0, b: 0, c: 0, d: 0};
    legacy_package_summary.package_source = 0;

    // If zen package has versioning data, we can transfer it to the package. Otherwise, the package is written unversioned
    // However, we still need to know the package file version to determine the disk format of the legacy package
    if zen_summary.versioning_info.is_some() {
        let zen_versions: &FZenPackageVersioningInfo = zen_summary.versioning_info.as_ref().unwrap();

        legacy_package_summary.versioning_info = FLegacyPackageVersioningInfo{
            legacy_file_version: 0,
            package_file_version: zen_versions.package_file_version,
            licensee_version: zen_versions.licensee_version,
            custom_versions: zen_versions.custom_versions.clone(),
            is_unversioned: false,
        };
    }
    else {
        // TODO: Derive package file version from Zen. This has to be done in FZenPackageVersioningInfo since there are parsing differences for zen asset
        if fallback_package_file_version.is_none() {
            bail!("Cannot build legacy asset from unversioned zen asset without explicit package file version. Please provide explicit package file version");
        }
        legacy_package_summary.versioning_info = FLegacyPackageVersioningInfo{
            legacy_file_version: 0,
            package_file_version: fallback_package_file_version.unwrap(),
            is_unversioned: true,
            ..FLegacyPackageVersioningInfo::default()
        };
    }

    // Create legacy package header
    let mut legacy_package = FLegacyPackageHeader{summary: legacy_package_summary, ..FLegacyPackageHeader::default()};

    // Copy the names from the zen container. Name map format is the same on the high level
    legacy_package.name_map = FPackageNameMap{ names: zen_summary.name_map.copy_raw_names() };

    // Copy data resources
    legacy_package.data_resources = zen_summary.bulk_data.iter().map(|zen_bulk_data| {
        // Zen also does not serialize outer_index, so we will write Null as outer index. Luckily, this information is not needed in runtime.
        let outer_index: FPackageIndex = FPackageIndex::create_null();
        // Note that Zen does not support compressed bulk data, so raw_size == serial_size
        let raw_size = zen_bulk_data.serial_size;
        // FObjectDataResource::Flags are always 0 for cooked packages, they are not used in runtime and never written when cooking
        let flags: u32 = 0;
        // Copy legacy bulk data flags from zen directly
        let legacy_bulk_data_flags = zen_bulk_data.flags;

        return FObjectDataResource{flags,
            serial_offset: zen_bulk_data.serial_offset,
            duplicate_serial_offset: zen_bulk_data.duplicate_serial_offset,
            serial_size: zen_bulk_data.serial_size,
            raw_size, outer_index, legacy_bulk_data_flags
        };
    }).collect();

    dbg!(zen_summary);

    // arch's sandbox
    cur.write_u32::<LE>(0xa687562)?;

    std::fs::write(out_path, out_buffer)?;

    Ok(())
}

// Attempts to derive a package file version suitable for reading this package
#[instrument(skip_all, name = "FLegacyPackageHeader - Derive Package Version")]
fn try_derive_package_file_version_from_package<S: Read + Seek>(s: &mut S, package_version_fallback: Option<FPackageFileVersion>) -> Result<FPackageFileVersion> {

    let stream_start_position = s.stream_position()?;

    // Read the members that are independent on the package version
    let (versioning_info, names, _, _, package_flags) = FPackageFileSummary::deserialize_summary_minimal_version_independent(s)?;
    s.seek(SeekFrom::Start(stream_start_position))?;

    // If package is versioned, deserialize the header directly using the package version
    if !versioning_info.is_unversioned {
        return Ok(versioning_info.package_file_version);
    }
    // If package is unversioned, but we have a fallback package version, serialize with it directly
    if package_version_fallback.is_some() {
        return Ok(package_version_fallback.unwrap());
    }
    // Otherwise, we need to make sure that the package is cooked and has no editor properties, for our intrinsics to work
    let package_flags_cooked_versioned = EPackageFlags::Cooked as u32 | EPackageFlags::FilterEditorOnly as u32;
    if package_flags & package_flags_cooked_versioned != package_flags_cooked_versioned {
        bail!("Cannot deserialize unversioned package that is not cooked and has editor data filtered out without fallback package version");
    }

    // Unreal Engine serializes name map directly following the package summary. Although it is not safe to assume that it follows the header immediately,
    // for the engine cooked packages it does, and we and other asset editing software tries to preserve the engine ordering, so we can try to use it to deduce the header size
    // to then deduce the package file version for this package
    let header_size = names.offset as usize;
    let mut header_read_payload: Vec<u8> = Vec::with_capacity(header_size);
    s.read(&mut header_read_payload)?;

    // Try these package versions for the supported engine versions. First version to read the full header size and not overflow is the presumed package version
    let package_versions_to_try: Vec<FPackageFileVersion> = vec![
        // Note that AssetRegistryPackageBuildDependencies and PropertyTagCompleteTypeName cannot be told apart, so package will always assume 5.5 instead of 5.4
        FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::AssetRegistryPackageBuildDependencies), // UE 5.5
        FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::PropertyTagCompleteTypeName), // UE 5.4
        FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::DataResources), // UE 5.3 and 5.2
        FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::AddSoftObjectPathList), // UE 5.1
        FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::LargeWorldCoordinates), // UE 5.0
        FPackageFileVersion::create_ue4(EUnrealEngineObjectUE4Version::CorrectLicenseeFlag), // UE 4.27 and 4.26
    ];

    // Try to read the package summary for each version, and read at least one import and one export
    // That should be enough to tell the relevant versions apart
    for package_version in package_versions_to_try {

        let mut read_cursor = Cursor::new(header_read_payload.clone());
        let package_summary_or_error: Result<FPackageFileSummary> = FPackageFileSummary::deserialize(&mut read_cursor, Some(package_version));
        let current_cursor_position = read_cursor.position() as usize;

        // If we failed to read the package summary, try again with another version
        // Make sure we have read the entire header before declaring this a success
        if package_summary_or_error.is_err() || current_cursor_position != header_size {
            continue
        }
        let package_summary: FPackageFileSummary = package_summary_or_error?;

        // Standard serialization order is the order of data in packages serialized by UE (standard is name map -> imports -> exports -> depends_on)
        // We rely on that order to make estimation of the size of individual serialized entries. For packages that do not follow the standard order, we cannot derive package version from their summary
        let is_standard_serialization_order = package_summary.names.offset < package_summary.imports.offset &&
            package_summary.imports.offset < package_summary.exports.offset &&
            package_summary.exports.offset < package_summary.depends_offset;
        if !is_standard_serialization_order {
            continue
        }

        // Only check imports if this package actually has some
        if package_summary.imports.count > 0 {

            // Attempt to read first import with this version to make sure imports can be parsed
            // We make an assumption here that exports directly follow imports, which is a correct assumption for UE
            let imports_start_offset = stream_start_position + package_summary.imports.offset as u64;
            let combined_imports_length = (package_summary.exports.offset - package_summary.imports.offset) as u64;
            let single_import_size = combined_imports_length / package_summary.imports.count as u64;

            // If we failed to seek to the import map, this version does not work
            if s.seek(SeekFrom::Start(imports_start_offset)).is_err() {
                continue
            }
            let mut first_import_data: Vec<u8> = Vec::with_capacity(single_import_size as usize);
            if s.read(&mut first_import_data).is_err() {
                continue;
            }
            // If we failed to deserialize the import, or did not read all the data, this is not the right version
            let mut first_import_cursor = Cursor::new(first_import_data);
            let first_import: Result<FObjectImport> = FObjectImport::deserialize(&mut first_import_cursor, &package_summary);
            if first_import.is_err() || first_import_cursor.position() != single_import_size {
                continue
            }
        }

        // Only check exports if package has some
        if package_summary.exports.count > 0 {

            // Attempt to read the first export with this version to make sure exports can be parsed
            // We make an assumption here that exports directly follow imports, which is a correct assumption for UE
            let exports_start_offset = stream_start_position + package_summary.exports.offset as u64;
            let combined_exports_length = (package_summary.depends_offset - package_summary.exports.offset) as u64;
            let single_export_size = combined_exports_length / package_summary.exports.count as u64;

            // If we failed to seek to the export map, this version does not work
            if s.seek(SeekFrom::Start(exports_start_offset)).is_err() {
                continue
            }
            let mut first_export_data: Vec<u8> = Vec::with_capacity(single_export_size as usize);
            if s.read(&mut first_export_data).is_err() {
                continue;
            }
            // If we failed to deserialize the export, or did not read all the data, this is not the right version
            let mut first_export_cursor = Cursor::new(first_export_data);
            let first_export: Result<FObjectExport> = FObjectExport::deserialize(&mut first_export_cursor, &package_summary);
            if first_export.is_err() || first_export_cursor.position() != single_export_size {
                continue
            }
        }

        // Jump back to the start of the stream
        s.seek(SeekFrom::Start(stream_start_position))?;
        // This looks like a right package version for our needs
        return Ok(package_version)
    }

    // Jump back to the start of the stream
    s.seek(SeekFrom::Start(stream_start_position))?;
    // We failed to derive the package version from the summary, return Err
    Err(anyhow!("Failed to derive package file version from the package. Please provide an explicit package version to deserialize this unversioned package"))
}
