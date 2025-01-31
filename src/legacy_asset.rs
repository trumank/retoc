use crate::zen::{EUnrealEngineObjectUE4Version, EUnrealEngineObjectUE5Version, FCustomVersion, FPackageFileVersion, FPackageIndex};
use crate::{break_down_name_string, ser::*, FGuid};
use crate::logging::*;
use anyhow::{bail, Result};
use std::borrow::Cow;
use std::cmp::max;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use tracing::instrument;
use crate::version_heuristics::heuristic_package_version_from_legacy_package;

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct FMinimalName {
    index: i32,
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
impl Writeable for FMinimalName {
    #[instrument(skip_all, name = "FMinimalName")]
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.index)?;
        stream.ser(&self.number)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct FCountOffsetPair {
    pub(crate) count: i32,
    pub(crate) offset: i32,
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
        s.ser(&self.count)?;
        s.ser(&self.offset)?;
        Ok(())
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
        s.ser(&self.export_count)?;
        s.ser(&self.name_count)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FEngineVersion {
    pub(crate) engine_major: u16,
    pub(crate) engine_minor: u16,
    pub(crate) engine_patch: u16,
    pub(crate) changelist: u32,
    pub(crate) branch: String,
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
        s.ser(&self.engine_major)?;
        s.ser(&self.engine_minor)?;
        s.ser(&self.engine_patch)?;
        s.ser(&self.changelist)?;
        s.ser(&self.branch.clone())?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FLegacyPackageVersioningInfo
{
    pub(crate) legacy_file_version: i32,
    pub(crate) package_file_version: FPackageFileVersion,
    pub(crate) licensee_version: i32,
    pub(crate) custom_versions: Vec<FCustomVersion>,
    pub(crate) is_unversioned: bool,
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
        s.ser(&legacy_file_version)?;

        // There should never be a UE3 version written
        let legacy_ue3_version: i32 = 0;
        s.ser(&legacy_ue3_version)?;

        // Write raw file version for UE4 and UE5 (if package is UE5)
        // Note that we should not write any versions if this package was loaded as unversioned, since our own version is only used internally and the game should still assume latest
        let raw_file_version_ue4: i32 = if self.is_unversioned { 0 } else { self.package_file_version.file_version_ue4 };
        s.ser(&raw_file_version_ue4)?;
        if legacy_file_version == FLegacyPackageVersioningInfo::LEGACY_FILE_VERSION_UE5 {
            let raw_file_version_ue5: i32 = if self.is_unversioned { 0 } else { self.package_file_version.file_version_ue5 };
            s.ser(&raw_file_version_ue5)?;
        }

        let licensee_version = if self.is_unversioned { 0 } else { self.licensee_version };
        s.ser(&licensee_version)?;
        s.ser(&self.custom_versions.clone())?;
        Ok(())
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
pub(crate) struct FLegacyPackageFileSummary
{
    pub(crate) versioning_info: FLegacyPackageVersioningInfo,
    pub(crate) total_header_size: i32,
    pub(crate) package_name: String,
    pub(crate) package_flags: u32,
    pub(crate) names: FCountOffsetPair,
    // never written for cooked packages
    pub(crate) soft_object_paths: FCountOffsetPair,
    pub(crate) exports: FCountOffsetPair,
    pub(crate) imports: FCountOffsetPair,
    // empty placeholder for cooked packages
    pub(crate) depends_offset: i32,
    pub(crate) package_guid: FGuid,
    pub(crate) package_source: u32,
    world_tile_info_data_offset: i32,
    chunk_ids: Vec<i32>,
    preload_dependencies: FCountOffsetPair,
    pub(crate) names_referenced_from_export_data_count: i32,
    data_resource_offset: i32,
    // empty placeholder for cooked packages
    asset_registry_data_offset: i32,
    // not used for cooked packages
    bulk_data_start_offset: i64,
}
impl FLegacyPackageFileSummary {
    pub(crate) const PACKAGE_FILE_TAG: u32 = 0x9E2A83C1;
    pub(crate) fn has_package_flags(&self, package_flags: EPackageFlags) -> bool { (self.package_flags & package_flags as u32) != 0 }
    pub(crate) fn is_filter_editor_only(&self) -> bool { self.has_package_flags(EPackageFlags::FilterEditorOnly) }
    pub(crate) fn uses_unversioned_property_serialization(&self) -> bool { self.has_package_flags(EPackageFlags::UsesUnversionedProperties) }
}
impl FLegacyPackageFileSummary {
    #[instrument(skip_all, name = "FLegacyPackageFileSummary")]
    pub(crate) fn deserialize<S: Read>(s: &mut S, package_version_fallback: Option<FPackageFileVersion>) -> Result<Self> {

        // Check asset magic first
        let asset_magic_tag: u32 = s.de()?;
        if asset_magic_tag != FLegacyPackageFileSummary::PACKAGE_FILE_TAG {
            bail!("Package file magic mismatch: {} (expected {})", asset_magic_tag, FLegacyPackageFileSummary::PACKAGE_FILE_TAG);
        }

        let mut versioning_info: FLegacyPackageVersioningInfo = s.de()?;
        // We need a valid package file version to deserialize this package, so we rely on having a fallback if the package is unversioned
        if versioning_info.is_unversioned {
            if package_version_fallback.is_none() {
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
        let bulk_data_start_offset: i64 = s.de()?;

        // Legacy world composition data, but can very much be written on UE4 games
        let world_tile_info_data_offset: i32 = s.de()?;

        let chunk_ids: Vec<i32> = s.de()?;
        let preload_dependencies: FCountOffsetPair = s.de()?;

        // Assume all names are referenced if this is an old package
        let names_referenced_from_export_data_count: i32 = if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::NamesReferencedFromExportData as i32 { s.de()? } else { names.count };

        // Package trailers should never be written for cooked packages, they are only used for saving EditorBulkData in editor domain with package virtualization
        let _payload_toc_offset: i64 = if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::PayloadTOC as i32 { s.de()? } else { -1 };

        // Data resource offset is only written with new bulk data save format, otherwise bulk data meta is simply saved inline
        let data_resource_offset: i32 = if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32 { s.de()? } else { -1 };

        Ok(FLegacyPackageFileSummary{
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
    #[instrument(skip_all, name = "FLegacyPackageFileSummary - Minimal")]
    pub(crate) fn deserialize_summary_minimal_version_independent<S: Read>(s: &mut S) -> Result<(FLegacyPackageVersioningInfo, FCountOffsetPair, String, i32, u32)> {

        // Check asset magic first
        let asset_magic_tag: u32 = s.de()?;
        if asset_magic_tag != FLegacyPackageFileSummary::PACKAGE_FILE_TAG {
            bail!("Package file magic mismatch: {} (expected {})", asset_magic_tag, FLegacyPackageFileSummary::PACKAGE_FILE_TAG);
        }

        let versioning_info: FLegacyPackageVersioningInfo = s.de()?;
        let total_header_size: i32 = s.de()?;
        let package_name: String = s.de()?;
        let package_flags: u32 = s.de()?;

        let names: FCountOffsetPair = s.de()?;
        Ok((versioning_info, names, package_name, total_header_size, package_flags))
    }

    #[instrument(skip_all, name = "FLegacyPackageFileSummary")]
    fn serialize<S: Write>(&self, s: &mut S) -> Result<()> {

        let asset_magic_tag: u32 = FLegacyPackageFileSummary::PACKAGE_FILE_TAG;
        s.ser(&asset_magic_tag)?;

        // Make sure we are not attempting to write versions before UE4 NonOuterPackageImport. Our export/import serialization does not support such old versions
        if self.versioning_info.package_file_version.file_version_ue4 < EUnrealEngineObjectUE4Version::NonOuterPackageImport as i32 {
            bail!("Attempt to write UE4 package file version {}, which is below minimum supported version {}", self.versioning_info.package_file_version.file_version_ue4, EUnrealEngineObjectUE4Version::NonOuterPackageImport as i32);
        }

        s.ser(&self.versioning_info.clone())?;
        s.ser(&self.total_header_size)?;
        s.ser(&self.package_name.clone())?;
        s.ser(&self.package_flags)?;

        s.ser(&self.names)?;
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::AddSoftObjectPathList as i32 {
            s.ser(&self.soft_object_paths)?;
        }

        // Not written when editor only data is filtered out
        if !self.is_filter_editor_only() {
            let localization_id: String = "".to_string();
            s.ser(&localization_id)?;
        }
        // Not written when cooking or filtering editor only data
        let gatherable_text_data = FCountOffsetPair{count: 0, offset: 0};
        s.ser(&gatherable_text_data)?;

        s.ser(&self.exports)?;
        s.ser(&self.imports)?;
        // Serialized for cooked packages, but is always an empty array for each export
        s.ser(&self.depends_offset)?;

        // Cooked packages never have soft package references or searchable names
        let soft_package_references = FCountOffsetPair{count: 0, offset: 0};
        s.ser(&soft_package_references)?;
        let searchable_names_offset: i32 = 0;
        s.ser(&searchable_names_offset)?;
        // Cooked packages do not have thumbnails
        let thumbnails_table_offset: i32 = 0;
        s.ser(&thumbnails_table_offset)?;

        s.ser(&self.package_guid)?;

        // Package generations are always saved as one entry for modern packages, but not used in runtime
        // Note that the FLinkerLoad expects there to still be a single generation, it will crash if there is none
        let package_generations: Vec<FGenerationInfo> = vec![FGenerationInfo{export_count: self.exports.count, name_count: self.names.count}];
        s.ser(&package_generations)?;
        // Persistent package GUID is never written for cooked packages
        if !self.is_filter_editor_only() {
            let persistent_package_guid = FGuid{a: 0, b: 0, c: 0, d: 0};
            s.ser(&persistent_package_guid)?;
        }

        // Saved and compatible engine versions are always empty for cooked packages
        let saved_by_engine_version = FEngineVersion{engine_major: 0, engine_minor: 0, engine_patch: 0, changelist: 0, branch: "".to_string()};
        let compatible_with_engine_version = FEngineVersion{engine_major: 0, engine_minor: 0, engine_patch: 0, changelist: 0, branch: "".to_string()};
        s.ser(&saved_by_engine_version)?;
        s.ser(&compatible_with_engine_version)?;

        // Unused, always 0 for modern packages
        let compression_flags: u32 = 0;
        s.ser(&compression_flags)?;
        // Unused, always empty array for modern UE packages, UE will refuse to load packages where this is not an empty array
        let num_compressed_chunks: i32 = 0;
        s.ser(&num_compressed_chunks)?;

        s.ser(&self.package_source)?;

        // No longer used, always empty
        let additional_packages_to_cook: Vec<String> = vec![];
        s.ser(&additional_packages_to_cook)?;

        // Serialized for packages with filtered editor only data as 1 integer (0x0), not read in runtime
        s.ser(&self.asset_registry_data_offset)?;
        // Written as an offset, but is never read for cooked packages as the data is never written in the header file
        s.ser(&self.bulk_data_start_offset)?;
        // Legacy world composition data, but can very much be written on UE4 games
        s.ser(&self.world_tile_info_data_offset)?;

        s.ser(&self.chunk_ids.clone())?;
        s.ser(&self.preload_dependencies)?;

        // Only write number of referenced names if this is a UE5 package
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::NamesReferencedFromExportData as i32 { s.ser(&self.names_referenced_from_export_data_count)?; }

        // Package trailers should never be written for cooked packages, they are only used for saving EditorBulkData in editor domain with package virtualization
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::PayloadTOC as i32 {
            let payload_toc_offset: i64 = -1;
            s.ser(&payload_toc_offset)?;
        }

        // Data resource offset is only written with new bulk data save format, otherwise bulk data meta is simply saved inline
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32 { s.ser(&self.data_resource_offset)?; }

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FPackageNameMap {
    names: Vec<String>,
    name_lookup: HashMap<String, usize>,
}
impl FPackageNameMap {
    pub(crate) fn create() -> Self { FPackageNameMap{ names: Vec::new(), name_lookup: HashMap::new() } }
    pub(crate) fn create_from_names(names: Vec<String>) -> Self {
        let mut name_lookup: HashMap<String, usize> = HashMap::with_capacity(names.len());
        for (name_index, name) in names.iter().cloned().enumerate() {
            name_lookup.insert(name, name_index);
        }
        Self{names, name_lookup}
    }
    pub(crate) fn num_names(&self) -> usize { self.names.len() }
    #[instrument(skip_all, name = "FPackageNameMap")]
    pub(crate) fn read<S: Read + Seek>(stream: &mut S, summary: &FLegacyPackageFileSummary) -> Result<FPackageNameMap> {

        stream.seek(SeekFrom::Start(summary.names.offset as u64))?;

        let mut names: Vec<String> = Vec::with_capacity(summary.names.count as usize);
        let mut name_lookup: HashMap<String, usize> = HashMap::with_capacity(summary.names.count as usize);

        for index in 0..summary.names.count {

            let name_string: String = stream.de()?;
            let _non_case_preserving_hash: u16 = stream.de()?;
            let _case_preserving_hash: u16 = stream.de()?;

            names.push(name_string.clone());
            name_lookup.insert(name_string, index as usize);
        }

        Ok(Self{names, name_lookup})
    }
    #[instrument(skip_all, name = "FPackageNameMap")]
    pub(crate) fn write<S: Write + Seek>(&self, stream: &mut S, summary: &mut FLegacyPackageFileSummary, package_summary_offset: u64) -> Result<()> {

        // Tell the summary where the names start and how many there are
        summary.names.offset = (stream.stream_position()? - package_summary_offset) as i32;
        summary.names.count = self.names.len() as i32;

        for i in 0..self.names.len() {

            // Write the name string
            stream.ser(&self.names[i].clone())?;

            // Write 0 for case preserving and non-case preserving hashes. They are not used by the game
            let non_case_preserving_hash: u16 = 0;
            let case_preserving_hash: u16 = 0;
            stream.ser(&non_case_preserving_hash)?;
            stream.ser(&case_preserving_hash)?;
        }
        Ok(())
    }
    pub(crate) fn get(&self, name: FMinimalName) -> Cow<'_, str> {
        let bare_name = &self.names[name.index as usize];
        if name.number != 0 {
            format!("{bare_name}_{}", name.number - 1).into()
        } else {
            bare_name.into()
        }
    }
    pub(crate) fn store(&mut self, name: &str) -> FMinimalName {
        let (name_without_number, name_number) = break_down_name_string(name);

        // Attempt to resolve the existing name through lookup
        if let Some(existing_index) = self.name_lookup.get(name_without_number) {
            return FMinimalName{ index: *existing_index as i32, number: name_number };
        }

        // Create a new name and add it to the names list and to the name lookup
        let new_name_index = self.names.len();
        self.name_lookup.insert(name_without_number.to_string(), new_name_index);
        self.names.push(name_without_number.to_string());
        FMinimalName{ index: new_name_index as i32, number: name_number }
    }
    pub(crate) fn copy_raw_names(&self) -> Vec<String> { self.names.clone() }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FObjectImport {
    pub(crate) class_package: FMinimalName,
    pub(crate) class_name: FMinimalName,
    pub(crate) outer_index: FPackageIndex,
    pub(crate) object_name: FMinimalName,
    pub(crate) is_optional: bool,
}
impl FObjectImport
{
    #[instrument(skip_all, name = "FObjectImport")]
    pub(crate) fn deserialize<S: Read>(s: &mut S, summary: &FLegacyPackageFileSummary) -> Result<Self> {

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
    fn serialize<S: Write>(&self, s: &mut S, summary: &FLegacyPackageFileSummary) -> Result<()> {

        s.ser(&self.class_package)?;
        s.ser(&self.class_name)?;
        s.ser(&self.outer_index)?;
        s.ser(&self.object_name)?;

        // We should never be serializing uncooked packages, might be worth to assert here instead of writing an empty name
        if !summary.is_filter_editor_only() {
            let package_name: FMinimalName = FMinimalName::default();
            s.ser(&package_name)?;
        }

        let should_serialize_optional = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::OptionalResources as i32;
        if should_serialize_optional {
            s.ser(&self.is_optional)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FObjectExport {
    pub(crate) class_index: FPackageIndex,
    pub(crate) super_index: FPackageIndex,
    pub(crate) template_index: FPackageIndex,
    pub(crate) outer_index: FPackageIndex,
    pub(crate) object_name: FMinimalName,
    pub(crate) object_flags: u32,
    pub(crate) serial_size: i64,
    pub(crate) serial_offset: i64,
    pub(crate) is_not_for_client: bool,
    pub(crate) is_not_for_server: bool,
    pub(crate) is_inherited_instance: bool,
    // if false, the object must be kept for editor builds running client/server builds even if it is to be stripped based on not for server/client
    pub(crate) is_not_always_loaded_for_editor_game: bool,
    pub(crate) is_asset: bool,
    pub(crate) generate_public_hash: bool,
    pub(crate) first_export_dependency_index: i32,
    pub(crate) serialize_before_serialize_dependencies: i32,
    pub(crate) create_before_serialize_dependencies: i32,
    pub(crate) serialize_before_create_dependencies: i32,
    pub(crate) create_before_create_dependencies: i32,
    pub(crate) script_serialization_start_offset: i64,
    pub(crate) script_serialization_end_offset: i64,
}
impl FObjectExport
{
    #[instrument(skip_all, name = "FObjectExport")]
    pub(crate) fn deserialize<S: Read>(s: &mut S, summary: &FLegacyPackageFileSummary) -> Result<Self> {

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
    fn serialize<S: Write>(&self, s: &mut S, summary: &FLegacyPackageFileSummary) -> Result<()> {

        s.ser(&self.class_index)?;
        s.ser(&self.super_index)?;
        s.ser(&self.template_index)?;
        s.ser(&self.outer_index)?;
        s.ser(&self.object_name)?;
        s.ser(&self.object_flags)?;
        s.ser(&self.serial_size)?;
        s.ser(&self.serial_offset)?;

        // Forced exports as a concept do not exist in modern engine versions, this property is always false
        let is_forced_export: bool = false;
        s.ser(&is_forced_export)?;

        s.ser(&self.is_not_for_client)?;
        s.ser(&self.is_not_for_server)?;

        // Package GUID serialization of exports has been removed in UE5. Before then, we serialize an empty GUID
        let should_serialize_package_guid = summary.versioning_info.package_file_version.file_version_ue5 < EUnrealEngineObjectUE5Version::RemoveObjectExportPackageGUID as i32;
        if should_serialize_package_guid {
            let package_guid: FGuid = FGuid{a: 0, b: 0, c: 0, d: 0};
            s.ser(&package_guid)?;
        }

        // Added in UE5. Default to false for old assets
        let should_serialize_inherited_instance = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::TrackObjectExportIsInherited as i32;
        if should_serialize_inherited_instance {
            s.ser(&self.is_inherited_instance)?;
        }

        // Package flags are only relevant for forced exports, which do not exist as a concept anymore, so this value is always 0
        let package_flags: u32 = 0;
        s.ser(&package_flags)?;

        s.ser(&self.is_not_always_loaded_for_editor_game)?;
        s.ser(&self.is_asset)?;

        // Assume public hash to be generated for assets before UE5
        let should_serialize_generate_public_hash = summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::OptionalResources as i32;
        if should_serialize_generate_public_hash {
            s.ser(&self.generate_public_hash)?;
        }

        s.ser(&self.first_export_dependency_index)?;
        s.ser(&self.serialize_before_serialize_dependencies)?;
        s.ser(&self.create_before_serialize_dependencies)?;
        s.ser(&self.serialize_before_create_dependencies)?;
        s.ser(&self.create_before_create_dependencies)?;

        let should_serialize_script_props = !summary.uses_unversioned_property_serialization() && summary.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::ScriptSerializationOffset as i32;
        if should_serialize_script_props {
            s.ser(&self.script_serialization_start_offset)?;
            s.ser(&self.script_serialization_end_offset)?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct FObjectDataResource {
    pub(crate) flags: u32,
    pub(crate)  serial_offset: i64,
    pub(crate) duplicate_serial_offset: i64,
    pub(crate) serial_size: i64,
    pub(crate) raw_size: i64,
    pub(crate) outer_index: FPackageIndex,
    pub(crate) legacy_bulk_data_flags: u32,
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
        s.ser(&self.flags)?;
        s.ser(&self.serial_offset)?;
        s.ser(&self.duplicate_serial_offset)?;
        s.ser(&self.serial_size)?;
        s.ser(&self.raw_size)?;
        s.ser(&self.outer_index)?;
        s.ser(&self.legacy_bulk_data_flags)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FLegacyPackageHeader {
    pub(crate) summary: FLegacyPackageFileSummary,
    pub(crate) name_map: FPackageNameMap,
    pub(crate) imports: Vec<FObjectImport>,
    pub(crate) exports: Vec<FObjectExport>,
    pub(crate) preload_dependencies: Vec<FPackageIndex>,
    pub(crate) data_resources: Vec<FObjectDataResource>,
}
impl FLegacyPackageHeader {
    pub(crate) fn deserialize<S: Read + Seek>(s: &mut S, package_version_fallback: Option<FPackageFileVersion>) -> Result<FLegacyPackageHeader> {

        // Determine the package version first. We need package version to parse the summary and the rest of the header
        let package_file_version = heuristic_package_version_from_legacy_package(s, package_version_fallback)?;

        // Deserialize package summary
        let package_summary_offset: u64 = s.stream_position()?;
        let package_summary: FLegacyPackageFileSummary = FLegacyPackageFileSummary::deserialize(s, Some(package_file_version))?;

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
        if package_summary.data_resource_offset > 0 {

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
    pub(crate) fn serialize<S: Write + Seek>(&self, s: &mut S, desired_header_size: Option<usize>, log: &Log) -> Result<()> {

        let package_summary_offset: u64 = s.stream_position()?;
        let mut package_summary: FLegacyPackageFileSummary = self.summary.clone();

        // Write initial package summary. We will overwrite it again once we have the offsets of the relevant data members
        FLegacyPackageFileSummary::serialize(&package_summary, s)?;

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
            FObjectImport::serialize(object_import, s, &package_summary)?;
        }

        // Serialize export map
        let exports_start_offset_from_stream_start = s.stream_position()?;
        let exports_start_offset = (exports_start_offset_from_stream_start - package_summary_offset) as i32;
        package_summary.exports = FCountOffsetPair{count: self.exports.len() as i32, offset: exports_start_offset};
        for object_export in &self.exports {
            FObjectExport::serialize(object_export, s, &package_summary)?;
        }

        // Serialize depends map. This is just an empty placeholder for cooked assets
        let depends_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.depends_offset = depends_start_offset;
        let empty_depends_list: Vec<FPackageIndex> = Vec::new();
        for _ in 0..self.exports.len() {
            s.ser(&empty_depends_list.clone())?;
        }

        // Serialize asset registry data. This is just an empty placeholder for cooked assets
        let asset_registry_data_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.asset_registry_data_offset = asset_registry_data_start_offset;
        let asset_object_data_count: i32 = 0;
        s.ser(&asset_object_data_count)?;

        // World composition data from the package summary is not used in runtime and is only written for legacy world composition assets in 4.27, so write 0
        package_summary.world_tile_info_data_offset = 0;

        // Serialize preload dependencies
        let preload_dependencies_start_offset = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.preload_dependencies = FCountOffsetPair{count: self.preload_dependencies.len() as i32, offset: preload_dependencies_start_offset};
        for preload_dependency in &self.preload_dependencies {
            s.ser(&preload_dependency.clone())?;
        }

        // Serialize data resources if they are present. Write -1 if there are no data resources
        package_summary.data_resource_offset = -1;
        if !self.data_resources.is_empty() {

            let data_resources_start_offset = (s.stream_position()? - package_summary_offset) as i32;
            package_summary.data_resource_offset = data_resources_start_offset;

            // Might be worth moving into the enum once UE adds more data resource versions
            let data_resource_version: u32 = 1;
            s.ser(&data_resource_version)?;

            let data_resource_count: i32 = self.data_resources.len() as i32;
            s.ser(&data_resource_count)?;
            for data_resource in &self.data_resources {
                s.ser(&data_resource.clone())?;
            }
        }

        // Write zero padding after normal header data to maintain the zen asset binary equality if desired
        let data_total_header_size = (s.stream_position()? - package_summary_offset) as usize;
        if desired_header_size.is_some() && desired_header_size.unwrap() > data_total_header_size {
            let extra_null_padding_bytes = desired_header_size.unwrap() - data_total_header_size;
            s.write_all(&vec![0; extra_null_padding_bytes])?;
        }

        // Set total size of the serialized header. The rest of the data is not considered the part of it
        let total_header_size = (s.stream_position()? - package_summary_offset) as i32;
        package_summary.total_header_size = total_header_size;
        let position_after_writing_header = s.stream_position()?;

        // Export serial offsets include total header size into them, even if exports are split into a separate file
        // So we need to re-write export entries, now that we know the size of the header to adjust their offsets by
        s.seek(SeekFrom::Start(exports_start_offset_from_stream_start))?;
        let mut end_of_last_export_offset: i64 = total_header_size as i64;
        for object_export in &self.exports {

            let mut modified_object_export = object_export.clone();
            modified_object_export.serial_offset += total_header_size as i64;

            end_of_last_export_offset = max(end_of_last_export_offset, modified_object_export.serial_offset + modified_object_export.serial_size);
            FObjectExport::serialize(&modified_object_export, s, &package_summary)?;
        }

        // This would be written directly after the exports blobs. Even though this value is never used in cooked games, we can infer it by looking at the furthest written export blob and setting to be directly after it
        package_summary.bulk_data_start_offset = end_of_last_export_offset;

        // Go back to the initial package summary and overwrite it with a patched-up version
        s.seek(SeekFrom::Start(package_summary_offset))?;
        FLegacyPackageFileSummary::serialize(&package_summary, s)?;

        // Dump fully patched up package summary if needed
        debug!(log, "{:#?}", package_summary);

        // Seek back to the position after the header
        s.seek(SeekFrom::Start(position_after_writing_header))?;
        Ok(())
    }
}

// Returns package name and package-relative path to the object
pub(crate) fn get_package_object_full_name(package: &FLegacyPackageHeader, object_index: FPackageIndex, path_separator: char, lowercase_path: bool, package_name_override: Option<&str>) -> (String, String) {

    // From the outermost to the innermost, e.g. SubObject;Asset;PackageName
    let mut package_object_outer_chain: Vec<FPackageIndex> = Vec::with_capacity(4);
    let mut current_object_index = object_index;

    // Walk the import chain to resolve this import
    while !current_object_index.is_null() {
        package_object_outer_chain.push(current_object_index);

        if current_object_index.is_import() {
            let current_import_index = current_object_index.to_import_index() as usize;
            current_object_index = package.imports[current_import_index].outer_index;

        } else if current_object_index.is_export() {
            let current_export_index = current_object_index.to_export_index() as usize;
            current_object_index = package.exports[current_export_index].outer_index;
        }
    }
    // Reserve the outer chain now
    package_object_outer_chain.reverse();

    let package_name: String;
    let start_object_index: usize;

    if package_object_outer_chain[0].is_import() {
        let package_import_index = package_object_outer_chain[0].to_import_index() as usize;

        // If the innermost package index is an import, it's a package name. Otherwise, this package name is the package name
        package_name = package.name_map.get(package.imports[package_import_index].object_name).to_string();
        start_object_index = 1;

    } else {
        // This is an export, package name is this package name, and we should start path building at index 0
        // Use the provided package name override if it is available instead of the actual package name. This is necessary to produce correct global import index for exports on legacy UE4 zen assets
        package_name = package_name_override.unwrap_or(&package.summary.package_name).to_string();
        start_object_index = 0;
    };

    // Build full object name now. We append all elements and use / as a path separator
    let mut full_object_name: String = package_name.clone();
    for outer in &package_object_outer_chain[start_object_index..] {

        // Append object path separator
        full_object_name.push(path_separator);

        // Append the name of the object if it's an import
        if outer.is_import() {
            let import_index = outer.to_import_index() as usize;
            full_object_name.push_str(&package.name_map.get(package.imports[import_index].object_name));

            // Append the name of the object if it's an import
        } else if outer.is_export() {
            let export_index = outer.to_export_index() as usize;
            full_object_name.push_str(&package.name_map.get(package.exports[export_index].object_name));
        }
    }
    // Make sure the entire path is lowercase. This is a requirement for GetPublicExportHash
    if lowercase_path {
        full_object_name.make_ascii_lowercase();
    }
    (package_name, full_object_name)
}

// Attempts to resolve an original package name from a localized package name. Returns None if the provided package name is not a localized package name. Returns source package name and culture name otherwise
pub(crate) fn convert_localized_package_name_to_source(package_name: &str) -> Option<(String, String)> {
    // If the first character is not a /, this is not a localized package (or a valid package name, for that matter)
    if !package_name.starts_with('/') {
        return None;
    }
    // Split package name into Mount Point, L10N and the actual package name. We skip the first slash to keep the logic simpler and append it later
    let package_name_splits: Vec<&str> = package_name[1..].splitn(4, '/').collect();
    
    // If we have less than 3 parts, or the second part is not localization sub-folder, this package is not a localized package
    if package_name_splits.len() != 4 || package_name_splits[1] != "L10N" {
        return None
    }
    // This is a localized package otherwise. Full path to it is part1 + part3
    let mount_point = package_name_splits[0];
    let culture_name = package_name_splits[2].to_string();
    let package_path = package_name_splits[3];
    let source_package_name = format!("/{mount_point}/{package_path}");
    
    Some((source_package_name, culture_name))
}

#[derive(Default, Clone)]
pub(crate) struct FSerializedAssetBundle {
    pub(crate) asset_file_buffer: Vec<u8>, // uasset
    pub(crate) exports_file_buffer: Vec<u8>, // uexp
    pub(crate) bulk_data_buffer: Option<Vec<u8>>, // .ubulk
    pub(crate) optional_bulk_data_buffer: Option<Vec<u8>>, // .uptnl
    pub(crate) memory_mapped_bulk_data_buffer: Option<Vec<u8>>, // .m.ubulk
}

// Constants for use by asset conversion
pub(crate) const CORE_OBJECT_PACKAGE_NAME: &str = "/Script/CoreUObject";
pub(crate) const ENGINE_PACKAGE_NAME: &str = "/Script/Engine";
pub(crate) const OBJECT_CLASS_NAME: &str = "Object";
pub(crate) const CLASS_CLASS_NAME: &str = "Class";
pub(crate) const PACKAGE_CLASS_NAME: &str = "Package";
pub(crate) const PRESTREAM_PACKAGE_CLASS_NAME: &str = "PrestreamPackage";
