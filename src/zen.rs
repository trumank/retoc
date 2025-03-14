use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use strum::FromRepr;
use tracing::instrument;

use crate::align_usize;
use crate::name_map::{read_name_batch, read_name_batch_parts, write_name_batch, write_name_batch_parts, EMappedNameType};
use crate::script_objects::FPackageObjectIndex;
use crate::ser::{WriteExt, Writeable};
use crate::{align_u64, break_down_name_string, name_map::{FMappedName, FNameMap}, EIoStoreTocVersion, FGuid, FPackageId, FSHAHash, ReadExt, Readable};
use crate::container_header::{EIoContainerHeaderVersion, StoreEntry};
use crate::version_heuristics::{heuristic_zen_has_bulk_data, heuristic_zen_package_version};

pub(crate) fn get_package_name(data: &[u8], container_header_version: EIoContainerHeaderVersion) -> Result<String> {
    FZenPackageHeader::get_package_name(&mut Cursor::new(data), container_header_version)
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub(crate) struct FZenPackageSummary {
    pub(crate) has_versioning_info: u32,
    pub(crate) header_size: u32,
    pub(crate) name: FMappedName,
    pub(crate) source_name: FMappedName, // version == Initial
    pub(crate) package_flags: u32,
    pub(crate) cooked_header_size: u32,
    pub(crate) imported_public_export_hashes_offset: i32,
    import_map_offset: i32,
    export_map_offset: i32,
    export_bundle_entries_offset: i32,
    graph_data_offset: i32,
    dependency_bundle_headers_offset: i32,
    dependency_bundle_entries_offset: i32,
    imported_package_names_offset: i32,

    // if EIoContainerHeaderVersion == Initial
    name_map_names_offset: i32,
    name_map_names_size: i32,
    name_map_hashes_offset: i32,
    name_map_hashes_size: i32,
    graph_data_size: i32,
}
impl FZenPackageSummary {
    #[instrument(skip_all, name = "FZenPackageSummary")]
    fn deserialize<S: Read>(s: &mut S, container_header_version: EIoContainerHeaderVersion) -> Result<Self> {

        let mut has_versioning_info: u32 = 0;
        let mut header_size: u32 = 0;
        if container_header_version > EIoContainerHeaderVersion::Initial {
            has_versioning_info = s.de()?;
            header_size = s.de()?;
        }
        let name: FMappedName = s.de()?;
        let mut source_name: FMappedName = Default::default();
        if container_header_version <= EIoContainerHeaderVersion::Initial {
            source_name = s.de()?;
        }
        let package_flags: u32 = s.de()?;
        let cooked_header_size: u32 = s.de()?;

        let mut imported_public_export_hashes_offset = -1;

        let mut name_map_names_offset: i32 = -1;
        let mut name_map_names_size: i32 = -1;
        let mut name_map_hashes_offset: i32 = -1;
        let mut name_map_hashes_size: i32 = -1;
        if container_header_version <= EIoContainerHeaderVersion::Initial {
            name_map_names_offset = s.de()?;
            name_map_names_size = s.de()?;
            name_map_hashes_offset = s.de()?;
            name_map_hashes_size = s.de()?;
        } else {
            imported_public_export_hashes_offset = s.de()?;
        }

        let import_map_offset: i32 = s.de()?;
        let export_map_offset: i32 = s.de()?;
        let export_bundle_entries_offset: i32 = s.de()?;

        let mut graph_data_offset: i32 = -1;
        let mut dependency_bundle_headers_offset: i32 = -1;
        let mut dependency_bundle_entries_offset: i32 = -1;
        let mut imported_package_names_offset: i32 = -1;

        // Dependency bundles are written in EIoContainerHeaderVersion::NoExportInfo and beyond, before that graph data is written
        if container_header_version >= EIoContainerHeaderVersion::NoExportInfo {
            dependency_bundle_headers_offset = s.de()?;
            dependency_bundle_entries_offset = s.de()?;
            imported_package_names_offset = s.de()?;
        } else {
            graph_data_offset = s.de()?;
        }

        let mut graph_data_size: i32 = -1;
        if container_header_version <= EIoContainerHeaderVersion::Initial {
            graph_data_size = s.de()?;
            let _pad: i32 = s.de()?;
            // Header size is GraphDataOffset + GraphDataSize
            header_size = (graph_data_offset + graph_data_size) as u32;
        }

        Ok(Self{
            has_versioning_info,
            header_size,
            name,
            source_name,
            package_flags,
            cooked_header_size,
            imported_public_export_hashes_offset,
            import_map_offset,
            export_map_offset,
            export_bundle_entries_offset,
            graph_data_offset,
            dependency_bundle_headers_offset,
            dependency_bundle_entries_offset,
            imported_package_names_offset,

            name_map_names_offset,
            name_map_names_size,
            name_map_hashes_offset,
            name_map_hashes_size,
            graph_data_size,
        })
    }

    #[instrument(skip_all, name = "FZenPackageSummary")]
    fn serialize<S: Write>(&self, s: &mut S, container_header_version: EIoContainerHeaderVersion) -> Result<()> {

        if container_header_version > EIoContainerHeaderVersion::Initial {
            s.ser(&self.has_versioning_info)?;
            s.ser(&self.header_size)?;
        }

        s.ser(&self.name)?;
        if container_header_version <= EIoContainerHeaderVersion::Initial {
            s.ser(&self.source_name)?;
        }

        s.ser(&self.package_flags)?;
        s.ser(&self.cooked_header_size)?;

        if container_header_version <= EIoContainerHeaderVersion::Initial {
            s.ser(&self.name_map_names_offset)?;
            s.ser(&self.name_map_names_size)?;
            s.ser(&self.name_map_hashes_offset)?;
            s.ser(&self.name_map_hashes_size)?;
        } else {
            s.ser(&self.imported_public_export_hashes_offset)?;
        }

        s.ser(&self.import_map_offset)?;
        s.ser(&self.export_map_offset)?;
        s.ser(&self.export_bundle_entries_offset)?;

        if container_header_version >= EIoContainerHeaderVersion::NoExportInfo {
            s.ser(&self.dependency_bundle_headers_offset)?;
            s.ser(&self.dependency_bundle_entries_offset)?;
            s.ser(&self.imported_package_names_offset)?;
        } else {
            s.ser(&self.graph_data_offset)?;
        }

        if container_header_version <= EIoContainerHeaderVersion::Initial {
            s.ser(&self.graph_data_size)?;
            s.ser(&0i32)?; // pad
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, FromRepr)]
#[repr(u32)]
pub(crate) enum EZenPackageVersion {
    Initial,
    DataResourceTable,
    ImportedPackageNames,
    #[default] ExtraDependencies,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub(crate) struct FPackageFileVersion
{
    pub(crate) file_version_ue4: i32,
    pub(crate) file_version_ue5: i32,
}
impl FPackageFileVersion {
    pub(crate) fn create_ue4(version: EUnrealEngineObjectUE4Version) -> Self { FPackageFileVersion{file_version_ue4: version as i32, file_version_ue5: 0} }
    pub(crate) fn create_ue5(version: EUnrealEngineObjectUE5Version) -> Self { FPackageFileVersion{file_version_ue4: EUnrealEngineObjectUE4Version::CorrectLicenseeFlag as i32, file_version_ue5: version as i32} }
    pub(crate) fn is_ue5(self) -> bool { self.file_version_ue5 != 0 }
}
impl Readable for FPackageFileVersion {
    #[instrument(skip_all, name = "FPackageFileVersion")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            file_version_ue4: s.de()?,
            file_version_ue5: s.de()?,
        })
    }
}
impl Writeable for FPackageFileVersion {
    #[instrument(skip_all, name = "FPackageFileVersion")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.file_version_ue4)?;
        s.ser(&self.file_version_ue5)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub(crate) struct FCustomVersion
{
    pub(crate) key: FGuid,
    pub(crate) version: i32,
}
impl Readable for FCustomVersion {
    #[instrument(skip_all, name = "FCustomVersion")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            key: s.de()?,
            version: s.de()?,
        })
    }
}
impl Writeable for FCustomVersion {
    #[instrument(skip_all, name = "FCustomVersion")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.key)?;
        s.ser(&self.version)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct FZenPackageVersioningInfo
{
    pub(crate) zen_version: EZenPackageVersion,
    pub(crate) package_file_version: FPackageFileVersion,
    pub(crate) licensee_version: i32,
    pub(crate) custom_versions: Vec<FCustomVersion>,
}
impl Readable for FZenPackageVersioningInfo {
    #[instrument(skip_all, name = "FZenPackageVersioningInfo")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let zen_version_raw: u32 = s.de()?;
        Ok(Self{
            zen_version: EZenPackageVersion::from_repr(zen_version_raw).unwrap(),
            package_file_version: s.de()?,
            licensee_version: s.de()?,
            custom_versions: s.de()?
        })
    }
}
impl Writeable for FZenPackageVersioningInfo {
    #[instrument(skip_all, name = "FZenPackageVersioningInfo")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {

        let zen_version_raw: u32 = self.zen_version as u32;
        s.ser(&zen_version_raw)?;
        s.ser(&self.package_file_version)?;
        s.ser(&self.licensee_version)?;
        s.ser(&self.custom_versions)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
#[repr(C)] // Needed to determine the number of bulk data entries
pub(crate) struct FBulkDataMapEntry {
    pub(crate) serial_offset: i64,
    pub(crate) duplicate_serial_offset: i64,
    pub(crate) serial_size: i64,
    pub(crate) flags: u32,
    pub(crate) pad: u32,
}
impl Readable for FBulkDataMapEntry {
    #[instrument(skip_all, name = "FBulkDataMapEntry")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            serial_offset: s.de()?,
            duplicate_serial_offset: s.de()?,
            serial_size: s.de()?,
            flags: s.de()?,
            pad: s.de()?,
        })
    }
}
impl Writeable for FBulkDataMapEntry {
    #[instrument(skip_all, name = "FBulkDataMapEntry")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {

        s.ser(&self.serial_offset)?;
        s.ser(&self.duplicate_serial_offset)?;
        s.ser(&self.serial_size)?;
        s.ser(&self.flags)?;
        s.ser(&self.pad)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRepr)]
#[repr(u8)]
pub(crate) enum EExportFilterFlags {
    None = 0,
    NotForClient = 1,
    NotForServer = 2,
}

// This is only a small set of object flags that we want to interpret
#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
#[repr(u32)]
pub(crate) enum EObjectFlags {
    Public = 0x00000001,
    Standalone = 0x00000002,
    Transactional = 0x00000008,
    ClassDefaultObject = 0x00000010,
    ArchetypeObject = 0x00000020,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)] // Needed to determine the number of export entries
pub(crate) struct FExportMapEntry {
    pub(crate) cooked_serial_offset: u64,
    pub(crate) cooked_serial_size: u64,
    pub(crate) object_name: FMappedName,
    pub(crate) outer_index: FPackageObjectIndex,
    pub(crate) class_index: FPackageObjectIndex,
    pub(crate) super_index: FPackageObjectIndex,
    pub(crate) template_index: FPackageObjectIndex,
    pub(crate) public_export_hash: u64,
    pub(crate) object_flags: u32,
    // Contrary to the popular belief and the name of this field, this is not in fact bitflags - this is just a single enum value
    pub(crate) filter_flags: EExportFilterFlags,
    pub(crate) padding: [u8; 3],
}
impl Readable for FExportMapEntry {
    #[instrument(skip_all, name = "FExportMapEntry")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            cooked_serial_offset: s.de()?,
            cooked_serial_size: s.de()?,
            object_name: s.de()?,
            outer_index: s.de()?,
            class_index: s.de()?,
            super_index: s.de()?,
            template_index: s.de()?,
            public_export_hash: s.de()?,
            object_flags: s.de()?,
            filter_flags: EExportFilterFlags::from_repr(s.de()?).ok_or_else(|| { anyhow!("Failed to decode filter flags") })?,
            padding: s.de()?
        })
    }
}
impl Writeable for FExportMapEntry {
    #[instrument(skip_all, name = "FExportMapEntry")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.cooked_serial_offset)?;
        s.ser(&self.cooked_serial_size)?;
        s.ser(&self.object_name)?;
        s.ser(&self.outer_index)?;
        s.ser(&self.class_index)?;
        s.ser(&self.super_index)?;
        s.ser(&self.template_index)?;
        s.ser(&self.public_export_hash)?;
        s.ser(&self.object_flags)?;
        let raw_filter_flags: u8 = self.filter_flags as u8;
        s.ser(&raw_filter_flags)?;
        s.ser(&self.padding)?;
        Ok(())
    }
}

impl FExportMapEntry {
    // Reinterprets public export hash as global import index. Only valid for legacy zen packages before UE5.0
    pub(crate) fn legacy_global_import_index(&self) -> FPackageObjectIndex {
        FPackageObjectIndex::create_from_raw(self.public_export_hash)
    }
    // Returns true if this is a public export
    pub(crate) fn is_public_export(&self) -> bool {
        // Even if this is a new style export, the risk of collision here is so low that we can check both the new style hash and the old style global import index
        // FPackageObjectIndex::create_null() returns u64 that has all bits set, and the chances of the export hash having ALL of its bits set are the same as the chances of it having none of its bits set(e.g. being 0)
        self.public_export_hash != 0 && self.legacy_global_import_index() != FPackageObjectIndex::create_null()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, FromRepr)]
#[repr(u32)]
pub(crate) enum EExportCommandType {
    #[default] Create,
    Serialize,
    Count,
}
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(C)] // Needed to determine the number of export bundle entries
pub(crate) struct FExportBundleEntry {
    pub(crate) local_export_index: u32,
    pub(crate) command_type: EExportCommandType,
}
impl Readable for FExportBundleEntry {
    #[instrument(skip_all, name = "FExportBundleEntry")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            local_export_index: s.de()?,
            command_type: EExportCommandType::from_repr(s.de()?).unwrap()
        })
    }
}
impl Writeable for FExportBundleEntry {
    #[instrument(skip_all, name = "FExportBundleEntry")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.local_export_index)?;
        let raw_command_type: u32 = self.command_type as u32;
        s.ser(&raw_command_type)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
#[repr(C)] // Needed to determine the number of bundle headers
pub(crate) struct FDependencyBundleHeader {
    pub(crate) first_entry_index: i32,
    // Note that this is defined as uint32 EntryCount[ExportCommandType_Count][ExportCommandType_Count], but this is a really awkward definition to work with,
    // so here it is defined as 4 individual properties: [Create][Create], [Create][Serialize], [Serialize][Create] and [Serialize][Serialize]
    pub(crate) create_before_create_dependencies: u32,
    pub(crate) serialize_before_create_dependencies: u32,
    pub(crate) create_before_serialize_dependencies: u32,
    pub(crate) serialize_before_serialize_dependencies: u32,
}
impl Readable for FDependencyBundleHeader {
    #[instrument(skip_all, name = "FDependencyBundleHeader")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            first_entry_index: s.de()?,
            create_before_create_dependencies: s.de()?,
            serialize_before_create_dependencies: s.de()?,
            create_before_serialize_dependencies: s.de()?,
            serialize_before_serialize_dependencies: s.de()?,
        })
    }
}
impl Writeable for FDependencyBundleHeader {
    #[instrument(skip_all, name = "FDependencyBundleHeader")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.first_entry_index)?;
        s.ser(&self.create_before_create_dependencies)?;
        s.ser(&self.serialize_before_create_dependencies)?;
        s.ser(&self.create_before_serialize_dependencies)?;
        s.ser(&self.serialize_before_serialize_dependencies)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
pub(crate) struct FPackageIndex {
    index: i32, // positive is index into the export map, negative is index into import map, zero is none
}
impl FPackageIndex {
    pub(crate) fn create_null() -> FPackageIndex { FPackageIndex{index: 0} }
    pub(crate) fn create_import(import_index: u32) -> FPackageIndex { FPackageIndex{index: -(import_index as i32) - 1 } }
    pub(crate) fn create_export(export_index: u32) -> FPackageIndex { FPackageIndex{index: (export_index as i32) + 1 } }

    pub(crate) fn is_import(&self) -> bool { self.index < 0 }
    pub(crate) fn is_export(&self) -> bool { self.index > 0 }
    pub(crate) fn is_null(&self) -> bool { self.index == 0 }

    pub(crate) fn to_import_index(self) -> u32 {
        assert!(self.index < 0);
        (-self.index - 1) as u32
    }
    pub(crate) fn to_export_index(self) -> u32 {
        assert!(self.index > 0);
        (self.index - 1) as u32
    }
}
impl Readable for FPackageIndex {
    #[instrument(skip_all, name = "FPackageIndex")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            index: s.de()?,
        })
    }
}
impl Writeable for FPackageIndex {
    #[instrument(skip_all, name = "FPackageIndex")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.index)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(C)] // Needed to determine the number of bundle entries
pub(crate) struct FDependencyBundleEntry {
    pub(crate) local_import_or_export_index: FPackageIndex,
}
impl Readable for FDependencyBundleEntry {
    #[instrument(skip_all, name = "FDependencyBundleEntry")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            local_import_or_export_index: s.de()?,
        })
    }
}
impl Writeable for FDependencyBundleEntry {
    #[instrument(skip_all, name = "FDependencyBundleEntry")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.local_import_or_export_index)?;
        Ok(())
    }
}

// Actual UE type name not known, type layout from AsyncLoading2.cpp SetupSerializedArcs on 5.2.2
#[derive(Debug, Copy, Clone, Default, Hash, PartialEq, Eq)]
pub(crate) struct FInternalDependencyArc {
    pub(crate) from_export_bundle_index: i32,
    pub(crate) to_export_bundle_index: i32,
}
impl Readable for FInternalDependencyArc {
    #[instrument(skip_all, name = "FInternalDependencyArc")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            from_export_bundle_index: s.de()?,
            to_export_bundle_index: s.de()?,
        })
    }
}
impl Writeable for FInternalDependencyArc {
    #[instrument(skip_all, name = "FInternalDependencyArc")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.from_export_bundle_index)?;
        s.ser(&self.to_export_bundle_index)?;
        Ok(())
    }
}

// Actual UE type name not known, type layout from AsyncLoading2.cpp SetupSerializedArcs on 5.2.2
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub(crate) struct FExternalDependencyArc {
    pub(crate) from_import_index: i32,
    pub(crate) from_command_type: EExportCommandType,
    pub(crate) to_export_bundle_index: i32,
}
impl Readable for FExternalDependencyArc {
    #[instrument(skip_all, name = "FExternalDependencyArc")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let from_import_index: i32 = s.de()?;
        let from_command_type: u8 = s.de()?;
        let to_export_bundle_index: i32 = s.de()?;

        Ok(Self{
            from_import_index,
            // EExportCommandType serialization is inconsistent: it is serialized as uint8 in external arcs, but as uint32 in export bundle entries
            from_command_type: EExportCommandType::from_repr(from_command_type as u32).unwrap(),
            to_export_bundle_index,
        })
    }
}
impl Writeable for FExternalDependencyArc {
    #[instrument(skip_all, name = "FExternalDependencyArc")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.from_import_index)?;
        // EExportCommandType serialization is inconsistent: it is serialized as uint8 in external arcs, but as uint32 in export bundle entries
        let raw_from_command_type: u8 = self.from_command_type as u8;
        s.ser(&raw_from_command_type)?;
        s.ser(&self.to_export_bundle_index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ExternalPackageDependency {
    pub(crate) from_package_id: FPackageId,
    // External dependency arcs link an import from this package to an export bundle in this package
    pub(crate) external_dependency_arcs: Vec<FExternalDependencyArc>,
    // Legacy dependency arcs link export bundle in imported package to an export bundle in current package
    pub(crate) legacy_dependency_arcs: Vec<FInternalDependencyArc>,
}

// Legacy, UE 5.2 and below, when there were multiple export bundles instead of just one
#[derive(Debug, Copy, Clone, Default)]
#[repr(C)] // needed to determine the offset of the arc data
pub(crate) struct FExportBundleHeader
{
    // Serial offset to the first serialized export in this bundle. Each bundle begins with an export, and all serialized exports in the bundle are laid out in sequence,
    // one after another. cooked serial offset on the exports is not actually used for locating export blobs by the async loader
    // This is relative to the zen header size
    pub(crate) serial_offset: u64,
    // Index into ExportBundleEntries to the first entry belonging to this export bundle
    pub(crate) first_entry_index: u32,
    // Number of entries in this export bundle
    pub(crate) entry_count: u32,
}
impl FExportBundleHeader {
    #[instrument(skip_all, name = "FExportBundleHeader")]
    pub(crate) fn deserialize<S: Read>(s: &mut S, container_header_version: EIoContainerHeaderVersion) -> Result<Self> {
        // For legacy UE4 packages, serial offset of the bundle is not written, it is implied because bundles are always laid out sequentially
        let serial_offset = if container_header_version > EIoContainerHeaderVersion::Initial {
            s.de()?
        } else {
            u64::MAX
        };

        Ok(Self{
            serial_offset,
            first_entry_index: s.de()?,
            entry_count: s.de()?,
        })
    }

    #[instrument(skip_all, name = "FExportBundleHeader")]
    pub(crate) fn serialize<S: Write>(&self, s: &mut S, container_header_version: EIoContainerHeaderVersion) -> Result<()> {

        if container_header_version > EIoContainerHeaderVersion::Initial {
            s.ser(&self.serial_offset)?;
        }
        s.ser(&self.first_entry_index)?;
        s.ser(&self.entry_count)?;
        Ok(())
    }
}
impl Writeable for FExportBundleHeader {
    #[instrument(skip_all, name = "FExportBundleHeader")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.serial_offset)?;
        s.ser(&self.first_entry_index)?;
        s.ser(&self.entry_count)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, FromRepr)]
#[repr(i32)]
pub(crate) enum EUnrealEngineObjectUE5Version {
    InitialVersion = 1000,
    NamesReferencedFromExportData = 1001,
    PayloadTOC = 1002,
    OptionalResources = 1003,
    LargeWorldCoordinates = 1004,
    RemoveObjectExportPackageGUID = 1005,
    TrackObjectExportIsInherited = 1006,
    FSoftObjectPathRemoveAssetPathNames = 1007,
    AddSoftObjectPathList = 1008,
    DataResources = 1009,
    ScriptSerializationOffset = 1010,
    PropertyTagExtensionAndOverridableSerialization = 1011,
    PropertyTagCompleteTypeName = 1012,
    AssetRegistryPackageBuildDependencies = 1013,
}
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, FromRepr)]
#[repr(i32)]
pub(crate) enum EUnrealEngineObjectUE4Version {
    NonOuterPackageImport = 520,
    AssetRegistryDependencyFlags = 521,
    CorrectLicenseeFlag = 522,
}

#[derive(Debug, Clone, Default)]
struct FZenPackageImportedPackageNamesContainer {
    imported_package_names: Vec<String>,
}
impl Readable for FZenPackageImportedPackageNamesContainer {
    #[instrument(skip_all, name = "FZenPackageImportedPackageNamesContainer")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let mut imported_package_names: Vec<String> = read_name_batch(s)?;

        let imported_package_name_numbers: Vec<i32> = s.de_ctx(imported_package_names.len())?;
        for (index, item) in imported_package_names.iter_mut().enumerate() {
            if imported_package_name_numbers[index] != 0 {
                *item = format!("{item}_{}", imported_package_name_numbers[index] - 1)
            }
        }
        Ok(Self{ imported_package_names })
    }
}
impl Writeable for FZenPackageImportedPackageNamesContainer {
    #[instrument(skip_all, name = "FZenPackageImportedPackageNamesContainer")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {

        let mut imported_package_names: Vec<String> = Vec::with_capacity(self.imported_package_names.len());
        let mut imported_package_name_numbers: Vec<i32> = Vec::with_capacity(self.imported_package_names.len());

        for imported_package_name in &self.imported_package_names {
            let (name_without_number, name_number) = break_down_name_string(imported_package_name);

            imported_package_names.push(name_without_number.to_string());
            imported_package_name_numbers.push(name_number);
        }

        write_name_batch(s, &imported_package_names)?;
        s.ser_no_length(&imported_package_name_numbers)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct FZenPackageHeader {
    pub(crate) summary: FZenPackageSummary,
    pub(crate) versioning_info: FZenPackageVersioningInfo,
    pub(crate) name_map: FNameMap,
    pub(crate) bulk_data: Vec<FBulkDataMapEntry>,
    pub(crate) imported_public_export_hashes: Vec<u64>,
    pub(crate) import_map: Vec<FPackageObjectIndex>,
    pub(crate) export_map: Vec<FExportMapEntry>,
    // Only available before 5.3, where zen packages could have multiple export bundles and not just one
    pub(crate) export_bundle_headers: Vec<FExportBundleHeader>,
    // Meaning depending on the version. In 5.2 and below, this array is the data storage for export bundles, their contents are specified by export bundle headers
    // In 5.3 and later, there is only a single export bundle, and it's contents are represented by export bundle entries
    pub(crate) export_bundle_entries: Vec<FExportBundleEntry>,
    pub(crate) dependency_bundle_headers: Vec<FDependencyBundleHeader>,
    pub(crate) dependency_bundle_entries: Vec<FDependencyBundleEntry>,
    pub(crate) imported_package_names: Vec<String>,
    pub(crate) imported_packages: Vec<FPackageId>,
    pub(crate) shader_map_hashes: Vec<FSHAHash>,
    pub(crate) is_unversioned: bool,
    pub(crate) internal_dependency_arcs: Vec<FInternalDependencyArc>,
    pub(crate) external_package_dependencies: Vec<ExternalPackageDependency>,
    pub(crate) container_header_version: EIoContainerHeaderVersion,
}
impl FZenPackageHeader {
    pub(crate) fn package_name(&self) -> String {
        self.name_map.get(self.summary.name).to_string()
    }
    // Returns source package name for this package if it is present, or it's normal package name otherwise
    pub(crate) fn source_package_name(&self) -> String {
        if self.container_header_version <= EIoContainerHeaderVersion::Initial {
            let source_package_name = self.name_map.get(self.summary.source_name).to_string();
            if source_package_name != "None" {
                return source_package_name;
            }
        }
        self.package_name()
    }

    // Retrieves the package name from the package. Does the bare minimum of package reading to get the name out
    #[instrument(skip_all, name = "FZenPackageHeader - GetPackageName")]
    pub(crate) fn get_package_name<S: Read + Seek>(s: &mut S, container_header_version: EIoContainerHeaderVersion) -> Result<String> {
        let summary: FZenPackageSummary = FZenPackageSummary::deserialize(s, container_header_version)?;
        let name_map = if container_header_version > EIoContainerHeaderVersion::Initial {
            let _versioning_info: Option<FZenPackageVersioningInfo> = if summary.has_versioning_info != 0 { Some(s.de()?) } else { None };
            FNameMap::deserialize(s, EMappedNameType::Package)?
        } else {
            s.seek(SeekFrom::Start(summary.name_map_names_offset as u64))?;
            let names_buffer: Vec<u8> = s.de_ctx(summary.name_map_names_size as usize)?;
            FNameMap::create_from_names(EMappedNameType::Package, read_name_batch_parts(&names_buffer)?)
        };
        Ok(name_map.get(summary.name).to_string())
    }

    #[instrument(skip_all, name = "FZenPackageHeader")]
    pub(crate) fn deserialize<S: Read + Seek>(s: &mut S, optional_store_entry: Option<StoreEntry>, container_version: EIoStoreTocVersion, header_version: EIoContainerHeaderVersion, package_version_override: Option<FPackageFileVersion>) -> Result<Self> {

        let package_start_offset = s.stream_position()?;
        let summary: FZenPackageSummary = FZenPackageSummary::deserialize(s, header_version)?;
        let optional_versioning_info: Option<FZenPackageVersioningInfo> = if summary.has_versioning_info != 0 { Some(s.de()?) } else { None };

        let name_map = if header_version > EIoContainerHeaderVersion::Initial {
            FNameMap::deserialize(s, EMappedNameType::Package)?
        } else {
            s.seek(SeekFrom::Start(summary.name_map_names_offset as u64))?;
            let names_buffer: Vec<u8> = s.de_ctx(summary.name_map_names_size as usize)?;
            FNameMap::create_from_names(EMappedNameType::Package, read_name_batch_parts(&names_buffer)?)
        };

        let optional_package_version = optional_versioning_info.as_ref()
            .map(|x| { x.package_file_version })
            .or(package_version_override);

        let has_bulk_data: bool = if let Some(package_version) = optional_package_version.as_ref() {
            package_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32
        } else if header_version >= EIoContainerHeaderVersion::OptionalSegmentPackages {
            // Use the heuristic if we do not have the version data
            let current_start_relative_offset = (s.stream_position()? - package_start_offset) as i32;
            heuristic_zen_has_bulk_data(&summary, header_version, current_start_relative_offset)
        } else {
            // Bulk data did not exist before OptionalSegmentPackages
            false
        };

        // This is enough information to determine the package file version for unversioned zen packages
        let is_unversioned: bool = optional_versioning_info.is_none();
        let versioning_info: FZenPackageVersioningInfo = if optional_versioning_info.is_some() { optional_versioning_info.unwrap() } else {
            heuristic_zen_package_version(optional_package_version, container_version, header_version, has_bulk_data)?
        };

        let bulk_data: Vec<FBulkDataMapEntry> = if has_bulk_data {

            // In 5.4+, there is padding before the bulk data map size
            if versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::PropertyTagCompleteTypeName as i32 {
                let bulk_data_padding: u64 = s.de()?;
                for _ in 0..bulk_data_padding {
                    let _padding: u8 = s.de()?;
                }
            }
            let bulk_data_map_size: i64 = s.de()?;
            let bulk_data_count = bulk_data_map_size as usize / size_of::<FBulkDataMapEntry>();
            s.de_ctx(bulk_data_count)?
        } else { vec!() };

        let imported_public_export_hashes: Vec<u64> = if header_version > EIoContainerHeaderVersion::Initial {
            let imported_public_export_hashes_count = (summary.import_map_offset - summary.imported_public_export_hashes_offset) as usize / size_of::<u64>();
            let imported_public_export_hashes_start_offset = package_start_offset + summary.imported_public_export_hashes_offset as u64;

            s.seek(SeekFrom::Start(imported_public_export_hashes_start_offset))?;
            s.de_ctx(imported_public_export_hashes_count)?
        } else {
            vec![]
        };

        let import_map_count = (summary.export_map_offset - summary.import_map_offset) as usize / size_of::<FPackageObjectIndex>();
        let import_map_start_offset = package_start_offset + summary.import_map_offset as u64;

        s.seek(SeekFrom::Start(import_map_start_offset))?;
        let import_map: Vec<FPackageObjectIndex> = s.de_ctx(import_map_count)?;
        let export_map_count = (summary.export_bundle_entries_offset - summary.export_map_offset) as usize / size_of::<FExportMapEntry>();
        let export_map_start_offset = package_start_offset + summary.export_map_offset as u64;

        s.seek(SeekFrom::Start(export_map_start_offset))?;
        let export_map: Vec<FExportMapEntry> = s.de_ctx(export_map_count)?;

        let mut export_bundle_headers: Vec<FExportBundleHeader> = Vec::new();

        let export_bundle_entries_start_offset = package_start_offset + summary.export_bundle_entries_offset as u64;
        s.seek(SeekFrom::Start(export_bundle_entries_start_offset))?;
        let expected_export_bundle_entries_count = export_map_count * 2; // Each export must have Create and Serialize

        // New style export bundles entries, UE5.0+. Export bundle entries count is derived from the graph data offset
        let export_bundle_entries_count = if header_version >= EIoContainerHeaderVersion::LocalizedPackages {
            let export_bundle_entries_end_offset = if summary.dependency_bundle_headers_offset > 0 { summary.dependency_bundle_headers_offset } else { summary.graph_data_offset };
            (export_bundle_entries_end_offset - summary.export_bundle_entries_offset) as usize / size_of::<FExportBundleEntry>()

        } else {
            // Legacy export bundles, bundle headers followed by bundle entries. UE 4.27 and below. Export bundle entries count is derived from the total entry count of all export bundles
            let store_entry = optional_store_entry.as_ref()
                .ok_or_else(|| { anyhow!("Zen package versions before ImportedPackageNames cannot be parsed without their associated package store entry") })?;

            // In 4.27 there is no cooked serial offset, and export bundle headers are serialized before export bundle data
            export_bundle_headers.reserve(store_entry.export_bundle_count as usize);
            for _ in 0..store_entry.export_bundle_count {
                export_bundle_headers.push(FExportBundleHeader::deserialize(s, header_version)?);
            }
            export_bundle_headers.iter().map(|x| x.entry_count as usize).sum()
        };

        let export_bundle_entries: Vec<FExportBundleEntry> = s.de_ctx(export_bundle_entries_count)?;
        if export_bundle_entries_count != expected_export_bundle_entries_count {
            bail!("Expected to have Create and Serialize commands in export bundle for each export in the package. Got only {} export bundle entries with {} exports", export_bundle_entries_count, export_map_count);
        }

        let mut dependency_bundle_headers: Vec<FDependencyBundleHeader> = Vec::new();
        let mut dependency_bundle_entries: Vec<FDependencyBundleEntry> = Vec::new();
        let mut internal_dependency_arcs: Vec<FInternalDependencyArc> = Vec::new();
        let mut external_package_dependencies: Vec<ExternalPackageDependency> = Vec::new();

        if summary.dependency_bundle_headers_offset > 0 && summary.dependency_bundle_entries_offset > 0 {
            let dependency_bundle_headers_count = (summary.dependency_bundle_entries_offset - summary.dependency_bundle_headers_offset) as usize / size_of::<FDependencyBundleHeader>();
            let dependency_bundle_headers_start_offset = package_start_offset + summary.dependency_bundle_headers_offset as u64;
            if dependency_bundle_headers_count != export_map_count {
                bail!("Expected to have as many dependency bundle headers as the number of exports. Got {} dependency bundle headers for {} exports", dependency_bundle_headers_count, export_map_count);
            }

            s.seek(SeekFrom::Start(dependency_bundle_headers_start_offset))?;
            dependency_bundle_headers = s.de_ctx(dependency_bundle_headers_count)?;

            let dependency_bundle_entries_count = (summary.imported_package_names_offset - summary.dependency_bundle_entries_offset) as usize / size_of::<FDependencyBundleEntry>();
            let dependency_bundle_entries_start_offset = package_start_offset + summary.dependency_bundle_entries_offset as u64;

            s.seek(SeekFrom::Start(dependency_bundle_entries_start_offset))?;
            dependency_bundle_entries = s.de_ctx(dependency_bundle_entries_count)?;
        }
        else if summary.graph_data_offset > 0 {

            let store_entry = optional_store_entry.as_ref()
                .ok_or_else(|| { anyhow!("Zen package versions before ImportedPackageNames cannot be parsed without their associated package store entry") })?;

            let graph_data_start_offset = package_start_offset + summary.graph_data_offset as u64;
            s.seek(SeekFrom::Start(graph_data_start_offset))?;

            // New style graph data, UE5.0+
            if header_version >= EIoContainerHeaderVersion::LocalizedPackages {

                let export_bundles_count = store_entry.export_bundle_count as usize;
                export_bundle_headers.reserve(export_bundles_count);
                for _ in 0..export_bundles_count {
                    export_bundle_headers.push(FExportBundleHeader::deserialize(s, header_version)?);
                }

                internal_dependency_arcs = s.de()?;

                for imported_package_id in &store_entry.imported_packages {
                    let external_arcs: Vec<FExternalDependencyArc> = s.de()?;

                    external_package_dependencies.push(ExternalPackageDependency{
                        from_package_id: *imported_package_id,
                        external_dependency_arcs: external_arcs,
                        legacy_dependency_arcs: Vec::new(),
                    })
                }
            } else {
                // Old style graph data, UE 4.27 and below
                let referenced_package_count: i32 = s.de()?;

                for _ in 0..referenced_package_count {
                    let imported_package_id: FPackageId = s.de()?;
                    let legacy_arcs: Vec<FInternalDependencyArc> = s.de()?;

                    external_package_dependencies.push(ExternalPackageDependency{
                        from_package_id: imported_package_id,
                        external_dependency_arcs: Vec::new(),
                        legacy_dependency_arcs: legacy_arcs,
                    })
                }
            }
        }

        // This is technically not necessary to read, but that data can be used for verification and debugging
        let mut imported_package_names: FZenPackageImportedPackageNamesContainer = FZenPackageImportedPackageNamesContainer::default();
        if summary.imported_package_names_offset > 0 {
            let imported_package_names_start_offset = package_start_offset + summary.imported_package_names_offset as u64;
            s.seek(SeekFrom::Start(imported_package_names_start_offset))?;
            imported_package_names = s.de()?;
        }

        let imported_packages: Vec<FPackageId>;
        let mut shader_map_hashes: Vec<FSHAHash> = Vec::new();

        // Derive information from the store entry directly if it is available
        if let Some(store_entry) = optional_store_entry {
            imported_packages = store_entry.imported_packages;
            shader_map_hashes = store_entry.shader_map_hashes;
        }
        // If we have imported package names, we can derive imported_packages from it. Shader map hashes are empty in that case
        else if summary.imported_package_names_offset > 0 {
            imported_packages = imported_package_names.imported_package_names.iter().map(|x| { FPackageId::from_name(x) }).collect();
        // Package store entry is required to parse this package otherwise
        } else {
            bail!("Zen package versions before ImportedPackageNames cannot be parsed without their associated package store entry");
        }

        Ok(Self{
            summary,
            versioning_info,
            name_map,
            bulk_data,
            imported_public_export_hashes,
            import_map,
            export_map,
            export_bundle_headers,
            export_bundle_entries,
            dependency_bundle_headers,
            dependency_bundle_entries,
            imported_package_names: imported_package_names.imported_package_names,
            imported_packages,
            shader_map_hashes,
            is_unversioned,
            internal_dependency_arcs,
            external_package_dependencies,
            container_header_version: header_version,
        })
    }

    #[instrument(skip_all, name = "FZenPackageHeader")]
    pub(crate) fn serialize<S: Write + Seek>(&self, s: &mut S, store_entry: &mut StoreEntry, container_header_version: EIoContainerHeaderVersion) -> Result<Vec<u64>> {

        let mut package_summary = self.summary;
        package_summary.has_versioning_info = if self.is_unversioned { 0 } else { 1 };

        // Write dummy package summary. We will seek back to it once we have all the data necessary to populate it
        let package_summary_offset = s.stream_position()?;
        FZenPackageSummary::serialize(&package_summary, s, container_header_version)?;

        if container_header_version > EIoContainerHeaderVersion::Initial {
            // Write versioning info if this package is not unversioned
            if package_summary.has_versioning_info != 0 {
                s.ser(&self.versioning_info)?;
            }

            // Serialize name map directly after the package
            self.name_map.serialize(s)?;
        } else {
            // Serialize name map parts separately for legacy packages
            let (names_buffer, hashes_buffer) = write_name_batch_parts(&self.name_map.copy_raw_names())?;

            // Serialize name map names
            package_summary.name_map_names_offset = (s.stream_position()? - package_summary_offset) as i32;
            package_summary.name_map_names_size = names_buffer.len() as i32;
            s.write_all(&names_buffer)?;

            // Write padding so hashes are aligned
            s.write_all(&vec![0; align_usize(names_buffer.len(), 8) - names_buffer.len()])?;

            // Serialize name map hashes
            package_summary.name_map_hashes_offset = (s.stream_position()? - package_summary_offset) as i32;
            package_summary.name_map_hashes_size = hashes_buffer.len() as i32;
            s.write_all(&hashes_buffer)?;
        }

        // Bulk data is only serialized in UE5.2+ packages
        if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32 {

            // In UE5.4+, there is padding before the bulk data map size
            // Padding must ensure that bulk data size starts at 8-byte aligned reader position
            if self.versioning_info.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::PropertyTagCompleteTypeName as i32 {

                let current_writer_position = s.stream_position()? - package_summary_offset;
                let bulk_data_padding: u64 = align_u64(current_writer_position, 8) - current_writer_position;
                s.ser(&bulk_data_padding)?;
                for _ in 0..bulk_data_padding {
                    let padding: u8 = 0;
                    s.ser(&padding)?;
                }
            }

            // Remember the offset of the bulk map data size. We will need to patch it up once we have written the bulk data
            let bulk_data_map_size_offset = s.stream_position()?;
            let mut bulk_data_map_size: i64 = -1;
            s.ser(&bulk_data_map_size)?;
            let pre_bulk_data_map_position = s.stream_position()?;

            // Serialize bulk data map
            for bulk_data_map_entry in &self.bulk_data {
                s.ser(bulk_data_map_entry)?;
            }

            // We know the bulk data map size now, so seek back to its position and write it
            let post_bulk_data_map_position = s.stream_position()?;
            bulk_data_map_size = (post_bulk_data_map_position - pre_bulk_data_map_position) as i64;
            s.seek(SeekFrom::Start(bulk_data_map_size_offset))?;
            s.ser(&bulk_data_map_size)?;

            // Seek back to the end of the writer to continue writing zen asset data
            s.seek(SeekFrom::Start(post_bulk_data_map_position))?;
        }

        if container_header_version > EIoContainerHeaderVersion::Initial {
            // Imported public export hashes start directly after bulk data
            package_summary.imported_public_export_hashes_offset = (s.stream_position()? - package_summary_offset) as i32;
            for public_export_hash in &self.imported_public_export_hashes {
                s.ser(public_export_hash)?;
            }
        }

        // Import map starts directly after imported public hashes
        package_summary.import_map_offset = (s.stream_position()? - package_summary_offset) as i32;
        for import_map_package_index in &self.import_map {
            s.ser(import_map_package_index)?;
        }

        // Export map starts directly after import map
        package_summary.export_map_offset = (s.stream_position()? - package_summary_offset) as i32;
        for export_map_entry in &self.export_map {
            s.ser(export_map_entry)?;
        }

        // Export bundle entries start directly after export map
        package_summary.export_bundle_entries_offset = (s.stream_position()? - package_summary_offset) as i32;

        // For legacy UE4 packages, export bundle headers are written as a part of export bundle entries
        if container_header_version <= EIoContainerHeaderVersion::Initial {
            store_entry.export_bundle_count = self.export_bundle_headers.len() as i32;
            for export_bundle_header in &self.export_bundle_headers {
                FExportBundleHeader::serialize(export_bundle_header, s, self.container_header_version)?;
            }
        }
        // Serialize actual export bundle entries
        for export_bundle_entry in &self.export_bundle_entries {
            s.ser(export_bundle_entry)?;
        }

        // Write imported package IDs and shader map IDs into the store entry
        store_entry.imported_packages = self.imported_packages.clone();
        store_entry.shader_map_hashes = self.shader_map_hashes.clone();
        
        let mut legacy_external_arcs_serialized_offsets: Vec<u64> = Vec::new();

        // Write dependency bundles and imported package names in UE5.3+ zen packages
        if container_header_version >= EIoContainerHeaderVersion::NoExportInfo {

            // Dependency bundle headers start directly after export bundle entries
            package_summary.dependency_bundle_headers_offset = (s.stream_position()? - package_summary_offset) as i32;
            for dependency_bundle_header in &self.dependency_bundle_headers {
                s.ser(dependency_bundle_header)?;
            }

            // Dependency bundle entries start directly after dependency bundle headers
            package_summary.dependency_bundle_entries_offset = (s.stream_position()? - package_summary_offset) as i32;
            for dependency_bundle_entry in &self.dependency_bundle_entries {
                s.ser(dependency_bundle_entry)?;
            }

            // Serialize imported package names. They are not actually read by the game in runtime, but should be preserved
            package_summary.imported_package_names_offset = (s.stream_position()? - package_summary_offset) as i32;
            let imported_package_names = FZenPackageImportedPackageNamesContainer{imported_package_names: self.imported_package_names.clone()};
            s.ser(&imported_package_names)?;
        // Write graph data, which includes dependency bundle headers and arcs
        } else {

            // Write export count for packages with graph data
            store_entry.export_count = self.export_map.len() as i32;

            // Graph data starts directly after export bundle entries
            package_summary.graph_data_offset = (s.stream_position()? - package_summary_offset) as i32;

            // Write dependency arcs in the new style, starting from UE5.0
            if container_header_version > EIoContainerHeaderVersion::Initial {

                // Write export bundle count into the package store entry, and then write export bundle header for each of them
                store_entry.export_bundle_count = self.export_bundle_headers.len() as i32;
                for export_bundle_header in &self.export_bundle_headers {
                    s.ser(export_bundle_header)?;
                }

                // Write internal dependency arcs after export bundle headers
                s.ser(&self.internal_dependency_arcs)?;

                for imported_package_id in &self.imported_packages {

                    // Find all arcs that map to this specific package, and write them
                    let imported_package_arcs: Vec<FExternalDependencyArc> = self.external_package_dependencies.iter()
                        .filter(|x| &x.from_package_id == imported_package_id)
                        .flat_map(|x| x.external_dependency_arcs.iter().cloned())
                        .collect();
                    s.ser(&imported_package_arcs)?;
                }
            } else {
                // Serialize old style package references
                let non_empty_dependencies: Vec<&ExternalPackageDependency> = self.external_package_dependencies.iter()
                    .filter(|x| !x.legacy_dependency_arcs.is_empty())
                    .collect();
                let referenced_package_count: i32 = non_empty_dependencies.len() as i32;
                s.ser(&referenced_package_count)?;

                for package_dependency in non_empty_dependencies {
                    s.ser(&package_dependency.from_package_id)?;

                    // Serialize number of dependency arcs to this package
                    let num_legacy_dependency_arcs: i32 = package_dependency.legacy_dependency_arcs.len() as i32;
                    s.ser(&num_legacy_dependency_arcs)?;

                    // Serialize each individual dependency arc. Track it's serialized position so we can patch it up later
                    for dependency_arc in &package_dependency.legacy_dependency_arcs {
                        let dependency_arc_offset = s.stream_position()? - package_summary_offset;
                        legacy_external_arcs_serialized_offsets.push(dependency_arc_offset);

                        s.ser(&dependency_arc.from_export_bundle_index)?;
                        s.ser(&dependency_arc.to_export_bundle_index)?;
                    }
                }
            }

            // Track the end of the graph data for this package. This is only used and written for legacy UE4 zen packages
            let graph_data_end_offset = (s.stream_position()? - package_summary_offset) as i32;
            package_summary.graph_data_size = graph_data_end_offset - package_summary.graph_data_offset;
        }

        // We know the total size of the zen package header now
        let package_header_end_offset = s.stream_position()?;
        package_summary.header_size = (package_header_end_offset - package_summary_offset) as u32;

        // Go back to the package summary and patch it up with the offsets that we know now, and then seek back
        s.seek(SeekFrom::Start(package_summary_offset))?;
        FZenPackageSummary::serialize(&package_summary, s, container_header_version)?;
        s.seek(SeekFrom::Start(package_header_end_offset))?;

        Ok(legacy_external_arcs_serialized_offsets)
    }
}

#[cfg(test)]
mod test {
    use crate::PackageTestMetadata;

    use super::*;
    use anyhow::Context as _;
    use fs_err as fs;
    use std::{io::BufReader, path::Path};
    use crate::legacy_asset::convert_localized_package_name_to_source;

    #[test]
    fn test_zen_asset_parsing() -> Result<()> {
        let assets = [
            //"tests/UE5.4/BP_Russian_pool_table.uasset",
            //"tests/UE4.27/Table_SignsJanuary.uasset",
            //"tests/UE4.27/T_Emissive.uasset",
            //"tests/UE4.27/BP_SpicyChoppingDamage.uasset",
            //"tests/UE4.27/BP_Aphid_Meat_Prop.uasset",
            //"tests/UE4.27/EQ_StaticObstacleTargetLocation.uasset",
            "tests/UE4.27/SPR_UI_Battle.uasset",
        ];
        for asset in assets {
            run_parse(Path::new(asset)).context(asset)?;
        }
        Ok(())
    }

    fn run_parse(path: &Path) -> Result<()> {
        let mut stream = BufReader::new(fs::File::open(path)?);
        let metadata = serde_json::from_slice::<PackageTestMetadata>(&fs::read(path.with_extension("metadata.json"))?)?;

        let header = FZenPackageHeader::deserialize(&mut stream, metadata.store_entry, metadata.toc_version, metadata.container_header_version, metadata.package_file_version)?;
        let package_name = header.package_name();

        //assert_eq!(package_name, "/Game/Billiards/Blueprints/BP_Russian_pool_table");
        //assert_eq!(header.name_map.get(header.export_map[5].object_name), "SCS_Node_10");

        dbg!(package_name.clone());
        dbg!(convert_localized_package_name_to_source(&package_name));
        dbg!(header.source_package_name());
        //dbg!(header);
        Ok(())
    }
}
