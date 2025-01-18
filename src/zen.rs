use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use anyhow::{anyhow, bail, Result};
use strum::FromRepr;
use tracing::instrument;

use crate::name_map::read_name_batch;
use crate::script_objects::FPackageObjectIndex;
use crate::ser::{WriteExt, Writeable};
use crate::{name_map::{FMappedName, FNameMap}, EIoStoreTocVersion, FGuid, FPackageId, FSHAHash, ReadExt, Readable};
use crate::container_header::{EIoContainerHeaderVersion, StoreEntry};
use crate::version_heuristics::{heuristic_zen_has_bulk_data, heuristic_zen_package_version};

pub(crate) fn get_package_name(data: &[u8], container_header_version: EIoContainerHeaderVersion) -> Result<String> {
    FZenPackageHeader::get_package_name(&mut Cursor::new(data), container_header_version)
}

// just enough to get PackageName
pub(crate) struct FPackageFileSummary {
    pub(crate) tag: u32,
    pub(crate) file_version_ue4: u32,
    pub(crate) file_version_ue5: Option<u32>,
    pub(crate) total_header_size: u32,
    pub(crate) package_name: String,
}
impl Readable for FPackageFileSummary {
    #[instrument(skip_all, name = "FPackageFileSummary")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let tag = s.de()?;
        assert_eq!(tag, 0x9e2a83c1);

        let legacy_file_version: i32 = s.de()?;

        if legacy_file_version != -4 {
            let _legacy_ue3_version: i32 = s.de()?;
        }
        let file_version_ue4 = s.de()?;
        assert_eq!(file_version_ue4, 0);
        let file_version_ue5 = if legacy_file_version <= -8 {
            Some(s.de()?)
        } else {
            None
        };
        assert_eq!(file_version_ue5, Some(0));
        let _file_version_licensee_ue: u32 = s.de()?;
        if legacy_file_version <= -2 {
            let custom_versions: u32 = s.de()?; // empty (unversioned)
            assert_eq!(custom_versions, 0);
        }

        let total_header_size = s.de()?;
        let package_name = s.de()?;

        Ok(Self {
            tag,
            file_version_ue4,
            file_version_ue5,
            total_header_size,
            package_name,
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct FZenPackageSummary {
    pub(crate) has_versioning_info: u32,
    pub(crate) header_size: u32,
    pub(crate) name: FMappedName,
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
}
impl FZenPackageSummary {
    #[instrument(skip_all, name = "FZenPackageSummary")]
    fn deserialize<S: Read>(s: &mut S, container_header_version: EIoContainerHeaderVersion) -> Result<Self> {

        let has_versioning_info: u32 = s.de()?;
        let header_size: u32 = s.de()?;
        let name: FMappedName = s.de()?;
        let package_flags: u32 = s.de()?;
        let cooked_header_size: u32 = s.de()?;
        let imported_public_export_hashes_offset: i32 = s.de()?;
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

        Ok(Self{has_versioning_info, header_size, name, package_flags, cooked_header_size, imported_public_export_hashes_offset,
            import_map_offset, export_map_offset, export_bundle_entries_offset, graph_data_offset,
            dependency_bundle_headers_offset, dependency_bundle_entries_offset, imported_package_names_offset})
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, FromRepr)]
#[repr(u32)]
pub(crate) enum EZenPackageVersion {
    Initial,
    DataResourceTable,
    ImportedPackageNames,
    ExtraDependencies,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
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
        Ok({})
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
        Ok({})
    }
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug)]
#[repr(C)] // Needed to determine the number of bulk data entries
pub(crate) struct FBulkDataMapEntry {
    pub(crate) serial_offset: i64,
    pub(crate) duplicate_serial_offset: i64,
    pub(crate) serial_size: i64,
    pub(crate) flags: u32,
    pad: u32,
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

#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
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

#[derive(Debug, Clone)]
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
    padding: [u8; 3],
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

#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
#[repr(u32)]
pub(crate) enum EExportCommandType {
    Create,
    Serialize
}
#[derive(Debug, Copy, Clone)]
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

#[derive(Debug, Copy, Clone, Default)]
#[repr(C)] // Needed to determine the number of bundle headers
pub(crate) struct FDependencyBundleHeader
{
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

    pub(crate) fn to_import_index(&self) -> u32 {
        assert!(self.index < 0);
        (-self.index - 1) as u32
    }
    pub(crate) fn to_export_index(&self) -> u32 {
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

#[derive(Debug)]
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

// Actual UE type name not known, type layout from AsyncLoading2.cpp SetupSerializedArcs on 5.2.2
#[derive(Debug, Copy, Clone, Default)]
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

// Actual UE type name not known, type layout from AsyncLoading2.cpp SetupSerializedArcs on 5.2.2
#[derive(Debug, Copy, Clone)]
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
#[derive(Debug, Clone)]
pub(crate) struct FImportedPackageDependency {
    pub(crate) dependency_arcs: Vec<FExternalDependencyArc>,
}
impl Readable for FImportedPackageDependency {
    #[instrument(skip_all, name = "FImportedPackageDependency")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{dependency_arcs: s.de()?})
    }
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
impl Readable for FExportBundleHeader {
    #[instrument(skip_all, name = "FExportBundleHeader")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            serial_offset: s.de()?,
            first_entry_index: s.de()?,
            entry_count: s.de()?,
        })
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

#[derive(Debug)]
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
    pub(crate) imported_package_dependencies: Vec<FImportedPackageDependency>,
}
impl FZenPackageHeader {
    pub(crate) fn package_name(&self) -> String {
        self.name_map.get(self.summary.name).to_string()
    }

    // Retrieves the package name from the package. Does the bare minimum of package reading to get the name out
    #[instrument(skip_all, name = "FZenPackageHeader - GetPackageName")]
    pub(crate) fn get_package_name<S: Read>(s: &mut S, container_header_version: EIoContainerHeaderVersion) -> Result<String> {
        let summary: FZenPackageSummary = FZenPackageSummary::deserialize(s, container_header_version)?;
        let _versioning_info: Option<FZenPackageVersioningInfo> = if summary.has_versioning_info != 0 { Some(s.de()?) } else { None };
        let name_map: FNameMap = s.de()?;
        Ok(name_map.get(summary.name).to_string())
    }

    #[instrument(skip_all, name = "FZenPackageHeader")]
    pub(crate) fn deserialize<S: Read + Seek>(s: &mut S, store_entry: StoreEntry, container_version: EIoStoreTocVersion, header_version: EIoContainerHeaderVersion, package_version_override: Option<FPackageFileVersion>) -> Result<Self> {

        let package_start_offset = s.stream_position()?;
        let summary: FZenPackageSummary = FZenPackageSummary::deserialize(s, header_version)?;
        let optional_versioning_info: Option<FZenPackageVersioningInfo> = if summary.has_versioning_info != 0 { Some(s.de()?) } else { None };
        let name_map: FNameMap = s.de()?;

        let optional_package_version = optional_versioning_info.as_ref()
            .map(|x| { x.package_file_version })
            .or_else(|| { package_version_override });

        let has_bulk_data: bool = if let Some(package_version) = optional_package_version.as_ref() {
            package_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32
        } else {
            // Use the heuristic if we do not have the version data
            let current_start_relative_offset = (s.stream_position()? - package_start_offset) as i32;
            heuristic_zen_has_bulk_data(&summary, header_version, current_start_relative_offset)
        };

        // This is enough information to determine the package file version for unversioned zen packages
        let is_unversioned: bool = optional_versioning_info.is_none();
        let versioning_info: FZenPackageVersioningInfo = if optional_versioning_info.is_some() { optional_versioning_info.unwrap() } else {
            heuristic_zen_package_version(optional_package_version, container_version, header_version, has_bulk_data)?
        };

        let bulk_data: Vec<FBulkDataMapEntry> = if has_bulk_data {
            let bulk_data_map_size: i64 = s.de()?;
            let bulk_data_count = bulk_data_map_size as usize / size_of::<FBulkDataMapEntry>();
            s.de_ctx(bulk_data_count)?
        } else { vec!() };

        let imported_public_export_hashes_count = (summary.import_map_offset - summary.imported_public_export_hashes_offset) as usize / size_of::<u64>();
        let imported_public_export_hashes_start_offset = package_start_offset + summary.imported_public_export_hashes_offset as u64;

        s.seek(SeekFrom::Start(imported_public_export_hashes_start_offset))?;
        let imported_public_export_hashes: Vec<u64> = s.de_ctx(imported_public_export_hashes_count)?;

        let import_map_count = (summary.export_map_offset - summary.import_map_offset) as usize / size_of::<FPackageObjectIndex>();
        let import_map_start_offset = package_start_offset + summary.import_map_offset as u64;

        s.seek(SeekFrom::Start(import_map_start_offset))?;
        let import_map: Vec<FPackageObjectIndex> = s.de_ctx(import_map_count)?;
        let export_map_count = (summary.export_bundle_entries_offset - summary.export_map_offset) as usize / size_of::<FExportMapEntry>();
        let export_map_start_offset = package_start_offset + summary.export_map_offset as u64;

        s.seek(SeekFrom::Start(export_map_start_offset))?;
        let export_map: Vec<FExportMapEntry> = s.de_ctx(export_map_count)?;

        let export_bundle_entries_end_offset = if summary.dependency_bundle_headers_offset > 0 { summary.dependency_bundle_headers_offset } else { summary.graph_data_offset };
        let export_bundle_entries_count = (export_bundle_entries_end_offset - summary.export_bundle_entries_offset) as usize / size_of::<FExportBundleEntry>();
        let export_bundle_entries_start_offset = package_start_offset + summary.export_bundle_entries_offset as u64;
        let expected_export_bundle_entries_count = export_map_count * 2; // Each export must have Create and Serialize
        if export_bundle_entries_count != expected_export_bundle_entries_count {
            bail!("Expected to have Create and Serialize commands in export bundle for each export in the package. Got only {} export bundle entries with {} exports", export_bundle_entries_count, export_map_count);
        }

        s.seek(SeekFrom::Start(export_bundle_entries_start_offset))?;
        let export_bundle_entries: Vec<FExportBundleEntry> = s.de_ctx(export_bundle_entries_count)?;

        let mut dependency_bundle_headers: Vec<FDependencyBundleHeader> = Vec::new();
        let mut dependency_bundle_entries: Vec<FDependencyBundleEntry> = Vec::new();
        let mut internal_dependency_arcs: Vec<FInternalDependencyArc> = Vec::new();
        let mut imported_package_dependencies: Vec<FImportedPackageDependency> = Vec::new();
        let mut export_bundle_headers: Vec<FExportBundleHeader> = Vec::new();

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

            let graph_data_start_offset = package_start_offset + summary.graph_data_offset as u64;
            s.seek(SeekFrom::Start(graph_data_start_offset))?;

            let export_bundles_count = store_entry.export_bundle_count as usize;
            export_bundle_headers = s.de_ctx(export_bundles_count)?;

            internal_dependency_arcs = s.de()?;

            let external_packages_count = store_entry.imported_packages.len();
            imported_package_dependencies = s.de_ctx(external_packages_count)?;
        }

        // This is technically not necessary to read, but that data can be used for verification and debugging
        let mut imported_package_names: Vec<String> = Vec::new();
        if summary.imported_package_names_offset > 0 {
            let imported_package_names_start_offset = package_start_offset + summary.imported_package_names_offset as u64;
            s.seek(SeekFrom::Start(imported_package_names_start_offset))?;

            imported_package_names = read_name_batch(s)?;

            let imported_package_name_numbers: Vec<i32> = s.de_ctx(imported_package_names.len())?;
            for (index, item) in imported_package_names.iter_mut().enumerate() {
                if imported_package_name_numbers[index] != 0 {
                    *item = format!("{item}_{}", imported_package_name_numbers[index] - 1)
                }
            }
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
            imported_package_names,
            imported_packages: store_entry.imported_packages,
            shader_map_hashes: store_entry.shader_map_hashes,
            is_unversioned,
            internal_dependency_arcs,
            imported_package_dependencies,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use fs_err as fs;
    use std::io::BufReader;

    #[test]
    fn test_zen_asset_parsing() -> Result<()> {
        let mut stream = BufReader::new(fs::File::open(
            "tests/UE5.4/BP_Russian_pool_table.uasset",
        )?);

        let header = ser_hex::read("out/zen_asset_parsing.trace.json", &mut stream, |x| {
            FZenPackageHeader::deserialize(x, StoreEntry::default(), EIoStoreTocVersion::OnDemandMetaData, EIoContainerHeaderVersion::NoExportInfo, None)
        })?;
        let package_name = header.package_name();

        assert_eq!(package_name, "/Game/Billiards/Blueprints/BP_Russian_pool_table");
        assert_eq!(header.name_map.get(header.export_map[5].object_name), "SCS_Node_10");

        //dbg!(package_name);
        //dbg!(header);
        Ok(())
    }
}
