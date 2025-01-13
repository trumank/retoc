use std::io::{Cursor, Read, Seek, SeekFrom};

use anyhow::Result;
use strum::FromRepr;
use tracing::instrument;

use crate::{name_map::{FMinimalName, FNameMap}, FGuid, ReadExt, Readable};
use crate::name_map::read_name_batch;
use crate::script_objects::FPackageObjectIndex;
use crate::ser::ReadableCtx;

pub(crate) fn get_package_name(data: &[u8]) -> Result<String> {
    let header: FZenPackageHeader = FZenPackageHeader::deserialize(&mut Cursor::new(data))?;
    Ok(header.name_map.get(header.summary.name).to_string())
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

        let legacy_file_version: i32 = dbg!(s.de()?);

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

#[derive(Debug)]
pub(crate) struct FZenPackageSummary {
    has_versioning_info: u32,
    header_size: u32,
    name: FMinimalName,
    package_flags: u32,
    cooked_header_size: u32,
    imported_public_export_hashes_offset: i32,
    import_map_offset: i32,
    export_map_offset: i32,
    export_bundle_entries_offset: i32,
    dependency_bundle_headers_offset: i32,
    dependency_bundle_entries_offset: i32,
    imported_package_names_offset: i32,
}
impl Readable for FZenPackageSummary {
    #[instrument(skip_all, name = "FZenPackageSummary")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            has_versioning_info: s.de()?,
            header_size: s.de()?,
            name: s.de()?,
            package_flags: s.de()?,
            cooked_header_size: s.de()?,
            imported_public_export_hashes_offset: s.de()?,
            import_map_offset: s.de()?,
            export_map_offset: s.de()?,
            export_bundle_entries_offset: s.de()?,
            dependency_bundle_headers_offset: s.de()?,
            dependency_bundle_entries_offset: s.de()?,
            imported_package_names_offset: s.de()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
#[repr(u32)]
enum EZenPackageVersion {
    Initial,
    DataResourceTable,
    ImportedPackageNames,
    ExtraDependencies,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct FPackageFileVersion
{
    file_version_ue4: i32,
    file_version_ue5: i32,
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

#[derive(Debug, Copy, Clone, PartialEq, Default)]
struct FCustomVersion
{
    key: FGuid,
    version: i32,
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

#[derive(Debug, Clone, PartialEq)]
struct FZenPackageVersioningInfo
{
    zen_version: EZenPackageVersion,
    package_file_version: FPackageFileVersion,
    licensee_version: i32,
    custom_versions: Vec<FCustomVersion>,
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
    serial_offset: i64,
    duplicate_serial_offset: i64,
    serial_size: i64,
    flags: u32,
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

#[derive(Debug)]
#[repr(C)] // Needed to determine the number of export entries
pub(crate) struct FExportMapEntry {
    cooked_serial_offset: u64,
    cooked_serial_size: u64,
    object_name: FMinimalName,
    outer_index: FPackageObjectIndex,
    class_index: FPackageObjectIndex,
    super_index: FPackageObjectIndex,
    template_index: FPackageObjectIndex,
    public_export_hash: u64,
    object_flags: u32,
    filter_flags: u8,
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
            filter_flags: s.de()?,
            padding: s.de()?
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
#[repr(u32)]
enum EExportCommandType {
    Create,
    Serialize
}
#[derive(Debug)]
#[repr(C)] // Needed to determine the number of export bundle entries
struct FExportBundleEntry {
    local_export_index: u32,
    command_type: EExportCommandType,
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

#[derive(Debug)]
#[repr(C)] // Needed to determine the number of bundle headers
struct FDependencyBundleHeader
{
    first_entry_index: i32,
    // Note that this is defined as uint32 EntryCount[ExportCommandType_Count][ExportCommandType_Count], but this is a really awkward definition to work with,
    // so here it is defined as 4 individual properties: [Create][Create], [Create][Serialize], [Serialize][Create] and [Serialize][Serialize]
    create_before_create_dependencies: u32,
    create_before_serialize_dependencies: u32,
    serialize_before_create_dependencies: u32,
    serialize_before_serialize_dependencies: u32,
}
impl Readable for FDependencyBundleHeader {
    #[instrument(skip_all, name = "FDependencyBundleHeader")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            first_entry_index: s.de()?,
            create_before_create_dependencies: s.de()?,
            create_before_serialize_dependencies: s.de()?,
            serialize_before_create_dependencies: s.de()?,
            serialize_before_serialize_dependencies: s.de()?,
        })
    }
}

#[derive(Debug)]
#[repr(C)] // Needed to determine the number of bundle entries
struct FDependencyBundleEntry {
    local_import_or_export_index: i32,
}
impl Readable for FDependencyBundleEntry {
    #[instrument(skip_all, name = "FDependencyBundleEntry")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self{
            local_import_or_export_index: s.de()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, FromRepr)]
#[repr(i32)]
enum EUnrealEngineObjectUE5Version {
    DataResources = 1009,
}

#[derive(Debug)]
pub(crate) struct FZenPackageHeader {
    summary: FZenPackageSummary,
    versioning_info: Option<FZenPackageVersioningInfo>,
    name_map: FNameMap,
    bulk_data: Vec<FBulkDataMapEntry>,
    imported_public_export_hashes: Vec<u64>,
    import_map: Vec<FPackageObjectIndex>,
    export_map: Vec<FExportMapEntry>,
    export_bundle_entries: Vec<FExportBundleEntry>,
    dependency_bundle_headers: Vec<FDependencyBundleHeader>,
    dependency_bundle_entries: Vec<FDependencyBundleEntry>,
    imported_package_names: Vec<String>,
}
impl FZenPackageHeader {
    #[instrument(skip_all, name = "FZenPackageHeader")]
    pub(crate) fn deserialize<S: Read + Seek>(s: &mut S) -> Result<Self> {

        let package_start_offset = s.stream_position()?;
        let summary: FZenPackageSummary = s.de()?;
        let versioning_info: Option<FZenPackageVersioningInfo> = if summary.has_versioning_info != 0 { Some(s.de()?) } else { None };
        let name_map: FNameMap = s.de()?;

        let has_bulk_data = versioning_info.as_ref().map(|x| x.package_file_version.file_version_ue5 >= EUnrealEngineObjectUE5Version::DataResources as i32).unwrap_or(true);
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

        let export_bundle_entries_count = (summary.dependency_bundle_headers_offset - summary.export_bundle_entries_offset) as usize / size_of::<FExportBundleEntry>();
        let export_bundle_entries_start_offset = package_start_offset + summary.dependency_bundle_headers_offset as u64;
        let expected_export_bundle_entries_count = export_map_count * 2; // Each export must have Create and Serialize
        assert_eq!(export_bundle_entries_count, expected_export_bundle_entries_count, "Expected to have Create and Serialize commands in export bundle for each export in the package. Got only {} export bundle entries with {} exports",
            export_bundle_entries_count, export_map_count);

        s.seek(SeekFrom::Start(export_bundle_entries_start_offset))?;
        let export_bundle_entries: Vec<FExportBundleEntry> = s.de_ctx(export_bundle_entries_count)?;

        let dependency_bundle_headers_count = (summary.dependency_bundle_entries_offset - summary.dependency_bundle_headers_offset) as usize / size_of::<FDependencyBundleHeader>();
        let dependency_bundle_headers_start_offset = package_start_offset + summary.dependency_bundle_headers_offset as u64;
        assert_eq!(dependency_bundle_headers_count, export_map_count, "Expected to have as many dependency bundle headers as the number of exports. Got {} dependency bundle headers for {} exports", dependency_bundle_headers_count, export_map_count);

        s.seek(SeekFrom::Start(dependency_bundle_headers_start_offset))?;
        let dependency_bundle_headers = s.de_ctx(dependency_bundle_headers_count)?;

        let dependency_bundle_entries_count = (summary.imported_package_names_offset - summary.dependency_bundle_entries_offset) as usize / size_of::<FDependencyBundleEntry>();
        let dependency_bundle_entries_start_offset = package_start_offset + summary.dependency_bundle_entries_offset as u64;

        s.seek(SeekFrom::Start(dependency_bundle_entries_start_offset))?;
        let dependency_bundle_entries = s.de_ctx(dependency_bundle_entries_count)?;

        // This is technically not necessary to read, but that data can be used for verification and debugging
        let imported_package_names_start_offset = package_start_offset + summary.imported_package_names_offset as u64;
        s.seek(SeekFrom::Start(imported_package_names_start_offset))?;

        let mut imported_package_names: Vec<String> = read_name_batch(s)?;
        let imported_package_name_numbers: Vec<i32> = s.de_ctx(imported_package_names.len())?;
        for (index, item) in imported_package_names.iter_mut().enumerate() {
            if imported_package_name_numbers[index] != 0 {
                *item = format!("{item}_{}", imported_package_name_numbers[index] - 1)
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
            export_bundle_entries,
            dependency_bundle_headers,
            dependency_bundle_entries,
            imported_package_names,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{fs::File, io::BufReader};

    #[test]
    fn test_zen() -> Result<()> {
        let mut stream = BufReader::new(File::open(
            //"zen_out/AbioticFactor/Content/Audio/Abiotic_Dialog_NarrativeNPC.uasset",
            "bad.uasset",
        )?);

        let header = ser_hex::read("trace.json", &mut stream, FZenPackageHeader::de)?;
        header.name_map.get(header.summary.name);

        //dbg!(field);

        Ok(())
    }
}
