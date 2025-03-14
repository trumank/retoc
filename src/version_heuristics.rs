use crate::container_header::EIoContainerHeaderVersion;
use crate::legacy_asset::{EPackageFlags, FLegacyPackageFileSummary, FLegacyPackageHeader, FObjectExport, FObjectImport, ENGINE_PACKAGE_NAME};
use crate::zen::{EUnrealEngineObjectUE4Version, EUnrealEngineObjectUE5Version, EZenPackageVersion, FPackageFileVersion, FZenPackageSummary, FZenPackageVersioningInfo};
use crate::EIoStoreTocVersion;
use anyhow::{anyhow, bail};
use std::io::{Cursor, Read, Seek, SeekFrom};

// Returns true if given zen package should deserialize bulk data
pub(crate) fn heuristic_zen_has_bulk_data(summary: &FZenPackageSummary, container_header_version: EIoContainerHeaderVersion, current_reader_pos: i32) -> bool {
    // Otherwise, we can check if we have bulk data by the following intrinsics
    // Bulk data is always present if container header version is EIoContainerHeaderVersion::NoExportInfo or above (UE 5.3+)
    // Bulk data is never present if container header version is below EIoContainerHeaderVersion::OptionalSegmentPackages (UE 5.1)
    // So the only versions we need to be able to tell apart are 5.1 and 5.2, in 5.2 case data is present and in 5.1 case it is not
    if container_header_version < EIoContainerHeaderVersion::OptionalSegmentPackages { return false }
    if container_header_version >= EIoContainerHeaderVersion::NoExportInfo { return true }

    // If we have no bulk data, imported public export hashes will start immediately at the current offset. Otherwise, we will have uint64 there telling us the size of the bulk data
    summary.imported_public_export_hashes_offset > current_reader_pos
}

// Derives zen package file version from package file version and container header version
pub(crate) fn heuristic_zen_version_from_package_file_version(package_file_version: FPackageFileVersion, container_header_version: EIoContainerHeaderVersion) -> EZenPackageVersion {
    // UE 4.27, 5.0 and 5.1: initial
    if package_file_version.file_version_ue5 <= EUnrealEngineObjectUE5Version::AddSoftObjectPathList as i32 {
        EZenPackageVersion::Initial
    // Can be either 5.2 or 5.3, cannot tell apart from just package file version. Need to look at container header as well
    } else if package_file_version.file_version_ue5 <= EUnrealEngineObjectUE5Version::DataResources as i32 {
        // UE 5.2: data resource table
        if container_header_version < EIoContainerHeaderVersion::NoExportInfo {
            EZenPackageVersion::DataResourceTable
        // UE 5.3: extra dependencies
        } else {
            EZenPackageVersion::ExtraDependencies
        }
    // UE 5.4+: extra dependencies
    } else {
        EZenPackageVersion::ExtraDependencies
    }
}

// Establishes a zen package version from the provided package version hint and information from the container
pub(crate) fn heuristic_zen_package_version(optional_package_version: Option<FPackageFileVersion>, container_version: EIoStoreTocVersion, container_header_version: EIoContainerHeaderVersion, has_bulk_data: bool) -> anyhow::Result<FZenPackageVersioningInfo> {

    // Establish a package file version from the hint or from the provided metadata
    let package_file_version: FPackageFileVersion = optional_package_version.or_else(|| {
        if container_header_version <= EIoContainerHeaderVersion::Initial {
            // 4.26, 4.27 are Initial. their zen and package file versions are identical
            Some(FPackageFileVersion::create_ue4(EUnrealEngineObjectUE4Version::CorrectLicenseeFlag))
        } else if container_header_version == EIoContainerHeaderVersion::LocalizedPackages {
            Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::LargeWorldCoordinates))
        } else if container_header_version == EIoContainerHeaderVersion::OptionalSegmentPackages {
            // 5.1 does not have bulk data, 5.2 does
            if !has_bulk_data {
                Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::AddSoftObjectPathList))
            } else {
                Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::DataResources))
            }
        } else if container_header_version == EIoContainerHeaderVersion::NoExportInfo {
            // 5.4 has EIoStoreTocVersion::OnDemandMetaData, 5.3 does not
            if container_version < EIoStoreTocVersion::OnDemandMetaData {
                Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::DataResources))
            } else {
                Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::PropertyTagCompleteTypeName))
            }
        } else if container_header_version == EIoContainerHeaderVersion::SoftPackageReferences {
            // 5.5 has EIoStoreTocVersion::ReplaceIoChunkHashWithIoHash, if it's a different UE version assume we cannot use the heuristic
            if container_version == EIoStoreTocVersion::ReplaceIoChunkHashWithIoHash {
                Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::AssetRegistryPackageBuildDependencies))
            } else { None }
        } else { None }
    }).ok_or_else(|| { anyhow!("Failed to derive the UE package version from container version and header version. Please provide the engine version manually") })?;

    // Derive zen package version from engine version
    let zen_version: EZenPackageVersion = heuristic_zen_version_from_package_file_version(package_file_version, container_header_version);

    // Assume 0 for licensee version and no custom versions, they are not relevant for the package header serialization
    Ok(FZenPackageVersioningInfo{
        zen_version, package_file_version,
        licensee_version: 0,
        custom_versions: Vec::new()
    })
}

// Attempts to derive a package file version suitable for reading the provided package
pub(crate) fn heuristic_package_version_from_legacy_package<S: Read + Seek>(s: &mut S, package_version_fallback: Option<FPackageFileVersion>) -> anyhow::Result<FPackageFileVersion> {

    let stream_start_position = s.stream_position()?;

    // Read the members that are independent on the package version
    let (versioning_info, names, _, _, package_flags) = FLegacyPackageFileSummary::deserialize_summary_minimal_version_independent(s)?;
    s.seek(SeekFrom::Start(stream_start_position))?;

    // If package is versioned, deserialize the header directly using the package version
    if !versioning_info.is_unversioned {
        return Ok(versioning_info.package_file_version);
    }
    // If package is unversioned, but we have a fallback package version, serialize with it directly
    if let Some(package_version_fallback) = package_version_fallback {
        return Ok(package_version_fallback);
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
    s.read_exact(&mut header_read_payload)?;

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
        let package_summary_or_error: anyhow::Result<FLegacyPackageFileSummary> = FLegacyPackageFileSummary::deserialize(&mut read_cursor, Some(package_version));
        let current_cursor_position = read_cursor.position() as usize;

        // If we failed to read the package summary, try again with another version
        // Make sure we have read the entire header before declaring this a success
        if package_summary_or_error.is_err() || current_cursor_position != header_size {
            continue
        }
        let package_summary: FLegacyPackageFileSummary = package_summary_or_error?;

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
            let first_import: anyhow::Result<FObjectImport> = FObjectImport::deserialize(&mut first_import_cursor, &package_summary);
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
            let first_export: anyhow::Result<FObjectExport> = FObjectExport::deserialize(&mut first_export_cursor, &package_summary);
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

// Returns true if the preload dependency on the given import can be downgraded to the Create dependency (from Serialize dependency)
pub(crate) fn heuristic_can_downgrade_import_preload_dependency_generic(package: &FLegacyPackageHeader, from_import_index: usize) -> bool {
    let object_import = package.imports[from_import_index].clone();
    let class_package = package.name_map.get(object_import.class_package).to_string();
    let class_name = package.name_map.get(object_import.class_name).to_string();

    // Allow downgrading references to data tables from any asset to create
    class_package == ENGINE_PACKAGE_NAME && (class_name == "DataTable" || class_name == "CompositeDataTable")
}
