use crate::EIoStoreTocVersion;
use crate::container_header::EIoContainerHeaderVersion;
use crate::legacy_asset::{EPackageFlags, FLegacyPackageFileSummary, FObjectExport, FObjectImport};
use crate::zen::{EUnrealEngineObjectUE4Version, EUnrealEngineObjectUE5Version, EZenPackageVersion, FPackageFileVersion, FZenPackageSummary, FZenPackageVersioningInfo};
use anyhow::{anyhow, bail};
use std::io::{Cursor, Read, Seek, SeekFrom};

// Returns true if given zen package should deserialize bulk data
pub(crate) fn heuristic_zen_has_bulk_data(summary: &FZenPackageSummary, container_header_version: EIoContainerHeaderVersion, current_reader_pos: i32) -> bool {
    // Otherwise, we can check if we have bulk data by the following intrinsics
    // Bulk data is always present if container header version is EIoContainerHeaderVersion::NoExportInfo or above (UE 5.3+)
    // Bulk data is never present if container header version is below EIoContainerHeaderVersion::OptionalSegmentPackages (UE 5.1)
    // So the only versions we need to be able to tell apart are 5.1 and 5.2, in 5.2 case data is present and in 5.1 case it is not
    if container_header_version < EIoContainerHeaderVersion::OptionalSegmentPackages {
        return false;
    }
    if container_header_version >= EIoContainerHeaderVersion::NoExportInfo {
        return true;
    }

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
            EZenPackageVersion::ExportDependencies
        }
    // UE 5.4+: extra dependencies
    } else {
        EZenPackageVersion::ExportDependencies
    }
}

// Establishes a zen package version from the provided package version hint and information from the container
pub(crate) fn heuristic_zen_package_version(optional_package_version: Option<FPackageFileVersion>, container_version: EIoStoreTocVersion, container_header_version: EIoContainerHeaderVersion, has_bulk_data: bool) -> anyhow::Result<FZenPackageVersioningInfo> {
    // Establish a package file version from the hint or from the provided metadata
    let package_file_version: FPackageFileVersion = optional_package_version
        .or_else(|| {
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
                } else {
                    None
                }
            } else if container_header_version == EIoContainerHeaderVersion::SoftPackageReferencesOffset {
                // 5.6 is OsSubObjectShadowSerialization
                Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::OsSubObjectShadowSerialization))
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow!("Failed to derive the UE package version from container version and header version. Please provide the engine version manually"))?;

    // Derive zen package version from engine version
    let zen_version: EZenPackageVersion = heuristic_zen_version_from_package_file_version(package_file_version, container_header_version);

    // Assume 0 for licensee version and no custom versions, they are not relevant for the package header serialization
    Ok(FZenPackageVersioningInfo {
        zen_version,
        package_file_version,
        licensee_version: 0,
        custom_versions: Vec::new(),
    })
}

// Attempts to derive a package file version suitable for reading the provided package
pub(crate) fn heuristic_package_version_from_legacy_package<S: Read + Seek>(s: &mut S, package_version_fallback: Option<FPackageFileVersion>) -> anyhow::Result<FPackageFileVersion> {
    let stream_start_position = s.stream_position()?;

    // Read the members that are independent on the package version
    let versioning_info = FLegacyPackageFileSummary::deserialize_summary_minimal_version_independent(s)?;
    s.seek(SeekFrom::Start(stream_start_position))?;

    // If package is versioned, deserialize the header directly using the package version
    if !versioning_info.is_unversioned {
        return Ok(versioning_info.package_file_version);
    }
    // If package is unversioned, but we have a fallback package version, serialize with it directly
    if let Some(package_version_fallback) = package_version_fallback {
        return Ok(package_version_fallback);
    }
    Err(anyhow!("Failed to derive package file version from the package. Please provide an explicit package version to deserialize this unversioned package"))
}
