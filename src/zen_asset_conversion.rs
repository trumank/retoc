use crate::container_header::EIoContainerHeaderVersion;
use crate::legacy_asset::FLegacyPackageHeader;
use crate::name_map::{EMappedNameType, FNameMap};
use crate::version_heuristics::heuristic_zen_version_from_package_file_version;
use crate::zen::{EZenPackageVersion, FBulkDataMapEntry, FZenPackageHeader, FZenPackageVersioningInfo};

struct ZenPackageBuilder {
    legacy_package: FLegacyPackageHeader,
    zen_package: FZenPackageHeader,
    container_header_version: EIoContainerHeaderVersion,
}

fn create_asset_builder(package: FLegacyPackageHeader, container_header_version: EIoContainerHeaderVersion) -> ZenPackageBuilder {
    ZenPackageBuilder{legacy_package: package, zen_package: FZenPackageHeader::default(), container_header_version}
}

fn setup_package_summary(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {

    let is_unversioned = builder.legacy_package.summary.versioning_info.is_unversioned;

    // Copy package flags
    builder.zen_package.summary.package_flags = builder.legacy_package.summary.package_flags;

    // Copy versioning info from the package, except the zen version, which is derived from package file version
    let zen_version: EZenPackageVersion = heuristic_zen_version_from_package_file_version(
        builder.legacy_package.summary.versioning_info.package_file_version, builder.container_header_version);

    builder.zen_package.is_unversioned = is_unversioned;
    builder.zen_package.versioning_info = FZenPackageVersioningInfo{
        zen_version,
        package_file_version: builder.legacy_package.summary.versioning_info.package_file_version,
        licensee_version: builder.legacy_package.summary.versioning_info.licensee_version,
        custom_versions: builder.legacy_package.summary.versioning_info.custom_versions.clone(),
    };

    // Copy name map from the cooked package up to the number of names referenced by exports
    // We do not actually need the rest of the name map
    let name_map_size = builder.legacy_package.summary.names_referenced_from_export_data_count as usize;
    let name_map_slice = builder.legacy_package.name_map.copy_raw_names()[0..name_map_size].to_vec();
    builder.zen_package.name_map = FNameMap::create_from_names(EMappedNameType::Package, name_map_slice);

    // Set package name on the zen package from the legacy package header
    builder.zen_package.summary.name = builder.zen_package.name_map.store(&builder.legacy_package.summary.package_name);
    // Copy size of the cooked header from the legacy package
    builder.zen_package.summary.cooked_header_size = builder.legacy_package.summary.total_header_size as u32;

    // Copy bulk resources from the legacy package without modifications
    builder.zen_package.bulk_data = builder.legacy_package.data_resources.iter().map(|x| {
        FBulkDataMapEntry{
            serial_offset: x.serial_offset,
            duplicate_serial_offset: x.duplicate_serial_offset,
            serial_size: x.serial_size,
            flags: x.legacy_bulk_data_flags,
            pad: 0,
        }
    }).collect();
    Ok({})
}
