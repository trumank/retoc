use std::collections::HashMap;
use anyhow::bail;
use crate::container_header::EIoContainerHeaderVersion;
use crate::FPackageId;
use crate::legacy_asset::FLegacyPackageHeader;
use crate::name_map::{EMappedNameType, FNameMap};
use crate::script_objects::{FPackageImportReference, FPackageObjectIndex, FPackageObjectIndexType};
use crate::version_heuristics::heuristic_zen_version_from_package_file_version;
use crate::zen::{EZenPackageVersion, FBulkDataMapEntry, FPackageIndex, FZenPackageHeader, FZenPackageVersioningInfo};

/// NOTE: assumes leading slash is already stripped
fn get_public_export_hash(package_relative_export_path: &str) -> u64 {
    cityhasher::hash(
        &package_relative_export_path
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<u8>>()
    )
}

struct ZenPackageBuilder {
    legacy_package: FLegacyPackageHeader,
    zen_package: FZenPackageHeader,
    container_header_version: EIoContainerHeaderVersion,
    package_import_lookup: HashMap<FPackageId, u32>,
    export_hash_lookup: HashMap<u64, u32>,
}

fn create_asset_builder(package: FLegacyPackageHeader, container_header_version: EIoContainerHeaderVersion) -> ZenPackageBuilder {
    ZenPackageBuilder{
        legacy_package: package, zen_package:
        FZenPackageHeader::default(),
        container_header_version,
        package_import_lookup: HashMap::new(),
        export_hash_lookup: HashMap::new(),
    }
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

fn resolve_zen_package_import(builder: &mut ZenPackageBuilder, package_id: FPackageId, package_name: &str, export_hash: u64) -> FPackageImportReference {

    // Resolve index of the imported package, if it's not found add it into the import list and into package names list
    let imported_package_index = if let Some(existing_index) = builder.package_import_lookup.get(&package_id) {
        *existing_index
    } else {
        let new_imported_package_index = builder.zen_package.imported_packages.len() as u32;
        builder.zen_package.imported_packages.push(package_id);
        builder.zen_package.imported_package_names.push(package_name.to_string());

        builder.package_import_lookup.insert(package_id, new_imported_package_index);
        new_imported_package_index
    };

    // Resolve index of the imported export hash
    let imported_public_export_hash_index = if let Some(existing_index) = builder.export_hash_lookup.get(&export_hash) {
        *existing_index
    } else {
        let new_imported_export_hash_index = builder.zen_package.imported_public_export_hashes.len() as u32;
        builder.zen_package.imported_public_export_hashes.push(export_hash);

        builder.export_hash_lookup.insert(export_hash, new_imported_export_hash_index);
        new_imported_export_hash_index
    };
    FPackageImportReference{imported_package_index, imported_public_export_hash_index}
}

// Returns package name and package-relative export path. Package-relative export path is lowercased and is prefixed with /, and uses / as a separator
fn resolve_legacy_package_import(package: &FLegacyPackageHeader, import_index: usize) -> anyhow::Result<(String, String)> {

    // From the outermost to the innermost, e.g. SubObject;Asset;PackageName
    // On average, imports are 2 levels nested, e.g. package name + asset name, but reserve 4 levels to cover the vast majority of cases
    let mut import_index_chain: Vec<usize> = Vec::with_capacity(4);
    let mut current_outer_index = FPackageIndex::create_import(import_index as u32);

    // Walk the import chain to resolve this import
    while !current_outer_index.is_null() {

        // We should never receive exports as outers when resolving imports here
        if current_outer_index.is_export() {
            bail!("Received export {} as outer for import {}, malformed legacy package?", current_outer_index.to_export_index(), import_index)
        }
        // This is an import, add it to the list and set next outer to this import's outer
        let current_import_index = current_outer_index.to_import_index() as usize;
        import_index_chain.push(current_import_index);
        current_outer_index = package.imports[current_import_index].outer_index;
    }

    // innermost import is the package name. We should keep it's casing
    let package_import_index = import_index_chain.last().unwrap().clone();
    let package_name = package.name_map.get(package.imports[package_import_index].object_name).to_string();

    // Build full import name now. We append all elements in reverse, and use / as a path separator. full import name is also lowercase
    let mut full_import_name = package_name.clone();
    for import_index in import_index_chain.iter().rev().skip(1) {
        full_import_name.push('/');
        full_import_name.push_str(&package.name_map.get(package.imports[*import_index].object_name));
    }
    // Make sure the entire path is lowercase. This is a requirement for GetPublicExportHash
    full_import_name.make_ascii_lowercase();

    Ok((package_name, full_import_name))
}

fn convert_legacy_import_to_object_index(builder: &mut ZenPackageBuilder, import_index: usize) -> anyhow::Result<FPackageObjectIndex> {

    let (package_name, full_import_name) = resolve_legacy_package_import(&builder.legacy_package, import_index)?;

    // If this is a script import, just resolve it directly using the full import name as an index into script objects
    let is_script_import = package_name.starts_with("/Script/");
    if is_script_import {
        let script_import_hash = FPackageObjectIndex::generate_import_hash_from_object_path(&full_import_name);
        return Ok(FPackageObjectIndex::new(FPackageObjectIndexType::ScriptImport, script_import_hash));
    }

    // If this is a package import (full import name length is the same as package name), emit Null
    // Zen does not preserve Package imports, and they cannot be represented at all in terms of FPackageObjectIndex
    let is_package_import = package_name.len() == full_import_name.len();
    if is_package_import {
        return Ok(FPackageObjectIndex::null());
    }

    // This is a normal import of the export of another package otherwise. Create FPackageId from package ID and public export hash from package relative path
    let package_id = FPackageId::from_name(&package_name);
    //let public_export_hash =
    bail!("TODO")
}

fn build_zen_import_map(builder: &mut ZenPackageBuilder) {

    //
    builder.zen_package.import_map.reserve(builder.legacy_package.imports.len());



}
