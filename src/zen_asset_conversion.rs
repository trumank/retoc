use std::cmp::max;
use crate::container_header::{EIoContainerHeaderVersion, StoreEntry};
use crate::legacy_asset::{EPackageFlags, FLegacyPackageFileSummary, FLegacyPackageHeader, FSerializedAssetBundle};
use crate::name_map::{EMappedNameType, FNameMap};
use crate::script_objects::{FPackageImportReference, FPackageObjectIndex, FPackageObjectIndexType};
use crate::version_heuristics::heuristic_zen_version_from_package_file_version;
use crate::zen::{EExportCommandType, EExportFilterFlags, EObjectFlags, EZenPackageVersion, FBulkDataMapEntry, FDependencyBundleEntry, FDependencyBundleHeader, FExportBundleEntry, FExportBundleHeader, FExportMapEntry, FExternalDependencyArc, FImportedPackageDependency, FInternalDependencyArc, FPackageFileVersion, FPackageIndex, FZenPackageHeader, FZenPackageVersioningInfo};
use crate::{EIoChunkType, FIoChunkId, FPackageId};
use anyhow::bail;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Write};
use byteorder::{ReadBytesExt, LE};
use topo_sort::{SortResults, TopoSort};
use crate::iostore_writer::IoStoreWriter;

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
    package_id: FPackageId,
    zen_package: FZenPackageHeader,
    container_header_version: EIoContainerHeaderVersion,
    package_import_lookup: HashMap<FPackageId, u32>,
    export_hash_lookup: HashMap<u64, u32>,
}

// Flow is create_asset_builder -> setup_zen_package_summary -> build_zen_import_map -> build_zen_export_map -> build_zen_preload_dependencies -> serialize_zen_asset
fn create_asset_builder(package: FLegacyPackageHeader, container_header_version: EIoContainerHeaderVersion) -> ZenPackageBuilder {
    ZenPackageBuilder{
        package_id: FPackageId::from_name(&package.summary.package_name),
        legacy_package: package,
        zen_package: FZenPackageHeader::default(),
        container_header_version,
        package_import_lookup: HashMap::new(),
        export_hash_lookup: HashMap::new(),
    }
}

fn setup_zen_package_summary(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {

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

    // Make sure not to attempt to put uncooked packages into zen
    if (builder.legacy_package.summary.package_flags & (EPackageFlags::Cooked as u32)) == 0 {
        bail!("Detected absent PKG_Cooked flag in legacy package summary. Uncooked assets cannot be converted to Zen. Are you sure the asset has been Cooked?");
    }
    // Make sure we do not have any soft object paths serialized in the header. These cannot be represented in zen packages and should never be written when cooking
    if builder.legacy_package.summary.soft_object_paths.count > 0 {
        bail!("Detected soft object paths serialized as a part of the package header. Such paths cannot be represented in Zen packages and should never be written for cooked packages. Are you sure the package is cooked?");
    }

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
fn resolve_legacy_package_object(package: &FLegacyPackageHeader, object_index: FPackageIndex) -> anyhow::Result<(String, String)> {

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

    let mut package_name: String;
    let mut start_object_index: usize;

    if package_object_outer_chain[0].is_import() {
        let package_import_index = package_object_outer_chain[0].to_import_index() as usize;

        // If the innermost package index is an import, it's a package name. Otherwise, this package name is the package name
        package_name = package.name_map.get(package.imports[package_import_index].object_name).to_string();
        start_object_index = 1;

    } else {
         // This is an export, package name is this package name and we should start path building at index 0
        package_name = package.summary.package_name.clone();
        start_object_index = 0;
    };

    // Build full object name now. We append all elements and use / as a path separator
    let mut full_object_name: String = package_name.clone();
    for i in start_object_index..package_object_outer_chain.len() {

        // Append object path separator
        full_object_name.push('/');

        // Append the name of the object if it's an import
        if package_object_outer_chain[i].is_import() {
            let import_index = package_object_outer_chain[i].to_import_index() as usize;
            full_object_name.push_str(&package.name_map.get(package.imports[import_index].object_name));

            // Append the name of the object if it's an import
        } else if package_object_outer_chain[i].is_export() {
            let export_index = package_object_outer_chain[i].to_export_index() as usize;
            full_object_name.push_str(&package.name_map.get(package.exports[export_index].object_name));
        }
    }
    // Make sure the entire path is lowercase. This is a requirement for GetPublicExportHash
    full_object_name.make_ascii_lowercase();

    Ok((package_name, full_object_name))
}

fn convert_legacy_import_to_object_index(builder: &mut ZenPackageBuilder, import_index: usize) -> anyhow::Result<FPackageObjectIndex> {

    let (package_name, full_import_name) = resolve_legacy_package_object(&builder.legacy_package, FPackageIndex::create_import(import_index as u32))?;

    // If this is a script import, just resolve it directly using the full import name as an index into script objects
    let is_script_import = package_name.starts_with("/Script/");
    if is_script_import {
        return Ok(FPackageObjectIndex::create_script_import(&full_import_name));
    }

    // If this is a package import (full import name length is the same as package name), emit Null
    // Zen does not preserve Package imports, and they cannot be represented at all in terms of FPackageObjectIndex
    let is_package_import = package_name.len() == full_import_name.len();
    if is_package_import {
        return Ok(FPackageObjectIndex::create_null());
    }

    // This is a normal import of the export of another package otherwise. Create FPackageId from package ID and public export hash from package relative path
    let package_id = FPackageId::from_name(&package_name);
    let public_export_hash = get_public_export_hash(&full_import_name[package_name.len() + 1..]);

    // Resolve import reference now, and convert it to object index
    let import_reference = resolve_zen_package_import(builder, package_id, &package_name, public_export_hash);
    Ok(FPackageObjectIndex::create_package_import(import_reference))
}

fn build_zen_import_map(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {

    builder.zen_package.import_map.reserve(builder.legacy_package.imports.len());

    for import_index in 0..builder.legacy_package.imports.len() {
        let import_object_index = convert_legacy_import_to_object_index(builder, import_index)?;
        builder.zen_package.import_map.push(import_object_index)
    }
    Ok({})
}

fn remap_package_index_reference(builder: &mut ZenPackageBuilder, package_index: FPackageIndex) -> FPackageObjectIndex {

    if package_index.is_export() {
        return FPackageObjectIndex::create_export(package_index.to_export_index())
    }
    if package_index.is_import() {
        return builder.zen_package.import_map[package_index.to_import_index() as usize]
    }
    FPackageObjectIndex::create_null()
}

fn build_zen_export_map(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {

    builder.zen_package.export_map.reserve(builder.legacy_package.exports.len());

    for export_index in 0..builder.legacy_package.exports.len() {

        let object_export = builder.legacy_package.exports[export_index].clone();
        let total_header_size = builder.legacy_package.summary.total_header_size as u64;
        let object_name = builder.legacy_package.name_map.get(object_export.object_name).to_string();

        // Zen cooked serial offset does not include header size, but legacy asset one does
        let cooked_serial_offset = object_export.serial_offset as u64 - total_header_size;
        let mapped_object_name = builder.zen_package.name_map.store(&object_name);

        let outer_index = remap_package_index_reference(builder, object_export.outer_index);
        let class_index = remap_package_index_reference(builder, object_export.class_index);
        let super_index = remap_package_index_reference(builder, object_export.super_index);
        let template_index = remap_package_index_reference(builder, object_export.template_index);

        let should_have_public_export_hash = (object_export.object_flags & EObjectFlags::Public as u32) != 0 || object_export.generate_public_hash;
        let public_export_hash: u64 = if should_have_public_export_hash {
            let (export_package_name, full_export_name) = resolve_legacy_package_object(&builder.legacy_package, FPackageIndex::create_export(export_index as u32))?;
            get_public_export_hash(&full_export_name[export_package_name.len() + 1..])
        } else { 0 };

        let filter_flags: EExportFilterFlags = if object_export.is_not_for_server {
            EExportFilterFlags::NotForServer
        } else if object_export.is_not_for_client {
            EExportFilterFlags::NotForClient
        } else {
            EExportFilterFlags::None
        };

        let zen_export = FExportMapEntry{
            cooked_serial_offset,
            cooked_serial_size: object_export.serial_size as u64,
            object_name: mapped_object_name,
            object_flags: object_export.object_flags,
            outer_index, class_index, super_index, template_index,
            public_export_hash,
            filter_flags, padding: [0; 3]
        };
        builder.zen_package.export_map.push(zen_export);
    }
    Ok({})
}

#[derive(Debug, Copy, Clone, PartialEq, Default, Eq, Hash)]
struct ZenDependencyGraphNode {
    package_index: FPackageIndex,
    command_type: EExportCommandType,
}

fn build_zen_dependency_bundles_legacy(builder: &mut ZenPackageBuilder, sorted_deps: &Vec<ZenDependencyGraphNode>, export_dependencies: &HashMap<ZenDependencyGraphNode, Vec<ZenDependencyGraphNode>>) {

    let mut current_export_bundle_header_index: i64 = -1;
    let mut current_export_offset: u64 = 0;
    let mut export_to_bundle_map: HashMap<ZenDependencyGraphNode, usize> = HashMap::new();

    // Create export bundles from the export list sorted by the dependencies
    for graph_node_index in 0..sorted_deps.len() {

        let dependency_graph_node = sorted_deps[graph_node_index].clone();

        // Skip non-export items in the dependency graph. Imports will occasionally appear in the graph when there is a requirement for both a creation and a serialization
        if !dependency_graph_node.package_index.is_export() {
            continue
        }

        let export_index = dependency_graph_node.package_index.to_export_index() as usize;
        let export_command_type = dependency_graph_node.command_type;

        // Open a new export bundle if we do not have one running
        if current_export_bundle_header_index == -1 {

            current_export_bundle_header_index = builder.zen_package.export_bundle_headers.len() as i64;

            let first_entry_index = builder.zen_package.export_bundle_entries.len() as u32;
            let serial_offset = current_export_offset;

            builder.zen_package.export_bundle_headers.push(FExportBundleHeader{
                serial_offset, first_entry_index, entry_count: 0
            })
        }

        // Add current export as an entry into the currently open bundle
        builder.zen_package.export_bundle_entries.push(FExportBundleEntry{
            local_export_index: export_index as u32,
            command_type: export_command_type,
        });
        // Associate this export command with this bundle. This is needed to build internal and external dependency arcs
        export_to_bundle_map.insert(dependency_graph_node, current_export_bundle_header_index as usize);

        // Increment the entry count for the bundle
        builder.zen_package.export_bundle_headers[current_export_bundle_header_index as usize].entry_count += 1;

        // Account for this export in the current export offset if this export is Serialize command
        if export_command_type == EExportCommandType::Serialize {
            current_export_offset += builder.zen_package.export_map[export_index].cooked_serial_size;
        }

        // Export bundles end at a public export with an export hash. So if this is a public export, close the current bundle
        let is_public_export = builder.zen_package.export_map[export_index].public_export_hash != 0;
        if is_public_export {
            current_export_bundle_header_index = -1;
        }
    }

    // Used to avoid adding duplicate dependencies between export bundles and other export bundles/imports
    let mut internal_dependency_arcs: HashSet<FInternalDependencyArc> = HashSet::new();
    let mut external_dependency_arcs: HashSet<FExternalDependencyArc> = HashSet::new();

    // Function to create export dependency arcs to the export's export bundle from another export's export bundle, or from an entry in the import map
    let mut create_dependency_arc_from_node = |to_export_bundle_index: i32, dependency_node: &ZenDependencyGraphNode, mut_builder: &mut ZenPackageBuilder| {

        // This is an export-to-export dependency
        if dependency_node.package_index.is_export() {
            let from_export_bundle_index = export_to_bundle_map.get(&dependency_node).unwrap().clone() as i32;

            // Skip dependencies between exports that belong to the same bundle, they are already sorted
            if from_export_bundle_index != to_export_bundle_index {

                // If we have not previously created a dependency from that export bundle to our export bundle, add one
                let internal_dependency_arc = FInternalDependencyArc{from_export_bundle_index, to_export_bundle_index};
                if !internal_dependency_arcs.contains(&internal_dependency_arc) {

                    internal_dependency_arcs.insert(internal_dependency_arc.clone());
                    mut_builder.zen_package.internal_dependency_arcs.push(internal_dependency_arc);
                }
            }
        }
        // This is an import-to-export dependency. We need to add a dependency arc for it unless it's a script import or a removed package import
        else if dependency_node.package_index.is_import() {

            let from_import_index = dependency_node.package_index.to_import_index() as i32;
            let from_command_type = dependency_node.command_type;
            let package_object_import = mut_builder.zen_package.import_map[from_import_index as usize];

            // Do not add external arcs for script imports and removed package imports (represented as Null in the zen import map)
            if package_object_import.kind() == FPackageObjectIndexType::PackageImport {

                let imported_package_index = package_object_import.package_import().unwrap().imported_package_index as usize;
                let external_dependency_arc = FExternalDependencyArc{from_import_index, from_command_type, to_export_bundle_index};

                // Only add the dependency arc if we have not previously created in
                if !external_dependency_arcs.contains(&external_dependency_arc) {

                    external_dependency_arcs.insert(external_dependency_arc.clone());
                    mut_builder.zen_package.imported_package_dependencies[imported_package_index].dependency_arcs.push(external_dependency_arc);
                }
            }
        }
    };

    // Pre-initialize imported package dependencies with the number of imported package IDs
    builder.zen_package.imported_package_dependencies.reserve(builder.zen_package.imported_packages.len());
    for _ in 0..builder.zen_package.imported_packages.len() {
        builder.zen_package.imported_package_dependencies.push(FImportedPackageDependency{ dependency_arcs: Vec::new() });
    }

    // Build internal and external dependency arcs
    for export_index in 0..builder.zen_package.export_map.len() {

        let export_create_node = ZenDependencyGraphNode{package_index: FPackageIndex::create_export(export_index as u32), command_type: EExportCommandType::Create};
        let export_serialize_node = ZenDependencyGraphNode{package_index: FPackageIndex::create_export(export_index as u32), command_type: EExportCommandType::Serialize};

        let export_create_bundle_index = export_to_bundle_map.get(&export_create_node).unwrap().clone();
        let export_serialize_bundle_index = export_to_bundle_map.get(&export_serialize_node).unwrap().clone();

        for export_create_dependency in export_dependencies.get(&export_create_node).unwrap_or(&Vec::new()) {
            create_dependency_arc_from_node(export_create_bundle_index as i32, export_create_dependency, builder);
        }
        for export_serialize_dependency in export_dependencies.get(&export_serialize_node).unwrap_or(&Vec::new()) {
            create_dependency_arc_from_node(export_serialize_bundle_index as i32, export_serialize_dependency, builder);
        }
    }
}

fn build_zen_dependency_bundle_new(builder: &mut ZenPackageBuilder, sorted_deps: &Vec<ZenDependencyGraphNode>, export_dependencies: &HashMap<ZenDependencyGraphNode, Vec<ZenDependencyGraphNode>>) {

    // Create a single dependency bundle with all exports
    for graph_node_index in 0..sorted_deps.len() {

        let dependency_graph_node = sorted_deps[graph_node_index].clone();

        // Skip non-export items in the dependency graph. Imports will occasionally appear in the graph when there is a requirement for both a creation and a serialization
        if !dependency_graph_node.package_index.is_export() {
            continue
        }

        let export_index = dependency_graph_node.package_index.to_export_index() as usize;
        let export_command_type = dependency_graph_node.command_type;

        // Add current export as an entry into the currently open bundle
        builder.zen_package.export_bundle_entries.push(FExportBundleEntry{
            local_export_index: export_index as u32,
            command_type: export_command_type,
        });
    }

    // Collects all dependencies of the given node with the given command type
    let collect_export_dependencies = |to_dependency_node: &ZenDependencyGraphNode, from_command_type: EExportCommandType, immut_builder: &ZenPackageBuilder| -> Vec<FDependencyBundleEntry> {
        let mut result_dependencies: Vec<FDependencyBundleEntry> = Vec::new();

        for from_dependency_node in export_dependencies.get(&to_dependency_node).unwrap_or(&Vec::new()) {

            // Skip nodes that do not have the matching command type, and nodes to ourselves (e.g. serialize depends on create)
            if from_dependency_node.command_type == from_command_type && from_dependency_node.package_index != to_dependency_node.package_index {

                // If this is an export, add the dependency bundle entry at all times
                if from_dependency_node.package_index.is_export() {
                    result_dependencies.push(FDependencyBundleEntry{ local_import_or_export_index: from_dependency_node.package_index });
                }
                // Otherwise, if this is an import, we only add it if it's a package export import
                else if from_dependency_node.package_index.is_import() {

                    let zen_import_package_index = immut_builder.zen_package.import_map[from_dependency_node.package_index.to_import_index() as usize];
                    if zen_import_package_index.kind() == FPackageObjectIndexType::PackageImport {

                        result_dependencies.push(FDependencyBundleEntry{ local_import_or_export_index: from_dependency_node.package_index });
                    }
                }
            }
        }
        result_dependencies
    };

    // Build dependency bundles
    for export_index in 0..builder.zen_package.export_map.len() {

        let export_create_node = ZenDependencyGraphNode { package_index: FPackageIndex::create_export(export_index as u32), command_type: EExportCommandType::Create };
        let export_serialize_node = ZenDependencyGraphNode { package_index: FPackageIndex::create_export(export_index as u32), command_type: EExportCommandType::Serialize };

        let mut create_before_create_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_create_node, EExportCommandType::Create, builder);
        let mut serialize_before_create_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_create_node, EExportCommandType::Serialize, builder);
        let mut create_before_serialize_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_serialize_node, EExportCommandType::Create, builder);
        let mut serialize_before_serialize_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_serialize_node, EExportCommandType::Serialize, builder);

        // Create dependency header for this export
        let first_entry_index = builder.zen_package.dependency_bundle_entries.len() as i32;

        builder.zen_package.dependency_bundle_headers.push(FDependencyBundleHeader{
            first_entry_index,
            create_before_create_dependencies: create_before_create_deps.len() as u32,
            serialize_before_create_dependencies: serialize_before_create_deps.len() as u32,
            create_before_serialize_dependencies: create_before_serialize_deps.len() as u32,
            serialize_before_serialize_dependencies: serialize_before_serialize_deps.len() as u32,
        });

        // Push dependency bundle entries into the zen asset following the first index
        builder.zen_package.dependency_bundle_entries.append(&mut create_before_create_deps);
        builder.zen_package.dependency_bundle_entries.append(&mut serialize_before_create_deps);
        builder.zen_package.dependency_bundle_entries.append(&mut create_before_serialize_deps);
        builder.zen_package.dependency_bundle_entries.append(&mut serialize_before_serialize_deps);
    }
}

fn build_zen_preload_dependencies(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {

    // Build a dependency graph with each export and it's preload dependencies
    let mut topological_sort: TopoSort<ZenDependencyGraphNode> = TopoSort::new();
    let mut export_dependencies: HashMap<ZenDependencyGraphNode, Vec<ZenDependencyGraphNode>> = HashMap::new();
    let mut import_nodes_already_populated: HashSet<ZenDependencyGraphNode> = HashSet::new();

    for export_index in 0..builder.legacy_package.exports.len() {

        let export_package_index = FPackageIndex::create_export(export_index as u32);
        let object_export = builder.legacy_package.exports[export_index].clone();

        let create_graph_node = ZenDependencyGraphNode{package_index: export_package_index, command_type: EExportCommandType::Create};
        let serialize_graph_node = ZenDependencyGraphNode{package_index: export_package_index, command_type: EExportCommandType::Serialize};

        let mut create_dependencies: Vec<ZenDependencyGraphNode> = Vec::new();
        let mut serialize_dependencies: Vec<ZenDependencyGraphNode> = Vec::new();

        // Collect create and serialize dependencies for this export
        if object_export.first_export_dependency_index != -1 {

            // Serialize before serialize dependencies
            for i in 0..object_export.serialize_before_serialize_dependencies {

                let preload_dependency_index = object_export.first_export_dependency_index + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode{package_index: preload_dependency, command_type: EExportCommandType::Serialize};
                serialize_dependencies.push(dependency);
            }

            // Create before serialize dependencies
            for i in 0..object_export.create_before_serialize_dependencies {

                let preload_dependency_index = object_export.first_export_dependency_index + object_export.serialize_before_serialize_dependencies + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode{package_index: preload_dependency, command_type: EExportCommandType::Create};
                serialize_dependencies.push(dependency);
            }

            // Serialize before create dependencies
            for i in 0..object_export.serialize_before_create_dependencies {

                let preload_dependency_index = object_export.first_export_dependency_index + object_export.serialize_before_serialize_dependencies +
                    object_export.create_before_serialize_dependencies + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode{package_index: preload_dependency, command_type: EExportCommandType::Serialize};
                create_dependencies.push(dependency);
            }

            // Create before create dependencies
            for i in 0..object_export.create_before_create_dependencies {

                let preload_dependency_index = object_export.first_export_dependency_index + object_export.serialize_before_serialize_dependencies +
                    object_export.create_before_serialize_dependencies + object_export.serialize_before_create_dependencies + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode{package_index: preload_dependency, command_type: EExportCommandType::Create};
                create_dependencies.push(dependency);
            }
        }

        // Add synthetic dependency from import serialization to import create to make some of the export dependency requirements that are invalid
        // actually trigger the cyclic topological sort error instead of producing incorrect assets
        for import_serialize_node in create_dependencies.iter().chain(&serialize_dependencies) {

            // We only want to handle Serialize dependencies for imported package indices here, not for other exports
            // Also only populate each dependency once, and then add it to the map
            if import_serialize_node.package_index.is_import() && import_serialize_node.command_type == EExportCommandType::Serialize &&
                !import_nodes_already_populated.contains(import_serialize_node) {

                import_nodes_already_populated.insert(import_serialize_node.clone());
                let import_create_node = ZenDependencyGraphNode{package_index: import_serialize_node.package_index, command_type: EExportCommandType::Create};
                topological_sort.insert(import_serialize_node.clone(), [import_create_node]);
            }
        }

        //This export's serialize has a dependency on this export's create
        serialize_dependencies.push(create_graph_node);

        // Add create and serialize graph nodes for this export
        topological_sort.insert(create_graph_node, create_dependencies.clone());
        topological_sort.insert(serialize_graph_node, serialize_dependencies.clone());

        // Remember dependencies associated with each node. This is necessary for building dependency arcs later
        export_dependencies.insert(create_graph_node, create_dependencies);
        export_dependencies.insert(serialize_graph_node, serialize_dependencies);
    }

    // Make sure that we are able to sort the entire list without cycles
    let sorted_node_list = match topological_sort.into_vec_nodes() {
        SortResults::Partial(_) => bail!("Failed to create dependency bundles for the package because of the circular dependency in preload dependencies"),
        SortResults::Full(full_result) => full_result,
    };

    // Use legacy path for versions before NoExportInfo, and a new one for versions after
    if builder.container_header_version >= EIoContainerHeaderVersion::NoExportInfo {
        build_zen_dependency_bundle_new(builder, &sorted_node_list, &export_dependencies);
    } else {
        build_zen_dependency_bundles_legacy(builder, &sorted_node_list, &export_dependencies);
    }
    Ok({})
}

fn write_exports_in_bundle_order<S: Write>(writer: &mut S, builder: &ZenPackageBuilder, exports_buffer: &Vec<u8>) -> anyhow::Result<()> {

    let total_header_size = builder.legacy_package.summary.total_header_size as u64;
    let mut current_export_offset: u64 = 0;
    let mut largest_exports_buffer_export_end_offset: usize = 0;

    for export_bundle_header_index in 0..builder.zen_package.export_bundle_headers.len() {

        let export_bundle_header = builder.zen_package.export_bundle_headers[export_bundle_header_index].clone();

        // Make sure bundle data is actually being placed at the correct offset
        if export_bundle_header.serial_offset != current_export_offset {
            bail!("Export bundle {} serial offset does not match it's actual placement. Expected bundle data to be placed at {}, but it's placed at {}",
            export_bundle_header_index, export_bundle_header.serial_offset, current_export_offset);
        }

        for i in 0..export_bundle_header.entry_count {

            let export_bundle_entry_index = export_bundle_header.first_entry_index + i;
            let export_bundle_entry = builder.zen_package.export_bundle_entries[export_bundle_entry_index as usize].clone();

            // Only Serialize command actually means the export data placement
            if export_bundle_entry.command_type == EExportCommandType::Serialize {

                let export_index = export_bundle_entry.local_export_index as usize;

                // Export serial offset here is actually relative to the legacy package header size, so we need to subtract it to get the real position in the exports buffer
                let export_serial_offset = (builder.legacy_package.exports[export_index].serial_offset as u64 - total_header_size) as usize;
                let export_serial_size = builder.legacy_package.exports[export_index].serial_size as usize;
                let export_end_serial_offset = export_serial_offset + export_serial_size;

                // Serialize the export at this position and increment the current position
                largest_exports_buffer_export_end_offset = max(largest_exports_buffer_export_end_offset, export_end_serial_offset);
                writer.write(&exports_buffer[export_serial_offset..export_end_serial_offset])?;
                current_export_offset += export_serial_size as u64;
            }
        }
    }

    // There can be extra data after the export blobs in the export buffer that we should try to preserve
    // Note that normally there is also a package end magic there, that we want explicitly NOT to preserve because zen assets before 5.2 do not include end magic
    let extra_data_start_offset = largest_exports_buffer_export_end_offset;
    let mut extra_data_length = exports_buffer.len() - largest_exports_buffer_export_end_offset;

    // Check if last 4 bytes are package file magic, and if they are, do not consider them as extra data
    let package_end_tag_start_offset = exports_buffer.len() - size_of::<u32>();
    if extra_data_length >= size_of::<u32>() && Cursor::new(&exports_buffer[package_end_tag_start_offset..]).read_u32::<LE>()? == FLegacyPackageFileSummary::PACKAGE_FILE_TAG {
        extra_data_length -= size_of::<u32>();
    }
    // If we have any actual extra data, write it to the zen asset
    if extra_data_length > 0 {
        let extra_data_end_offset = extra_data_start_offset + extra_data_length;
        writer.write(&exports_buffer[extra_data_start_offset..extra_data_end_offset])?;
    }
    Ok({})
}

fn serialize_zen_asset(builder: &ZenPackageBuilder, legacy_asset_bundle: &FSerializedAssetBundle) -> anyhow::Result<(StoreEntry, Vec<u8>)> {

    let mut result_package_buffer: Vec<u8> = Vec::new();
    let mut result_package_writer = Cursor::new(&mut result_package_buffer);
    let mut result_store_entry: StoreEntry = StoreEntry::default();

    // Serialize package header
    FZenPackageHeader::serialize(&builder.zen_package, &mut result_package_writer, &mut result_store_entry, builder.container_header_version)?;

    if builder.container_header_version >= EIoContainerHeaderVersion::NoExportInfo {
        // Write export buffer without any changes if we are following cooked offsets
        result_package_writer.write(&legacy_asset_bundle.exports_file_buffer)?;
    } else {
        // Write export buffer in bundle order otherwise, moving exports around to follow bundle serialization order
        write_exports_in_bundle_order(&mut result_package_writer, builder, &legacy_asset_bundle.exports_file_buffer)?;
    }
    Ok((result_store_entry, result_package_buffer))
}

fn write_zen_asset(writer: &mut IoStoreWriter, builder: &ZenPackageBuilder, legacy_asset_bundle: &FSerializedAssetBundle) -> anyhow::Result<FPackageId> {

    let (result_store_entry, result_package_buffer) = serialize_zen_asset(builder, legacy_asset_bundle)?;

    // TODO: write file paths for all of the chunks written down

    // Write package chunk into zen
    let package_chunk_id = FIoChunkId::from_package_id(builder.package_id, 0, EIoChunkType::ExportBundleData);
    writer.write_package_chunk(package_chunk_id, None, &result_package_buffer, &result_store_entry)?;

    // Write bulk data chunk if it is present
    if let Some(bulk_data_buffer) = &legacy_asset_bundle.bulk_data_buffer {
        let bulk_data_chunk_id = FIoChunkId::from_package_id(builder.package_id, 0, EIoChunkType::BulkData);
        writer.write_chunk(bulk_data_chunk_id, None, bulk_data_buffer)?;
    }
    // Write optional bulk data chunk if it is present
    if let Some(optional_bulk_data_buffer) = &legacy_asset_bundle.optional_bulk_data_buffer {
        let optional_bulk_data_chunk_id = FIoChunkId::from_package_id(builder.package_id, 0, EIoChunkType::OptionalBulkData);
        writer.write_chunk(optional_bulk_data_chunk_id, None, optional_bulk_data_buffer)?;
    }
    // Write memory mapped bulk data chunk if it is present
    if let Some(memory_mapped_bulk_data_buffer) = &legacy_asset_bundle.memory_mapped_bulk_data_buffer {
        let memory_mapped_bulk_data_chunk_id = FIoChunkId::from_package_id(builder.package_id, 0, EIoChunkType::MemoryMappedBulkData);
        writer.write_chunk(memory_mapped_bulk_data_chunk_id, None, memory_mapped_bulk_data_buffer)?;
    }
    Ok(builder.package_id)
}

fn build_zen_asset_internal(legacy_asset: &FSerializedAssetBundle, container_header_version: EIoContainerHeaderVersion, package_version_fallback: Option<FPackageFileVersion>) -> anyhow::Result<ZenPackageBuilder> {

    // Read legacy package header
    let mut asset_header_reader = Cursor::new(&legacy_asset.asset_file_buffer);
    let legacy_package_header = FLegacyPackageHeader::deserialize(&mut asset_header_reader, package_version_fallback)?;

    // Construct zen asset from the package header
    let mut builder = create_asset_builder(legacy_package_header, container_header_version);

    // Build zen asset data
    setup_zen_package_summary(&mut builder)?;
    build_zen_import_map(&mut builder)?;
    build_zen_export_map(&mut builder)?;
    build_zen_preload_dependencies(&mut builder)?;

    Ok(builder)
}

// Builds zen asset and returns the resulting package ID, chunk data buffer, and it's store entry. Zen package conversion does not modify bulk data in any way.
pub(crate) fn build_serialize_zen_asset(legacy_asset: &FSerializedAssetBundle, container_header_version: EIoContainerHeaderVersion, package_version_fallback: Option<FPackageFileVersion>) -> anyhow::Result<(FPackageId, StoreEntry, Vec<u8>)> {

    let builder = build_zen_asset_internal(legacy_asset, container_header_version, package_version_fallback)?;

    let (store_entry, package_data) = serialize_zen_asset(&builder, legacy_asset)?;
    Ok((builder.package_id, store_entry, package_data))
}

// Builds zen asset and writes it into the container using the provided serialized legacy asset and package version
pub(crate) fn build_write_zen_asset(writer: &mut IoStoreWriter, legacy_asset: &FSerializedAssetBundle, package_version_fallback: Option<FPackageFileVersion>) -> anyhow::Result<FPackageId> {

    let builder = build_zen_asset_internal(legacy_asset, writer.container_header_version(), package_version_fallback)?;

    // Serialize the resulting asset into the container writer
    write_zen_asset(writer, &builder, legacy_asset)
}

#[cfg(test)]
mod test {
    use std::process::abort;
    use super::*;
    use fs_err as fs;
    use crate::{EIoStoreTocVersion, FSHAHash};
    use crate::zen::EUnrealEngineObjectUE5Version;

    #[test]
    fn test_zen_asset_identity_conversion() -> anyhow::Result<()> {

        let asset_header_buffer = fs::read("tests/UE5.4/BP_Table_Lamp.uasset")?;
        let asset_exports_buffer = fs::read("tests/UE5.4/BP_Table_Lamp.uexp")?;

        let serialized_asset_bundle = FSerializedAssetBundle{
            asset_file_buffer: asset_header_buffer,
            exports_file_buffer: asset_exports_buffer.clone(),
            bulk_data_buffer: None, optional_bulk_data_buffer: None, memory_mapped_bulk_data_buffer: None,
        };

        // UE5.4, NoExportInfo zen header, OnDemandMetaData TOC version, and PropertyTagCompleteTypeName package file version
        let package_file_version = Some(FPackageFileVersion::create_ue5(EUnrealEngineObjectUE5Version::PropertyTagCompleteTypeName));
        let container_header_version = EIoContainerHeaderVersion::NoExportInfo;
        let container_toc_version = EIoStoreTocVersion::OnDemandMetaData;

        let original_zen_asset = fs::read("tests/UE5.4/BP_Table_Lamp.uzenasset")?;
        let original_zen_asset_package = FZenPackageHeader::deserialize(&mut Cursor::new(&original_zen_asset), None, container_toc_version, container_header_version, package_file_version.clone())?;
        
        let (_, _, converted_zen_asset) = build_serialize_zen_asset(&serialized_asset_bundle, container_header_version, package_file_version.clone())?;
        let converted_zen_asset_package = FZenPackageHeader::deserialize(&mut Cursor::new(&converted_zen_asset), None, container_toc_version, container_header_version, package_file_version.clone())?;

        //dbg!(original_zen_asset_package.clone());
        //dbg!(converted_zen_asset_package.clone());

        // Make sure the header is equal between the original and the converted asset, minus the load order data
        assert_eq!(original_zen_asset_package.name_map.copy_raw_names(), converted_zen_asset_package.name_map.copy_raw_names());
        assert_eq!(original_zen_asset_package.imported_package_names.clone(), converted_zen_asset_package.imported_package_names.clone());
        assert_eq!(original_zen_asset_package.imported_packages.clone(), converted_zen_asset_package.imported_packages.clone());
        assert_eq!(original_zen_asset_package.imported_public_export_hashes.clone(), converted_zen_asset_package.imported_public_export_hashes.clone());
        assert_eq!(original_zen_asset_package.import_map.clone(), converted_zen_asset_package.import_map.clone());
        assert_eq!(original_zen_asset_package.export_map.clone(), converted_zen_asset_package.export_map.clone());
        assert_eq!(original_zen_asset_package.dependency_bundle_headers.clone(), converted_zen_asset_package.dependency_bundle_headers.clone());
        assert_eq!(original_zen_asset_package.dependency_bundle_entries.clone(), converted_zen_asset_package.dependency_bundle_entries.clone());

        // Make sure export blob is identical after the header size. Offsets in export map are relative to the end of the header so if they are correct and this data is correct exports are correct
        assert_eq!(original_zen_asset[(original_zen_asset_package.summary.header_size as usize)..].to_vec(), asset_exports_buffer.clone(), "Uexp file and the original zen asset exports do not match");
        assert_eq!(original_zen_asset[(original_zen_asset_package.summary.header_size as usize)..].to_vec(), converted_zen_asset[(converted_zen_asset_package.summary.header_size as usize)..].to_vec(), "Original zen asset and converted zen asset exports do not match");

        Ok(())
    }
}

