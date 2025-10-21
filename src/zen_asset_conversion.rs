use crate::container_header::{EIoContainerHeaderVersion, StoreEntry};
use crate::iostore_writer::IoStoreWriter;
use crate::legacy_asset::{EPackageFlags, FLegacyPackageFileSummary, FLegacyPackageHeader, FSerializedAssetBundle, convert_localized_package_name_to_source, get_package_object_full_name};
use crate::logging::{Log, debug, warning};
use crate::name_map::{EMappedNameType, FNameMap};
use crate::script_objects::{FPackageImportReference, FPackageObjectIndex, FPackageObjectIndexType, ZenScriptObjects};
use crate::ser::{ReadExt, WriteExt};
use crate::version_heuristics::heuristic_zen_version_from_package_file_version;
use crate::zen::{
    EExportCommandType, EExportFilterFlags, EObjectFlags, EZenPackageVersion, ExternalPackageDependency, FBulkDataMapEntry, FCellExportMapEntry, FDependencyBundleEntry, FDependencyBundleHeader, FExportBundleEntry, FExportBundleHeader, FExportMapEntry, FExternalDependencyArc, FInternalDependencyArc,
    FPackageFileVersion, FPackageIndex, FZenPackageHeader, FZenPackageVersioningInfo, ZenScriptCellsStore,
};
use crate::{EIoChunkType, FIoChunkId, FPackageId, FSHAHash, UEPath, UEPathBuf};
use anyhow::{Context, anyhow, bail};
use byteorder::{LE, ReadBytesExt};
use std::cmp::{Ordering, max};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::{Arc, RwLock};

/// NOTE: assumes leading slash is already stripped
pub(crate) fn get_public_export_hash(package_relative_export_path: &str) -> u64 {
    cityhasher::hash(package_relative_export_path.encode_utf16().flat_map(u16::to_le_bytes).collect::<Vec<u8>>())
}
pub(crate) fn get_cell_export_hash(verse_path: &str) -> u64 {
    cityhasher::hash(verse_path.as_bytes())
}

#[derive(Debug, Clone, Default)]
struct ZenLegacyPackageExternalArcFixupData {
    fixup_from_bundle_id: i32,
    from_package_id: FPackageId,
    from_import_index: FPackageObjectIndex,
    from_command_type: EExportCommandType,
    debug_full_import_name: Option<String>,
}
#[derive(Debug, Clone, Default)]
struct ZenLegacyPackageExportBundleMapping {
    export_index: FPackageObjectIndex,
    export_command_type: EExportCommandType,
    export_bundle_index: i32,
    _debug_full_export_name: Option<String>,
}

struct ZenPackageBuilder<'a> {
    legacy_package: FLegacyPackageHeader,
    script_objects: Option<Arc<ZenScriptObjects>>,
    script_cells: Option<Arc<ZenScriptCellsStore>>,
    package_id: FPackageId,
    package_name: String,
    zen_package: FZenPackageHeader,
    container_header_version: EIoContainerHeaderVersion,
    package_import_lookup: HashMap<FPackageId, u32>,
    import_to_package_id_lookup: HashMap<FPackageObjectIndex, FPackageId>,
    export_hash_lookup: HashMap<u64, u32>,
    // If this is a localized package, name of the culture for which this package is localized
    localized_package_culture: Option<String>,
    // If this package is a redirect target from another package (including by localization), this is the name of the original package that should get redirected to this package
    source_package_name: Option<String>,
    // True if we should write placeholder legacy external arc values for UE4 external arcs. We will then need a fix-up pass after the initial serialization to fix them up with real from bundle indices
    fixup_legacy_external_arcs: bool,
    // Information necessary for the fixup of the legacy external dependency arcs
    legacy_external_arc_fixup_data: Vec<ZenLegacyPackageExternalArcFixupData>,
    legacy_external_arc_counter: i32,
    legacy_export_bundle_mapping: Vec<ZenLegacyPackageExportBundleMapping>,
    // full names of package objects by their index, useful for debugging
    debug_full_package_object_names: HashMap<FPackageIndex, String>,
    log: &'a Log,
}

// Flow is create_asset_builder -> setup_zen_package_summary -> build_zen_import_map -> build_zen_export_map -> build_zen_preload_dependencies -> serialize_zen_asset
fn create_asset_builder<'a>(
    package: FLegacyPackageHeader,
    container_header_version: EIoContainerHeaderVersion,
    fixup_legacy_external_arcs: bool,
    source_package_name: Option<String>,
    script_objects: Option<Arc<ZenScriptObjects>>,
    script_cells: Option<Arc<ZenScriptCellsStore>>,
    log: &'a Log,
) -> ZenPackageBuilder<'a> {
    let package_name = source_package_name.as_deref().unwrap_or(&package.summary.package_name).to_string();
    ZenPackageBuilder {
        package_id: FPackageId::from_name(&package_name),
        package_name,
        legacy_package: package,
        zen_package: FZenPackageHeader {
            container_header_version,
            ..FZenPackageHeader::default()
        },
        container_header_version,
        package_import_lookup: HashMap::new(),
        import_to_package_id_lookup: HashMap::new(),
        export_hash_lookup: HashMap::new(),
        localized_package_culture: None,
        source_package_name,
        fixup_legacy_external_arcs,
        legacy_external_arc_fixup_data: Vec::new(),
        legacy_external_arc_counter: 0,
        legacy_export_bundle_mapping: Vec::new(),
        debug_full_package_object_names: HashMap::new(),
        script_objects,
        script_cells,
        log,
    }
}

fn setup_zen_package_summary(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {
    let is_unversioned = builder.legacy_package.summary.versioning_info.is_unversioned;

    // Copy package flags
    builder.zen_package.summary.package_flags = builder.legacy_package.summary.package_flags;

    // Copy versioning info from the package, except the zen version, which is derived from package file version
    let zen_version: EZenPackageVersion = heuristic_zen_version_from_package_file_version(builder.legacy_package.summary.versioning_info.package_file_version, builder.container_header_version);

    builder.zen_package.is_unversioned = is_unversioned;
    builder.zen_package.versioning_info = FZenPackageVersioningInfo {
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
    // PKG_Cooked is only present in UE5.0+ packages. For earlier versions, check for FilterEditorOnlyData instead
    if builder.legacy_package.summary.versioning_info.package_file_version.is_ue5() {
        if (builder.legacy_package.summary.package_flags & (EPackageFlags::Cooked as u32)) == 0 {
            bail!("Detected absent PKG_Cooked flag in legacy package summary. Uncooked assets cannot be converted to Zen. Are you sure the asset has been Cooked?");
        }
    } else if (builder.legacy_package.summary.package_flags & (EPackageFlags::FilterEditorOnly as u32)) == 0 {
        bail!("Detected absent PKG_FilterEditorOnly flag in legacy package summary. Assets with editor data cannot be converted to Zen. Are you sure the asset has been Cooked?");
    }

    // Make sure we do not have any soft object paths serialized in the header. These cannot be represented in zen packages and should never be written when cooking
    if builder.legacy_package.summary.soft_object_paths.count > 0 {
        bail!("Detected soft object paths serialized as a part of the package header. Such paths cannot be represented in Zen packages and should never be written for cooked packages. Are you sure the package is cooked?");
    }

    // Set package name on the zen package from the legacy package header
    // (deferred to match name map order of editor)
    // builder.zen_package.summary.name = builder.zen_package.name_map.store(&builder.legacy_package.summary.package_name);
    // Copy size of the cooked header from the legacy package
    builder.zen_package.summary.cooked_header_size = builder.legacy_package.summary.versioning_info.total_header_size as u32;

    // Check if this is a localized package, and track the culture and source package name if it is
    if let Some((source_package_name, culture_name)) = convert_localized_package_name_to_source(&builder.legacy_package.summary.package_name) {
        // Store source package name and the culture name for which this package is localized
        builder.source_package_name = Some(source_package_name);
        builder.localized_package_culture = Some(culture_name);
    }

    // Setup source package name for the UE4 zen packages. UE5.0+ zen packages do not internally track source package name, it is a part of the container header only
    if builder.container_header_version <= EIoContainerHeaderVersion::Initial {
        // Require a source package name be either found or supplied externally from somewhere
        let source_package_name = builder.source_package_name.as_deref().expect("source_package_name required");
        builder.zen_package.summary.source_name = builder.zen_package.name_map.store(source_package_name);
    }

    // Copy bulk resources from the legacy package without modifications
    builder.zen_package.bulk_data = builder
        .legacy_package
        .data_resources
        .iter()
        .map(|x| FBulkDataMapEntry {
            serial_offset: x.serial_offset,
            duplicate_serial_offset: x.duplicate_serial_offset,
            serial_size: x.serial_size,
            flags: x.legacy_bulk_data_flags,
            cooked_index: x.cooked_index.unwrap_or(0),
            pad: [0, 0, 0],
        })
        .collect();
    Ok(())
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
    FPackageImportReference {
        imported_package_index,
        imported_public_export_hash_index,
    }
}

// Returns package name and package-relative export path. Package-relative export path is lowercased and is prefixed with /, and uses / as a separator
fn resolve_legacy_package_object(package: &ZenPackageBuilder, object_index: FPackageIndex) -> anyhow::Result<(String, String)> {
    // If this package is a redirect or a localized package, we want to use the name of the source package when resolving exports from it, not it's original name
    // This does not actually matter for UE5.0+ packages because their export hashes are package relative, but for UE4 this is important for being able to resolve references to localized package exports
    let package_name_override = package.source_package_name.as_deref();

    // Zen uses / as path separator, and always lowercases the package relative object path
    Ok(get_package_object_full_name(&package.legacy_package, object_index, '/', true, package_name_override))
}

fn convert_legacy_import_to_object_index(builder: &mut ZenPackageBuilder, import_index: usize) -> anyhow::Result<FPackageObjectIndex> {
    let (package_name, full_import_name) = resolve_legacy_package_object(builder, FPackageIndex::create_import(import_index as u32))?;

    // If this is a script import, just resolve it directly using the full import name as an index into script objects
    let is_script_import = package_name.starts_with("/Script/");
    if is_script_import {
        let script_object_index = FPackageObjectIndex::create_script_import(&full_import_name);
        if let Some(script_objects) = &builder.script_objects
            && !script_objects.script_object_lookup.contains_key(&script_object_index)
        {
            warning!(builder.log, "Package {} is referencing missing script import {}", &builder.package_name, &full_import_name);
        }
        return Ok(script_object_index);
    }
    // When Verse is involved, package imports that contain "/_Verse/VNI" in their names are treated as script imports if they are contained in script objects
    if package_name.contains("/_Verse/VNI") {
        let script_object_index = FPackageObjectIndex::create_script_import(&full_import_name);
        // We need script objects to handle VNI imports
        if builder.script_objects.is_none() {
            bail!("Script objects are required to convert legacy import of Verse VNI package {}", full_import_name);
        }
        // If this VNI package is generated by the UHT we should convert it to script import, otherwise, it is treated as a normal package import
        if builder.script_objects.as_ref().unwrap().script_object_lookup.contains_key(&script_object_index) {
            return Ok(script_object_index);
        }
    }

    // If this is a package import (full import name length is the same as package name), emit Null
    // Zen does not preserve Package imports, and they cannot be represented at all in terms of FPackageObjectIndex
    let is_package_import = package_name.len() == full_import_name.len();
    if is_package_import {
        return Ok(FPackageObjectIndex::create_null());
    }
    let package_id = FPackageId::from_name(&package_name);

    // Store the debug mapping of the ID of this import to the full name of it
    builder.debug_full_package_object_names.insert(FPackageIndex::create_import(import_index as u32), full_import_name.clone());

    // New style imports with export hashes and package IDs
    let result_package_import = if builder.container_header_version > EIoContainerHeaderVersion::Initial {
        // This is a normal import of the export of another package otherwise. Create FPackageId from package ID and public export hash from package relative path
        let public_export_hash = get_public_export_hash(&full_import_name[package_name.len() + 1..]);

        // Resolve import reference now, and convert it to object index
        let import_reference = resolve_zen_package_import(builder, package_id, &package_name, public_export_hash);
        FPackageObjectIndex::create_package_import(import_reference)
    } else {
        // Old style (UE4.27) imports with full name of the export just converted into FPackageObjectIndex
        let global_import_index = FPackageObjectIndex::create_legacy_package_import_from_path(&full_import_name);

        // Note that we still have to track this package ID in our imported packages, even if we are not indexing into it
        if let std::collections::hash_map::Entry::Vacant(e) = builder.package_import_lookup.entry(package_id) {
            let package_import_index = builder.zen_package.imported_packages.len() as u32;
            builder.zen_package.imported_packages.push(package_id);
            e.insert(package_import_index);
        }

        builder.import_to_package_id_lookup.insert(global_import_index, package_id);

        global_import_index
    };

    Ok(result_package_import)
}

fn convert_cell_import_to_object_index(builder: &mut ZenPackageBuilder, cell_import_index: usize) -> anyhow::Result<FPackageObjectIndex> {
    let cell_import = builder.legacy_package.cell_imports[cell_import_index].clone();
    let (package_name, _) = resolve_legacy_package_object(builder, cell_import.package_index)?;

    // If this is a script import, we want to create it from the full verse path
    let is_script_import = package_name.starts_with("/Script/");
    if is_script_import {
        let script_object_index = FPackageObjectIndex::create_script_import_from_verse_path(&cell_import.verse_path);
        // Check that the cell actually exists in the store. This is important for converting zen back to legacy assets, since missing script cell references cannot be converted back from the hash
        if let Some(script_cells) = &builder.script_cells
            && script_cells.find_script_cell(&script_object_index).is_none()
        {
            warning!(builder.log, "Package {} is referencing missing Verse Cell import {} (from native package {})", &builder.package_name, &cell_import.verse_path, &package_name);
        }
        return Ok(script_object_index);
    }

    // Cell imports will never point to packages, so we do not have to check for this case here unlike with regular imports
    let package_id = FPackageId::from_name(&package_name);
    let cell_export_hash = get_cell_export_hash(&cell_import.verse_path);

    // Resolve import reference now, and convert it to object index
    let import_reference = resolve_zen_package_import(builder, package_id, &package_name, cell_export_hash);
    Ok(FPackageObjectIndex::create_package_import(import_reference))
}

fn build_zen_import_map(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {
    builder.zen_package.import_map.reserve(builder.legacy_package.imports.len());
    builder.zen_package.cell_import_map.reserve(builder.legacy_package.cell_imports.len());

    for import_index in 0..builder.legacy_package.imports.len() {
        let import_object_index = convert_legacy_import_to_object_index(builder, import_index)?;
        builder.zen_package.import_map.push(import_object_index)
    }
    for cell_import_index in 0..builder.legacy_package.cell_imports.len() {
        let import_object_index = convert_cell_import_to_object_index(builder, cell_import_index)?;
        builder.zen_package.cell_import_map.push(import_object_index);
    }

    if builder.container_header_version > EIoContainerHeaderVersion::Initial {
        // Sort imports by package ID and rebuild maps pointing to sorted index
        // Create a sorted list of (old_index, package_id, package_name)
        let mut sorted_packages: Vec<(usize, FPackageId, String)> = builder
            .zen_package
            .imported_packages
            .iter()
            .enumerate()
            .map(|(index, &package_id)| {
                let package_name = builder.zen_package.imported_package_names[index].clone();
                (index, package_id, package_name)
            })
            .collect();
        sorted_packages.sort_by_key(|(_, package_id, _)| *package_id);

        // Build index remapping table: old_index -> new_sorted_index
        let mut index_remap: Vec<u32> = vec![0; sorted_packages.len()];
        for (new_index, (old_index, id, name)) in sorted_packages.into_iter().enumerate() {
            index_remap[old_index] = new_index as u32;

            builder.zen_package.imported_packages[new_index] = id;
            builder.zen_package.imported_package_names[new_index] = name;
        }

        // Update package_import_lookup with sorted indices
        for (new_index, package_id) in builder.zen_package.imported_packages.iter().enumerate() {
            builder.package_import_lookup.insert(*package_id, new_index as u32);
        }

        // Remap import_map and build import_to_package_id_lookup
        for import in &mut builder.zen_package.import_map {
            if import.kind() == FPackageObjectIndexType::PackageImport
                && let Some(mut package_import) = import.package_import()
            {
                let old_package_index = package_import.imported_package_index;
                let new_package_index = index_remap[old_package_index as usize];
                package_import.imported_package_index = new_package_index;
                *import = FPackageObjectIndex::create_package_import(package_import);

                // Map the resulting import to the original package ID it came from. This is necessary to resolve legacy UE4 imports into package ID
                let package_id = builder.zen_package.imported_packages[package_import.imported_package_index as usize];
                builder.import_to_package_id_lookup.insert(*import, package_id);
            }
        }

        // Remap cell_import_map to the new imported package indices
        for cell_import in &mut builder.zen_package.cell_import_map {
            if cell_import.kind() == FPackageObjectIndexType::PackageImport
                && let Some(mut package_import) = cell_import.package_import()
            {
                let old_package_index = package_import.imported_package_index;
                let new_package_index = index_remap[old_package_index as usize];
                package_import.imported_package_index = new_package_index;
                *cell_import = FPackageObjectIndex::create_package_import(package_import);
            }
        }
    }

    Ok(())
}

fn remap_package_index_reference(builder: &mut ZenPackageBuilder, package_index: FPackageIndex) -> FPackageObjectIndex {
    if package_index.is_export() {
        return FPackageObjectIndex::create_export(package_index.to_export_index());
    }
    if package_index.is_import() {
        return builder.zen_package.import_map[package_index.to_import_index() as usize];
    }
    FPackageObjectIndex::create_null()
}

fn build_zen_export_map(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {
    builder.zen_package.export_map.reserve(builder.legacy_package.exports.len());
    builder.zen_package.cell_export_map.reserve(builder.legacy_package.cell_exports.len());

    for export_index in 0..builder.legacy_package.exports.len() {
        let object_export = builder.legacy_package.exports[export_index].clone();
        let total_header_size = builder.legacy_package.summary.versioning_info.total_header_size as u64;
        let object_name = builder.legacy_package.name_map.get(object_export.object_name).to_string();

        let mut cooked_serial_offset = object_export.serial_offset as u64;
        if builder.container_header_version > EIoContainerHeaderVersion::Initial {
            // Zen cooked serial offset does not include header size, but legacy asset one does
            cooked_serial_offset -= total_header_size;
        }
        let mapped_object_name = builder.zen_package.name_map.store(&object_name);

        let outer_index = remap_package_index_reference(builder, object_export.outer_index);
        let class_index = remap_package_index_reference(builder, object_export.class_index);
        let super_index = remap_package_index_reference(builder, object_export.super_index);
        let template_index = remap_package_index_reference(builder, object_export.template_index);

        let gen_hash = (object_export.object_flags & EObjectFlags::Public as u32) != 0 || object_export.generate_public_hash;
        let (export_package_name, full_export_name) = resolve_legacy_package_object(builder, FPackageIndex::create_export(export_index as u32))?;

        // Use global import index converted to the raw representation for legacy packages, and get_public_export_hash otherwise
        let public_export_hash: u64 = if builder.container_header_version > EIoContainerHeaderVersion::Initial {
            if gen_hash { get_public_export_hash(&full_export_name[export_package_name.len() + 1..]) } else { 0 }
        } else {
            if gen_hash { FPackageObjectIndex::create_legacy_package_import_from_path(&full_export_name) } else { FPackageObjectIndex::create_null() }.to_raw()
        };

        let filter_flags: EExportFilterFlags = if object_export.is_not_for_server {
            EExportFilterFlags::NotForServer
        } else if object_export.is_not_for_client {
            EExportFilterFlags::NotForClient
        } else {
            EExportFilterFlags::None
        };

        // Store the debug mapping of the ID of this import to the full name of it
        builder.debug_full_package_object_names.insert(FPackageIndex::create_export(export_index as u32), full_export_name.clone());

        let zen_export = FExportMapEntry {
            cooked_serial_offset,
            cooked_serial_size: object_export.serial_size as u64,
            object_name: mapped_object_name,
            object_flags: object_export.object_flags,
            outer_index,
            class_index,
            super_index,
            template_index,
            public_export_hash,
            filter_flags,
            padding: [0; 3],
        };
        builder.zen_package.export_map.push(zen_export);
    }

    for cell_export_index in 0..builder.legacy_package.cell_exports.len() {
        let cell_export = builder.legacy_package.cell_exports[cell_export_index].clone();
        let total_header_size = builder.legacy_package.summary.versioning_info.total_header_size as u64;

        let serial_offset = cell_export.serial_offset as u64 - total_header_size;
        let serial_layout_size: u64 = cell_export.serial_layout_size as u64;
        let serial_size: u64 = cell_export.serial_size as u64;

        let cpp_class_info = builder.legacy_package.name_map.get(cell_export.cpp_class_info).to_string();
        let mapped_cpp_class_info = builder.zen_package.name_map.store(&cpp_class_info);

        // Export hash is zero if the verse path is empty, otherwise it is calculated from the verse path
        let public_export_hash: u64 = if !cell_export.verse_path.is_empty() { get_cell_export_hash(&cell_export.verse_path) } else { 0 };

        let zen_cell_export = FCellExportMapEntry {
            cooked_serial_offset: serial_offset,
            cooked_serial_layout_size: serial_layout_size,
            cooked_serial_size: serial_size,
            cpp_class_info: mapped_cpp_class_info,
            public_export_hash,
        };
        builder.zen_package.cell_export_map.push(zen_cell_export);
    }
    Ok(())
}

#[derive(Debug, Copy, Clone, PartialEq, Default, Eq, Hash)]
struct ZenDependencyGraphNode {
    package_index: FPackageIndex,
    command_type: EExportCommandType,
}

fn build_zen_dependency_bundles_legacy(builder: &mut ZenPackageBuilder, export_load_order: &[ZenExportGraphNode], export_dependencies: &HashMap<ZenDependencyGraphNode, Vec<ZenDependencyGraphNode>>) {
    let mut current_export_bundle_header_index: i64 = -1;
    let mut current_export_offset: u64 = 0;
    let mut export_to_bundle_map: HashMap<ZenDependencyGraphNode, usize> = HashMap::new();

    // Create export bundles from the export list sorted by the dependencies
    for graph_node in export_load_order {
        let dependency_graph_node = graph_node.node;

        // Skip non-export items in the dependency graph. Imports will occasionally appear in the graph when there is a requirement for both a creation and a serialization
        if !dependency_graph_node.package_index.is_export() {
            continue;
        }

        let export_index = dependency_graph_node.package_index.to_export_index() as usize;
        let export_command_type = dependency_graph_node.command_type;

        // Open a new export bundle if we do not have one running
        if current_export_bundle_header_index == -1 {
            current_export_bundle_header_index = builder.zen_package.export_bundle_headers.len() as i64;

            let first_entry_index = builder.zen_package.export_bundle_entries.len() as u32;
            let serial_offset = current_export_offset;

            builder.zen_package.export_bundle_headers.push(FExportBundleHeader { serial_offset, first_entry_index, entry_count: 0 })
        }

        // Add current export as an entry into the currently open bundle
        builder.zen_package.export_bundle_entries.push(FExportBundleEntry {
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
        let is_public_export = builder.zen_package.export_map[export_index].is_public_export();

        // If we perform the fix-up on the serialized package data later, store the information necessary to perform the fixup of another package imports
        if is_public_export && builder.fixup_legacy_external_arcs && builder.container_header_version <= EIoContainerHeaderVersion::Initial {
            let export_global_index = builder.zen_package.export_map[export_index].legacy_global_import_index();
            let full_export_name = builder.debug_full_package_object_names.get(&FPackageIndex::create_export(export_index as u32)).cloned();

            builder.legacy_export_bundle_mapping.push(ZenLegacyPackageExportBundleMapping {
                export_index: export_global_index,
                export_command_type,
                export_bundle_index: current_export_bundle_header_index as i32,
                _debug_full_export_name: full_export_name,
            });
        }

        // Export bundles end at a public export with an export hash. So if this is a public export, close the current bundle
        if is_public_export {
            current_export_bundle_header_index = -1;
        }
    }

    // Used to avoid adding duplicate dependencies between export bundles and other export bundles/imports
    let mut internal_dependency_arcs: HashSet<FInternalDependencyArc> = HashSet::new();
    let mut external_dependency_arcs: HashSet<FExternalDependencyArc> = HashSet::new();
    let mut legacy_dependency_arcs: HashSet<(FPackageId, FInternalDependencyArc)> = HashSet::new();

    // Function to create export dependency arcs to the export's export bundle from another export's export bundle, or from an entry in the import map
    let mut create_dependency_arc_from_node = |to_export_bundle_index: i32, dependency_node: &ZenDependencyGraphNode, mut_builder: &mut ZenPackageBuilder| {
        // This is an export-to-export dependency
        if dependency_node.package_index.is_export() {
            let from_export_bundle_index = *export_to_bundle_map.get(dependency_node).unwrap() as i32;

            // Skip dependencies between exports that belong to the same bundle, they are already sorted
            if from_export_bundle_index != to_export_bundle_index {
                // If we have not previously created a dependency from that export bundle to our export bundle, add one
                let internal_dependency_arc = FInternalDependencyArc { from_export_bundle_index, to_export_bundle_index };
                if !internal_dependency_arcs.contains(&internal_dependency_arc) {
                    internal_dependency_arcs.insert(internal_dependency_arc);
                    // Note that internal dependency arcs are discarded in UE4.27, and export bundle N always has an implicit internal dependency arc to bundle N-1
                    // Since we create bundles in the export load order, such an implicit ordering works well and does not need to be represented by the internal dependency arc
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
                // New graph data will map a specific import to the specific export bundle for UE5.0+ zen assets
                if mut_builder.container_header_version > EIoContainerHeaderVersion::Initial {
                    let imported_package_index = package_object_import.package_import().unwrap().imported_package_index as usize;
                    let external_dependency_arc = FExternalDependencyArc {
                        from_import_index,
                        from_command_type,
                        to_export_bundle_index,
                    };

                    // Only add the dependency arc if we have not previously created in
                    if !external_dependency_arcs.contains(&external_dependency_arc) {
                        external_dependency_arcs.insert(external_dependency_arc);
                        // We lay out external package dependencies to match imported package indices, so this is always safe
                        mut_builder.zen_package.external_package_dependencies[imported_package_index].external_dependency_arcs.push(external_dependency_arc);
                    }
                } else {
                    let imported_package_id = *mut_builder.import_to_package_id_lookup.get(&package_object_import).unwrap();
                    let imported_package_index = *mut_builder.package_import_lookup.get(&imported_package_id).unwrap() as usize;

                    // Legacy UE4 graph data will only map the export bundle index in this package to export bundle index in the imported package
                    // This requires knowledge of the export bundle layout of another package, which we do not have if fix-up is not possible.
                    // If we are intending to fix up the serialized data later though, write a placeholder value and emit the information necessary for the fixup
                    let from_export_bundle_index: i32 = if mut_builder.fixup_legacy_external_arcs {
                        let current_fixup_id = mut_builder.legacy_external_arc_counter;
                        let full_import_name = mut_builder.debug_full_package_object_names.get(&dependency_node.package_index).cloned();

                        let fixup_data = ZenLegacyPackageExternalArcFixupData {
                            fixup_from_bundle_id: current_fixup_id,
                            from_package_id: imported_package_id,
                            from_import_index: package_object_import,
                            from_command_type,
                            debug_full_import_name: full_import_name,
                        };
                        // Add the fixup data to the hash map and increment the counter, and write current fixup ID as the bundle index
                        mut_builder.legacy_external_arc_fixup_data.push(fixup_data);
                        mut_builder.legacy_external_arc_counter += 1;
                        current_fixup_id
                    } else {
                        // Assume the first index because there is only one export bundle because export bundles are per package and there is only ever one package per asset.
                        0
                    };

                    // Prevent adding duplicate dependencies on the packages
                    let legacy_dependency_arc = FInternalDependencyArc { from_export_bundle_index, to_export_bundle_index };
                    if !legacy_dependency_arcs.contains(&(imported_package_id, legacy_dependency_arc)) {
                        legacy_dependency_arcs.insert((imported_package_id, legacy_dependency_arc));
                        mut_builder.zen_package.external_package_dependencies[imported_package_index].legacy_dependency_arcs.push(legacy_dependency_arc);
                    }
                }
            }
        }
    };

    // Pre-initialize external package dependencies with the number of imported package IDs
    builder.zen_package.external_package_dependencies.reserve(builder.zen_package.imported_packages.len());
    for imported_package_id in &builder.zen_package.imported_packages {
        builder.zen_package.external_package_dependencies.push(ExternalPackageDependency {
            from_package_id: *imported_package_id,
            external_dependency_arcs: Vec::new(),
            legacy_dependency_arcs: Vec::new(),
        });
    }

    // Build internal and external dependency arcs
    for export_index in 0..builder.zen_package.export_map.len() {
        let export_create_node = ZenDependencyGraphNode {
            package_index: FPackageIndex::create_export(export_index as u32),
            command_type: EExportCommandType::Create,
        };
        let export_serialize_node = ZenDependencyGraphNode {
            package_index: FPackageIndex::create_export(export_index as u32),
            command_type: EExportCommandType::Serialize,
        };

        let export_create_bundle_index = *export_to_bundle_map.get(&export_create_node).unwrap();
        let export_serialize_bundle_index = *export_to_bundle_map.get(&export_serialize_node).unwrap();

        for export_create_dependency in export_dependencies.get(&export_create_node).unwrap_or(&Vec::new()) {
            create_dependency_arc_from_node(export_create_bundle_index as i32, export_create_dependency, builder);
        }
        for export_serialize_dependency in export_dependencies.get(&export_serialize_node).unwrap_or(&Vec::new()) {
            create_dependency_arc_from_node(export_serialize_bundle_index as i32, export_serialize_dependency, builder);
        }
    }
}

fn build_zen_dependency_bundle_new(builder: &mut ZenPackageBuilder, export_load_order: &[ZenExportGraphNode], export_dependencies: &HashMap<ZenDependencyGraphNode, Vec<ZenDependencyGraphNode>>) {
    // Create a single dependency bundle with all exports
    for dependency_graph in export_load_order {
        let dependency_graph_node = dependency_graph.node;

        // Skip non-export items in the dependency graph. Imports will occasionally appear in the graph when there is a requirement for both a creation and a serialization
        if !dependency_graph_node.package_index.is_export() {
            continue;
        }

        let export_index = dependency_graph_node.package_index.to_export_index() as usize;
        let export_command_type = dependency_graph_node.command_type;

        // Add current export as an entry into the currently open bundle
        builder.zen_package.export_bundle_entries.push(FExportBundleEntry {
            local_export_index: export_index as u32,
            command_type: export_command_type,
        });
    }

    // Collects all dependencies of the given node with the given command type
    let collect_export_dependencies = |to_dependency_node: &ZenDependencyGraphNode, from_command_type: EExportCommandType, immut_builder: &ZenPackageBuilder| -> Vec<FDependencyBundleEntry> {
        let mut result_dependencies: Vec<FDependencyBundleEntry> = Vec::new();

        for from_dependency_node in export_dependencies.get(to_dependency_node).unwrap_or(&Vec::new()) {
            // Skip nodes that do not have the matching command type, and skip exact self-references (same export, same command type)
            if from_dependency_node.command_type == from_command_type && !(from_dependency_node.package_index == to_dependency_node.package_index && from_dependency_node.command_type == to_dependency_node.command_type) {
                // If this is an export, add the dependency bundle entry at all times
                if from_dependency_node.package_index.is_export() {
                    result_dependencies.push(FDependencyBundleEntry {
                        local_import_or_export_index: from_dependency_node.package_index,
                    });
                }
                // Otherwise, if this is an import, we only add it if it's a package export import
                else if from_dependency_node.package_index.is_import() {
                    // Note that we are working with "extended" package indices here, where raw import index could refer either to normal import or to cell import
                    // We need to handle both here separately since both of them are relevant for dependency sorting
                    let raw_import_index = from_dependency_node.package_index.to_import_index() as usize;
                    let zen_import_package_index = if raw_import_index < immut_builder.zen_package.import_map.len() {
                        immut_builder.zen_package.import_map[raw_import_index]
                    } else {
                        immut_builder.zen_package.cell_import_map[raw_import_index - immut_builder.zen_package.import_map.len()]
                    };

                    if zen_import_package_index.kind() == FPackageObjectIndexType::PackageImport {
                        result_dependencies.push(FDependencyBundleEntry {
                            local_import_or_export_index: from_dependency_node.package_index,
                        });
                    }
                }
            }
        }
        result_dependencies
    };

    // Build dependency bundles. Note that we need to build them for both normal exports and cell exports, which together form an "extended" set of package indices
    let total_export_count = builder.zen_package.export_map.len() + builder.zen_package.cell_export_map.len();
    for extended_export_index in 0..total_export_count {
        let export_create_node = ZenDependencyGraphNode {
            package_index: FPackageIndex::create_export(extended_export_index as u32),
            command_type: EExportCommandType::Create,
        };
        let export_serialize_node = ZenDependencyGraphNode {
            package_index: FPackageIndex::create_export(extended_export_index as u32),
            command_type: EExportCommandType::Serialize,
        };

        let mut create_before_create_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_create_node, EExportCommandType::Create, builder);
        let mut serialize_before_create_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_create_node, EExportCommandType::Serialize, builder);
        let mut create_before_serialize_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_serialize_node, EExportCommandType::Create, builder);
        let mut serialize_before_serialize_deps: Vec<FDependencyBundleEntry> = collect_export_dependencies(&export_serialize_node, EExportCommandType::Serialize, builder);

        // Create dependency header for this export
        let first_entry_index = builder.zen_package.dependency_bundle_entries.len() as i32;

        builder.zen_package.dependency_bundle_headers.push(FDependencyBundleHeader {
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

#[derive(PartialEq, Eq, Copy, Clone, Hash)]
struct ZenExportGraphNode {
    node: ZenDependencyGraphNode,
    is_public_export: bool,
}

#[derive(PartialEq, Eq, Copy, Clone, Hash)]
struct OrdNew(ZenExportGraphNode);
impl From<ZenExportGraphNode> for OrdNew {
    fn from(value: ZenExportGraphNode) -> Self {
        Self(value)
    }
}
impl Deref for OrdNew {
    type Target = ZenExportGraphNode;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl PartialOrd for OrdNew {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdNew {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .is_public_export
            .cmp(&self.is_public_export)
            .then(self.node.command_type.cmp(&other.node.command_type))
            .then(self.node.package_index.to_export_index().cmp(&other.node.package_index.to_export_index()))
            .reverse()
    }
}

#[derive(PartialEq, Eq, Copy, Clone, Hash)]
struct OrdOld(ZenExportGraphNode);
impl From<ZenExportGraphNode> for OrdOld {
    fn from(value: ZenExportGraphNode) -> Self {
        Self(value)
    }
}
impl Deref for OrdOld {
    type Target = ZenExportGraphNode;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl PartialOrd for OrdOld {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdOld {
    fn cmp(&self, other: &Self) -> Ordering {
        self.node.package_index.to_export_index().cmp(&other.node.package_index.to_export_index()).then(self.node.command_type.cmp(&other.node.command_type)).reverse()
    }
}

fn sort_dependencies_in_load_order<O>(export_graph_nodes: &Vec<ZenExportGraphNode>, dependency_to_dependants: &HashMap<ZenExportGraphNode, Vec<ZenExportGraphNode>>) -> anyhow::Result<Vec<ZenExportGraphNode>>
where
    O: From<ZenExportGraphNode> + Ord + Deref<Target = ZenExportGraphNode>,
{
    let mut incoming_edge_count: HashMap<ZenExportGraphNode, usize> = HashMap::new();

    // Prime all nodes that have dependencies
    for to_nodes in dependency_to_dependants.values() {
        for to_node in to_nodes {
            *incoming_edge_count.entry(*to_node).or_default() += 1;
        }
    }

    // Prime list of nodes that have no dependencies on other nodes
    let mut nodes_with_no_incoming_edges: BinaryHeap<O> = BinaryHeap::with_capacity(export_graph_nodes.len());
    for export_node in export_graph_nodes {
        if *incoming_edge_count.entry(*export_node).or_default() == 0 {
            nodes_with_no_incoming_edges.push((*export_node).into());
        }
    }

    // Take nodes with no dependencies until we run out of them
    let mut load_order: Vec<ZenExportGraphNode> = Vec::with_capacity(export_graph_nodes.len());
    while !nodes_with_no_incoming_edges.is_empty() {
        let removed_node = nodes_with_no_incoming_edges.pop().unwrap();
        load_order.push(*removed_node);

        // Remove one edge from all the nodes that depend on this node
        if let Some(node_dependants) = dependency_to_dependants.get(&removed_node) {
            for to_node in node_dependants {
                // Make sure the to node has the edge for this node
                let incoming_edge_count = incoming_edge_count.entry(*to_node).or_default();
                *incoming_edge_count -= 1;

                // If to node no longer has any dependencies that are still unsatisfied, add it to the list of nodes with no incoming edges to be processed later
                if *incoming_edge_count == 0 {
                    nodes_with_no_incoming_edges.push((*to_node).into());
                }
            }
        }
    }

    // Make sure we actually sorted all the dependencies. If we did not we have a circular dependency on one of the nodes
    if load_order.len() != export_graph_nodes.len() {
        bail!("Failed to sort exports in load order because of circular dependencies");
    }
    Ok(load_order)
}

fn build_zen_preload_dependencies(builder: &mut ZenPackageBuilder) -> anyhow::Result<()> {
    // Build a dependency map with each export and it's preload dependencies
    let export_count = builder.legacy_package.exports.len();
    let cell_export_count = builder.legacy_package.cell_exports.len();
    let total_export_count = export_count + cell_export_count;
    let mut export_dependencies: HashMap<ZenDependencyGraphNode, Vec<ZenDependencyGraphNode>> = HashMap::with_capacity(total_export_count * 2);
    let mut export_graph_nodes: Vec<ZenExportGraphNode> = Vec::with_capacity(total_export_count * 2);

    for export_index in 0..export_count {
        let export_package_index = FPackageIndex::create_export(export_index as u32);
        let object_export = builder.legacy_package.exports[export_index].clone();

        let create_graph_node = ZenDependencyGraphNode {
            package_index: export_package_index,
            command_type: EExportCommandType::Create,
        };
        let serialize_graph_node = ZenDependencyGraphNode {
            package_index: export_package_index,
            command_type: EExportCommandType::Serialize,
        };

        let mut create_dependencies: Vec<ZenDependencyGraphNode> = Vec::new();
        let mut serialize_dependencies: Vec<ZenDependencyGraphNode> = Vec::new();

        // Collect create and serialize dependencies for this export
        if object_export.first_export_dependency_index != -1 {
            // Create before create dependencies. They go first because Package Store Optimizer puts them first
            for i in 0..object_export.create_before_create_dependencies {
                let preload_dependency_index = object_export.first_export_dependency_index + object_export.serialize_before_serialize_dependencies + object_export.create_before_serialize_dependencies + object_export.serialize_before_create_dependencies + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode {
                    package_index: preload_dependency,
                    command_type: EExportCommandType::Create,
                };
                create_dependencies.push(dependency);
            }

            // Serialize before create dependencies. They go second because Package Store Optimizer puts them second
            for i in 0..object_export.serialize_before_create_dependencies {
                let preload_dependency_index = object_export.first_export_dependency_index + object_export.serialize_before_serialize_dependencies + object_export.create_before_serialize_dependencies + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode {
                    package_index: preload_dependency,
                    command_type: EExportCommandType::Serialize,
                };
                create_dependencies.push(dependency);
            }

            // Create before serialize dependencies. They go third because Package Store Optimizer puts them third
            for i in 0..object_export.create_before_serialize_dependencies {
                let preload_dependency_index = object_export.first_export_dependency_index + object_export.serialize_before_serialize_dependencies + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode {
                    package_index: preload_dependency,
                    command_type: EExportCommandType::Create,
                };
                serialize_dependencies.push(dependency);
            }

            // Serialize before serialize dependencies. They go last because Package Store Optimizer puts them last
            for i in 0..object_export.serialize_before_serialize_dependencies {
                let preload_dependency_index = object_export.first_export_dependency_index + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode {
                    package_index: preload_dependency,
                    command_type: EExportCommandType::Serialize,
                };
                serialize_dependencies.push(dependency);
            }
        }

        // Add create and serialize graph nodes for this export
        // Nodes are added into the graph in export order, Create first, then Serialize. So Export0Create -> Export0Serialize -> Export1Create -> Export1Serialize -> etc
        let is_public_export = builder.zen_package.export_map[export_index].is_public_export();
        export_graph_nodes.push(ZenExportGraphNode { node: create_graph_node, is_public_export });
        export_graph_nodes.push(ZenExportGraphNode { node: serialize_graph_node, is_public_export });

        if builder.container_header_version < EIoContainerHeaderVersion::NoExportInfo {
            //This export's serialize has a dependency on this export's create. This dependency is added first because it is added before anything else by the Package Store Optimizer
            serialize_dependencies.push(create_graph_node);
        }

        // Remember dependencies associated with each node. This is necessary for building dependency arcs later
        export_dependencies.insert(create_graph_node, create_dependencies);
        export_dependencies.insert(serialize_graph_node, serialize_dependencies);
    }

    // Cell exports directly follow normal exports in preload dependencies bundle order. So their indices are offset by the export count
    for cell_export_index in 0..cell_export_count {
        let cell_export_package_index = FPackageIndex::create_export((export_count + cell_export_index) as u32);
        let cell_export = builder.legacy_package.cell_exports[cell_export_index].clone();

        let create_graph_node = ZenDependencyGraphNode {
            package_index: cell_export_package_index,
            command_type: EExportCommandType::Create,
        };
        let serialize_graph_node = ZenDependencyGraphNode {
            package_index: cell_export_package_index,
            command_type: EExportCommandType::Serialize,
        };

        let create_dependencies: Vec<ZenDependencyGraphNode> = Vec::new();
        let mut serialize_dependencies: Vec<ZenDependencyGraphNode> = Vec::new();

        // Collect create and serialize dependencies for this cell export
        if cell_export.first_export_dependency_index != -1 {
            // Create before serialize dependencies. They go first because Package Store Optimizer puts them first
            for i in 0..cell_export.create_before_serialize_dependencies {
                let preload_dependency_index = cell_export.first_export_dependency_index + cell_export.serialize_before_serialize_dependencies + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode {
                    package_index: preload_dependency,
                    command_type: EExportCommandType::Create,
                };
                serialize_dependencies.push(dependency);
            }

            // Serialize before serialize dependencies. They go second because Package Store Optimizer puts them second
            for i in 0..cell_export.serialize_before_serialize_dependencies {
                let preload_dependency_index = cell_export.first_export_dependency_index + i;
                let preload_dependency = builder.legacy_package.preload_dependencies[preload_dependency_index as usize];

                let dependency = ZenDependencyGraphNode {
                    package_index: preload_dependency,
                    command_type: EExportCommandType::Serialize,
                };
                serialize_dependencies.push(dependency);
            }
        }

        // Add create and serialize graph nodes for this export. Cell exports are not considered public exports for the purposes of dependency ordering
        export_graph_nodes.push(ZenExportGraphNode { node: create_graph_node, is_public_export: false });
        export_graph_nodes.push(ZenExportGraphNode { node: serialize_graph_node, is_public_export: false });

        // Remember dependencies associated with each node. This is necessary for building dependency arcs later
        export_dependencies.insert(create_graph_node, create_dependencies);
        export_dependencies.insert(serialize_graph_node, serialize_dependencies);
    }

    // Build a reverse lookup from export to exports that depend on it
    let mut dependency_to_dependants: HashMap<ZenExportGraphNode, Vec<ZenExportGraphNode>> = HashMap::with_capacity(export_count);
    for dependant_node in &export_graph_nodes {
        if let Some(dependencies) = export_dependencies.get(&dependant_node.node) {
            for raw_dependency_node in dependencies {
                // Skip non-export dependencies from exports. They do not matter for graph building purposes
                if !raw_dependency_node.package_index.is_export() {
                    continue;
                }
                // Determine whenever this export is public or not to create a ZenExportGraphNode
                // Note that we are working with "extended" package indices here, e.g. indices past normal import/export maps refer to cell import/export maps
                // So we need to check that the raw export index actually belongs to the export map and not cell export map here. Cell exports are never considered public.
                let raw_export_index = raw_dependency_node.package_index.to_export_index() as usize;
                let is_public_export = if raw_export_index < builder.zen_package.export_map.len() { builder.zen_package.export_map[raw_export_index].is_public_export() } else { false };

                // Create the dependency node and add the dependant node to its dependants list
                let dependency_node = ZenExportGraphNode { node: *raw_dependency_node, is_public_export };
                dependency_to_dependants.entry(dependency_node).or_default().push(*dependant_node);
            }
        }
    }

    // Sort the export graph nodes in load order
    let sorted_node_list = if builder.container_header_version > EIoContainerHeaderVersion::Initial {
        sort_dependencies_in_load_order::<OrdNew>(&export_graph_nodes, &dependency_to_dependants)
    } else {
        sort_dependencies_in_load_order::<OrdOld>(&export_graph_nodes, &dependency_to_dependants)
    }?;

    // Use legacy path for versions before NoExportInfo, and a new one for versions after
    if builder.container_header_version >= EIoContainerHeaderVersion::NoExportInfo {
        build_zen_dependency_bundle_new(builder, &sorted_node_list, &export_dependencies);
    } else {
        build_zen_dependency_bundles_legacy(builder, &sorted_node_list, &export_dependencies);
    }
    Ok(())
}

fn write_exports_in_bundle_order<S: Write>(writer: &mut S, builder: &ZenPackageBuilder, exports_buffer: &[u8]) -> anyhow::Result<()> {
    let total_header_size = builder.legacy_package.summary.versioning_info.total_header_size as u64;
    let mut current_export_offset: u64 = 0;
    let mut largest_exports_buffer_export_end_offset: usize = 0;

    for export_bundle_header_index in 0..builder.zen_package.export_bundle_headers.len() {
        let export_bundle_header = builder.zen_package.export_bundle_headers[export_bundle_header_index];

        // Make sure bundle data is actually being placed at the correct offset
        if export_bundle_header.serial_offset != current_export_offset {
            bail!(
                "Export bundle {} serial offset does not match it's actual placement. Expected bundle data to be placed at {}, but it's placed at {}",
                export_bundle_header_index,
                export_bundle_header.serial_offset,
                current_export_offset
            );
        }

        for i in 0..export_bundle_header.entry_count {
            let export_bundle_entry_index = export_bundle_header.first_entry_index + i;
            let export_bundle_entry = builder.zen_package.export_bundle_entries[export_bundle_entry_index as usize];

            // Only Serialize command actually means the export data placement
            if export_bundle_entry.command_type == EExportCommandType::Serialize {
                let export_index = export_bundle_entry.local_export_index as usize;

                // Export serial offset here is actually relative to the legacy package header size, so we need to subtract it to get the real position in the exports buffer
                let export_serial_offset = (builder.legacy_package.exports[export_index].serial_offset as u64 - total_header_size) as usize;
                let export_serial_size = builder.legacy_package.exports[export_index].serial_size as usize;
                let export_end_serial_offset = export_serial_offset + export_serial_size;

                // Serialize the export at this position and increment the current position
                largest_exports_buffer_export_end_offset = max(largest_exports_buffer_export_end_offset, export_end_serial_offset);
                writer.write_all(&exports_buffer[export_serial_offset..export_end_serial_offset])?;
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
        writer.write_all(&exports_buffer[extra_data_start_offset..extra_data_end_offset])?;
    }
    Ok(())
}

fn serialize_zen_asset(builder: &ZenPackageBuilder, legacy_asset_bundle: &FSerializedAssetBundle) -> anyhow::Result<(StoreEntry, Vec<u8>, Vec<u64>)> {
    let mut result_package_buffer: Vec<u8> = Vec::new();
    let mut result_package_writer = Cursor::new(&mut result_package_buffer);
    let mut result_store_entry: StoreEntry = StoreEntry::default();

    // Serialize package header
    let legacy_external_arcs_serialized_offsets = FZenPackageHeader::serialize(&builder.zen_package, &mut result_package_writer, &mut result_store_entry, builder.container_header_version)?;

    if builder.container_header_version >= EIoContainerHeaderVersion::NoExportInfo {
        // Write export buffer without any changes if we are following cooked offsets
        result_package_writer.write_all(&legacy_asset_bundle.exports_file_buffer)?;
    } else {
        // Write export buffer in bundle order otherwise, moving exports around to follow bundle serialization order
        write_exports_in_bundle_order(&mut result_package_writer, builder, &legacy_asset_bundle.exports_file_buffer)?;
    }
    Ok((result_store_entry, result_package_buffer, legacy_external_arcs_serialized_offsets))
}

fn build_converted_zen_asset(builder: &ZenPackageBuilder, legacy_asset_bundle: FSerializedAssetBundle, path: &UEPath, package_name_to_referenced_shader_maps: &HashMap<String, Vec<FSHAHash>>) -> anyhow::Result<ConvertedZenAssetBundle> {
    let (mut result_store_entry, result_package_buffer, legacy_external_arc_serialized_offsets) = serialize_zen_asset(builder, &legacy_asset_bundle)?;

    let package_name = builder.source_package_name.as_ref().unwrap_or(&builder.legacy_package.summary.package_name).clone();

    // Append shader map hashes to the store entry from the package name to shader maps lookup
    if let Some(referenced_shader_maps) = package_name_to_referenced_shader_maps.get(&package_name) {
        result_store_entry.shader_map_hashes.append(&mut referenced_shader_maps.clone());
    }

    Ok(ConvertedZenAssetBundle {
        package_id: builder.package_id,
        package_name,
        path: path.into(),
        store_entry: result_store_entry,
        package_buffer: result_package_buffer,
        bulk_data_buffer: legacy_asset_bundle.bulk_data_buffer,
        optional_bulk_data_buffer: legacy_asset_bundle.optional_bulk_data_buffer,
        memory_mapped_bulk_data_buffer: legacy_asset_bundle.memory_mapped_bulk_data_buffer,
        source_package_name: builder.source_package_name.clone(),
        localized_package_culture_name: builder.localized_package_culture.clone(),
        legacy_external_arc_serialized_offsets,
        legacy_external_arc_fixup_data: builder.legacy_external_arc_fixup_data.clone(),
        legacy_export_bundle_mapping_data: builder.legacy_export_bundle_mapping.clone(),
    })
}

pub(crate) struct ConvertedZenAssetBundle {
    pub(crate) package_id: FPackageId,
    pub(crate) package_name: String,
    path: UEPathBuf,
    store_entry: StoreEntry,
    package_buffer: Vec<u8>,
    bulk_data_buffer: Option<Vec<u8>>,
    optional_bulk_data_buffer: Option<Vec<u8>>,
    memory_mapped_bulk_data_buffer: Option<Vec<u8>>,
    source_package_name: Option<String>,
    localized_package_culture_name: Option<String>,
    // Offsets into the package buffer at which legacy external arcs have been serialized. Needed for UE4 external arc fixup that requires knowing the layout of imported assets
    legacy_external_arc_serialized_offsets: Vec<u64>,
    legacy_external_arc_fixup_data: Vec<ZenLegacyPackageExternalArcFixupData>,
    legacy_export_bundle_mapping_data: Vec<ZenLegacyPackageExportBundleMapping>,
}
impl ConvertedZenAssetBundle {
    pub(crate) fn package_data_size(&self) -> usize {
        self.package_buffer.len()
    }
    pub(crate) fn fixup_legacy_external_arcs(&mut self, global_package_lookup: &HashMap<FPackageId, Arc<RwLock<ConvertedZenAssetBundle>>>, log: &Log) -> anyhow::Result<()> {
        for legacy_serialized_offset in &self.legacy_external_arc_serialized_offsets {
            // Seek to the relevant position and read the ID of the placeholder from bundle index
            let placeholder_from_bundle_index: i32 = {
                let mut package_buffer_reader = Cursor::new(&self.package_buffer);
                package_buffer_reader.seek(SeekFrom::Start(*legacy_serialized_offset))?;
                package_buffer_reader.de()?
            };

            // Resolve the fixup data for this arc
            let fixup_data = self
                .legacy_external_arc_fixup_data
                .iter()
                .find(|x| x.fixup_from_bundle_id == placeholder_from_bundle_index)
                .cloned()
                .ok_or_else(|| anyhow!("Failed to find fixup data for placeholder ID {}", placeholder_from_bundle_index))?;

            // Attempt to find the package in the lookup to which this import maps
            let result_from_bundle_index: i32 = if let Some(referenced_asset_bundle_lock) = global_package_lookup.get(&fixup_data.from_package_id) {
                // Resolve the export this reference is mapping to
                let referenced_asset_bundle = referenced_asset_bundle_lock.read().unwrap();
                let export_bundle_mapping = referenced_asset_bundle
                    .legacy_export_bundle_mapping_data
                    .iter()
                    .find(|x| x.export_index == fixup_data.from_import_index && x.export_command_type == fixup_data.from_command_type)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!(
                            "Failed to find export in the package {} ({}) mapping to the import {} (full name: {}) dependency {:?} in package {} ({})",
                            referenced_asset_bundle.package_name.clone(),
                            referenced_asset_bundle.package_id,
                            fixup_data.from_import_index,
                            fixup_data.debug_full_import_name.clone().unwrap_or(String::from("unknown")),
                            fixup_data.from_command_type,
                            self.package_name.clone(),
                            self.package_id
                        )
                    })?;

                debug!(
                    log,
                    "Applying fixup to package {} for import of package {} export {} command {:?}. Resolved export bundle for the export: {}",
                    self.package_id.clone(),
                    fixup_data.from_package_id.clone(),
                    fixup_data.from_import_index,
                    fixup_data.from_command_type,
                    export_bundle_mapping.export_bundle_index
                );

                // We found the export bundle this dependency maps to
                export_bundle_mapping.export_bundle_index
            } else {
                // This import is not found in the global package lookup, so assume it is external and use -1 as a value meaning "last export bundle in the package"
                -1
            };

            // Write the fixed-up from export bundle index to the correct position
            let mut package_buffer_writer = Cursor::new(&mut self.package_buffer);
            package_buffer_writer.seek(SeekFrom::Start(*legacy_serialized_offset))?;
            package_buffer_writer.ser(&result_from_bundle_index)?;
        }
        Ok(())
    }

    // Writes both the package data and the bulk data in one go
    pub(crate) fn write(&mut self, writer: &mut IoStoreWriter) -> anyhow::Result<()> {
        self.write_package_data(writer)?;
        self.write_and_release_bulk_data(writer)?;
        Ok(())
    }

    // Writes package data into the container, and releases the reference to it
    pub(crate) fn write_package_data(&mut self, writer: &mut IoStoreWriter) -> anyhow::Result<()> {
        let package_chunk_id = FIoChunkId::from_package_id(self.package_id, 0, EIoChunkType::ExportBundleData);
        writer.write_package_chunk(package_chunk_id, Some(&self.path), &self.package_buffer, &self.store_entry)?;

        // Add the localized package entry if this is a localized package
        if let Some(package_culture_name) = &self.localized_package_culture_name {
            writer.add_localized_package(package_culture_name, self.source_package_name.as_ref().unwrap(), self.package_id)?;
        }
        // If this is a redirected package, add the redirect to the redirect map
        else if let Some(source_package_name) = &self.source_package_name {
            writer.add_package_redirect(source_package_name, self.package_id)?;
        }

        self.package_buffer = Vec::new();
        Ok(())
    }

    // Writes bulk data into the container, and releases the reference to it so that it is no longer stored in memory. Needed for two-stage processing of legacy UE4.27 zen assets
    pub(crate) fn write_and_release_bulk_data(&mut self, writer: &mut IoStoreWriter) -> anyhow::Result<()> {
        // Write bulk data chunk if it is present
        if let Some(bulk_data_buffer) = &self.bulk_data_buffer {
            let bulk_data_chunk_id = FIoChunkId::from_package_id(self.package_id, 0, EIoChunkType::BulkData);
            writer.write_chunk(bulk_data_chunk_id, Some(&self.path.with_extension("ubulk")), bulk_data_buffer)?;
        }
        // Write optional bulk data chunk if it is present
        if let Some(optional_bulk_data_buffer) = &self.optional_bulk_data_buffer {
            let optional_bulk_data_chunk_id = FIoChunkId::from_package_id(self.package_id, 0, EIoChunkType::OptionalBulkData);
            writer.write_chunk(optional_bulk_data_chunk_id, Some(&self.path.with_extension("uptnl")), optional_bulk_data_buffer)?;
        }
        // Write memory mapped bulk data chunk if it is present
        if let Some(memory_mapped_bulk_data_buffer) = &self.memory_mapped_bulk_data_buffer {
            let memory_mapped_bulk_data_chunk_id = FIoChunkId::from_package_id(self.package_id, 0, EIoChunkType::MemoryMappedBulkData);
            writer.write_chunk(memory_mapped_bulk_data_chunk_id, Some(&self.path.with_extension("m.ubulk")), memory_mapped_bulk_data_buffer)?;
        }

        // Release the buffers to free the memory taken by them
        self.bulk_data_buffer = None;
        self.optional_bulk_data_buffer = None;
        self.memory_mapped_bulk_data_buffer = None;
        Ok(())
    }
}

fn build_zen_asset_internal<'a>(
    legacy_asset: &FSerializedAssetBundle,
    container_header_version: EIoContainerHeaderVersion,
    package_version_fallback: Option<FPackageFileVersion>,
    fixup_legacy_external_arcs: bool,
    source_package_name: Option<String>,
    script_objects: Option<Arc<ZenScriptObjects>>,
    script_cells: Option<Arc<ZenScriptCellsStore>>,
    log: &'a Log,
) -> anyhow::Result<ZenPackageBuilder<'a>> {
    // Read legacy package header
    let mut asset_header_reader = Cursor::new(&legacy_asset.asset_file_buffer);
    let legacy_package_header = FLegacyPackageHeader::deserialize(&mut asset_header_reader, package_version_fallback)?;

    // Construct zen asset from the package header
    let mut builder = create_asset_builder(legacy_package_header, container_header_version, fixup_legacy_external_arcs, source_package_name, script_objects, script_cells, log);

    // Build zen asset data
    setup_zen_package_summary(&mut builder)?;
    build_zen_import_map(&mut builder)?;
    build_zen_export_map(&mut builder)?;
    build_zen_preload_dependencies(&mut builder)?;

    // Finally store and set package summary name
    if builder.container_header_version > EIoContainerHeaderVersion::Initial {
        builder.zen_package.summary.name = builder.zen_package.name_map.store(&builder.legacy_package.summary.package_name);
    } else {
        builder.zen_package.summary.name = builder.zen_package.name_map.store(builder.source_package_name.as_deref().unwrap_or("None"));
    }

    Ok(builder)
}

// Builds zen asset and writes it into the container using the provided serialized legacy asset and package version
pub(crate) fn build_zen_asset(
    legacy_asset: FSerializedAssetBundle,
    package_name_to_referenced_shader_maps: &HashMap<String, Vec<FSHAHash>>,
    path: &UEPath,
    package_version_fallback: Option<FPackageFileVersion>,
    container_header_version: EIoContainerHeaderVersion,
    allow_fixup: bool,
    script_objects: Option<Arc<ZenScriptObjects>>,
    script_cells: Option<Arc<ZenScriptCellsStore>>,
    log: &Log,
) -> anyhow::Result<ConvertedZenAssetBundle> {
    let source_package_name = if container_header_version <= EIoContainerHeaderVersion::Initial {
        let stripped = path.strip_prefix("../../../").unwrap();
        let pak_path = crate::pak_path_to_game_path(stripped).with_context(|| format!("Failed to get Package Path from {}", stripped))?;
        let asset_path = pak_path.rsplit_once(".").unwrap();
        Some(asset_path.0.to_string())
    } else {
        None
    };

    // We want to fixup this asset once we have converted all the packages
    let final_allow_fixup = container_header_version <= EIoContainerHeaderVersion::Initial && allow_fixup;
    let builder = build_zen_asset_internal(&legacy_asset, container_header_version, package_version_fallback, final_allow_fixup, source_package_name, script_objects, script_cells, log)?;

    // Serialize the resulting asset into the container writer
    build_converted_zen_asset(&builder, legacy_asset, path, package_name_to_referenced_shader_maps)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::version::EngineVersion;
    use crate::{EIoStoreTocVersion, PackageTestMetadata};
    use fs_err as fs;

    // Builds zen asset and returns the resulting package ID, chunk data buffer, and it's store entry. Zen package conversion does not modify bulk data in any way.
    pub(crate) fn build_serialize_zen_asset(legacy_asset: &FSerializedAssetBundle, container_header_version: EIoContainerHeaderVersion, package_version_fallback: Option<FPackageFileVersion>, source_package_name: Option<String>) -> anyhow::Result<(FPackageId, StoreEntry, Vec<u8>)> {
        // Do not allow legacy external arc fixup, just emit the asset that does not require fixup immediately using only the information available from this asset
        let logger = Log::no_log();
        let builder = build_zen_asset_internal(legacy_asset, container_header_version, package_version_fallback, false, source_package_name, None, None, &logger)?;

        let (store_entry, package_data, _) = serialize_zen_asset(&builder, legacy_asset)?;
        Ok((builder.package_id, store_entry, package_data))
    }

    #[test]
    fn test_zen_asset_identity_conversion() -> anyhow::Result<()> {
        // let eng = EngineVersion::UE4_27;
        // let ue4_27 = (eng.toc_version(), eng.container_header_version(), eng.package_file_version());

        // Disabled because export bundles and external dependencies don't match exactly
        // run_test("tests/UE4.27/TestModUI", ue4_27, Some("/Game/_AssemblyStorm/TestMod/TestModUI".to_string()))?;

        let eng5_4 = EngineVersion::UE5_4;
        let ue5_4 = (eng5_4.toc_version(), eng5_4.container_header_version(), eng5_4.package_file_version());

        run_test("tests/UE5.4/BP_Table_Lamp", ue5_4, None)?;
        run_test("tests/UE5.4/Randy", ue5_4, None)?;

        let eng_5_5 = EngineVersion::UE5_5;
        let ue5_5 = (eng_5_5.toc_version(), eng_5_5.container_header_version(), eng_5_5.package_file_version());

        run_test("tests/UE5.5/T_Test", ue5_5, None)?;
        run_test("tests/UE5.5/SM_Cube", ue5_5, None)?;
        run_test("tests/UE5.5/BP_ThirdPersonCharacter", ue5_5, None)?;

        let eng5_6 = EngineVersion::UE5_6;
        let ue5_6 = (eng5_6.toc_version(), eng5_6.container_header_version(), eng5_6.package_file_version());

        run_test("tests/UE5.6/T_Quinn_01_D", ue5_6, None)?;
        run_test("tests/UE5.6/SM_Cube", ue5_6, None)?;
        run_test("tests/UE5.6/BP_ThirdPersonCharacter", ue5_6, None)?;
        run_test("tests/UE5.6/M_Mannequin", ue5_6, None)?;
        run_test("tests/UE5.6/SK_Mannequin", ue5_6, None)?;

        Ok(())
    }

    #[allow(unused)]
    fn get_dependency_name(package: &FZenPackageHeader, package_index: FPackageIndex) -> String {
        if package_index.is_export() {
            let export_idx = package_index.to_export_index() as usize;
            if export_idx < package.export_map.len() {
                let export = &package.export_map[export_idx];
                let name = package.name_map.get(export.object_name);
                return format!("Export[{}] {}", export_idx, name);
            }
        } else if package_index.is_import() {
            let import_idx = package_index.to_import_index() as usize;
            if import_idx < package.import_map.len() {
                let import = package.import_map[import_idx];
                return format!("Import[{}] {:?}", import_idx, import);
            }
        }
        format!("{:?}", package_index)
    }

    #[allow(unused)]
    fn print_dependency_structure(package: &FZenPackageHeader) -> String {
        let mut output = String::new();

        for (export_idx, header) in package.dependency_bundle_headers.iter().enumerate() {
            let export_name = if export_idx < package.export_map.len() {
                package.name_map.get(package.export_map[export_idx].object_name).to_string()
            } else {
                format!("Export{}", export_idx)
            };

            output.push_str(&format!("Export[{}] {}:\n", export_idx, export_name));

            if header.first_entry_index >= 0 {
                let start_idx = header.first_entry_index as usize;
                let mut current_idx = start_idx;

                for _ in 0..header.create_before_create_dependencies {
                    let entry = &package.dependency_bundle_entries[current_idx];
                    let dep_name = get_dependency_name(package, entry.local_import_or_export_index);
                    output.push_str(&format!("    CBC: {}\n", dep_name));
                    current_idx += 1;
                }

                for _ in 0..header.serialize_before_create_dependencies {
                    let entry = &package.dependency_bundle_entries[current_idx];
                    let dep_name = get_dependency_name(package, entry.local_import_or_export_index);
                    output.push_str(&format!("    SBC: {}\n", dep_name));
                    current_idx += 1;
                }

                for _ in 0..header.create_before_serialize_dependencies {
                    let entry = &package.dependency_bundle_entries[current_idx];
                    let dep_name = get_dependency_name(package, entry.local_import_or_export_index);
                    output.push_str(&format!("    CBS: {}\n", dep_name));
                    current_idx += 1;
                }

                for _i in 0..header.serialize_before_serialize_dependencies {
                    let entry = &package.dependency_bundle_entries[current_idx];
                    let dep_name = get_dependency_name(package, entry.local_import_or_export_index);
                    output.push_str(&format!("    SBS: {}\n", dep_name));
                    current_idx += 1;
                }
            }
            output.push('\n');
        }
        output
    }

    #[allow(unused)]
    fn print_export_bundle_structure(package: &FZenPackageHeader) -> String {
        let mut output = String::new();

        output.push_str(&format!("Export Bundle Headers ({} total):\n", package.export_bundle_headers.len()));
        for (bundle_idx, header) in package.export_bundle_headers.iter().enumerate() {
            output.push_str(&format!("Bundle[{}]: serial_offset={}, first_entry_index={}, entry_count={}\n", bundle_idx, header.serial_offset, header.first_entry_index, header.entry_count));

            let start_idx = header.first_entry_index as usize;
            for i in 0..header.entry_count {
                let entry_idx = start_idx + i as usize;
                if entry_idx < package.export_bundle_entries.len() {
                    let entry = &package.export_bundle_entries[entry_idx];
                    let export_name = package.name_map.get(package.export_map[entry.local_export_index as usize].object_name).to_string();
                    output.push_str(&format!("    Export[{}] {} - {:?}\n", entry.local_export_index, export_name, entry.command_type));
                }
            }
            output.push('\n');
        }

        output
    }

    fn run_test(path: impl AsRef<std::path::Path>, version: (EIoStoreTocVersion, EIoContainerHeaderVersion, FPackageFileVersion), source_package_name: Option<String>) -> anyhow::Result<()> {
        use pretty_assertions::assert_eq;

        let header = path.as_ref().with_extension("uasset");
        let exports = path.as_ref().with_extension("uexp");
        let original_zen = path.as_ref().with_extension("uzenasset");
        let metadata = path.as_ref().with_extension("metadata.json");

        let metadata = if let Ok(data) = fs::read(metadata) { Some(serde_json::from_slice::<PackageTestMetadata>(&data)?) } else { None };

        let asset_header_buffer = fs::read(header)?;
        let asset_exports_buffer = fs::read(exports)?;

        let serialized_asset_bundle = FSerializedAssetBundle {
            asset_file_buffer: asset_header_buffer,
            exports_file_buffer: asset_exports_buffer.clone(),
            bulk_data_buffer: None,
            optional_bulk_data_buffer: None,
            memory_mapped_bulk_data_buffer: None,
        };

        let original_zen_asset = fs::read(original_zen)?;
        let original_zen_asset_package = FZenPackageHeader::deserialize(&mut Cursor::new(&original_zen_asset), metadata.and_then(|m| m.store_entry), version.0, version.1, Some(version.2))?;

        let (_, store_entry, converted_zen_asset) = build_serialize_zen_asset(&serialized_asset_bundle, version.1, Some(version.2), source_package_name)?;
        let converted_zen_asset_package = FZenPackageHeader::deserialize(&mut Cursor::new(&converted_zen_asset), Some(store_entry), version.0, version.1, Some(version.2))?;

        // dbg!(&original_zen_asset_package);
        // dbg!(&converted_zen_asset_package);

        // let orig_bundles = print_export_bundle_structure(&original_zen_asset_package);
        // let conv_bundles = print_export_bundle_structure(&converted_zen_asset_package);

        // std::fs::write("orig.txt", &orig_deps)?;
        // std::fs::write("conv.txt", &conv_deps)?;

        // std::fs::write("orig_bundles.txt", &orig_bundles)?;
        // std::fs::write("conv_bundles.txt", &conv_bundles)?;

        // println!("Original export bundles:\n{}", orig_bundles);
        // println!("Converted export bundles:\n{}", conv_bundles);

        // Make sure the header is equal between the original and the converted asset, minus the load order data
        assert_eq!(original_zen_asset_package.name_map.copy_raw_names(), converted_zen_asset_package.name_map.copy_raw_names());
        assert_eq!(original_zen_asset_package.bulk_data, converted_zen_asset_package.bulk_data);
        assert_eq!(original_zen_asset_package.imported_package_names, converted_zen_asset_package.imported_package_names);
        assert_eq!(original_zen_asset_package.imported_packages, converted_zen_asset_package.imported_packages);
        assert_eq!(original_zen_asset_package.imported_public_export_hashes, converted_zen_asset_package.imported_public_export_hashes);
        assert_eq!(original_zen_asset_package.import_map, converted_zen_asset_package.import_map);
        assert_eq!(original_zen_asset_package.export_map, converted_zen_asset_package.export_map);
        assert_eq!(original_zen_asset_package.dependency_bundle_headers, converted_zen_asset_package.dependency_bundle_headers);
        assert_eq!(original_zen_asset_package.dependency_bundle_entries, converted_zen_asset_package.dependency_bundle_entries);
        assert_eq!(original_zen_asset_package.export_bundle_headers, converted_zen_asset_package.export_bundle_headers);
        assert_eq!(original_zen_asset_package.export_bundle_entries, converted_zen_asset_package.export_bundle_entries);
        assert_eq!(original_zen_asset_package.summary, converted_zen_asset_package.summary);
        assert_eq!(original_zen_asset_package.external_package_dependencies, converted_zen_asset_package.external_package_dependencies);

        assert_eq!(
            original_zen_asset[(original_zen_asset_package.summary.header_size as usize)..],
            converted_zen_asset[(converted_zen_asset_package.summary.header_size as usize)..],
            "Original zen asset and converted zen asset exports do not match"
        );

        assert_eq!(original_zen_asset, converted_zen_asset, "Original and converted asset binary equality check failed");
        Ok(())
    }
}
