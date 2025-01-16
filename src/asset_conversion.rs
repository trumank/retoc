use crate::container_header::StoreEntryRef;
use crate::iostore::IoStoreTrait;
use crate::legacy_asset::{FLegacyPackageFileSummary, FLegacyPackageHeader, FLegacyPackageVersioningInfo, FObjectDataResource, FObjectExport, FObjectImport, FPackageNameMap};
use crate::name_map::FMappedName;
use crate::script_objects::{FPackageObjectIndex, FPackageObjectIndexType, FScriptObjectEntry, ZenScriptObjects};
use crate::ser::ReadExt;
use crate::zen::{EExportFilterFlags, EObjectFlags, FExportMapEntry, FPackageFileVersion, FPackageIndex, FZenPackageHeader, FZenPackageVersioningInfo};
use crate::{EIoChunkType, FGuid, FIoChunkId, FPackageId};
use anyhow::{anyhow, bail};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::path::Path;
use std::rc::Rc;

// Cache that stores the packages that were retrieved for the purpose of dependency resolution, to avoid loading and parsing them multiple times
pub(crate) struct FZenPackageContext<'a> {
    store_access: &'a dyn IoStoreTrait,
    script_objects_loaded: bool,
    script_objects: Option<ZenScriptObjects>,
    script_objects_resolved_as_classes: HashSet<FPackageObjectIndex>,
    package_headers_cache: HashMap<FPackageId, Rc<FZenPackageHeader>>,
    packages_failed_load: HashSet<FPackageId>,
}
impl<'a> FZenPackageContext<'a> {
    pub(crate) fn create(store_access: &'a dyn IoStoreTrait) -> Self {
        Self {
            store_access,
            script_objects_loaded: false,
            script_objects: None,
            script_objects_resolved_as_classes: HashSet::new(),
            package_headers_cache: HashMap::new(),
            packages_failed_load: HashSet::new()
        }
    }
    fn load_script_objects(&mut self) -> anyhow::Result<()> {
        // Only attempt to load script objects once
        if !self.script_objects_loaded {
            self.script_objects_loaded = true;

            let script_objects_id = FIoChunkId::create(0, 0, EIoChunkType::ScriptObjects);
            let script_objects_data = self.store_access.read(script_objects_id)?;
            let script_objects: ZenScriptObjects = Cursor::new(script_objects_data).de()?;

            // Do a quick check over all script objects to track which ones are being pointed to by the CDOs. These are 100% UClasses
            for script_object in &script_objects.script_objects {
                if script_object.cdo_class_index.kind() != FPackageObjectIndexType::Null {
                    self.script_objects_resolved_as_classes.insert(script_object.cdo_class_index);
                }
            }
            self.script_objects = Some(script_objects);
        }
        // Make sure the loading has succeeded, otherwise we cannot parse any packages
        else if self.script_objects.is_none() {
            bail!("Failed to load script objects from the archive");
        }
        Ok({})
    }
    fn find_script_object(&self, script_object_index: FPackageObjectIndex) -> anyhow::Result<FScriptObjectEntry> {
        if script_object_index.kind() != FPackageObjectIndexType::ScriptImport {
            bail!("Package Object index that is not a ScriptImport passed to resolve_script_import: {}", script_object_index);
        }
        let script_objects: &ZenScriptObjects = self.script_objects.as_ref().ok_or_else(|| { anyhow!("Failed to read script objects") })?;
        let script_object_entry = script_objects.script_object_lookup.get(&script_object_index).ok_or_else(|| { anyhow!("Failed to find script object with ID {}", script_object_index) })?;
        Ok(script_object_entry.clone())
    }
    fn resolve_script_object_name(&self, script_object_name: FMappedName) -> anyhow::Result<String> {
        let script_objects: &ZenScriptObjects = self.script_objects.as_ref().ok_or_else(|| { anyhow!("Failed to read script objects") })?;
        Ok(script_objects.global_name_map.get(script_object_name).to_string())
    }
    fn is_script_object_class(&self, script_object_index: FPackageObjectIndex) -> bool {
        self.script_objects_resolved_as_classes.contains(&script_object_index)
    }
    fn lookup(&mut self, package_id: FPackageId) -> anyhow::Result<Rc<FZenPackageHeader>> {

        // If we already have a package in the cache, return it
        if let Some(existing_package) = self.package_headers_cache.get(&package_id) {
            return Ok(existing_package.clone());
        }
        // If we have previously attempted to load a package and failed, return that
        if self.packages_failed_load.contains(&package_id) {
            return Err(anyhow!("Package has failed loading previously"));
        }

        // Optional package segments cannot be imported from other packages, and optional segments are also not supported outside of editor. So ChunkIndex is always 0
        let package_chunk_id = FIoChunkId::from_package_id(package_id, 0, EIoChunkType::ExportBundleData);
        let package_data = self.store_access.read(package_chunk_id);
        let package_store_entry_ref = self.store_access.package_store_entry(package_id);

        // Mark the package as failed load if it's chunk failed to load
        if let Err(read_error) = package_data {
            self.packages_failed_load.insert(package_id);
            return Err(read_error)
        }
        if package_store_entry_ref.is_none() {
            self.packages_failed_load.insert(package_id);
            return Err(anyhow!("Failed to find Package Store Entry for Package Id {}", package_id));
        }

        let mut zen_package_buffer = Cursor::new(package_data?);
        let container_version = self.store_access.container_file_version().ok_or_else(|| { anyhow!("Failed to retrieve container TOC version") })?;
        let container_header_version = self.store_access.container_header_version().ok_or_else(|| { anyhow!("Failed to retrieve container header version") })?;

        let zen_package_header = FZenPackageHeader::deserialize(&mut zen_package_buffer, StoreEntryRef::to_owned(&package_store_entry_ref.unwrap()), container_version, container_header_version);

        // Mark the package as failed if we failed to parse the header
        if let Err(header_error) = zen_package_header {
            self.packages_failed_load.insert(package_id);
            return Err(header_error);
        }

        // Move the package header into a shared pointer and store it into the map
        let shared_package_header: Rc<FZenPackageHeader> = Rc::new(zen_package_header?);
        self.package_headers_cache.insert(package_id, shared_package_header.clone());
        Ok(shared_package_header)
    }
    fn read_full_package_data(&self, package_id: FPackageId) -> anyhow::Result<Vec<u8>> {
        let package_chunk_id = FIoChunkId::from_package_id(package_id, 0, EIoChunkType::ExportBundleData);
        self.store_access.read(package_chunk_id)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
struct ResolvedZenImport {
    class_package: String,
    class_name: String,
    object_name: String,
    outer: Option<Box<ResolvedZenImport>>,
}
impl ResolvedZenImport {
    const CORE_OBJECT_PACKAGE_NAME: &'static str = "/Script/CoreUObject";
    const OBJECT_CLASS_NAME: &'static str = "Object";
    const CLASS_CLASS_NAME: &'static str = "Class";
    const PACKAGE_CLASS_NAME: &'static str = "Package";
}
fn resolve_script_import(package_cache: &FZenPackageContext, import: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {

    let script_object = package_cache.find_script_object(import)?;
    let object_name = package_cache.resolve_script_object_name(script_object.object_name)?;

    // If this is a native package (outer is null), we know that it's type is /Script/CoreUObject.Package
    if script_object.outer_index.is_null() {

        return Ok(ResolvedZenImport{
            class_package: ResolvedZenImport::CORE_OBJECT_PACKAGE_NAME.to_string(),
            class_name: ResolvedZenImport::PACKAGE_CLASS_NAME.to_string(),
            outer: None, object_name,
        })
    }
    if script_object.outer_index.kind() != FPackageObjectIndexType::ScriptImport {
        bail!("Outer script object {} for import {} is not a script import", script_object.outer_index, import);
    }

    // Resolve outer index
    let resolved_outer_import = resolve_script_import(&package_cache, script_object.outer_index)?;

    // If this object is known to be a UClass because a CDO is pointing at it, it's type is UClass
    if package_cache.is_script_object_class(import) {

        return Ok(ResolvedZenImport{
            class_package: ResolvedZenImport::CORE_OBJECT_PACKAGE_NAME.to_string(),
            class_name: ResolvedZenImport::CLASS_CLASS_NAME.to_string(),
            outer: Some(Box::new(resolved_outer_import)), object_name,
        })
    }

    // If this object is parented to the package, starts with Default__ and has a CDO class index, it's a CDO, and it points to it's class
    let is_cdo_object = resolved_outer_import.outer.is_none() && object_name.starts_with("Default__");
    if is_cdo_object && !script_object.cdo_class_index.is_null() {

        // Resolve CDO class name and outer package. Class must always be 1 level deep in the package
        let resolved_class = resolve_script_import(&package_cache, script_object.cdo_class_index)?;
        let resolved_class_package = resolved_class.outer.ok_or_else(|| { anyhow!("Failed to resolve CDO class package") })?;
        if !resolved_class_package.outer.is_none() {
            bail!("Resolved CDO class outer was not a UPackage for class {} of CDO {}", script_object.cdo_class_index, import);
        }
        return Ok(ResolvedZenImport{
            class_package: resolved_class_package.object_name,
            class_name: resolved_class.object_name,
            outer: Some(Box::new(resolved_outer_import)), object_name,
        })
    }

    // This is not a CDO object, not a UClass, and not a UPackage
    // This could be a UFunction or UScriptStruct if it's top level, UFunction if it's child of UClass, or completely anything if it's child of a CDO
    // Since we cannot differentiate between these cases, the safest class name to emit for the import is /Script/CoreUObject.Object
    // The class for import must adhere to the type of the imported object, which Object always does, and in runtime the game does not need that precise of an information
    // of the import class to resolve native imports.
    Ok(ResolvedZenImport{
        class_package: ResolvedZenImport::CORE_OBJECT_PACKAGE_NAME.to_string(),
        class_name: ResolvedZenImport::OBJECT_CLASS_NAME.to_string(),
        outer: Some(Box::new(resolved_outer_import)), object_name,
    })
}
fn resolve_package_import(package_cache: &mut FZenPackageContext, package_header: &FZenPackageHeader, import: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {

    let package_import = import.package_import().ok_or_else(|| { anyhow!("Failed to resolve import {} as package import", import) })?;

    // Make sure package index points to a valid index in the imported packages
    if package_import.imported_package_index as usize >= package_header.imported_packages.len() {
        bail!("Imported Package index out of bounds for import {}: package index is {}, but package has only {} imports total",
            import, package_import.imported_package_index, package_header.imported_packages.len());
    }
    let package_id: FPackageId = package_header.imported_packages[package_import.imported_package_index as usize];

    // Make sure the hash index points to a valid hash in the package header
    if package_import.imported_public_export_hash_index as usize >= package_header.imported_public_export_hashes.len() {
        bail!("Imported Public Export Hash index out of bounds for import {}: hash index is {}, but package has only {} hashes total",
            import, package_import.imported_public_export_hash_index, package_header.imported_public_export_hashes.len());
    }
    let public_export_hash: u64 = package_header.imported_public_export_hashes[package_import.imported_public_export_hash_index as usize];

    // Resolve the imported package, and abort if we cannot resolve it
    let resolved_import_package = package_cache.lookup(package_id)?;

    // Sanity check - if we have imported package names, this package name must match the imported package name
    if !package_header.imported_package_names.is_empty() {
        let expected_imported_package_name = &package_header.imported_package_names[package_import.imported_package_index as usize];
        let actual_imported_package_name = resolved_import_package.package_name();
        if *expected_imported_package_name != actual_imported_package_name {
            bail!("Imported package name mismatch: Expected to resolve imported package {}, but resolved {}", *expected_imported_package_name, actual_imported_package_name);
        }
    }

    // Resolve the export that we are interested in. Note that the package passed to the resolve_package_export_internal must be the imported package, not this package
    let imported_export = resolved_import_package.export_map.iter().find(|x| { x.public_export_hash != 0 && x.public_export_hash == public_export_hash })
        .ok_or_else(|| { anyhow!("Failed to resolve public export with hash {} on package {} (imported by {})",  public_export_hash, resolved_import_package.package_name(), package_header.package_name()) })?;

    resolve_package_export_internal(package_cache, resolved_import_package.as_ref(), imported_export)
}
fn resolve_package_export(package_cache: &mut FZenPackageContext, package_header: &FZenPackageHeader, export: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {

    // Resolve the import object index
    if export.kind() != FPackageObjectIndexType::Export {
        bail!("ResolvePackageExport called on non-export index: {}", export);
    }
    let export_index = export.export().unwrap();
    let resolved_export = &package_header.export_map[export_index as usize];

    resolve_package_export_internal(package_cache, package_header, resolved_export)
}
fn resolve_package_export_internal(package_cache: &mut FZenPackageContext, package_header: &FZenPackageHeader, export: &FExportMapEntry) -> anyhow::Result<ResolvedZenImport> {
    // Resolve export name from the name map
    let export_name = package_header.name_map.get(export.object_name).to_string();
    // Resolve the class of the export. We are only interested in the class object name and package
    let resolved_export_class = resolve_generic_zen_import_import(package_cache, package_header, export.class_index, false)?;

    // Resolve the outer of this export as import. Note that if outer is null, this is a top level export, and it's outer is a UPackage object itbuilder
    let resolved_export_outer = resolve_generic_zen_import_import(package_cache, package_header, export.outer_index, true)?;

    // Resolved class of the export must be exactly 1 level deep, so it must have an outer that is a package
    if resolved_export_class.outer.is_none() || resolved_export_class.outer.as_ref().unwrap().outer.is_some() {
        bail!("Resolved class of export {} of package {} has invalid class {} that does not have a package as it's outer", export_name, package_header.package_name(), resolved_export_class.object_name);
    }
    // Exports always have an outer, if their outer index is null their outer is a package itbuilder
    Ok(ResolvedZenImport{
        class_package: resolved_export_class.outer.unwrap().object_name,
        class_name: resolved_export_class.object_name,
        object_name: export_name,
        outer: Some(Box::new(resolved_export_outer))
    })
}
fn resolve_builder_package_as_import(package_header: &FZenPackageHeader) -> anyhow::Result<ResolvedZenImport> {
    Ok(ResolvedZenImport{
        class_package: ResolvedZenImport::CORE_OBJECT_PACKAGE_NAME.to_string(),
        class_name: ResolvedZenImport::PACKAGE_CLASS_NAME.to_string(),
        object_name: package_header.package_name(),
        outer: None,
    })
}
fn resolve_generic_zen_import_import(package_cache: &mut FZenPackageContext, package_header: &FZenPackageHeader, import: FPackageObjectIndex, resolve_null_as_this_package: bool) -> anyhow::Result<ResolvedZenImport> {
    match import.kind()
    {
        FPackageObjectIndexType::ScriptImport => resolve_script_import(package_cache, import),
        FPackageObjectIndexType::PackageImport => resolve_package_import(package_cache, package_header, import),
        FPackageObjectIndexType::Export => resolve_package_export(package_cache, package_header, import),
        FPackageObjectIndexType::Null => if resolve_null_as_this_package { resolve_builder_package_as_import(package_header) } else {
            bail!("Encountered Null object while parsing the import map entry {} of package {}", import, package_header.name_map.get(package_header.summary.name))
        },
    }
}

struct LegacyAssetBuilder<'a, 'b> {
    package_context: &'a mut FZenPackageContext<'b>,
    package_id: FPackageId,
    zen_package: Rc<FZenPackageHeader>,
    legacy_package: FLegacyPackageHeader,
    resolved_import_lookup: HashMap<ResolvedZenImport, FPackageIndex>,
    zen_import_lookup: HashMap<FPackageObjectIndex, FPackageIndex>,
    // Maps desired (real) import position to the current (temporary) import position
    original_import_order: HashMap<usize, usize>,
}

// Lifetime: create_asset_builder -> begin_build_summary -> copy_package_sections -> build_import_map -> build_export_map -> resolve_export_dependencies -> finalize_asset -> write_asset
fn create_asset_builder<'a, 'b>(package_context: &'a mut FZenPackageContext<'b>, package_id: FPackageId) -> anyhow::Result<LegacyAssetBuilder<'a, 'b>> {
    let zen_package: Rc<FZenPackageHeader> = package_context.lookup(package_id)?;
    package_context.load_script_objects()?;
    Ok(LegacyAssetBuilder{
        package_context,
        package_id,
        zen_package,
        legacy_package: FLegacyPackageHeader::default(),
        resolved_import_lookup: HashMap::new(),
        zen_import_lookup: HashMap::new(),
        original_import_order: HashMap::new()
    })
}
fn begin_build_summary(builder: &mut LegacyAssetBuilder, fallback_package_file_version: Option<FPackageFileVersion>) -> anyhow::Result<()> {
    // Populate package summary with basic data
    let mut legacy_package_summary = FLegacyPackageFileSummary::default();
    legacy_package_summary.package_name = builder.zen_package.name_map.get(builder.zen_package.summary.name).to_string();
    legacy_package_summary.package_flags = builder.zen_package.summary.package_flags;
    legacy_package_summary.package_guid = FGuid{a: 0, b: 0, c: 0, d: 0};
    legacy_package_summary.package_source = 0;

    // If zen package has versioning data, we can transfer it to the package. Otherwise, the package is written unversioned
    // However, we still need to know the package file version to determine the disk format of the legacy package
    if builder.zen_package.versioning_info.is_some() {
        let zen_versions: &FZenPackageVersioningInfo = builder.zen_package.versioning_info.as_ref().unwrap();

        legacy_package_summary.versioning_info = FLegacyPackageVersioningInfo{
            package_file_version: zen_versions.package_file_version,
            licensee_version: zen_versions.licensee_version,
            custom_versions: zen_versions.custom_versions.clone(),
            is_unversioned: false,
            ..FLegacyPackageVersioningInfo::default()
        };
    }
    else {
        // TODO: Derive package file version from Zen. This has to be done in FZenPackageVersioningInfo since there are parsing differences for zen asset
        if fallback_package_file_version.is_none() {
            bail!("Cannot build legacy asset from unversioned zen asset without explicit package file version. Please provide explicit package file version");
        }
        legacy_package_summary.versioning_info = FLegacyPackageVersioningInfo{
            package_file_version: fallback_package_file_version.unwrap(),
            is_unversioned: true,
            ..FLegacyPackageVersioningInfo::default()
        };
    }

    // Create legacy package header
    builder.legacy_package = FLegacyPackageHeader {summary: legacy_package_summary, ..FLegacyPackageHeader::default()};
    Ok({})
}
fn copy_package_sections(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {
    // Copy the names from the zen container. Name map format is the same on the high level
    builder.legacy_package.name_map = FPackageNameMap::create_from_names(builder.zen_package.name_map.copy_raw_names());
    // Current number of names is the minimum that should be kept
    builder.legacy_package.summary.names_referenced_from_export_data_count = builder.legacy_package.name_map.num_names() as i32;

    // Copy data resources
    builder.legacy_package.data_resources = builder.zen_package.bulk_data.iter().map(|zen_bulk_data| {
        // Zen also does not serialize outer_index, so we will write Null as outer index. Luckily, this information is not needed in runtime.
        let outer_index: FPackageIndex = FPackageIndex::create_null();
        // Note that Zen does not support compressed bulk data, so raw_size == serial_size
        let raw_size = zen_bulk_data.serial_size;
        // FObjectDataResource::Flags are always 0 for cooked packages, they are not used in runtime and never written when cooking
        let flags: u32 = 0;
        // Copy legacy bulk data flags from zen directly
        let legacy_bulk_data_flags = zen_bulk_data.flags;

        return FObjectDataResource {
            flags,
            serial_offset: zen_bulk_data.serial_offset,
            duplicate_serial_offset: zen_bulk_data.duplicate_serial_offset,
            serial_size: zen_bulk_data.serial_size,
            raw_size,
            outer_index,
            legacy_bulk_data_flags
        };
    }).collect();
    Ok({})
}
fn resolve_local_package_object(builder: &mut LegacyAssetBuilder, package_object: FPackageObjectIndex) -> anyhow::Result<FPackageIndex> {

    // Null package object resolves to null package index
    if package_object.kind() == FPackageObjectIndexType::Null {
        return Ok(FPackageIndex::create_null());
    }

    // Exports resolve into the index into the export map. Since exports are never shuffled around, we can just return the same export index
    if package_object.kind() == FPackageObjectIndexType::Export {
        let local_export_index = package_object.export().unwrap();
        return Ok(FPackageIndex::create_export(local_export_index));
    }

    // This is a script import or a package import otherwise. Attempt to look it up through zen import lookup (fast path)
    if let Some(import_index) = builder.zen_import_lookup.get(&package_object) {
        return Ok(*import_index)
    }

    // This is an import that has not been mapped yet. Resolve it, and then add it to the import map
    let resolved_import = resolve_generic_zen_import_import(builder.package_context, builder.zen_package.as_ref(), package_object, false)?;
    let import_map_index = find_or_add_resolved_import(builder, &resolved_import);

    // Add it to the quick zen import lookup and return the resulting index
    builder.zen_import_lookup.insert(package_object, import_map_index);
    Ok(import_map_index)
}
fn find_or_add_resolved_import(builder: &mut LegacyAssetBuilder, import: &ResolvedZenImport) -> FPackageIndex {
    if let Some(existing_import) = builder.resolved_import_lookup.get(import) {
        return *existing_import
    }

    // Outer can either be Null for UPackage imports or an existing import. Imports must never have exports as outers in cooked assets (can actually happen in uncooked OFPA actor assets though)
    let outer_index: FPackageIndex = if import.outer.is_some() {
        find_or_add_resolved_import(builder, import.outer.as_ref().unwrap())
    } else {
        FPackageIndex::create_null()
    };

    // Resolve minimal names for each element of the import
    let class_package = builder.legacy_package.name_map.store(&import.class_package);
    let class_name = builder.legacy_package.name_map.store(&import.class_name);
    let object_name = builder.legacy_package.name_map.store(&import.object_name);

    // Construct a new import entry. All Zen imports are currently non-optional, Zen does not seem to support importing objects from optional package segments
    let new_import_index = FPackageIndex::create_import(builder.legacy_package.imports.len() as u32);
    builder.legacy_package.imports.push(FObjectImport { class_package, class_name, outer_index, object_name, is_optional: false });
    builder.resolved_import_lookup.insert(import.clone(), new_import_index);

    new_import_index
}
fn build_import_map(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {
    for import_index in 0..builder.zen_package.import_map.len() {

        let import_object_index = builder.zen_package.import_map[import_index];

        // Skip holes in the zen import map
        if import_object_index.is_null() {
            continue
        }
        // Resolve the local import reference. We must ALWAYS get an import here, not a null or export
        let import_map_index = resolve_local_package_object(builder, import_object_index)?;
        if !import_map_index.is_import() {
            bail!("Import map package object index {} did not resolve into an import for package {}", import_object_index, builder.zen_package.package_name());
        }

        // Track the original position of this import in the import map. We will need to re-sort the imports and exports later to stick to it
        builder.original_import_order.insert(import_index, import_map_index.to_import_index() as usize);
    }
    Ok({})
}
fn build_export_map(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {
    builder.legacy_package.exports.reserve(builder.zen_package.export_map.len());

    for export_index in 0..builder.zen_package.export_map.len() {
        let zen_export: FExportMapEntry = builder.zen_package.as_ref().export_map[export_index].clone();

        // Resolve class, outer, template index
        let class_index = resolve_local_package_object(builder, zen_export.class_index)?;
        let super_index = resolve_local_package_object(builder, zen_export.super_index)?;
        let template_index = resolve_local_package_object(builder, zen_export.template_index)?;
        let outer_index = resolve_local_package_object(builder, zen_export.outer_index)?;

        let export_name_string = builder.zen_package.name_map.get(zen_export.object_name).to_string();
        let object_name = builder.legacy_package.name_map.store(&export_name_string);
        let object_flags = zen_export.object_flags;

        // Zen's serial offset is relative to the start of export data, which is at HeaderSize
        // Legacy asset's serial offset is absolute, but since exports are split up into a separate file,
        // it is offset in runtime by the size of the cooked header.
        // Since determining the final offset written into the file requires knowing the size of the package header and adding it to the real position in the .uexp file,
        // we retain the relative position here, and package summary serialization code will fix it up with the serialized header size later
        let serial_size: i64 = zen_export.cooked_serial_size as i64;
        let serial_offset: i64 = zen_export.cooked_serial_offset as i64;

        // In legacy assets these are not excluding (can be both not for client and not for server, but only for editor), but in cooked assets generally it is only not for one of either
        let is_not_for_client = zen_export.filter_flags == EExportFilterFlags::NotForClient;
        let is_not_for_server = zen_export.filter_flags == EExportFilterFlags::NotForServer;

        // No way to reliably infer this without access to C++ code of the game. This does not affect the asset behavior in runtime (or in the editor), so assume false
        let is_inherited_instance = false;
        // Assume that object does not need load for editor game. This is true for the vast majority of objects, and does not affect the asset in runtime.
        let is_not_always_loaded_for_editor_game = false;
        // This is not used in runtime, and this information is not preserved by zen. However, we can infer this pretty reliably: Top-level exports marked as RF_Public | RF_Standalone | RF_Transactional are always assets
        let asset_object_flags: u32 = (EObjectFlags::Public as u32) | (EObjectFlags::Standalone as u32) | (EObjectFlags::Transactional as u32);
        let is_asset = zen_export.outer_index.is_null() && (object_flags & asset_object_flags) == asset_object_flags;
        // This is false for all objects in the engine presently, but we can infer if it should be true: If export is not marked as RF_Public still has an export hash, generate_public_hash must be true
        let generate_public_hash = (object_flags & (EObjectFlags::Public as u32)) != 0 && zen_export.public_export_hash != 0;

        // There is no real way to safely infer these from the zen package. They are only used in the editor for loading assets of undefined types to try and salvage as much data out of them as possible when using versioned serialization
        // Providing incorrect values here will crash the editor in such a case because of the serial size mismatch. However, we can make an assumption that the entire export is only script properties, and nothing else
        // In that case, script_serialization_start_offset = 0 which would be the start of the export, and script_serialization_end_offset = serial_size, which would be the entire export
        let script_serialization_start_offset: i64 = 0;
        let script_serialization_end_offset: i64 = serial_size;

        let mut new_object_export = FObjectExport {
            class_index, super_index, template_index, outer_index, object_name, object_flags, serial_size, serial_offset,
            is_not_for_client, is_not_for_server, is_not_always_loaded_for_editor_game, is_inherited_instance, is_asset, generate_public_hash,
            script_serialization_start_offset, script_serialization_end_offset,
            first_export_dependency_index: -1,
            ..FObjectExport::default()
        };
        // Add the export to the resulting asset export map
        builder.legacy_package.exports.push(new_object_export);
    }
    Ok({})
}
fn resolve_export_dependencies(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {

    for export_index in 0..builder.zen_package.dependency_bundle_headers.len() {

        let mut export_object = builder.legacy_package.exports[export_index].clone();

        // Extract dependencies from zen first
        let bundle_header = builder.zen_package.dependency_bundle_headers[export_index];
        let first_dependency_index = bundle_header.first_entry_index as usize;

        // Note that local_import_or_export_index from zen use import map ordering that is not currently valid for this asset, and will only become valid later, but
        // super_index/outer_index/class_index use currently active import map ordering.
        // To fix that, we remap zen package indices to the currently active import order, and then in finalize_asset we will remap all imports back to the final order
        let remap_zen_package_index = |x: FPackageIndex| -> FPackageIndex {
            if x.is_import() { FPackageIndex::create_import(*builder.original_import_order.get(&(x.to_import_index() as usize)).unwrap() as u32) } else { x }
        };

        // Create before create dependencies
        let last_create_before_create_index = first_dependency_index + bundle_header.create_before_create_dependencies as usize;
        let mut create_before_create_deps: Vec<FPackageIndex> =
            builder.zen_package.dependency_bundle_entries[first_dependency_index..last_create_before_create_index]
            .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();

        // Serialize before create dependencies
        let last_serialize_before_create_index = last_create_before_create_index + bundle_header.serialize_before_create_dependencies as usize;
        let mut serialize_before_create_deps: Vec<FPackageIndex> =
            builder.zen_package.dependency_bundle_entries[last_create_before_create_index..last_serialize_before_create_index]
                .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();

        // Create before serialize dependencies
        let last_create_before_serialize_index = last_serialize_before_create_index + bundle_header.create_before_serialize_dependencies as usize;
        let mut create_before_serialize_deps: Vec<FPackageIndex> =
            builder.zen_package.dependency_bundle_entries[last_serialize_before_create_index..last_create_before_serialize_index]
                .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();

        // Serialize before serialize dependencies
        let last_serialize_before_serialize_index = last_create_before_serialize_index + bundle_header.serialize_before_serialize_dependencies as usize;
        let mut serialize_before_serialize_deps: Vec<FPackageIndex> =
            builder.zen_package.dependency_bundle_entries[last_create_before_serialize_index..last_serialize_before_serialize_index]
                .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();

        // Ensure that we have outer and super as create before create dependencies
        if !export_object.outer_index.is_null() && !create_before_create_deps.contains(&export_object.outer_index) {
            create_before_create_deps.push(export_object.outer_index);
        }
        if !export_object.super_index.is_null() && !create_before_create_deps.contains(&export_object.super_index) {
            create_before_create_deps.push(export_object.super_index);;
        }
        // Ensure that we have class and archetype as serialize before create dependencies
        if !export_object.class_index.is_null() && !serialize_before_create_deps.contains(&export_object.class_index) {
            serialize_before_create_deps.push(export_object.class_index);
        }
        if !export_object.template_index.is_null() && !serialize_before_create_deps.contains(&export_object.template_index) {
            serialize_before_create_deps.push(export_object.template_index);
        }

        // If we have no dependencies altogether, do not write first dependency import on the export
        if create_before_create_deps.is_empty() && serialize_before_create_deps.is_empty() && create_before_serialize_deps.is_empty() && serialize_before_serialize_deps.is_empty() {
            continue
        }

        // Set the index of the preload dependencies start, and the numbers of each dependency
        export_object.first_export_dependency_index = builder.legacy_package.preload_dependencies.len() as i32;
        export_object.serialize_before_serialize_dependencies = serialize_before_serialize_deps.len() as i32;
        export_object.create_before_serialize_dependencies = create_before_serialize_deps.len() as i32;
        export_object.serialize_before_create_dependencies = serialize_before_create_deps.len() as i32;
        export_object.create_before_create_dependencies = create_before_create_deps.len() as i32;

        // Update the export object now
        builder.legacy_package.exports[export_index] = export_object;

        // Append preload dependencies for this export now to the legacy package
        builder.legacy_package.preload_dependencies.append(&mut serialize_before_serialize_deps);
        builder.legacy_package.preload_dependencies.append(&mut create_before_serialize_deps);
        builder.legacy_package.preload_dependencies.append(&mut serialize_before_create_deps);
        builder.legacy_package.preload_dependencies.append(&mut create_before_create_deps);
    }

    Ok({})
}
fn finalize_asset(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {

    // Remap import map to the current indices
    let import_map_size = max(builder.legacy_package.imports.len(), builder.zen_package.import_map.len());
    let mut import_remap_map: HashMap<usize, usize> = HashMap::with_capacity(import_map_size);
    let mut new_import_map: Vec<FObjectImport> = Vec::with_capacity(import_map_size);

    let current_import_indices_with_predefined_positions: HashSet<usize> = builder.original_import_order.values().map(|x| {*x}).collect();
    let mut current_legacy_asset_import_index: usize = 0;

    for final_import_index in 0..import_map_size {

        // If there is an original import to put in this position, use it
        if let Some(existing_import_position) = builder.original_import_order.get(&final_import_index) {

            import_remap_map.insert(*existing_import_position, final_import_index);
            new_import_map.push(builder.legacy_package.imports[*existing_import_position].clone());
            continue;
        }
        // Skip over the current imports that have predefined positions
        while current_import_indices_with_predefined_positions.contains(&current_legacy_asset_import_index) {
            current_legacy_asset_import_index += 1;
        }
        // We should NEVER end up with less imports than we have to fill the holes, since zen never strips non-upackage exports
        if current_legacy_asset_import_index >= builder.legacy_package.imports.len() {
            bail!("Failed to find import map entry to fill the hole at {} while filling import map up to size {}", final_import_index, import_map_size);
        }

        // Take the current position and increment it by one
        let existing_import_position = current_legacy_asset_import_index;
        current_legacy_asset_import_index += 1;

        import_remap_map.insert(existing_import_position, final_import_index);
        new_import_map.push(builder.legacy_package.imports[existing_import_position].clone());
    }
    builder.legacy_package.imports = new_import_map;

    // Remap existing references to the imports in the import and export maps
    let remap_package_index = |package_index: FPackageIndex| -> FPackageIndex {
        if package_index.is_import() {
            let new_import_index = *import_remap_map.get(&(package_index.to_import_index() as usize)).unwrap();
            return FPackageIndex::create_import(new_import_index as u32)
        }
        package_index
    };

    for x in builder.legacy_package.exports.iter_mut() {
        x.class_index = remap_package_index(x.class_index);
        x.super_index = remap_package_index(x.super_index);
        x.template_index = remap_package_index(x.template_index);
        x.outer_index = remap_package_index(x.outer_index);
    }
    for x in builder.legacy_package.imports.iter_mut() {
        x.outer_index = remap_package_index(x.outer_index)
    }
    for x in builder.legacy_package.data_resources.iter_mut() {
        x.outer_index = remap_package_index(x.outer_index)
    }
    for x in builder.legacy_package.preload_dependencies.iter_mut() {
        *x = remap_package_index(*x);
    }
    Ok({})
}

// Builds an asset from zen container, returns a builder that can be used to write the payload to the file or read it directly
pub(crate) fn build_asset_from_zen<'a, 'b>(package_context: &'a mut FZenPackageContext<'b>, package_id: FPackageId, fallback_package_file_version: Option<FPackageFileVersion>) -> anyhow::Result<LegacyAssetBuilder<'a, 'b>> {

    let mut asset_builder = create_asset_builder(package_context, package_id)?;
    begin_build_summary(&mut asset_builder, fallback_package_file_version)?;
    copy_package_sections(&mut asset_builder)?;
    build_import_map(&mut asset_builder)?;
    build_export_map(&mut asset_builder)?;
    resolve_export_dependencies(&mut asset_builder)?;
    finalize_asset(&mut asset_builder)?;

    Ok(asset_builder)
}
// Serializes an asset into in-memory buffer. First entry is the uasset file, and the second one is the uexp file
pub(crate) fn serialize_asset(builder: &LegacyAssetBuilder) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    // Write the asset file first
    let mut out_asset_buffer: Vec<u8> = Vec::new();
    let mut asset_cursor = Cursor::new(&mut out_asset_buffer);
    FLegacyPackageHeader::serialize(&builder.legacy_package, &mut asset_cursor, false)?;

    // Copy the raw export data from the chunk into the exports file
    // Note that exports actually start after the zen header
    let raw_exports_data = builder.package_context.read_full_package_data(builder.package_id)?;
    let export_data_blob = raw_exports_data[builder.zen_package.summary.header_size as usize..].to_vec();

    Ok((out_asset_buffer, export_data_blob))
}
// Writes asset to the file. Additionally writes to the uexp file next to it
pub(crate) fn write_asset<P: AsRef<Path>>(builder: &LegacyAssetBuilder, out_asset_path: P, debug_output: bool) -> anyhow::Result<()> {

    // Dump zen package and legacy package for debugging
    if debug_output {
        dbg!(builder.zen_package.clone());
        dbg!(builder.legacy_package.clone());
    }

    // Write the asset file first
    let mut out_asset_buffer: Vec<u8> = Vec::new();
    let mut asset_cursor = Cursor::new(&mut out_asset_buffer);
    FLegacyPackageHeader::serialize(&builder.legacy_package, &mut asset_cursor, debug_output)?;

    std::fs::write(out_asset_path.as_ref(), out_asset_buffer)?;

    // Copy the raw export data from the chunk into the exports file
    // Note that exports actually start after the zen header
    let raw_exports_data = builder.package_context.read_full_package_data(builder.package_id)?;
    let export_file_path = out_asset_path.as_ref().with_extension("uexp");

    std::fs::write(export_file_path, &raw_exports_data[builder.zen_package.summary.header_size as usize..])?;
    Ok({})
}

pub(crate) fn build_legacy<P: AsRef<Path>>(
    package_context: &mut FZenPackageContext,
    package_id: FPackageId,
    out_path: P,
    fallback_package_file_version: Option<FPackageFileVersion>,
    debug_output: bool,
) -> anyhow::Result<()> {

    // Build the asset from zen
    let asset_builder = build_asset_from_zen(package_context, package_id, fallback_package_file_version)?;
    // Write the asset to the file
    write_asset(&asset_builder, out_path, debug_output)?;
    Ok(())
}
