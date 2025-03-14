use crate::iostore::IoStoreTrait;
use crate::legacy_asset::{FLegacyPackageFileSummary, FLegacyPackageHeader, FLegacyPackageVersioningInfo, FObjectDataResource, FObjectExport, FObjectImport, FPackageNameMap, FSerializedAssetBundle, CLASS_CLASS_NAME, CORE_OBJECT_PACKAGE_NAME, OBJECT_CLASS_NAME, PACKAGE_CLASS_NAME, PRESTREAM_PACKAGE_CLASS_NAME};
use crate::logging::Log;
use crate::name_map::FMappedName;
use crate::script_objects::{FPackageObjectIndex, FPackageObjectIndexType, FScriptObjectEntry, ZenScriptObjects};
use crate::ser::WriteExt;
use crate::zen::{EExportCommandType, EExportFilterFlags, EObjectFlags, FExportMapEntry, FExternalDependencyArc, FInternalDependencyArc, FPackageFileVersion, FPackageIndex, FZenPackageHeader, FZenPackageVersioningInfo};
use crate::{EIoChunkType, FGuid, FIoChunkId, FPackageId, FileWriterTrait, UEPath};
use crate::logging::*;
use anyhow::{anyhow, bail};
use key_mutex::KeyMutex;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use crate::container_header::EIoContainerHeaderVersion;

// Cache that stores the packages that were retrieved for the purpose of dependency resolution, to avoid loading and parsing them multiple times
pub(crate) struct FZenPackageContext<'a> {
    store_access: &'a dyn IoStoreTrait,
    fallback_package_file_version: Option<FPackageFileVersion>,
    log: &'a Log,

    inner_state: Arc<RwLock<FZenPackageContextMutableState>>,
    script_objects: Arc<RwLock<Option<FZenPackageContextScriptObjects>>>,
    package_lookup_locks: KeyMutex<FPackageId, ()>,
}
#[derive(Default)]
struct FZenPackageContextMutableState {
    package_headers_cache: HashMap<FPackageId, Arc<FZenPackageHeader>>,
    packages_failed_load: HashSet<FPackageId>,
    has_logged_detected_package_version: bool,
}
struct FZenPackageContextScriptObjects {
    script_objects: ZenScriptObjects,
    script_objects_resolved_as_classes: HashSet<FPackageObjectIndex>,
}
impl<'a> FZenPackageContext<'a> {
    pub(crate) fn create(store_access: &'a dyn IoStoreTrait, fallback_package_file_version: Option<FPackageFileVersion>, log: &'a Log) -> Self {
        Self {
            store_access,
            fallback_package_file_version,
            log,

            script_objects: Default::default(),
            inner_state: Default::default(),
            package_lookup_locks: KeyMutex::new(),
        }
    }
    fn get_script_objects(&self) -> anyhow::Result<RwLockReadGuard<Option<FZenPackageContextScriptObjects>>> {
        let read_lock = self.script_objects.read().unwrap();
        if read_lock.is_some() {
            Ok(read_lock)
        } else {
            // data has not been popluated so grab a write guard for us to populate
            drop(read_lock);
            let mut write_lock = self.script_objects.write().unwrap();
            if write_lock.is_some() {
                // data has been populated so someone else got to it first. downgrade to a read lock and return
                drop(write_lock);
                return Ok(self.script_objects.read().unwrap());
            }

            let script_objects = self.store_access.load_script_objects()?;
            let mut script_objects_resolved_as_classes = HashSet::new();

            // Do a quick check over all script objects to track which ones are being pointed to by the CDOs. These are 100% UClasses
            for script_object in &script_objects.script_objects {
                if script_object.cdo_class_index.kind() != FPackageObjectIndexType::Null {
                    script_objects_resolved_as_classes.insert(script_object.cdo_class_index);
                }
            }
            *write_lock = Some(FZenPackageContextScriptObjects {
                script_objects,
                script_objects_resolved_as_classes,
            });
            // convert to read lock and return
            drop(write_lock);
            Ok(self.script_objects.read().unwrap())
        }
    }
    fn find_script_object(&self, script_object_index: FPackageObjectIndex) -> anyhow::Result<FScriptObjectEntry> {
        if script_object_index.kind() != FPackageObjectIndexType::ScriptImport {
            bail!("Package Object index that is not a ScriptImport passed to resolve_script_import: {}", script_object_index);
        }
        let script_objects_lock = self.get_script_objects().unwrap();
        let script_objects: &ZenScriptObjects = &script_objects_lock.as_ref().unwrap().script_objects;
        let script_object_entry = script_objects.script_object_lookup.get(&script_object_index).ok_or_else(|| { anyhow!("Failed to find script object with ID {}", script_object_index) })?;
        Ok(*script_object_entry)
    }
    fn resolve_script_object_name(&self, script_object_name: FMappedName) -> anyhow::Result<String> {
        let script_objects_lock = self.get_script_objects().unwrap();
        let script_objects: &ZenScriptObjects = &script_objects_lock.as_ref().unwrap().script_objects;
        Ok(script_objects.global_name_map.get(script_object_name).to_string())
    }
    fn is_script_object_class(&self, script_object_index: FPackageObjectIndex) -> bool {
        let script_objects_lock = self.get_script_objects().unwrap();
        let script_objects_resolved_as_classes = &script_objects_lock.as_ref().unwrap().script_objects_resolved_as_classes;
        script_objects_resolved_as_classes.contains(&script_object_index)
    }
    /// perform lookup but do not actually load package if it does not exist
    fn try_lookup(&self, package_id: FPackageId) -> anyhow::Result<Option<Arc<FZenPackageHeader>>> {
        let read_lock = self.inner_state.read().unwrap();

        // If we already have a package in the cache, return it
        if let Some(existing_package) = read_lock.package_headers_cache.get(&package_id) {
            return Ok(Some(existing_package.clone()));
        }
        // If we have previously attempted to load a package and failed, return that
        if read_lock.packages_failed_load.contains(&package_id) {
            return Err(anyhow!("Package has failed loading previously"));
        }

        Ok(None)
    }
    fn lookup(&self, package_id: FPackageId) -> anyhow::Result<Arc<FZenPackageHeader>> {
        if let Some(package) = self.try_lookup(package_id)? {
            return Ok(package)
        }

        // package does not exist in cache so grab a lookup lock before starting lookup process
        let _package_lookup_lock = self.package_lookup_locks.lock(package_id).unwrap();

        // check package still hasn't been loaded since locking
        if let Some(package) = self.try_lookup(package_id)? {
            return Ok(package)
        }
        // Lookup redirect package ID first before trying the provided package ID
        let redirected_package_id = self.store_access.lookup_package_redirect(package_id).unwrap_or(package_id);

        // Optional package segments cannot be imported from other packages, and optional segments are also not supported outside of editor. So ChunkIndex is always 0
        let package_chunk_id = FIoChunkId::from_package_id(redirected_package_id, 0, EIoChunkType::ExportBundleData);
        let package_data = self.store_access.read(package_chunk_id);
        let package_store_entry_ref = self.store_access.package_store_entry(redirected_package_id);

        // Mark the package as failed load if it's chunk failed to load
        if let Err(read_error) = package_data {
            let mut write_lock = self.inner_state.write().unwrap();
            write_lock.packages_failed_load.insert(package_id);
            return Err(read_error)
        }
        if package_store_entry_ref.is_none() {
            let mut write_lock = self.inner_state.write().unwrap();
            write_lock.packages_failed_load.insert(package_id);
            return Err(anyhow!("Failed to find Package Store Entry for Package Id {}", package_id));
        }

        let mut zen_package_buffer = Cursor::new(package_data?);
        let container_version = self.store_access.container_file_version().ok_or_else(|| { anyhow!("Failed to retrieve container TOC version") })?;
        let container_header_version = self.store_access.container_header_version().ok_or_else(|| { anyhow!("Failed to retrieve container header version") })?;

        let zen_package_header = FZenPackageHeader::deserialize(&mut zen_package_buffer, package_store_entry_ref, container_version, container_header_version, self.fallback_package_file_version);

        // Mark the package as failed if we failed to parse the header
        if let Err(header_error) = zen_package_header {
            let mut write_lock = self.inner_state.write().unwrap();
            write_lock.packages_failed_load.insert(package_id);
            return Err(header_error);
        }

        // Move the package header into a shared pointer and store it into the map
        let shared_package_header: Arc<FZenPackageHeader> = Arc::new(zen_package_header?);
        let mut write_lock = self.inner_state.write().unwrap();
        write_lock.package_headers_cache.insert(package_id, shared_package_header.clone());
        Ok(shared_package_header)
    }
    fn read_full_package_data(&self, package_id: FPackageId) -> anyhow::Result<Vec<u8>> {
        // Lookup redirect package ID first before trying the provided package ID
        let redirected_package_id = self.store_access.lookup_package_redirect(package_id).unwrap_or(package_id);

        let package_chunk_id = FIoChunkId::from_package_id(redirected_package_id, 0, EIoChunkType::ExportBundleData);
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
fn resolve_script_import(package_cache: &FZenPackageContext, import: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {

    let script_object = package_cache.find_script_object(import)?;
    let object_name = package_cache.resolve_script_object_name(script_object.object_name)?;

    // If this is a native package (outer is null), we know that it's type is /Script/CoreUObject.Package
    if script_object.outer_index.is_null() {

        return Ok(ResolvedZenImport{
            class_package: CORE_OBJECT_PACKAGE_NAME.to_string(),
            class_name: PACKAGE_CLASS_NAME.to_string(),
            outer: None, object_name,
        })
    }
    if script_object.outer_index.kind() != FPackageObjectIndexType::ScriptImport {
        bail!("Outer script object {} for import {} is not a script import", script_object.outer_index, import);
    }

    // Resolve outer index
    let resolved_outer_import = resolve_script_import(package_cache, script_object.outer_index)?;

    // If this object is known to be a UClass because a CDO is pointing at it, it's type is UClass
    if package_cache.is_script_object_class(import) {

        return Ok(ResolvedZenImport{
            class_package: CORE_OBJECT_PACKAGE_NAME.to_string(),
            class_name: CLASS_CLASS_NAME.to_string(),
            outer: Some(Box::new(resolved_outer_import)), object_name,
        })
    }

    // If this object is parented to the package, starts with Default__ and has a CDO class index, it's a CDO, and it points to it's class
    let is_cdo_object = resolved_outer_import.outer.is_none() && object_name.starts_with("Default__");
    if is_cdo_object && !script_object.cdo_class_index.is_null() {

        // Resolve CDO class name and outer package. Class must always be 1 level deep in the package
        let resolved_class = resolve_script_import(package_cache, script_object.cdo_class_index)?;
        let resolved_class_package = resolved_class.outer.ok_or_else(|| { anyhow!("Failed to resolve CDO class package") })?;
        if resolved_class_package.outer.is_some() {
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
        class_package: CORE_OBJECT_PACKAGE_NAME.to_string(),
        class_name: OBJECT_CLASS_NAME.to_string(),
        outer: Some(Box::new(resolved_outer_import)), object_name,
    })
}
fn resolve_package_import(package_cache: &FZenPackageContext, package_header: &FZenPackageHeader, import: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {

    // Use new resolution logic (using public export hashes) on UE5.0 and above
    if package_header.container_header_version >= EIoContainerHeaderVersion::LocalizedPackages {
        resolve_package_import_internal_new(package_cache, package_header, import)
    } else {
        // Use legacy resolution logic (using imported package ID enumeration, which is considerably slower)
        resolve_package_import_internal_legacy(package_cache, package_header, import)
    }
}
fn resolve_package_import_internal_new(package_cache: &FZenPackageContext, package_header: &FZenPackageHeader, import: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {
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
    let imported_export = resolved_import_package.export_map.iter().find(|x| { x.is_public_export() && x.public_export_hash == public_export_hash })
        .ok_or_else(|| { anyhow!("Failed to resolve public export with hash {} on package {} (imported by {})",  public_export_hash, resolved_import_package.package_name(), package_header.package_name()) })?;

    resolve_package_export_internal(package_cache, resolved_import_package.as_ref(), imported_export)
}
fn resolve_package_import_internal_legacy(package_cache: &FZenPackageContext, package_header: &FZenPackageHeader, import: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {

    // Since we do not know which imported package this import is coming from because imported object indices are black box, iterate all of them and find the one that has a matching export
    for imported_package_id in &package_header.imported_packages {

        // Resolve the imported package, and abort if we cannot resolve it
        let resolved_import_package = package_cache.lookup(*imported_package_id)?;
        let potential_imported_export = resolved_import_package.export_map.iter().find(|x| { x.legacy_global_import_index() == import });

        // If we found an actual export we have imported, resolve it from this package internally
        if let Some(imported_export) = potential_imported_export {
            return resolve_package_export_internal(package_cache, resolved_import_package.as_ref(), imported_export)
        }
    }
    // Failed to find a matching export, bail
    bail!("Failed to resolve imported package object index {} on any of the imported packages (imported by {})", import, package_header.package_name());
}
fn resolve_package_export(package_cache: &FZenPackageContext, package_header: &FZenPackageHeader, export: FPackageObjectIndex) -> anyhow::Result<ResolvedZenImport> {

    // Resolve the import object index
    if export.kind() != FPackageObjectIndexType::Export {
        bail!("ResolvePackageExport called on non-export index: {}", export);
    }
    let export_index = export.export().unwrap();
    let resolved_export = &package_header.export_map[export_index as usize];

    resolve_package_export_internal(package_cache, package_header, resolved_export)
}
fn resolve_package_export_internal(package_cache: &FZenPackageContext, package_header: &FZenPackageHeader, export: &FExportMapEntry) -> anyhow::Result<ResolvedZenImport> {
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
        class_package: CORE_OBJECT_PACKAGE_NAME.to_string(),
        class_name: PACKAGE_CLASS_NAME.to_string(),
        object_name: package_header.source_package_name(),
        outer: None,
    })
}
fn resolve_generic_zen_import_import(package_cache: &FZenPackageContext, package_header: &FZenPackageHeader, import: FPackageObjectIndex, resolve_null_as_this_package: bool) -> anyhow::Result<ResolvedZenImport> {
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
    package_context: &'a FZenPackageContext<'b>,
    package_id: FPackageId,
    zen_package: Arc<FZenPackageHeader>,
    legacy_package: FLegacyPackageHeader,
    resolved_import_lookup: HashMap<ResolvedZenImport, FPackageIndex>,
    zen_import_lookup: HashMap<FPackageObjectIndex, FPackageIndex>,
    // Maps desired (real) import position to the current (temporary) import position
    original_import_order: HashMap<usize, usize>,
    // Whenever export data needs to be rebuilt from scratch in the order of the exports
    needs_to_rebuild_exports_data: bool,
    // True if we have failed to resolve some entries in the import map. Used to avoid some of the logic that assumes that the import map is fully populated
    has_failed_import_map_entries: bool,
}

// Lifetime: create_asset_builder -> begin_build_summary -> copy_package_sections -> build_import_map -> build_export_map -> resolve_prestream_package_imports -> resolve_export_dependencies -> finalize_asset -> write_asset
fn create_asset_builder<'a, 'b>(package_context: &'a FZenPackageContext<'b>, package_id: FPackageId) -> anyhow::Result<LegacyAssetBuilder<'a, 'b>> {
    let zen_package: Arc<FZenPackageHeader> = package_context.lookup(package_id)?;
    drop(package_context.get_script_objects()?);
    Ok(LegacyAssetBuilder{
        package_context,
        package_id,
        zen_package,
        legacy_package: FLegacyPackageHeader::default(),
        resolved_import_lookup: HashMap::new(),
        zen_import_lookup: HashMap::new(),
        original_import_order: HashMap::new(),
        needs_to_rebuild_exports_data: false,
        has_failed_import_map_entries: false,
    })
}
fn begin_build_summary(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {
    // Populate package summary with basic data
    let mut legacy_package_summary = FLegacyPackageFileSummary::default();
    legacy_package_summary.package_name = builder.zen_package.name_map.get(builder.zen_package.summary.name).to_string();
    legacy_package_summary.package_flags = builder.zen_package.summary.package_flags;
    legacy_package_summary.package_guid = FGuid{a: 0, b: 0, c: 0, d: 0};
    legacy_package_summary.package_source = 0;

    let zen_versions: FZenPackageVersioningInfo = builder.zen_package.versioning_info.clone();

    // If the zen package is unversioned, fallback package version is not provided, we have not logged it before
    if builder.zen_package.is_unversioned && builder.package_context.fallback_package_file_version.is_none() && builder.package_context.log.allow_stdout() {
        // using inner state lock to flag whether we have logged something once xd
        let mut state_lock = builder.package_context.inner_state.write().unwrap();
        if !state_lock.has_logged_detected_package_version {
            state_lock.has_logged_detected_package_version = true;
            log!(builder.package_context.log, "Detected package version: FPackageFileVersion(UE4: {}, UE5: {}), EZenPackageVersion: {}",
                     zen_versions.package_file_version.file_version_ue4, zen_versions.package_file_version.file_version_ue5, zen_versions.zen_version as u32);
        }
    }

    legacy_package_summary.versioning_info = FLegacyPackageVersioningInfo{
        package_file_version: zen_versions.package_file_version,
        licensee_version: zen_versions.licensee_version,
        custom_versions: zen_versions.custom_versions.clone(),
        is_unversioned: builder.zen_package.is_unversioned,
        ..FLegacyPackageVersioningInfo::default()
    };

    // Create legacy package header
    builder.legacy_package = FLegacyPackageHeader {summary: legacy_package_summary, ..FLegacyPackageHeader::default()};
    Ok(())
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

        FObjectDataResource {
            flags,
            serial_offset: zen_bulk_data.serial_offset,
            duplicate_serial_offset: zen_bulk_data.duplicate_serial_offset,
            serial_size: zen_bulk_data.serial_size,
            raw_size,
            outer_index,
            legacy_bulk_data_flags
        }
    }).collect();
    Ok(())
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

// Builds the import map entries for the legacy package. Returns true if some imports failed to resolve
fn build_import_map(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {
    for import_index in 0..builder.zen_package.import_map.len() {

        let import_object_index = builder.zen_package.import_map[import_index];

        // Skip holes in the zen import map
        if import_object_index.is_null() {
            continue
        }
        // Resolve the local import reference
        let result_import_map_index = resolve_local_package_object(builder, import_object_index);

        // Handle potential failure to resolve the import. This is not an unusual situation when specific assets are referenced by the cooked content, but are not cooked themselves
        let import_map_index = if result_import_map_index.is_err() {

            // TODO: Do not log messages that refer to packages that have failed loading previously not to spam the output. There should be a better way to handle this.
            let loading_error_message = result_import_map_index.unwrap_err().to_string();
            if !loading_error_message.contains("failed loading previously") {
                log!(builder.package_context.log, "Failed to resolve import map object {} (index {}) for package {}: {}", import_object_index, import_index, builder.zen_package.package_name(), loading_error_message);
            }
            builder.has_failed_import_map_entries = true;

            // Insert the entry into the import map, and associate this import index with the null import
            let null_package_import = create_and_add_unknown_package_import(builder);
            let import_map_index = FPackageIndex::create_import(builder.legacy_package.imports.len() as u32);
            let null_object_import = create_unknown_object_import_map_entry(builder, null_package_import);

            builder.legacy_package.imports.push(null_object_import);
            builder.zen_import_lookup.insert(import_object_index, import_map_index);
            import_map_index
        } else {
            result_import_map_index?
        };

        // We must ALWAYS get an import here, not a null or export
        if !import_map_index.is_import() {
            bail!("Import map package object index {} did not resolve into an import for package {}", import_object_index, builder.zen_package.package_name());
        }
        // Track the original position of this import in the import map. We will need to re-sort the imports and exports later to stick to it
        builder.original_import_order.insert(import_index, import_map_index.to_import_index() as usize);
    }
    Ok(())
}
fn build_export_map(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {
    builder.legacy_package.exports.reserve(builder.zen_package.export_map.len());

    // If there are export bundle headers, export blobs have been moved from their original positions, and we need to rebuild the export section
    builder.needs_to_rebuild_exports_data = !builder.zen_package.export_bundle_headers.is_empty();

    for export_index in 0..builder.zen_package.export_map.len() {
        let zen_export: FExportMapEntry = builder.zen_package.as_ref().export_map[export_index].clone();

        // Resolve class, outer, template index. These are required dependencies for the export construction and, as such, package extraction
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
        // In 5.2 and earlier, serial offset on exports is meaningless, actual offsets need to be calculated from bundle headers, so do not use that offset in <=5.2
        let serial_size: i64 = zen_export.cooked_serial_size as i64;
        let serial_offset: i64 = if !builder.needs_to_rebuild_exports_data { zen_export.cooked_serial_offset as i64 } else { -1 };

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
        // Note that this property did not exist before UE5.0, so it is always false on earlier versions
        let generate_public_hash = builder.zen_package.container_header_version >= EIoContainerHeaderVersion::LocalizedPackages &&
            (object_flags & (EObjectFlags::Public as u32)) == 0 && zen_export.is_public_export();

        // There is no real way to safely infer these from the zen package. They are only used in the editor for loading assets of undefined types to try and salvage as much data out of them as possible when using versioned serialization
        // Providing incorrect values here will crash the editor in such a case because of the serial size mismatch. However, we can make an assumption that the entire export is only script properties, and nothing else
        // In that case, script_serialization_start_offset = 0 which would be the start of the export, and script_serialization_end_offset = serial_size, which would be the entire export
        let script_serialization_start_offset: i64 = 0;
        let script_serialization_end_offset: i64 = serial_size;

        let new_object_export = FObjectExport {
            class_index, super_index, template_index, outer_index, object_name, object_flags, serial_size, serial_offset,
            is_not_for_client, is_not_for_server, is_not_always_loaded_for_editor_game, is_inherited_instance, is_asset, generate_public_hash,
            script_serialization_start_offset, script_serialization_end_offset,
            first_export_dependency_index: -1,
            ..FObjectExport::default()
        };
        // Add the export to the resulting asset export map
        builder.legacy_package.exports.push(new_object_export);
    }

    // If we need to rebuild exports, assign the serial offsets on them in the order in which we will lay them out in the file
    if builder.needs_to_rebuild_exports_data {

        // Note that this is relative to the package header, not to the actual start of the file
        let mut current_export_serial_offset = 0;

        for export_index in 0..builder.legacy_package.exports.len() {

            let export_serial_size = builder.legacy_package.exports[export_index].serial_size;
            let new_export_serial_offset = current_export_serial_offset;

            builder.legacy_package.exports[export_index].serial_offset = new_export_serial_offset;
            current_export_serial_offset += export_serial_size;
        }
    }
    Ok(())
}

// Export dependency representation not bound to a global list
#[derive(Debug, Clone, Default)]
struct FStandaloneExportDependencies {
    serialize_before_serialize: Vec<FPackageIndex>,
    create_before_serialize: Vec<FPackageIndex>,
    serialize_before_create: Vec<FPackageIndex>,
    create_before_create: Vec<FPackageIndex>,
}
fn resolve_export_dependencies_internal_dependency_bundles(builder: &mut LegacyAssetBuilder, export_dependencies: &mut [FStandaloneExportDependencies]) {

    for (export_index, bundle_header) in builder.zen_package.dependency_bundle_headers.iter().enumerate() {

        let dependencies = &mut export_dependencies[export_index];

        // Extract dependencies from zen first
        let first_dependency_index = bundle_header.first_entry_index as usize;

        // Note that local_import_or_export_index from zen use import map ordering that is not currently valid for this asset, and will only become valid later, but
        // super_index/outer_index/class_index use currently active import map ordering.
        // To fix that, we remap zen package indices to the currently active import order, and then in finalize_asset we will remap all imports back to the final order
        let remap_zen_package_index = |x: FPackageIndex| -> FPackageIndex {
            if x.is_import() { FPackageIndex::create_import(*builder.original_import_order.get(&(x.to_import_index() as usize)).unwrap() as u32) } else { x }
        };

        // Create before create dependencies
        let last_create_before_create_index = first_dependency_index + bundle_header.create_before_create_dependencies as usize;
        dependencies.create_before_create =
            builder.zen_package.dependency_bundle_entries[first_dependency_index..last_create_before_create_index]
                .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();

        // Serialize before create dependencies
        let last_serialize_before_create_index = last_create_before_create_index + bundle_header.serialize_before_create_dependencies as usize;
        dependencies.serialize_before_create =
            builder.zen_package.dependency_bundle_entries[last_create_before_create_index..last_serialize_before_create_index]
                .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();

        // Create before serialize dependencies
        let last_create_before_serialize_index = last_serialize_before_create_index + bundle_header.create_before_serialize_dependencies as usize;
        dependencies.create_before_serialize =
            builder.zen_package.dependency_bundle_entries[last_serialize_before_create_index..last_create_before_serialize_index]
                .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();

        // Serialize before serialize dependencies
        let last_serialize_before_serialize_index = last_create_before_serialize_index + bundle_header.serialize_before_serialize_dependencies as usize;
        dependencies.serialize_before_serialize =
            builder.zen_package.dependency_bundle_entries[last_create_before_serialize_index..last_serialize_before_serialize_index]
                .iter().map(|x| x.local_import_or_export_index).map(remap_zen_package_index).collect();
    }
}

#[derive(Copy, Clone, Default)]
struct ExternalDependencyBundleExportState {
    import_index: FPackageObjectIndex,
    has_been_created: bool,
    has_been_serialized: bool,
    load_order_index: i64,
}

#[derive(Clone)]
struct ExportBundleGroup {
    export_indices: Vec<usize>,
    command_type: EExportCommandType,
}
fn resolve_export_dependencies_internal_dependency_arcs(builder: &mut LegacyAssetBuilder, export_dependencies: &mut [FStandaloneExportDependencies]) -> anyhow::Result<()> {

    // "to export" is the one that depends on the given state of the "from export/import", e.g. "to export" is the export that has "from export/import" as a dependency
    let mut add_export_dependency = |from_index: FPackageIndex, to_export_index: usize, from_type: EExportCommandType, to_type: EExportCommandType| -> anyhow::Result<()> {

        // Do not attempt to create the dependencies between the same export. They are always implied and break the loader if explicitly stated
        if from_index.is_export() && from_index.to_export_index() as usize == to_export_index {
            // If this is the same export, the "from" command should always be Create, and to command should always be Serialize
            // Create depending on Serialize of the same export is always an error, and Create-Create or Serialize-Serialize are impossible dependencies
            if from_type != EExportCommandType::Create || to_type != EExportCommandType::Serialize {
                bail!("Invalid export bundle composition for the asset, export {} has a {:?} before {:?} dependency on itself", to_export_index, from_type, to_type);
            }
        // for "to export" to be in Created state, it needs "from export" to be in Created (or Serialized) state
        } else if to_type == EExportCommandType::Create {
            if from_type == EExportCommandType::Create {
                // create before create dependency
                export_dependencies[to_export_index].create_before_create.push(from_index);
            } else if from_type == EExportCommandType::Serialize {
                // serialize before create dependency
                export_dependencies[to_export_index].serialize_before_create.push(from_index);
            }
        // for "to export" to be in Serialized state, it needs "from export" to be in Created (or Serialized) state
        } else if to_type == EExportCommandType::Serialize {
            if from_type == EExportCommandType::Create {
                // create before serialize dependency
                export_dependencies[to_export_index].create_before_serialize.push(from_index);
            } else if from_type == EExportCommandType::Serialize {
                // serialize before serialize dependency
                export_dependencies[to_export_index].serialize_before_serialize.push(from_index);
            }
        }
        Ok(())
    };

    // Process intra-export bundle dependencies. The sequence in which elements are laid out in the export bundle determines their dependencies relative to each other
    // This introduces some additional dependencies to specific objects and forces the linker to load them exactly in the bundle order,
    // but this is how they are supposed to be loaded by Zen Loader anyway
    for bundle_header_index in 0..builder.zen_package.export_bundle_headers.len() {

        let bundle_header = builder.zen_package.export_bundle_headers[bundle_header_index];
        for i in 1..bundle_header.entry_count {

            let from_bundle_entry_index = bundle_header.first_entry_index + i - 1;
            let from_bundle_entry = builder.zen_package.export_bundle_entries[from_bundle_entry_index as usize];

            let to_bundle_entry_index = bundle_header.first_entry_index + i;
            let to_bundle_entry = builder.zen_package.export_bundle_entries[to_bundle_entry_index as usize];

            let from_index = FPackageIndex::create_export(from_bundle_entry.local_export_index);
            let from_command_type = from_bundle_entry.command_type;

            let to_export_index = to_bundle_entry.local_export_index as usize;
            let to_command_type = to_bundle_entry.command_type;

            add_export_dependency(from_index, to_export_index, from_command_type, to_command_type)?;
        }
    }

    // Take the internal dependency arcs for the package, but for legacy packages we might need to create them ourselves
    let mut internal_dependency_arcs = builder.zen_package.internal_dependency_arcs.clone();

    // There are no internal dependency arcs available for legacy packages. Their export bundles are always serialized sequentially (e.g. 0 -> 1 -> 2), so they have
    // implicit dependency between the next bundle and the previous bundle. So create the synthetic preload dependencies for that
    if builder.zen_package.container_header_version <= EIoContainerHeaderVersion::Initial {

        // Create an internal dependency arc from this export bundle to the previous export bundle
        for export_bundle_index in 1..builder.zen_package.export_bundle_headers.len() {
            internal_dependency_arcs.push(FInternalDependencyArc{
                from_export_bundle_index: (export_bundle_index - 1) as i32,
                to_export_bundle_index: export_bundle_index as i32,
            })
        }
    }

    // Process internal dependencies (export bundle to export bundle, e.g. export to export)
    // We link first element of the "to" bundle to the last element of the "from" bundle
    for internal_arc in internal_dependency_arcs {

        let from_export_bundle = builder.zen_package.export_bundle_headers[internal_arc.from_export_bundle_index as usize];
        let from_export_bundle_last_element_index = from_export_bundle.first_entry_index + from_export_bundle.entry_count - 1;
        let from_export_bundle_last_element = builder.zen_package.export_bundle_entries[from_export_bundle_last_element_index as usize];

        let from_export_index = FPackageIndex::create_export(from_export_bundle_last_element.local_export_index);
        let from_command_type = from_export_bundle_last_element.command_type;

        // Try to find the first Serialize element in the To bundle and not just the first element, since that works better for preventing circular dependencies
        let to_export_bundle = builder.zen_package.export_bundle_headers[internal_arc.to_export_bundle_index as usize];
        let to_export_bundle_first_element = builder.zen_package.export_bundle_entries[to_export_bundle.first_entry_index as usize];
        let to_export_index = to_export_bundle_first_element.local_export_index as usize;
        let to_command_type = to_export_bundle_first_element.command_type;

        add_export_dependency(from_export_index, to_export_index, from_command_type, to_command_type)?;
    }

    // Process external dependencies (import to export bundle, e.g. import to export)
    // We link all the external arcs to the first element of the "to" export bundle
    let all_external_arcs: Vec<FExternalDependencyArc> = builder.zen_package.external_package_dependencies.iter().flat_map(|x| { x.external_dependency_arcs.clone() }).collect();
    for external_arc in all_external_arcs {

        // Try to find the first Serialize element in the To bundle and not just the first element, since that works better for preventing circular dependencies
        let to_export_bundle = builder.zen_package.export_bundle_headers[external_arc.to_export_bundle_index as usize];
        let to_export_bundle_entry = builder.zen_package.export_bundle_entries[to_export_bundle.first_entry_index as usize];
        let to_export_index = to_export_bundle_entry.local_export_index as usize;
        let to_command_type = to_export_bundle_entry.command_type;

        let from_original_import_index = external_arc.from_import_index as usize;
        let from_command_type = external_arc.from_command_type;

        // Same logic here as in resolve_export_dependencies_internal_dependency_bundles - the import indices that are written into the zen asset are not the same indices that are used
        // during the intermediate steps of asset building when rehydrating import map, so we need to map the original index to the temporary import index,
        // and then all preload dependencies will get remapped back to the correct original indices when the asset is finalized
        let from_import_index_raw = *builder.original_import_order.get(&from_original_import_index).unwrap() as u32;
        let from_import_index = FPackageIndex::create_import(from_import_index_raw);

        add_export_dependency(from_import_index, to_export_index, from_command_type, to_command_type)?;
    }

    // If this is a legacy package (UE4.27 or below) where dependencies are between bundles from different packages, process them here
    // Note that this might result in resolution and loading of new packages, which is why this function can fail
    if builder.zen_package.container_header_version <= EIoContainerHeaderVersion::Initial {

        for external_package_dependency in builder.zen_package.external_package_dependencies.clone() {

            let imported_package_id = external_package_dependency.from_package_id;
            let import_package_result = builder.package_context.lookup(imported_package_id);

            // If we failed to look up a preload dependency, it is pretty bad for the global order, but should not stop us from attempting to load the package
            if import_package_result.is_err() {
                let loading_error_message = import_package_result.unwrap_err().to_string();
                if !loading_error_message.contains("failed loading previously") {
                    log!(builder.package_context.log, "Failed to resolve a preload dependency on package {} for package {} ({}): {}", 
                       imported_package_id, builder.package_id, builder.zen_package.package_name(), loading_error_message);
                }
                continue;
            }
            let resolved_import_package = import_package_result?;

            // Link the last element from the "from" export bundle to the first element of the "to" export bundle
            for bundle_to_bundle_dependency_arc in &external_package_dependency.legacy_dependency_arcs {

                // Handle special case: -1 value as from bundle index means depend on the last bundle present in the from package. This is used for importing base game packages in the DLCs, as well as in mods
                let from_bundle_index = if bundle_to_bundle_dependency_arc.from_export_bundle_index == -1 {
                    resolved_import_package.as_ref().export_bundle_headers.len() - 1
                } else { bundle_to_bundle_dependency_arc.from_export_bundle_index as usize };
                
                // Resolve the last element of the from export bundle
                let from_export_bundle = resolved_import_package.export_bundle_headers[from_bundle_index];
                let from_export_bundle_last_element_index = (from_export_bundle.first_entry_index + from_export_bundle.entry_count - 1) as usize;
                let from_export_bundle_entry = resolved_import_package.export_bundle_entries[from_export_bundle_last_element_index];
                
                // Create fully resolved zen import from that export
                let resolved_from_export_entry = resolved_import_package.export_map[from_export_bundle_entry.local_export_index as usize].clone();
                let resolved_from_import_result = resolve_package_export_internal(builder.package_context, &resolved_import_package, &resolved_from_export_entry);
                
                // Failure to create a full zen import is pretty bad here as well, but could still potentially be recovered from
                if resolved_from_import_result.is_err() {
                    let resolution_error_message = resolved_from_import_result.unwrap_err().to_string();
                    log!(builder.package_context.log, "Failed to resolve a preload dependency on package {} ({}) export {} for package {} ({}): {}", 
                            imported_package_id, resolved_import_package.package_name(), from_export_bundle_entry.local_export_index,
                            builder.package_id, builder.zen_package.package_name(), resolution_error_message);
                    continue;
                }
                
                // Add the entry into the import table if it does not exist there yet to get the resulting import index
                let from_import_index = find_or_add_resolved_import(builder, &resolved_from_import_result?);
                let from_command_type = from_export_bundle_entry.command_type;
                
                // Resolve the first element of the to export bundle
                let to_export_bundle = builder.zen_package.export_bundle_headers[bundle_to_bundle_dependency_arc.to_export_bundle_index as usize];
                let to_export_bundle_entry = builder.zen_package.export_bundle_entries[to_export_bundle.first_entry_index as usize];

                let to_export_index = to_export_bundle_entry.local_export_index as usize;
                let to_command_type = to_export_bundle_entry.command_type;

                // Add the resulting preload dependency
                add_export_dependency(from_import_index, to_export_index, from_command_type, to_command_type)?;
            }
        }
    }
    Ok(())
}

fn apply_standalone_dependencies_to_package(builder: &mut LegacyAssetBuilder, export_dependencies: &mut [FStandaloneExportDependencies]) {
    for (export_index, export_object) in builder.legacy_package.exports.iter_mut().enumerate() {

        let dependencies = &mut export_dependencies[export_index];

        // Ensure that we have outer and super as create before create dependencies
        if !export_object.outer_index.is_null() && !dependencies.create_before_create.contains(&export_object.outer_index) {
            dependencies.create_before_create.push(export_object.outer_index);
        }
        if !export_object.super_index.is_null() && !dependencies.create_before_create.contains(&export_object.super_index) {
            dependencies.serialize_before_serialize.push(export_object.super_index);
        }
        // Ensure that we have class and archetype as serialize before create dependencies
        if !export_object.class_index.is_null() && !dependencies.serialize_before_create.contains(&export_object.class_index) {
            dependencies.serialize_before_create.push(export_object.class_index);
        }
        if !export_object.template_index.is_null() && !dependencies.serialize_before_create.contains(&export_object.template_index) {
            dependencies.serialize_before_create.push(export_object.template_index);
        }

        // If we have no dependencies altogether, do not write first dependency import on the export
        if dependencies.create_before_create.is_empty() && dependencies.serialize_before_create.is_empty() && dependencies.create_before_serialize.is_empty() && dependencies.serialize_before_serialize.is_empty() {
            continue
        }

        // Set the index of the preload dependencies start, and the numbers of each dependency
        export_object.first_export_dependency_index = builder.legacy_package.preload_dependencies.len() as i32;
        export_object.serialize_before_serialize_dependencies = dependencies.serialize_before_serialize.len() as i32;
        export_object.create_before_serialize_dependencies = dependencies.create_before_serialize.len() as i32;
        export_object.serialize_before_create_dependencies = dependencies.serialize_before_create.len() as i32;
        export_object.create_before_create_dependencies = dependencies.create_before_create.len() as i32;

        // Append preload dependencies for this export now to the legacy package
        builder.legacy_package.preload_dependencies.append(&mut dependencies.serialize_before_serialize);
        builder.legacy_package.preload_dependencies.append(&mut dependencies.create_before_serialize);
        builder.legacy_package.preload_dependencies.append(&mut dependencies.serialize_before_create);
        builder.legacy_package.preload_dependencies.append(&mut dependencies.create_before_create);
    }
}

fn resolve_export_dependencies(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {

    // Create standalone dependency objects first
    let mut export_dependencies: Vec<FStandaloneExportDependencies> = Vec::with_capacity(builder.legacy_package.exports.len());
    for _ in 0..builder.legacy_package.exports.len() {
        export_dependencies.push(FStandaloneExportDependencies::default());
    }

    // If we have dependency bundle entries, this is new style (UE5.3+) dependencies
    if !builder.zen_package.dependency_bundle_entries.is_empty() {
        resolve_export_dependencies_internal_dependency_bundles(builder, &mut export_dependencies);
    }
    // If we have export bundle headers, this is old style dependencies
    else if !builder.zen_package.export_bundle_headers.is_empty() {
        resolve_export_dependencies_internal_dependency_arcs(builder, &mut export_dependencies)?;
    }

    // Apply standalone dependencies to the package global list
    apply_standalone_dependencies_to_package(builder, &mut export_dependencies);
    Ok(())
}

fn resolve_prestream_package_imports(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {

    // Do not attempt to resolve pre-stream dependencies if we have failed imports, since the logic we use for them will count missing import packages as false positives
    if builder.has_failed_import_map_entries {
        return Ok(())
    }

    // Figure out if we have left some package IDs that were in the imported packages, but are not presently in the import map
    // This is needed to account for PrestreamPackage imports, which are special import map entries that do not have objects, but indicate an intention to pre-stream another package
    let all_imported_package_ids: HashSet<FPackageId> = builder.legacy_package.imports.iter()
        .filter(|x| x.outer_index.is_null())
        .map(|x| builder.legacy_package.name_map.get(x.object_name).to_string())
        .filter(|x| !x.starts_with("/Script/"))
        .map(|x| FPackageId::from_name(&x))
        .collect();

    let prestream_package_ids: Vec<FPackageId> = builder.zen_package.imported_packages.iter().cloned()
        .filter(|x| !all_imported_package_ids.contains(x))
        .collect();

    // Attempt to resolve prestream package references if we have some
    for prestream_package_id in &prestream_package_ids {

        // Failure to resolve a prestream package reference is not critical to the package generation
        let resolved_prestream_package = builder.package_context.lookup(*prestream_package_id);
        if resolved_prestream_package.is_err() {
            log!(builder.package_context.log, "Failed to resolve a pre-stream request to the package {} from package {}",
                prestream_package_id.clone(), builder.zen_package.package_name());
            continue;
        }

        // Resolve the name of the package, put it into the name map and add an import
        let resolved_package_name = resolved_prestream_package.unwrap().source_package_name();
        let class_package = builder.legacy_package.name_map.store(CORE_OBJECT_PACKAGE_NAME);
        let class_name = builder.legacy_package.name_map.store(PRESTREAM_PACKAGE_CLASS_NAME);
        let object_name = builder.legacy_package.name_map.store(&resolved_package_name);

        // Log that we have resolved the pre-stream request successfully
        if builder.package_context.log.verbose_enabled() {
            log!(builder.package_context.log, "Resolved pre-stream package request {} from package {}",
                    resolved_package_name.clone(), builder.legacy_package.summary.package_name.clone());
        }

        builder.legacy_package.imports.push(FObjectImport{
            class_package, class_name,
            outer_index: FPackageIndex::create_null(),
            object_name,
            is_optional: false,
        });
    }
    Ok(())
}

fn create_unknown_package_import_map_entry(builder: &mut LegacyAssetBuilder) -> FObjectImport {
    let class_package = builder.legacy_package.name_map.store(CORE_OBJECT_PACKAGE_NAME);
    let class_name = builder.legacy_package.name_map.store(PACKAGE_CLASS_NAME);
    // The package name here can be anything except for a script import
    let object_name = builder.legacy_package.name_map.store("/Engine/UnknownPackage");

    FObjectImport{class_package, class_name, outer_index: FPackageIndex::create_null(), object_name, is_optional: false}
}
fn create_and_add_unknown_package_import(builder: &mut LegacyAssetBuilder) -> FPackageIndex {
    let new_import_index = builder.legacy_package.imports.len();
    let new_import_entry = create_unknown_package_import_map_entry(builder);
    builder.legacy_package.imports.push(new_import_entry);
    FPackageIndex::create_import(new_import_index as u32)
}
fn create_unknown_object_import_map_entry(builder: &mut LegacyAssetBuilder, outer_index: FPackageIndex) -> FObjectImport {
    let class_package = builder.legacy_package.name_map.store(CORE_OBJECT_PACKAGE_NAME);
    let class_name = builder.legacy_package.name_map.store(OBJECT_CLASS_NAME);
    // This object name can be anything
    let object_name = builder.legacy_package.name_map.store("UnknownExport");

    // This import has to have a package since it is not a package itself
    FObjectImport{class_package, class_name, outer_index, object_name, is_optional: false}
}

fn finalize_asset(builder: &mut LegacyAssetBuilder) -> anyhow::Result<()> {

    // Remap import map to the current indices
    let import_map_size = max(builder.legacy_package.imports.len(), builder.zen_package.import_map.len());
    let mut import_remap_map: HashMap<usize, usize> = HashMap::with_capacity(import_map_size);
    let mut new_import_map: Vec<FObjectImport> = Vec::with_capacity(import_map_size);

    let current_import_indices_with_predefined_positions: HashSet<usize> = builder.original_import_order.values().copied().collect();
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
        // We should never end up with fewer imports than we have to fill the holes, since zen never strips non-upackage exports
        if current_legacy_asset_import_index >= builder.legacy_package.imports.len() {
            // Attempt to handle this case gracefully by emitting a null import map entry. This allows us not to fail on extracting packages that might load fine otherwise (albeit with information loss)
            // Log the warning regardless though because the information is lost
            if !builder.has_failed_import_map_entries {
                log!(builder.package_context.log, "Failed to find import map entry to fill the hole at {} while filling import map up to size {} for package {}. Null import map entry will be used instead",
                    final_import_index, import_map_size, builder.legacy_package.summary.package_name.clone());
            }

            // We need a new import map entry here, and since it's not referenced by the original package, it can be a package entry, which will become Null again if asset is converted back to zen
            new_import_map.push(create_unknown_package_import_map_entry(builder));
            continue;
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
    Ok(())
}

// Builds an asset from zen container, returns a builder that can be used to write the payload to the file or read it directly
fn build_asset_from_zen<'a, 'b>(package_context: &'a FZenPackageContext<'b>, package_id: FPackageId) -> anyhow::Result<LegacyAssetBuilder<'a, 'b>> {

    let mut asset_builder = create_asset_builder(package_context, package_id)?;
    begin_build_summary(&mut asset_builder)?;
    copy_package_sections(&mut asset_builder)?;
    build_import_map(&mut asset_builder)?;
    build_export_map(&mut asset_builder)?;
    resolve_prestream_package_imports(&mut asset_builder)?;
    resolve_export_dependencies(&mut asset_builder)?;
    finalize_asset(&mut asset_builder)?;

    Ok(asset_builder)
}
fn rebuild_asset_export_data_internal(builder: &LegacyAssetBuilder, raw_exports_data: &[u8]) -> anyhow::Result<Vec<u8>> {

    // Calculate the total size of the exports blob
    let mut total_exports_serial_size = 0;
    for export_index in 0..builder.legacy_package.exports.len() {
        total_exports_serial_size += builder.legacy_package.exports[export_index].serial_size as usize;
    }
    // Include the footer in the total file size
    let total_exports_file_size = total_exports_serial_size + size_of::<u32>();

    // Allocate the underlying storage and copy export blobs into it from the export bundles
    let mut result_exports_data: Vec<u8> = Vec::with_capacity(total_exports_file_size);
    let mut exports_data_writer = Cursor::new(&mut result_exports_data);
    let mut end_of_last_export_bundle: usize = 0;
    // For UE4.27 and below; serial offset is implicit on export bundles, they start after the header and follow each other sequentially
    let mut current_package_offset: usize = builder.zen_package.summary.header_size as usize;

    for export_bundle_index in 0..builder.zen_package.export_bundle_headers.len() {

        let export_bundle = builder.zen_package.export_bundle_headers[export_bundle_index];
        let mut current_serial_offset = if export_bundle.serial_offset != u64::MAX {
            builder.zen_package.summary.header_size as usize + export_bundle.serial_offset as usize
        } else {
            // If this is a legacy UE4.27 package, we use the current offset within the package as the starting point, since serial offsets of bundles are not explicitly tracked
            current_package_offset
        };

        for i in 0..export_bundle.entry_count {
            let export_bundle_entry_index = export_bundle.first_entry_index + i;
            let export_bundle_entry = builder.zen_package.export_bundle_entries[export_bundle_entry_index as usize];

            // Only serialize commands actually encode export blobs
            if export_bundle_entry.command_type == EExportCommandType::Serialize {

                let export_index = export_bundle_entry.local_export_index as usize;
                let export_serial_size = builder.legacy_package.exports[export_index].serial_size as usize;
                let export_target_serial_offset = builder.legacy_package.exports[export_index].serial_offset as u64;
                let export_data_start_offset = current_serial_offset;
                let export_data_end_offset = export_data_start_offset + export_serial_size;

                // Write the blob for the current export and skip past it's data
                exports_data_writer.seek(SeekFrom::Start(export_target_serial_offset))?;
                exports_data_writer.write_all(&raw_exports_data[export_data_start_offset..export_data_end_offset])?;
                current_serial_offset += export_serial_size;
            }
        }
        end_of_last_export_bundle = max(end_of_last_export_bundle, current_serial_offset);
        current_package_offset = current_serial_offset;
    }

    // Jump to the end of the exports data
    exports_data_writer.seek(SeekFrom::Start(total_exports_serial_size as u64))?;

    // If there is any data past the last export, copy it into the final file
    // This should generally never happen for legacy zen assets, but still nice to handle this just in case
    let additional_data_post_exports_length = raw_exports_data.len() - end_of_last_export_bundle;
    if additional_data_post_exports_length != 0 {
        exports_data_writer.write_all(&raw_exports_data[end_of_last_export_bundle..])?;
    }

    // Append the package footer at the end
    let package_file_magic: u32 = FLegacyPackageFileSummary::PACKAGE_FILE_TAG;
    exports_data_writer.ser(&package_file_magic)?;

    Ok(result_exports_data)
}

// Serializes an asset into in-memory buffer. Returns each region of the associated asset as a separate buffer
fn serialize_asset(builder: &LegacyAssetBuilder) -> anyhow::Result<FSerializedAssetBundle> {
    // Write the asset file first
    let mut asset_file_buffer: Vec<u8> = Vec::new();
    let mut asset_cursor = Cursor::new(&mut asset_file_buffer);
    FLegacyPackageHeader::serialize(&builder.legacy_package, &mut asset_cursor, Some(builder.zen_package.summary.cooked_header_size as usize), builder.package_context.log)?;

    // Copy the raw export data from the chunk into the exports file
    let raw_exports_data = builder.package_context.read_full_package_data(builder.package_id)?;
    let exports_file_buffer = if builder.needs_to_rebuild_exports_data {
        // If we need to rebuild export data, do it now
        rebuild_asset_export_data_internal(builder, &raw_exports_data)?
    } else {
        // Otherwise we just need to strip the zen header from the exports
        raw_exports_data[builder.zen_package.summary.header_size as usize..].to_vec()
    };

    let bulk_data_chunk_id = FIoChunkId::from_package_id(builder.package_id, 0, EIoChunkType::BulkData);
    let optional_bulk_data_chunk_id = FIoChunkId::from_package_id(builder.package_id, 0, EIoChunkType::OptionalBulkData);
    let memory_mapped_bulk_data_chunk_id = FIoChunkId::from_package_id(builder.package_id, 0, EIoChunkType::MemoryMappedBulkData);

    let store_access: &dyn IoStoreTrait = builder.package_context.store_access;
    let bulk_data_buffer = if store_access.has_chunk_id(bulk_data_chunk_id) { Some(store_access.read(bulk_data_chunk_id)?) } else { None };
    let optional_bulk_data_buffer = if store_access.has_chunk_id(optional_bulk_data_chunk_id) { Some(store_access.read(optional_bulk_data_chunk_id)?) } else { None };
    let memory_mapped_bulk_data_buffer = if store_access.has_chunk_id(memory_mapped_bulk_data_chunk_id) { Some(store_access.read(memory_mapped_bulk_data_chunk_id)?) } else { None };

    Ok(FSerializedAssetBundle{asset_file_buffer, exports_file_buffer, bulk_data_buffer, optional_bulk_data_buffer, memory_mapped_bulk_data_buffer})
}

// Writes asset to the file. Additionally writes to the uexp file next to it
fn write_asset(builder: &LegacyAssetBuilder, out_asset_path: &UEPath, file_writer: &dyn FileWriterTrait) -> anyhow::Result<()> {

    // Dump zen package and legacy package for debugging
    debug!(builder.package_context.log, "{:#?}", builder.zen_package);
    debug!(builder.package_context.log, "{:#?}", builder.legacy_package);
    // Serialize the asset
    let serialized_asset = serialize_asset(builder)?;

    // Write the asset file
    file_writer.write_file(out_asset_path.to_string(), true, serialized_asset.asset_file_buffer)?;
    // Write the exports file
    let export_file_path = out_asset_path.with_extension("uexp");
    file_writer.write_file(export_file_path.to_string(), true, serialized_asset.exports_file_buffer)?;

    // Write the bulk data file
    if let Some(bulk_data_buffer) = serialized_asset.bulk_data_buffer {
        let bulk_data_file_path = out_asset_path.with_extension("ubulk");
        file_writer.write_file(bulk_data_file_path.to_string(), true, bulk_data_buffer)?;
    }
    // Write the optional bulk data file
    if let Some(optional_bulk_data_buffer) = serialized_asset.optional_bulk_data_buffer {
        let optional_bulk_data_file_path = out_asset_path.with_extension("uptnl");
        file_writer.write_file(optional_bulk_data_file_path.to_string(), true, optional_bulk_data_buffer)?;
    }
    // Write the memory mapped bulk data file
    if let Some(memory_mapped_bulk_data_buffer) = serialized_asset.memory_mapped_bulk_data_buffer {
        let memory_mapped_bulk_data_file_path = out_asset_path.with_extension("m.ubulk");
        file_writer.write_file(memory_mapped_bulk_data_file_path.to_string(), false, memory_mapped_bulk_data_buffer)?;
    }
    Ok(())
}

pub(crate) fn build_legacy(
    package_context: &FZenPackageContext,
    package_id: FPackageId,
    out_path: &UEPath,
    file_writer: &dyn FileWriterTrait,
) -> anyhow::Result<()> {

    // Build the asset from zen
    let asset_builder = build_asset_from_zen(package_context, package_id)?;
    // Write the asset to the file
    write_asset(&asset_builder, out_path, file_writer)?;
    Ok(())
}
