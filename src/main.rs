use anyhow::{Context, Result, bail};
use cityhasher::HashSet;
use clap::Parser;
use fs_err as fs;
use rayon::prelude::*;
use retoc::asset_conversion::{self, FZenPackageContext};
use retoc::container_header::EIoContainerHeaderVersion;
use retoc::iostore::{IoStoreTrait, PackageInfo};
use retoc::iostore_writer::IoStoreWriter;
use retoc::legacy_asset::FSerializedAssetBundle;
use retoc::logging::Log;
use retoc::name_map::{EMappedNameType, FNameMap, write_name_batch_parts};
use retoc::script_objects::{FPackageObjectIndex, FScriptObjectEntry, ZenScriptObjects};
use retoc::ser::{ReadExt as _, WriteExt};
use retoc::shader_library::{self, get_shader_asset_info_filename_from_library_filename, rebuild_shader_library_from_io_store};
use retoc::version::EngineVersion;
use retoc::zen::{FPackageFileVersion, FZenPackageHeader, VerseScriptCell, get_package_name};
use retoc::zen_asset_conversion::{self, ConvertedZenAssetBundle};
use retoc::{
    AesKey, Config, EIoChunkType, EIoStoreTocVersion, FGuid, FIoChunkId, FIoChunkIdRaw, FPackageId, FSFileReader, FSFileWriter, FSHAHash, FileReaderTrait, FileWriterTrait, NullFileWriter, PackageTestMetadata, PakFileReader, ParallelPakWriter, Toc, UEPath, UEPathBuf, build_verse_cell_store, info,
    iostore, manifest, verbose,
};
use std::borrow::Cow;
use std::ffi::OsStr;
use std::io::BufWriter;
use std::path::Path;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::HashMap,
    io::{BufReader, Cursor, Write},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};

#[derive(Parser, Debug)]
struct ActionManifest {
    #[arg(index = 1)]
    utoc: PathBuf,
}

#[derive(Parser, Debug)]
struct ActionInfo {
    #[arg(index = 1)]
    path: PathBuf,
}

#[derive(Parser, Debug)]
struct ActionList {
    #[arg(index = 1)]
    utoc: PathBuf,

    /// By default only unique chunks will be listed. --all will also list chunks overriden by patch containers
    #[arg(long)]
    all: bool,
    /// Show chunk content hash
    #[arg(long)]
    hash: bool,
    /// Show package ID
    #[arg(long)]
    package: bool,
    /// Show chunk size
    #[arg(long)]
    size: bool,
    /// Show chunk path
    #[arg(long)]
    path: bool,
    /// Show package store entry
    #[arg(long)]
    store: bool,
}

#[derive(Parser, Debug)]
struct ActionVerify {
    #[arg(index = 1)]
    utoc: PathBuf,
}

#[derive(Parser, Debug)]
struct ActionUnpack {
    #[arg(index = 1)]
    utoc: PathBuf,
    #[arg(index = 2)]
    output: PathBuf,
    #[arg(short, long, default_value = "false")]
    verbose: bool,
}

#[derive(Parser, Debug)]
struct ActionUnpackRaw {
    #[arg(index = 1)]
    utoc: PathBuf,
    #[arg(index = 2)]
    output: PathBuf,
}

#[derive(Parser, Debug)]
struct ActionPackRaw {
    #[arg(index = 1)]
    input: PathBuf,
    #[arg(index = 2)]
    utoc: PathBuf,
}

#[derive(Parser, Debug)]
struct ActionToLegacy {
    /// Input .utoc or directory with multiple .utoc (e.g. Content/Paks/)
    #[arg(index = 1)]
    input: PathBuf,
    /// Output directory or .pak
    #[arg(index = 2)]
    output: PathBuf,

    /// Asset file name filter
    #[arg(short, long)]
    filter: Vec<String>,

    /// Skip conversion of assets
    #[arg(long)]
    no_assets: bool,
    /// Skip conversion of shader libraries
    #[arg(long)]
    no_shaders: bool,
    /// Skip extraction of script objects
    #[arg(long)]
    no_script_objects: bool,
    /// Skip compression of shader libraries
    #[arg(long)]
    no_compres_shaders: bool,
    /// Do not output any files (dry run). Useful for testing conversion
    #[arg(short, long)]
    dry_run: bool,

    /// Engine version override
    #[arg(long)]
    version: Option<EngineVersion>,

    /// Allows specifying additional Verse script cells to be considered for verse native cell import resolution
    #[arg(long)]
    script_cell: Vec<VerseScriptCell>,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
    /// Debug logging
    #[arg(long)]
    debug: bool,
    /// Do not run in parallel. Useful for debugging
    #[arg(long)]
    no_parallel: bool,
}

#[derive(Parser, Debug)]
struct ActionToZen {
    /// Input directory or .pak
    #[arg(index = 1)]
    input: PathBuf,
    /// Output .utoc
    #[arg(index = 2)]
    output: PathBuf,

    /// Asset file name filter
    #[arg(short, long)]
    filter: Vec<String>,

    /// Engine version
    #[arg(long)]
    version: EngineVersion,

    /// Allows specifying additional Verse script cells to be considered for verse native cell import resolution
    #[arg(long)]
    script_cell: Vec<VerseScriptCell>,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
    /// Debug logging
    #[arg(long)]
    debug: bool,
    /// Do not run in parallel. Useful for debugging
    #[arg(long)]
    no_parallel: bool,
}

#[derive(Parser, Debug)]
struct ActionGet {
    /// Input .utoc or directory with multiple .utoc (e.g. Content/Paks/)
    #[arg(index = 1)]
    input: PathBuf,
    /// Chunk ID to get
    #[arg(index = 2)]
    chunk_id: FIoChunkIdRaw,

    /// Optional output path or stdout if "-" or omitted
    #[arg(index = 3)]
    output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct ActionDumpTest {
    #[arg(index = 1)]
    input: PathBuf,
    #[arg(index = 2)]
    output_dir: PathBuf,
    #[arg(index = 3)]
    package_id: FPackageId,
}

#[derive(Parser, Debug)]
struct ActionGenScriptObjects {
    /// Input reflection data JSON dump
    #[arg(index = 1)]
    input: PathBuf,
    /// Output .utoc file
    #[arg(index = 2)]
    output: PathBuf,
    /// Engine version
    #[arg(long)]
    version: EngineVersion,
}

#[derive(Parser, Debug)]
struct ActionPrintScriptObjects {
    /// Input .utoc file containing script objects
    #[arg(index = 1)]
    input: PathBuf,
}

#[derive(Parser, Debug)]
enum Action {
    /// Extract manifest from .utoc
    Manifest(ActionManifest),
    /// Show container info
    Info(ActionInfo),
    /// List fils in .utoc (directory index)
    List(ActionList),
    /// Verify IO Store container
    Verify(ActionVerify),
    /// Extracts chunks (files) from .utoc
    Unpack(ActionUnpack),

    /// Extracts raw chunks from container
    UnpackRaw(ActionUnpackRaw),
    /// Packs directory of raw chunks into container
    PackRaw(ActionPackRaw),

    /// Converts asests and shaders from Zen to Legacy
    ToLegacy(ActionToLegacy),
    /// Converts assets and shaders from Legacy to Zen
    ToZen(ActionToZen),

    /// Get chunk by index and write to stdout
    Get(ActionGet),

    /// Dump test
    DumpTest(ActionDumpTest),

    /// Generate script objects global container from UE reflection data JSON
    /// see https://github.com/trumank/meatloaf
    GenScriptObjects(ActionGenScriptObjects),

    /// Print script objects from container
    PrintScriptObjects(ActionPrintScriptObjects),
}

#[derive(Parser, Debug)]
#[clap(version)]
struct Args {
    #[arg(short, long)]
    aes_key: Option<String>,
    #[arg(long)]
    override_container_header_version: Option<EIoContainerHeaderVersion>,
    #[arg(long)]
    override_toc_version: Option<EIoStoreTocVersion>,
    #[command(subcommand)]
    action: Action,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut config = Config {
        container_header_version_override: args.override_container_header_version,
        toc_version_override: args.override_toc_version,
        ..Default::default()
    };
    if let Some(aes) = args.aes_key {
        config.aes_keys.insert(FGuid::default(), AesKey::from_str(&aes)?);
    }
    let config = Arc::new(config);

    match args.action {
        Action::Manifest(action) => action_manifest(action, config),
        Action::Info(action) => action_info(action, config),
        Action::List(action) => action_list(action, config),
        Action::Verify(action) => action_verify(action, config),
        Action::Unpack(action) => action_unpack(action, config),

        Action::UnpackRaw(action) => action_unpack_raw(action, config),
        Action::PackRaw(action) => action_pack_raw(action, config),

        Action::ToLegacy(action) => action_to_legacy(action, config),
        Action::ToZen(action) => action_to_zen(action, config),

        Action::Get(action) => action_get(action, config),

        Action::DumpTest(action) => action_dump_test(action, config),

        Action::GenScriptObjects(action) => action_gen_script_objects(action, config),

        Action::PrintScriptObjects(action) => action_print_script_objects(action, config),
    }
}

fn action_manifest(args: ActionManifest, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.utoc, config)?;

    let entries = Arc::new(Mutex::new(vec![]));

    let container_header_version = iostore.container_header_version().unwrap();
    let toc_version = iostore.container_file_version().unwrap();

    iostore.packages().par_bridge().try_for_each(|package_info| -> Result<()> {
        let chunk_id = FIoChunkId::from_package_id(package_info.id(), 0, EIoChunkType::ExportBundleData).with_version(toc_version);
        let package_path = iostore.chunk_path(chunk_id).with_context(|| format!("{:?} has no path name entry", package_info.id()))?;
        let data = package_info.container().read(chunk_id)?;

        let package_name = get_package_name(&data, container_header_version).with_context(|| package_path.to_string())?;

        let mut entry = manifest::Op {
            packagestoreentry: manifest::PackageStoreEntry { packagename: package_name },
            packagedata: vec![manifest::ChunkData {
                id: chunk_id.get_raw(),
                filename: package_path.to_string(),
            }],
            bulkdata: vec![],
        };

        let bulk_id = FIoChunkId::from_package_id(package_info.id(), 0, EIoChunkType::BulkData).with_version(toc_version);
        if iostore.has_chunk_id(bulk_id) {
            entry.bulkdata.push(manifest::ChunkData {
                id: bulk_id.get_raw(),
                filename: UEPath::new(&package_path).with_extension("ubulk").to_string(),
            });
        }

        entries.lock().unwrap().push(entry);
        Ok(())
    })?;

    let mut entries = Arc::into_inner(entries).unwrap().into_inner().unwrap();
    //entries.sort_by_key(|op| op.packagedata.first().map(|c| c.filename.clone()));
    entries.sort_by(|a, b| a.packagestoreentry.packagename.cmp(&b.packagestoreentry.packagename));

    let manifest = manifest::PackageStoreManifest { oplog: manifest::OpLog { entries } };

    let path = "pakstore.json";
    fs::write(path, serde_json::to_vec(&manifest)?)?;

    println!("wrote {} entries to {}", manifest.oplog.entries.len(), path);

    Ok(())
}

fn action_info(args: ActionInfo, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.path, config)?;
    iostore.print_info(0);
    Ok(())
}

fn action_list(args: ActionList, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.utoc, config)?;

    let chunks = if args.all { iostore.chunks_all() } else { iostore.chunks() };

    for chunk in chunks {
        let id = chunk.id();
        let chunk_type = id.get_chunk_type();

        let package_id: Cow<str> = if chunk_type == EIoChunkType::ExportBundleData { FPackageId(id.get_chunk_id()).0.to_string().into() } else { "-".into() };

        use std::fmt::Write;
        let mut line = String::new();
        let mut first = true;

        macro_rules! column {
            ($($arg:tt)*) => {
                if !first { write!(&mut line, " ").unwrap(); }
                #[allow(unused)]
                { first = false; }
                write!(&mut line, $($arg)*).unwrap()
            };
        }

        column!("{:30}", chunk.container().container_name());
        column!("{}", hex::encode(id.get_raw()));
        if args.hash {
            column!("{}", hex::encode(chunk.hash().0));
        }
        if args.package {
            column!("{:20}", package_id);
        }
        column!("{:20}", chunk_type.as_ref());
        if args.size {
            column!("{:10}", chunk.size());
        }
        if args.path {
            column!("{}", chunk.path().as_deref().unwrap_or("-"));
        }
        if args.store {
            let package_store_entry = if chunk_type == EIoChunkType::ExportBundleData {
                let entry = chunk.container().package_store_entry(id.get_package_id()).unwrap();
                format!("{entry:?}")
            } else {
                "-".to_string()
            };
            column!("{:?}", package_store_entry);
        }

        println!("{line}");
    }
    Ok(())
}

fn action_verify(args: ActionVerify, config: Arc<Config>) -> Result<()> {
    let mut stream = BufReader::new(fs::File::open(&args.utoc)?);
    let ucas = &args.utoc.with_extension("ucas");

    let toc: Toc = stream.de_ctx(config)?;

    // most of these don't match?!
    //let sigs = &toc.signatures.as_ref().unwrap().chunk_block_signatures;
    //let mut rdr = BufReader::new(fs::File::open(ucas)?);
    //for (i, b) in toc.compression_blocks.iter().enumerate() {
    //    rdr.seek(SeekFrom::Start(b.get_offset()))?;

    //    let data: Vec<u8> = rdr.ser_ctx(b.get_compressed_size() as usize)?;

    //    use sha1::{Digest, Sha1};
    //    let mut hasher = Sha1::new();
    //    hasher.update(&data);
    //    let hash = hasher.finalize();

    //    println!(
    //        "{:?} {} {} {}",
    //        sigs[i],
    //        hex::encode_upper(hash),
    //        b.get_compression_method_index(),
    //        sigs[i].data == hash.as_ref()
    //    );
    //}

    toc.chunk_metas.par_iter().enumerate().try_for_each_init(
        || BufReader::new(fs::File::open(ucas).unwrap()),
        |ucas, (i, meta)| -> Result<()> {
            //let chunk_id = toc.chunks[i];
            let data = toc.read(ucas, i as u32)?;

            //let chunk_type = chunk_id.get_chunk_type();
            //match chunk_type {
            //    EIoChunkType::ExportBundleData => {
            //        let package_name = get_package_name(&data)?;
            //        let package_id = FPackageId::from_name(&package_name);
            //        let id = FIoChunkId::from_package_id(package_id, 0, chunk_type);
            //        if id != chunk_id {
            //            bail!("chunk ID mismatch")
            //        }
            //    }
            //    _ => {} // TODO
            //}

            let hash = blake3::hash(&data);

            //println!(
            //    "{:>10} {:?} {} {:?}",
            //    i,
            //    hex::encode(meta.chunk_hash.data),
            //    hex::encode(hash.as_bytes()),
            //    meta.flags
            //);

            if meta.chunk_hash.0[..20] != hash.as_bytes()[..20] {
                bail!("hash mismatch for chunk #{i}")
            }

            Ok(())
        },
    )?;

    println!("verified");

    Ok(())
}

fn action_unpack(args: ActionUnpack, config: Arc<Config>) -> Result<()> {
    let mut stream = BufReader::new(fs::File::open(&args.utoc)?);
    let ucas = &args.utoc.with_extension("ucas");

    let toc: Toc = stream.de_ctx(config)?;

    let output = args.output;

    // TODO extract entries not found in directory index
    // TODO output chunk id manifest
    toc.file_map.keys().par_bridge().try_for_each_init(
        || BufReader::new(fs::File::open(ucas).unwrap()),
        |ucas, file_name| -> Result<()> {
            if args.verbose {
                println!("{file_name}");
            }
            let data = toc.read(ucas, toc.file_map[file_name])?;

            let path = output.join(file_name);
            let dir = path.parent().unwrap();
            fs::create_dir_all(dir)?;
            fs::write(path, &data)?;
            Ok(())
        },
    )?;

    println!("unpacked {} files to {}", toc.file_map.len(), output.to_string_lossy());

    Ok(())
}

mod raw {
    use std::collections::HashMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use retoc::{EIoStoreTocVersion, FIoChunkIdRaw};

    #[derive(Serialize, Deserialize)]
    pub(crate) struct RawIoManifest {
        pub(crate) chunk_paths: HashMap<ChunkId, String>,
        pub(crate) version: EIoStoreTocVersion,
        pub(crate) mount_point: String,
    }
    #[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct ChunkId(#[serde(serialize_with = "to_hex", deserialize_with = "from_hex")] pub(crate) FIoChunkIdRaw);
    impl From<FIoChunkIdRaw> for ChunkId {
        fn from(value: FIoChunkIdRaw) -> Self {
            Self(value)
        }
    }

    fn to_hex<S>(chunk_id: &FIoChunkIdRaw, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(chunk_id.id))
    }
    fn from_hex<'de, D>(deserializer: D) -> Result<FIoChunkIdRaw, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let v = hex::decode(s).map_err(serde::de::Error::custom)?;
        Ok(FIoChunkIdRaw {
            id: v.try_into().map_err(|v: Vec<u8>| serde::de::Error::invalid_length(v.len(), &"a 12 byte hex string"))?,
        })
    }
}

fn action_unpack_raw(args: ActionUnpackRaw, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.utoc, config)?;

    let output = args.output;
    let chunks_dir = output.join("chunks");
    let manifest_path = output.join("manifest.json");

    fs::create_dir(&output)?;
    fs::create_dir(&chunks_dir)?;

    let mut manifest = raw::RawIoManifest {
        chunk_paths: Default::default(),
        version: iostore.container_file_version().unwrap(),
        mount_point: "../../../".to_string(),
    };

    for chunk in iostore.chunks() {
        let data = chunk.read()?;
        fs::write(chunks_dir.join(hex::encode(chunk.id().get_raw())), data)?;
        if let Some(path) = chunk.path() {
            manifest.chunk_paths.insert(chunk.id().get_raw().into(), path);
        }
    }

    serde_json::to_writer_pretty(BufWriter::new(fs::File::create(manifest_path)?), &manifest)?;

    println!("unpacked {} chunks to {}", iostore.chunks().count(), output.to_string_lossy());

    Ok(())
}

fn action_pack_raw(args: ActionPackRaw, _config: Arc<Config>) -> Result<()> {
    let manifest: raw::RawIoManifest = serde_json::from_reader(BufReader::new(fs::File::open(args.input.join("manifest.json"))?))?;

    let mut writer = IoStoreWriter::new(args.utoc, manifest.version, None, manifest.mount_point.into())?;
    for entry in args.input.join("chunks").read_dir()? {
        let entry = entry?;
        let chunk_id = FIoChunkIdRaw::from_str(entry.file_name().to_string_lossy().as_ref())?;
        let path = manifest.chunk_paths.get(&chunk_id.into()).map(UEPath::new);
        let data = fs::read(entry.path())?;
        writer.write_chunk_raw(chunk_id, path, &data)?;
    }
    writer.finalize()?;
    Ok(())
}

fn action_to_legacy(args: ActionToLegacy, config: Arc<Config>) -> Result<()> {
    let log = Log::new_stdout(args.verbose, args.debug);
    if args.dry_run {
        action_to_legacy_inner(args, config, &NullFileWriter, &log)?;
    } else if args.output.extension() == Some(std::ffi::OsStr::new("pak")) {
        let mut file = BufWriter::new(fs::File::create(&args.output)?);
        let mut pak = repak::PakBuilder::new().compression([repak::Compression::Oodle]).writer(
            &mut file,
            repak::Version::V11, // TODO V11 is compatible with most IO store versions but will need to be changed for <= 4.26
            "../../../".to_string(),
            None,
        );

        // some stack space to store action result
        let mut result = None;
        let result_ref = &mut result;
        rayon::in_place_scope(|scope| -> Result<()> {
            let (tx, rx) = std::sync::mpsc::sync_channel(0);

            let writer = ParallelPakWriter { entry_builder: pak.entry_builder(), tx };

            scope.spawn(move |_| {
                *result_ref = Some(action_to_legacy_inner(args, config, &writer, &log));
            });

            for (path, entry) in rx {
                pak.write_entry(path, entry)?;
            }
            Ok(())
        })?;
        result.unwrap()?; // unwrap action result and return error if occured

        pak.write_index()?;
    } else {
        let file_writer = FSFileWriter::new(&args.output);
        action_to_legacy_inner(args, config, &file_writer, &log)?;
    }

    Ok(())
}

fn action_to_legacy_inner(args: ActionToLegacy, config: Arc<Config>, file_writer: &dyn FileWriterTrait, log: &Log) -> Result<()> {
    let iostore = iostore::open(&args.input, config.clone())?;
    if !args.no_assets {
        action_to_legacy_assets(&args, file_writer, &*iostore, log)?;
    }
    if !args.no_shaders {
        action_to_legacy_shaders(&args, file_writer, &*iostore, log)?;
    }
    if !args.no_script_objects && iostore.container_file_version().is_some() && iostore.container_file_version().unwrap() > EIoStoreTocVersion::PerfectHash {
        let script_objects = iostore.load_script_objects()?;
        let mut script_objects_buffer: Vec<u8> = Vec::new();
        script_objects.serialize_new(&mut Cursor::new(&mut script_objects_buffer))?;
        file_writer.write_file(String::from("scriptobjects.bin"), false, script_objects_buffer)?;
    }
    Ok(())
}

fn progress_style() -> indicatif::ProgressStyle {
    indicatif::ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {wide_msg}").unwrap().progress_chars("##-")
}

fn action_to_legacy_assets(args: &ActionToLegacy, file_writer: &dyn FileWriterTrait, iostore: &dyn IoStoreTrait, log: &Log) -> Result<()> {
    let mut packages_to_extract = vec![];
    for package_info in iostore.packages() {
        let chunk_id = FIoChunkId::from_package_id(package_info.id(), 0, EIoChunkType::ExportBundleData);
        let package_path = iostore.chunk_path(chunk_id).with_context(|| format!("{:?} has no path name entry. Cannot extract", package_info.id()))?;

        if !args.filter.is_empty() && !args.filter.iter().any(|f| package_path.contains(f)) {
            continue;
        }

        packages_to_extract.push((package_info, package_path));
    }

    // Construct Verse Cell Store and populate it with intrinsics, as well as command line overrides
    let script_cell_store = build_verse_cell_store(&args.script_cell);

    let package_file_version: Option<FPackageFileVersion> = args.version.map(|v| v.package_file_version());
    let package_context = FZenPackageContext::create(iostore, package_file_version, log, Some(script_cell_store.clone()));

    let count = packages_to_extract.len();
    let failed_count = AtomicUsize::new(0);
    let progress = Some(indicatif::ProgressBar::new(count as u64).with_style(progress_style()));
    log.set_progress(progress.as_ref());
    let prog_ref = progress.as_ref();

    let process = |(package_info, package_path): &(PackageInfo, String)| -> Result<()> {
        verbose!(log, "{package_path}");

        // TODO make configurable
        let path = package_path.strip_prefix("../../../").with_context(|| format!("failed to strip mount prefix from {package_path}"))?;

        prog_ref.inspect(|p| p.set_message(path.to_string()));

        let res = asset_conversion::build_legacy(&package_context, package_info.id(), UEPath::new(&path), file_writer).with_context(|| format!("Failed to convert {}", package_path.clone()));
        if let Err(err) = res {
            info!(log, "{err:#}");
            failed_count.fetch_add(1, Ordering::SeqCst);
        }
        prog_ref.inspect(|p| p.inc(1));
        Ok(())
    };

    if args.no_parallel {
        packages_to_extract.iter().try_for_each(process)?;
    } else {
        packages_to_extract.par_iter().try_for_each(process)?;
    }

    prog_ref.inspect(|p| p.finish_with_message(""));
    log.set_progress(None);

    let failed_count = failed_count.load(Ordering::SeqCst);
    info!(log, "Extracted {} ({failed_count} failed) legacy assets to {:?}", count - failed_count, args.output);

    Ok(())
}

fn action_to_legacy_shaders(args: &ActionToLegacy, file_writer: &dyn FileWriterTrait, iostore: &dyn IoStoreTrait, log: &Log) -> Result<()> {
    let compress_shaders = !args.no_compres_shaders;
    let mut libraries_extracted = 0;
    for chunk_info in iostore.chunks().filter(|x| x.id().get_chunk_type() == EIoChunkType::ShaderCodeLibrary) {
        let shader_library_path = chunk_info.path().with_context(|| format!("Failed to retrieve pathname for shader library chunk {:?}", chunk_info.id()))?;

        if !args.filter.is_empty() && !args.filter.iter().any(|f| shader_library_path.contains(f)) {
            continue;
        }
        verbose!(log, "Extracting Shader Library: {shader_library_path}");
        // TODO make configurable
        let path = shader_library_path.strip_prefix("../../../").with_context(|| format!("failed to strip mount prefix from {shader_library_path}"))?;

        let shader_asset_info_path = get_shader_asset_info_filename_from_library_filename(path)?;
        let (shader_library_buffer, shader_asset_info_buffer) = rebuild_shader_library_from_io_store(iostore, chunk_info.id(), log, compress_shaders)?;
        file_writer.write_file(path.to_string(), false, shader_library_buffer)?;
        file_writer.write_file(shader_asset_info_path, compress_shaders, shader_asset_info_buffer)?;
        libraries_extracted += 1;
    }

    info!(log, "Extracted {} shader code libraries to {:?}", libraries_extracted, args.output);

    Ok(())
}

fn action_to_zen(args: ActionToZen, config: Arc<Config>) -> Result<()> {
    let mount_point = UEPath::new("../../../");

    let input: Box<dyn FileReaderTrait> = if args.input.is_dir() { Box::new(FSFileReader::new(args.input)) } else { Box::new(PakFileReader::new(args.input)?) };

    let container_header_version = config.container_header_version_override.unwrap_or(args.version.container_header_version());

    let toc_version = config.toc_version_override.unwrap_or(args.version.toc_version());

    let mut writer = IoStoreWriter::new(&args.output, toc_version, Some(container_header_version), mount_point.into())?;

    let log = Log::new_stdout(args.verbose, args.debug);
    let mut asset_paths = vec![];
    let mut shader_lib_paths = vec![];
    let mut script_objects: Option<Arc<ZenScriptObjects>> = None;

    let check_path = |path: &UEPath| {
        if args.filter.is_empty() { true } else { args.filter.iter().any(|f| path.as_str().contains(f)) }
    };

    let files = input.list_files()?;
    let files_set: HashSet<&UEPathBuf> = HashSet::from_iter(files.iter());

    for path in &files {
        let ue_path = UEPath::new(&path);
        let ext = ue_path.extension();
        let is_asset = [Some("uasset"), Some("umap")].contains(&ext);
        if is_asset && check_path(path) {
            let uexp = ue_path.with_extension("uexp");
            if files_set.contains(&uexp) {
                asset_paths.push(path);
            } else {
                info!(&log, "Skipping {path} because it does not have a split exports file. Are you sure the package is cooked?");
            }
        }
        let is_shader_lib = Some("ushaderbytecode") == ext;
        if is_shader_lib && check_path(path) {
            shader_lib_paths.push(path);
        }
        // If folder we are given contains copy of script objects, parse them and use them for VNI support and import checking
        if toc_version > EIoStoreTocVersion::PerfectHash && path.file_name() == Some("scriptobjects.bin") {
            let script_object_buffer = input.read(path).with_context(|| format!("Failed to read script objects file: {}", path))?;
            script_objects = Some(Arc::new(ZenScriptObjects::deserialize_new(&mut Cursor::new(script_object_buffer))?));
        }
    }

    // Convert shader libraries first, since the data contained in their asset metadata is needed to build the package store entries
    let mut package_name_to_referenced_shader_maps: HashMap<String, Vec<FSHAHash>> = HashMap::new();
    for path in shader_lib_paths {
        info!(&log, "converting shader library {path}");
        let shader_library_buffer = input.read(path)?;
        let path = UEPath::new(&path);
        let asset_metadata_filename = UEPathBuf::from(get_shader_asset_info_filename_from_library_filename(path.file_name().unwrap())?);
        let asset_metadata_path = path.parent().map(|x| x.join(&asset_metadata_filename)).unwrap_or(asset_metadata_filename);

        // Read asset metadata and store it into the global map to be picked up by zen packages later
        if let Some(shader_asset_info_buffer) = input.read_opt(&asset_metadata_path)? {
            shader_library::read_shader_asset_info(&shader_asset_info_buffer, &mut package_name_to_referenced_shader_maps)?;
        }

        // Convert shader library to the container shader chunks
        shader_library::write_io_store_library(&mut writer, &shader_library_buffer, &mount_point.join(path), &log)?;
    }

    // Construct Verse Cell Store and populate it with intrinsics, as well as command line overrides
    let script_cell_store = build_verse_cell_store(&args.script_cell);

    let progress = Some(indicatif::ProgressBar::new(asset_paths.len() as u64).with_style(progress_style()));
    log.set_progress(progress.as_ref());
    let prog_ref = progress.as_ref();

    log.set_progress(prog_ref);

    // Convert assets now
    let container_header_version = writer.container_header_version();
    // Decide whenever we need all packages to be in memory at the same time to perform the fixup or not
    let needs_asset_import_fixup = container_header_version <= EIoContainerHeaderVersion::Initial;

    let process_assets = |tx: std::sync::mpsc::SyncSender<ConvertedZenAssetBundle>| -> Result<()> {
        let process = |path: &&UEPathBuf| -> Result<()> {
            verbose!(&log, "converting asset {path}");

            prog_ref.inspect(|p| p.set_message(path.to_string()));

            let bundle = FSerializedAssetBundle {
                asset_file_buffer: input.read(path)?,
                exports_file_buffer: input.read(&path.with_extension("uexp"))?,
                bulk_data_buffer: input.read_opt(&path.with_extension("ubulk"))?,
                optional_bulk_data_buffer: input.read_opt(&path.with_extension("uptnl"))?,
                memory_mapped_bulk_data_buffer: input.read_opt(&path.with_extension("m.ubulk"))?,
            };

            let converted = zen_asset_conversion::build_zen_asset(
                bundle,
                &package_name_to_referenced_shader_maps,
                &mount_point.join(path),
                Some(args.version.package_file_version()),
                container_header_version,
                needs_asset_import_fixup,
                script_objects.clone(),
                Some(script_cell_store.clone()),
                &log,
            )?;

            tx.send(converted)?;

            prog_ref.inspect(|p| p.inc(1));
            Ok(())
        };

        if args.no_parallel { asset_paths.iter().try_for_each(process) } else { asset_paths.par_iter().try_for_each(process) }
    };
    let mut result = None;
    let result_ref = &mut result;
    rayon::in_place_scope(|scope| -> Result<()> {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);

        scope.spawn(|_| {
            *result_ref = Some(process_assets(tx));
        });

        if needs_asset_import_fixup {
            let mut converted_lookup: HashMap<FPackageId, Arc<RwLock<ConvertedZenAssetBundle>>> = HashMap::new();
            let mut all_converted: Vec<Arc<RwLock<ConvertedZenAssetBundle>>> = Vec::new();
            let mut total_package_data_size: usize = 0;

            // Collect all assets into the lookup map first, and also into the processing list
            for mut converted in rx {
                // Write and release bulk data immediately, we do not have enough RAM to keep all the bulk data for all the packages in memory at the same time
                converted.write_and_release_bulk_data(&mut writer)?;
                total_package_data_size += converted.package_data_size();

                // Add the package data and the metadata necessary for the import fixup into the list
                let converted_arc = Arc::new(RwLock::new(converted));
                converted_lookup.insert(converted_arc.read().unwrap().package_id, converted_arc.clone());
                all_converted.push(converted_arc);
            }

            info!(log, "Applying import fix-ups to the converted assets. Package data in memory: {}MB", total_package_data_size / 1024 / 1024);
            prog_ref.inspect(|x| x.set_position(0));

            // Process fixups on all the assets in their original processing order
            for converted in &all_converted {
                converted.write().unwrap().fixup_legacy_external_arcs(&converted_lookup, &log)?;
                prog_ref.inspect(|x| x.inc(1));
            }
            info!(log, "Writing converted assets");
            prog_ref.inspect(|x| x.set_position(0));

            // Write all the package data for each asset once fixups are complete
            for converted in &all_converted {
                converted.write().unwrap().write_package_data(&mut writer)?;
                prog_ref.inspect(|x| x.inc(1));
            }
        } else {
            // Write the assets immediately otherwise as they are processed
            for mut converted in rx {
                converted.write(&mut writer)?;
            }
        }
        Ok(())
    })?;
    result.unwrap()?;

    prog_ref.inspect(|p| p.finish_with_message(""));
    log.set_progress(None);

    writer.finalize()?;

    // create empty pak file if one does not already exist (necessary for game to detect and load container)
    let pak_path = Path::new(&args.output).with_extension("pak");
    if !pak_path.exists() {
        repak::PakBuilder::new().writer(&mut BufWriter::new(fs::File::create(pak_path)?), repak::Version::V11, mount_point.to_string(), None).write_index()?;
    }

    Ok(())
}

fn action_get(args: ActionGet, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.input, config)?;
    let data = iostore.read_raw(args.chunk_id)?;

    let mut output: Box<dyn Write> = if let Some(output) = args.output {
        if output == OsStr::new("-") { Box::new(std::io::stdout()) } else { Box::new(BufWriter::new(fs::File::create(output)?)) }
    } else {
        Box::new(std::io::stdout())
    };

    output.write_all(&data)?;
    Ok(())
}

fn action_dump_test(args: ActionDumpTest, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.input, config)?;

    let chunk_id = FIoChunkId::from_package_id(args.package_id, 0, EIoChunkType::ExportBundleData);
    let game_path = iostore.chunk_path(chunk_id).context("no path found for package")?;
    let name = UEPath::new(&game_path).file_name().unwrap();
    let path = args.output_dir.join(name);
    let data = iostore.read(chunk_id)?;
    fs::write(args.output_dir.join(name), &data)?;

    let store_entry = iostore.package_store_entry(args.package_id).unwrap();

    let metadata = PackageTestMetadata {
        toc_version: iostore.container_file_version().unwrap(),
        container_header_version: iostore.container_header_version().unwrap(),
        package_file_version: None,
        store_entry: Some(store_entry),
    };

    fs::write(path.with_extension("metadata.json"), serde_json::to_vec_pretty(&metadata)?)?;

    {
        let mut stream = ser_hex::TraceStream::new(path.with_extension("trace.json"), Cursor::new(data));

        let header = FZenPackageHeader::deserialize(&mut stream, metadata.store_entry, metadata.toc_version, metadata.container_header_version, metadata.package_file_version)?;

        fs::write(path.with_extension("header.txt"), format!("{header:#?}"))?;
    }

    let chunk_id = FIoChunkId::from_package_id(args.package_id, 0, EIoChunkType::BulkData);
    if iostore.has_chunk_id(chunk_id) {
        let data = iostore.read(chunk_id)?;
        fs::write(path.with_extension("ubulk"), data)?;
    }

    let chunk_id = FIoChunkId::from_package_id(args.package_id, 0, EIoChunkType::OptionalBulkData);
    if iostore.has_chunk_id(chunk_id) {
        let data = iostore.read(chunk_id)?;
        fs::write(path.with_extension("uptnl"), data)?;
    }

    let chunk_id = FIoChunkId::from_package_id(args.package_id, 0, EIoChunkType::MemoryMappedBulkData);
    if iostore.has_chunk_id(chunk_id) {
        let data = iostore.read(chunk_id)?;
        fs::write(path.with_extension("m.ubulk"), data)?;
    }

    Ok(())
}

fn action_gen_script_objects(args: ActionGenScriptObjects, _config: Arc<Config>) -> Result<()> {
    let dump: ue_reflection::ReflectionData = serde_json::from_reader(BufReader::new(fs::File::open(&args.input)?))?;

    // map CDO => Class
    let mut cdos = HashMap::<&str, &str>::new();

    let mut names = FNameMap::create(EMappedNameType::Global);
    let mut script_objects = vec![];

    for (path, object) in &dump.objects {
        if path.starts_with("/Script/")
            && let ue_reflection::ObjectType::Class(class) = &object
            && let Some(cdo) = &class.class_default_object
        {
            cdos.insert(cdo, path);
        }
    }

    for (path, object) in &dump.objects {
        if path.starts_with("/Script/") {
            if let ue_reflection::ObjectType::Class(class) = &object
                && let Some(cdo) = &class.class_default_object
            {
                cdos.insert(cdo, path);
            }
            let mut components = path.rsplit(['/', '.', ':']);
            let name = components.next().unwrap();
            let count = components.count();
            let name = if count == 2 {
                // special case for /Script/ package objects
                path
            } else {
                name
            };

            let outer_index = object.get_object().outer.as_ref().map_or(FPackageObjectIndex::create_null(), |outer| FPackageObjectIndex::create_script_import(outer));
            let cdo_class_index = cdos.get(path.as_str()).map_or(FPackageObjectIndex::create_null(), |outer| FPackageObjectIndex::create_script_import(outer));
            script_objects.push(FScriptObjectEntry {
                object_name: names.store(name),
                global_index: FPackageObjectIndex::create_script_import(path),
                outer_index,
                cdo_class_index,
            });
        }
    }

    let mut writer = IoStoreWriter::new(&args.output, args.version.toc_version(), None, UEPathBuf::new())?;

    let use_new_format = args.version.toc_version() > EIoStoreTocVersion::PerfectHash;

    if use_new_format {
        let buf_script_objects = {
            let mut buf = vec![];
            names.serialize(&mut buf)?;
            WriteExt::ser(&mut buf, &script_objects)?;
            buf
        };

        writer.write_chunk(FIoChunkId::create(0, 0, EIoChunkType::ScriptObjects), None, &buf_script_objects)?;
    } else {
        let (buf_names, buf_name_hashes) = write_name_batch_parts(&names.copy_raw_names())?;
        let buf_script_objects = {
            let mut buf = vec![];
            WriteExt::ser(&mut buf, &script_objects)?;
            buf
        };

        writer.write_chunk(FIoChunkId::create(0, 0, EIoChunkType::LoaderGlobalNames), None, &buf_names)?;
        writer.write_chunk(FIoChunkId::create(0, 0, EIoChunkType::LoaderGlobalNameHashes), None, &buf_name_hashes)?;
        writer.write_chunk(FIoChunkId::create(0, 0, EIoChunkType::LoaderInitialLoadMeta), None, &buf_script_objects)?;
    }

    writer.finalize()?;

    println!("Generated script objects container with {} objects using {} format", script_objects.len(), if use_new_format { "new" } else { "old" });

    Ok(())
}

fn action_print_script_objects(args: ActionPrintScriptObjects, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.input, config)?;
    let script_objects = iostore.load_script_objects()?;
    script_objects.print();
    Ok(())
}
