mod asset_conversion;
mod compact_binary;
mod compression;
mod container_header;
mod file_pool;
mod iostore;
mod iostore_writer;
mod legacy_asset;
mod logging;
mod manifest;
mod name_map;
mod script_objects;
mod ser;
mod shader_library;
mod version;
mod version_heuristics;
mod zen;
mod zen_asset_conversion;

use anyhow::{bail, Context, Result};
use bitflags::bitflags;
use clap::Parser;
use compression::{decompress, CompressionMethod};
use fs_err as fs;
use iostore::IoStoreTrait;
use iostore_writer::IoStoreWriter;
use logging::Log;
use logging::*;
use rayon::prelude::*;
use ser::*;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::{Debug, Display, Formatter};
use std::io::BufWriter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::HashMap,
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};
use strum::{AsRefStr, FromRepr};
use tracing::instrument;
use version::EngineVersion;

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
struct ActionExtractLegacy {
    #[arg(index = 1)]
    utoc: PathBuf,
    #[arg(index = 2)]
    output: PathBuf,

    /// Skip extraction of assets
    #[arg(long)]
    no_assets: bool,
    /// Skip extraction of shader libraries
    #[arg(long)]
    no_shaders: bool,
    /// Skip compression of extracted shader libraries
    #[arg(long)]
    no_compres_shaders: bool,
    /// Do not output any file (dry run). Useful for testing conversion
    #[arg(short, long)]
    dry_run: bool,

    /// Engine version override
    #[arg(long)]
    version: Option<EngineVersion>,
    #[arg(short, long, default_value = "false")]
    verbose: bool,
    #[arg(long, default_value = "false")]
    debug: bool,
    #[arg(short, long)]
    filter: Option<String>,
}

#[derive(Parser, Debug)]
struct ActionGet {
    #[arg(index = 1)]
    utoc: PathBuf,
    #[arg(index = 2)]
    chunk_id: FIoChunkId,
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

    /// Extracts legacy assets from .utoc
    ExtractLegacy(ActionExtractLegacy),
    /// Get chunk by index and write to stdout
    Get(ActionGet),
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    aes: Option<String>,
    #[command(subcommand)]
    action: Action,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut config = Config::default();
    if let Some(aes) = args.aes {
        config
            .aes_keys
            .insert(FGuid::default(), AesKey::from_str(&aes)?);
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

        Action::ExtractLegacy(action) => action_extract_legacy(action, config),
        Action::Get(action) => action_get(action, config),
    }
}

fn action_manifest(args: ActionManifest, config: Arc<Config>) -> Result<()> {
    let toc: Toc = BufReader::new(fs::File::open(&args.utoc)?).de_ctx(config)?;
    let ucas = &args.utoc.with_extension("ucas");

    let entries = Arc::new(Mutex::new(vec![]));

    toc.file_map.keys().par_bridge().try_for_each_init(
        || {
            (
                entries.clone(),
                BufReader::new(fs::File::open(ucas).unwrap()),
            )
        },
        |(entries, ucas), file_name| -> Result<()> {
            let chunk_info = toc.get_chunk_info(file_name);

            if chunk_info.id.get_chunk_type() == EIoChunkType::ExportBundleData {
                let data = toc.read(ucas, toc.file_map[file_name])?;

                // TODO @trumank fix this
                let package_name = get_package_name(&data, EIoContainerHeaderVersion::NoExportInfo)
                    .with_context(|| file_name.to_string())?;

                let mut entry = manifest::Op {
                    packagestoreentry: manifest::PackageStoreEntry {
                        packagename: package_name,
                    },
                    packagedata: vec![manifest::ChunkData {
                        id: chunk_info.id,
                        filename: file_name.to_string(),
                    }],
                    bulkdata: vec![],
                };

                if let Some((path, _ext)) = file_name.rsplit_once(".") {
                    let bulk_path = format!("{path}.ubulk");
                    if let Some(&bulk) = toc.file_map_lower.get(&bulk_path.to_ascii_lowercase()) {
                        entry.bulkdata.push(manifest::ChunkData {
                            id: toc.chunks[bulk as usize],
                            filename: bulk_path,
                        });
                    }
                }

                entries.lock().unwrap().push(entry);
            }
            Ok(())
        },
    )?;

    let mut entries = Arc::into_inner(entries).unwrap().into_inner().unwrap();
    //entries.sort_by_key(|op| op.packagedata.first().map(|c| c.filename.clone()));
    entries.sort_by(|a, b| {
        a.packagestoreentry
            .packagename
            .cmp(&b.packagestoreentry.packagename)
    });

    let manifest = manifest::PackageStoreManifest {
        oplog: manifest::OpLog { entries },
    };

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

    for chunk in iostore.chunks() {
        let id = chunk.id();
        let chunk_type = id.get_chunk_type();
        //let package_store_entry = if chunk_type == EIoChunkType::ExportBundleData {
        //    let package_id = FPackageId(u64::from_le_bytes(id.id[0..8].try_into().unwrap()));
        //    let entry = chunk.container().package_store_entry(package_id).unwrap();
        //    format!("{entry:?}")
        //} else {
        //    "-".to_string()
        //};

        println!(
            "{:30}  {}  {:20}  {}",
            chunk.container().container_name(),
            hex::encode(id),
            chunk_type.as_ref(),
            chunk.path().as_deref().unwrap_or("-"),
            //package_store_entry,
        );
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
            let chunk_id = toc.chunks[i];
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

            if meta.chunk_hash.data[..20] != hash.as_bytes()[..20] {
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

    println!(
        "unpacked {} files to {}",
        toc.file_map.len(),
        output.to_string_lossy()
    );

    Ok(())
}

mod raw {
    use std::collections::HashMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::{EIoStoreTocVersion, FIoChunkId};

    #[derive(Serialize, Deserialize)]
    pub(crate) struct RawIoManifest {
        pub(crate) chunk_paths: HashMap<ChunkId, String>,
        pub(crate) version: EIoStoreTocVersion,
        pub(crate) mount_point: String,
    }
    #[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct ChunkId(
        #[serde(serialize_with = "to_hex", deserialize_with = "from_hex")] pub(crate) FIoChunkId,
    );
    impl From<FIoChunkId> for ChunkId {
        fn from(value: FIoChunkId) -> Self {
            Self(value)
        }
    }

    fn to_hex<S>(chunk_id: &FIoChunkId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(chunk_id.id))
    }
    fn from_hex<'de, D>(deserializer: D) -> Result<FIoChunkId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let v = hex::decode(s).map_err(serde::de::Error::custom)?;
        Ok(FIoChunkId {
            id: v.try_into().map_err(|v: Vec<u8>| {
                serde::de::Error::invalid_length(v.len(), &"a 12 byte hex string")
            })?,
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
        fs::write(chunks_dir.join(hex::encode(chunk.id())), data)?;
        if let Some(path) = chunk.path() {
            manifest.chunk_paths.insert(chunk.id().into(), path);
        }
    }

    serde_json::to_writer_pretty(BufWriter::new(fs::File::create(manifest_path)?), &manifest)?;

    println!(
        "unpacked {} chunks to {}",
        iostore.chunks().count(),
        output.to_string_lossy()
    );

    Ok(())
}

fn action_pack_raw(args: ActionPackRaw, _config: Arc<Config>) -> Result<()> {
    let manifest: raw::RawIoManifest = serde_json::from_reader(BufReader::new(fs::File::open(
        args.input.join("manifest.json"),
    )?))?;

    let mut writer = IoStoreWriter::new(args.utoc, manifest.version, manifest.mount_point)?;
    for entry in args.input.join("chunks").read_dir()? {
        let entry = entry?;
        let chunk_id = FIoChunkId::from_str(entry.file_name().to_string_lossy().as_ref())?;
        let path = manifest
            .chunk_paths
            .get(&chunk_id.into())
            .map(String::as_ref);
        let data = fs::read(entry.path())?;
        writer.write_chunk(chunk_id, path, &data)?;
    }
    writer.finalize()?;
    Ok(())
}

trait FileWriterTrait: Send + Sync {
    fn write_file(&self, path: String, allow_compress: bool, data: Vec<u8>) -> Result<()>;
}
struct FSFileWriter {
    dir: PathBuf,
}
impl FSFileWriter {
    fn new<P: Into<PathBuf>>(dir: P) -> Self {
        Self { dir: dir.into() }
    }
}
impl FileWriterTrait for FSFileWriter {
    fn write_file(&self, path: String, _allow_compress: bool, data: Vec<u8>) -> Result<()> {
        let path = self.dir.join(path);
        let dir = path.parent().unwrap();
        fs::create_dir_all(dir)?;
        Ok(fs::write(path, &data)?)
    }
}
struct PakFileWriter<'a> {
    inner: &'a mut repak::ParallelPakWriter,
}
impl FileWriterTrait for PakFileWriter<'_> {
    fn write_file(&self, path: String, allow_compress: bool, data: Vec<u8>) -> Result<()> {
        Ok(self.inner.write_file(path, allow_compress, data)?)
    }
}
struct NullFileWriter;
impl FileWriterTrait for NullFileWriter {
    fn write_file(&self, _path: String, _allow_compress: bool, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }
}

fn action_extract_legacy(args: ActionExtractLegacy, config: Arc<Config>) -> Result<()> {
    let log = Log::new(args.verbose, args.debug);
    if args.dry_run {
        action_extract_legacy_inner(args, config, &NullFileWriter, &log)?;
    } else if args.output.extension() == Some(std::ffi::OsStr::new("pak")) {
        let mut file = BufWriter::new(fs::File::create(&args.output)?);
        let mut pak = repak::PakBuilder::new()
            .compression([repak::Compression::LZ4])
            .writer(
                &mut file,
                repak::Version::V11, // TODO V11 is compatible with most IO store versions but will need to be changed for <= 4.26
                "../../../".to_string(),
                None,
            );

        pak.parallel(|writer| -> Result<()> {
            let file_writer = PakFileWriter { inner: writer };
            action_extract_legacy_inner(args, config, &file_writer, &log)?;
            Ok(())
        })?;

        pak.write_index()?;
    } else {
        let file_writer = FSFileWriter::new(&args.output);
        action_extract_legacy_inner(args, config, &file_writer, &log)?;
    }

    Ok(())
}

fn action_extract_legacy_inner(
    args: ActionExtractLegacy,
    config: Arc<Config>,
    file_writer: &dyn FileWriterTrait,
    log: &Log,
) -> Result<()> {
    let iostore = iostore::open(&args.utoc, config.clone())?;
    if !args.no_assets {
        action_extract_legacy_assets(&args, file_writer, &*iostore, log)?;
    }
    if !args.no_shaders {
        action_extract_legacy_shaders(&args, file_writer, &*iostore, log)?;
    }
    Ok(())
}

fn action_extract_legacy_assets(
    args: &ActionExtractLegacy,
    file_writer: &dyn FileWriterTrait,
    iostore: &dyn IoStoreTrait,
    log: &Log,
) -> Result<()> {
    let mut packages_to_extract = vec![];
    for package_info in iostore.packages() {
        let chunk_id =
            FIoChunkId::from_package_id(package_info.id(), 0, EIoChunkType::ExportBundleData);
        let package_path = package_info
            .container()
            .chunk_path(chunk_id)
            .with_context(|| {
                format!(
                    "{:?} has no path name entry. Cannot extract",
                    package_info.id()
                )
            })?;

        if let Some(filter) = &args.filter {
            if !package_path.contains(filter) {
                continue;
            }
        }

        packages_to_extract.push((package_info, package_path));
    }

    let package_file_version: Option<FPackageFileVersion> = args
        .version
        .map(|v| FPackageFileVersion::create_ue5(EngineVersion::object_ue5_version(v)));
    let package_context = FZenPackageContext::create(iostore, package_file_version, log);

    let count = packages_to_extract.len();
    let failed_count = AtomicUsize::new(0);
    let progress_style = indicatif::ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {wide_msg}",
    )
    .unwrap()
    .progress_chars("##-");
    let progress = Some(indicatif::ProgressBar::new(count as u64).with_style(progress_style));
    log.set_progress(progress.as_ref());
    let prog_ref = progress.as_ref();

    packages_to_extract
        .par_iter()
        .try_for_each(|(package_info, package_path)| -> Result<()> {
            verbose!(log, "{package_path}");

            // TODO make configurable
            let path = package_path
                .strip_prefix("../../../")
                .with_context(|| format!("failed to strip mount prefix from {package_path:?}"))?;

            prog_ref.inspect(|p| p.set_message(path.to_string()));

            let res = asset_conversion::build_legacy(
                &package_context,
                package_info.id(),
                &UEPath::new(&path),
                file_writer,
            );
            if let Err(err) = res {
                log!(log, "{err}");
                failed_count.fetch_add(1, Ordering::SeqCst);
            }
            prog_ref.inspect(|p| p.inc(1));
            Ok(())
        })?;
    prog_ref.inspect(|p| p.finish_with_message(""));
    log.set_progress(None);

    let failed_count = failed_count.load(Ordering::SeqCst);
    log!(
        log,
        "Extracted {} ({failed_count} failed) legacy assets to {:?}",
        count - failed_count,
        args.output
    );

    Ok(())
}

fn action_extract_legacy_shaders(
    args: &ActionExtractLegacy,
    file_writer: &dyn FileWriterTrait,
    iostore: &dyn IoStoreTrait,
    log: &Log,
) -> Result<()> {
    let compress_shaders = !args.no_compres_shaders;
    let mut libraries_extracted = 0;
    for chunk_info in iostore
        .chunks()
        .filter(|x| x.id().get_chunk_type() == EIoChunkType::ShaderCodeLibrary)
    {
        let shader_library_path = chunk_info.path().with_context(|| {
            format!(
                "Failed to retrieve pathname for shader library chunk {:?}",
                chunk_info.id()
            )
        })?;

        if let Some(filter) = &args.filter {
            if !shader_library_path.contains(filter) {
                continue;
            }
        }
        verbose!(log, "Extracting Shader Library: {shader_library_path}");
        // TODO make configurable
        let path = shader_library_path
            .strip_prefix("../../../")
            .with_context(|| {
                format!("failed to strip mount prefix from {shader_library_path:?}")
            })?;

        let shader_library_buffer = rebuild_shader_library_from_io_store(
            chunk_info.container(),
            chunk_info.id(),
            log,
            compress_shaders,
        )?;
        file_writer.write_file(path.to_string(), false, shader_library_buffer)?;
        libraries_extracted += 1;
    }

    log!(
        log,
        "Extracted {} shader code libraries to {:?}",
        libraries_extracted,
        args.output
    );

    Ok(())
}

fn action_get(args: ActionGet, config: Arc<Config>) -> Result<()> {
    let iostore = iostore::open(args.utoc, config)?;
    let data = iostore.read(args.chunk_id)?;
    std::io::stdout().write_all(&data)?;
    Ok(())
}

#[derive(Default)]
struct Config {
    aes_keys: HashMap<FGuid, AesKey>,
}

#[derive(Debug, Clone)]
struct AesKey(aes::Aes256);
impl std::str::FromStr for AesKey {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use aes::cipher::KeyInit;
        use base64::{engine::general_purpose, Engine as _};
        let try_parse = |bytes: Vec<_>| aes::Aes256::new_from_slice(&bytes).ok().map(AesKey);
        hex::decode(s.strip_prefix("0x").unwrap_or(s))
            .ok()
            .and_then(try_parse)
            .or_else(|| {
                general_purpose::STANDARD_NO_PAD
                    .decode(s.trim_end_matches('='))
                    .ok()
                    .and_then(try_parse)
            })
            .context("invalid AES key")
    }
}

impl Readable for FIoStoreTocHeader {
    #[instrument(skip_all, name = "FIoStoreTocHeader")]
    fn de<R: Read>(stream: &mut R) -> Result<Self> {
        let res = FIoStoreTocHeader {
            toc_magic: stream.de()?,
            version: stream.de()?,
            reserved0: stream.de()?,
            reserved1: stream.de()?,
            toc_header_size: stream.de()?,
            toc_entry_count: stream.de()?,
            toc_compressed_block_entry_count: stream.de()?,
            toc_compressed_block_entry_size: stream.de()?,
            compression_method_name_count: stream.de()?,
            compression_method_name_length: stream.de()?,
            compression_block_size: stream.de()?,
            directory_index_size: stream.de()?,
            partition_count: stream.de()?,
            container_id: stream.de()?,
            encryption_key_guid: stream.de()?,
            container_flags: stream.de()?,
            reserved3: stream.de()?,
            reserved4: stream.de()?,
            toc_chunk_perfect_hash_seeds_count: stream.de()?,
            partition_size: stream.de()?,
            toc_chunks_without_perfect_hash_count: stream.de()?,
            reserved7: stream.de()?,
            reserved8: stream.de()?,
        };
        if res.toc_magic != Self::MAGIC {
            bail!("unrecognized TOC magic, this is a .utoc file?")
        }
        assert_eq!(res.toc_header_size, 0x90);
        Ok(res)
    }
}
impl Writeable for FIoStoreTocHeader {
    #[instrument(skip_all, name = "FIoStoreTocHeader")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.toc_magic)?;
        s.ser(&self.version)?;
        s.ser(&self.reserved0)?;
        s.ser(&self.reserved1)?;
        s.ser(&self.toc_header_size)?;
        s.ser(&self.toc_entry_count)?;
        s.ser(&self.toc_compressed_block_entry_count)?;
        s.ser(&self.toc_compressed_block_entry_size)?;
        s.ser(&self.compression_method_name_count)?;
        s.ser(&self.compression_method_name_length)?;
        s.ser(&self.compression_block_size)?;
        s.ser(&self.directory_index_size)?;
        s.ser(&self.partition_count)?;
        s.ser(&self.container_id)?;
        s.ser(&self.encryption_key_guid)?;
        s.ser(&self.container_flags)?;
        s.ser(&self.reserved3)?;
        s.ser(&self.reserved4)?;
        s.ser(&self.toc_chunk_perfect_hash_seeds_count)?;
        s.ser(&self.partition_size)?;
        s.ser(&self.toc_chunks_without_perfect_hash_count)?;
        s.ser(&self.reserved7)?;
        s.ser(&self.reserved8)?;
        Ok(())
    }
}

#[instrument(skip_all)]
fn read_chunk_ids<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<Vec<FIoChunkId>> {
    stream.de_ctx(header.toc_entry_count as usize)
}

#[instrument(skip_all)]
fn read_chunk_offsets<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoOffsetAndLength>> {
    stream.de_ctx(header.toc_entry_count as usize)
}

#[instrument(skip_all)]
fn read_hash_map<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
) -> Result<(Vec<i32>, Vec<i32>)> {
    let mut perfect_hash_seeds_count = 0;
    let mut chunks_without_perfect_hash_count = 0;

    if header.version >= EIoStoreTocVersion::PerfectHashWithOverflow {
        perfect_hash_seeds_count = header.toc_chunk_perfect_hash_seeds_count;
        chunks_without_perfect_hash_count = header.toc_chunks_without_perfect_hash_count;
    } else if header.version >= EIoStoreTocVersion::PerfectHash {
        perfect_hash_seeds_count = header.toc_chunk_perfect_hash_seeds_count;
        chunks_without_perfect_hash_count = 0;
    }
    let chunk_perfect_hash_seeds = stream.de_ctx(perfect_hash_seeds_count as usize)?;
    let chunk_indices_without_perfect_hash =
        stream.de_ctx(chunks_without_perfect_hash_count as usize)?;

    Ok((chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash))
}

#[instrument(skip_all)]
fn read_compression_blocks<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoStoreTocCompressedBlockEntry>> {
    stream.de_ctx(header.toc_compressed_block_entry_count as usize)
}

#[instrument(skip_all)]
fn read_compression_methods<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<CompressionMethod>> {
    let mut methods = vec![];
    for _ in 0..header.compression_method_name_count {
        let name = String::from_utf8(
            stream
                .de_ctx::<Vec<u8>, _>(header.compression_method_name_length as usize)?
                .into_iter()
                .take_while(|&b| b != 0)
                .collect(),
        )?;
        let method = CompressionMethod::from_str(&name)
            .with_context(|| format!("unknown compression method: {name:?}"))?;
        methods.push(method);
    }
    Ok(methods)
}
#[instrument(skip_all)]
fn write_compression_methods<S: Write>(s: &mut S, toc: &Toc) -> Result<()> {
    for name in &toc.compression_methods {
        let buffer: Vec<u8> = name
            .as_ref()
            .as_bytes()
            .iter()
            .copied()
            .chain(std::iter::repeat(0))
            .take(32)
            .collect();
        s.ser_no_length(&buffer)?;
    }
    Ok(())
}

#[instrument(skip_all)]
fn read_chunk_block_signatures<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
) -> Result<Option<TocSignatures>> {
    let is_signed = header.container_flags.contains(EIoContainerFlags::Signed);
    Ok(if is_signed {
        let size = stream.de::<u32>()? as usize;
        Some(TocSignatures {
            toc_signature: stream.de_ctx(size)?,
            block_signature: stream.de_ctx(size)?,
            chunk_block_signatures: stream
                .de_ctx(header.toc_compressed_block_entry_count as usize)?,
        })
    } else {
        None
    })
}

#[instrument(skip_all)]
fn read_directory_index<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
    config: &Config,
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = stream.de_ctx(header.directory_index_size as usize)?;

    if header
        .container_flags
        .contains(EIoContainerFlags::Encrypted)
    {
        use aes::cipher::BlockDecrypt;

        let key = config
            .aes_keys
            .get(&header.encryption_key_guid)
            .context("missing encryption key")?;
        for block in buf.chunks_mut(16) {
            key.0.decrypt_block(block.into());
        }
    }

    Ok(buf)
}

#[instrument(skip_all)]
fn read_meta<S: Read>(
    stream: &mut S,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoStoreTocEntryMeta>> {
    read_array(header.toc_entry_count as usize, stream, |s| {
        let res = s.de()?;
        if header.version >= EIoStoreTocVersion::ReplaceIoChunkHashWithIoHash {
            s.read_exact(&mut [0; 3])?;
        }
        Ok(res)
    })
}
#[instrument(skip_all)]
fn write_meta<S: Write>(s: &mut S, toc: &Toc) -> Result<()> {
    for meta in &toc.chunk_metas {
        s.ser(meta)?;
        if toc.version >= EIoStoreTocVersion::ReplaceIoChunkHashWithIoHash {
            s.write_all(&[0; 3])?;
        }
    }
    Ok(())
}

// UTF-8 path with '/' as separator
type UEPath = typed_path::Utf8UnixPath;

#[derive(Default)]
struct Toc {
    config: Arc<Config>,

    // serialized members
    chunks: Vec<FIoChunkId>,
    chunk_offset_lengths: Vec<FIoOffsetAndLength>,
    chunk_perfect_hash_seeds: Vec<i32>,
    chunk_indices_without_perfect_hash: Vec<i32>,
    compression_blocks: Vec<FIoStoreTocCompressedBlockEntry>,
    compression_methods: Vec<CompressionMethod>,
    signatures: Option<TocSignatures>,
    chunk_metas: Vec<FIoStoreTocEntryMeta>,

    // serialized in header
    version: EIoStoreTocVersion,
    container_id: FIoContainerId,
    compression_block_size: u32,
    partition_size: u64,
    partition_count: u32,
    encryption_key_guid: FGuid,
    container_flags: EIoContainerFlags,

    // transient indexes
    directory_index: FIoDirectoryIndexResource,
    file_map: HashMap<String, u32>,
    file_map_lower: HashMap<String, u32>,
    file_map_rev: HashMap<u32, String>,
    chunk_id_map: HashMap<FIoChunkId, u32>,
}
struct TocSignatures {
    toc_signature: Vec<u8>,
    block_signature: Vec<u8>,
    chunk_block_signatures: Vec<FSHAHash>,
}
impl Readable for Toc {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        stream.de_ctx(Arc::new(Config::default()))
    }
}
impl ReadableCtx<Arc<Config>> for Toc {
    fn de<S: Read>(stream: &mut S, config: Arc<Config>) -> Result<Self> {
        let header: FIoStoreTocHeader = stream.de()?;

        let chunk_ids = read_chunk_ids(stream, &header)?;
        let chunk_offset_lengths = read_chunk_offsets(stream, &header)?;
        let (chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash) =
            read_hash_map(stream, &header)?;
        let compression_blocks = read_compression_blocks(stream, &header)?;
        let compression_methods = read_compression_methods(stream, &header)?;

        let signatures = read_chunk_block_signatures(stream, &header)?;
        let directory_index = read_directory_index(stream, &header, &config)?;
        let chunk_metas = read_meta(stream, &header)?;

        // build indexes
        let mut chunk_id_to_index: HashMap<FIoChunkId, u32> = Default::default();
        for (chunk_index, &chunk_id) in chunk_ids.iter().enumerate() {
            chunk_id_to_index.insert(chunk_id, chunk_index as u32);
        }

        let mut file_map: HashMap<String, u32> = Default::default();
        let mut file_map_lower: HashMap<String, u32> = Default::default();
        let mut file_map_rev: HashMap<u32, String> = Default::default();
        let directory_index = if !directory_index.is_empty() {
            FIoDirectoryIndexResource::de(&mut Cursor::new(directory_index))?
        } else {
            FIoDirectoryIndexResource::default()
        };
        directory_index.iter_root(|user_data, path| {
            let path = path.join("/");
            file_map_lower.insert(path.to_ascii_lowercase(), user_data);
            file_map.insert(path.clone(), user_data);
            file_map_rev.insert(user_data, path);
        });
        let chunk_id_map = chunk_ids
            .iter()
            .enumerate()
            .map(|(i, &chunk_id)| (chunk_id, i as u32))
            .collect();

        Ok(Toc {
            config,

            chunks: chunk_ids,
            chunk_offset_lengths,
            chunk_perfect_hash_seeds,
            chunk_indices_without_perfect_hash,
            compression_blocks,
            compression_methods,
            signatures,
            chunk_metas,

            version: header.version,
            container_id: header.container_id,
            compression_block_size: header.compression_block_size,
            partition_size: header.partition_size,
            partition_count: header.partition_count,
            encryption_key_guid: header.encryption_key_guid,
            container_flags: header.container_flags,

            directory_index,
            file_map,
            file_map_lower,
            file_map_rev,
            chunk_id_map,
        })
    }
}
impl Writeable for Toc {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        let mut directory_index_buffer = vec![];
        self.directory_index
            .ser(&mut Cursor::new(&mut directory_index_buffer))?;
        // TODO encrypt directory index

        let header = FIoStoreTocHeader {
            toc_magic: FIoStoreTocHeader::MAGIC,
            version: self.version,
            reserved0: 0,
            reserved1: 0,
            toc_header_size: std::mem::size_of::<FIoStoreTocHeader>() as u32,
            toc_entry_count: self.chunks.len() as u32,
            toc_compressed_block_entry_count: self.compression_blocks.len() as u32,
            toc_compressed_block_entry_size: std::mem::size_of::<FIoStoreTocCompressedBlockEntry>()
                as u32,
            compression_method_name_count: self.compression_methods.len() as u32,
            compression_method_name_length: 32,
            compression_block_size: self.compression_block_size,
            directory_index_size: directory_index_buffer.len() as u32,
            partition_count: 1,
            container_id: self.container_id,
            encryption_key_guid: Default::default(),
            container_flags: EIoContainerFlags::empty(),
            reserved3: 0,
            reserved4: 0,
            toc_chunk_perfect_hash_seeds_count: 0,
            partition_size: self.partition_size,
            toc_chunks_without_perfect_hash_count: 0, // TODO
            reserved7: 0,
            reserved8: [0, 0, 0, 0, 0],
        };
        s.ser(&header)?;

        s.ser_no_length(&self.chunks)?;
        s.ser_no_length(&self.chunk_offset_lengths)?;
        s.ser_no_length(&self.compression_blocks)?;
        write_compression_methods(s, &self)?;
        s.ser_no_length(&directory_index_buffer)?;
        write_meta(s, &self)?;

        Ok(())
    }
}
fn align_u64(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}
fn align_usize(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

// Breaks down a combined FName string into a base name and a number. Number is 0 if there is no number
pub(crate) fn break_down_name_string<'a>(name: &'a str) -> (&'a str, i32) {
    let mut name_without_number: &'a str = name;
    let mut name_number: i32 = 0; // 0 means no number

    // Attempt to break down the composite name into the name part and the number part
    if let Some((left, right)) = name.rsplit_once('_') {
        // Right part needs to be parsed as a valid signed integer that is >= 0 and converts back to the same string
        // Last part is important for not touching names like: Rocket_04 - 04 should stay a part of the name, not a number, otherwise we would actually get Rocket_4 when deserializing!
        if let Ok(parsed_number) = i32::from_str_radix(right, 10) {
            if parsed_number >= 0 && parsed_number.to_string() == right {
                name_without_number = left;
                name_number = parsed_number + 1; // stored as 1 more than the actual number
            }
        }
    }
    (name_without_number, name_number)
}

impl Toc {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    /// get absolute path (including mount point) for given chunk ID if has one
    fn file_name(&self, chunk_id: FIoChunkId) -> Option<String> {
        self.chunk_id_map
            .get(&chunk_id)
            .and_then(|index| self.file_map_rev.get(index))
            .map(|path| {
                UEPath::new(&self.directory_index.mount_point)
                    .join(path)
                    .to_string()
            })
    }
    //fn get_chunk_info(&self, toc_entry_index: u32) {
    fn get_chunk_info(&self, file_name: &str) -> FIoStoreTocChunkInfo {
        let toc_entry_index = self.file_map[file_name] as usize;
        let meta = &self.chunk_metas[toc_entry_index];
        let offset_and_length = &self.chunk_offset_lengths[toc_entry_index];

        let mut hash = FIoChunkHash { data: [0; 32] };
        // copy only first 20 bytes for some reason
        hash.data[..20].copy_from_slice(&meta.chunk_hash.data[..20]);

        let offset = offset_and_length.get_offset();
        let size = offset_and_length.get_length();

        let compression_block_size = self.compression_block_size;
        let first_block_index = (offset / compression_block_size as u64) as usize;
        let last_block_index = ((align_u64(offset + size, compression_block_size as u64) - 1)
            / compression_block_size as u64) as usize;

        let num_compressed_blocks = (1 + last_block_index - first_block_index) as u32;
        let offset_on_disk = self.compression_blocks[first_block_index].get_offset();
        let mut compressed_size = 0;
        let mut partition_index = -1;

        for block_index in first_block_index..=last_block_index {
            let compression_block = &self.compression_blocks[block_index];
            compressed_size += compression_block.get_compressed_size() as u64;
            if partition_index < 0 {
                partition_index = (compression_block.get_offset() / self.partition_size) as i32;
            }
        }

        let id = self.chunks[toc_entry_index];

        FIoStoreTocChunkInfo {
            id,
            file_name: file_name.to_string(),
            hash,
            offset: offset_and_length.get_offset(),
            offset_on_disk,
            size: offset_and_length.get_length(),
            compressed_size,
            num_compressed_blocks,
            partition_index,
            chunk_type: id.get_chunk_type(),
            has_valid_file_name: false,
            force_uncompressed: /* isContainerCompressed && */ !meta.flags.contains(FIoStoreTocEntryMetaFlags::Compressed),
            is_memory_mapped: meta.flags.contains(FIoStoreTocEntryMetaFlags::MemoryMapped),
            is_compressed: meta.flags.contains(FIoStoreTocEntryMetaFlags::Compressed),
        }
    }
    fn get_chunk_id_entry_index(&self, chunk_id: FIoChunkId) -> Result<u32> {
        self.chunk_id_map
            .get(&chunk_id)
            .copied()
            .with_context(|| "container does not contain entry for {chunk_id}")
    }
    fn read<C: Read + Seek>(&self, cas_stream: &mut C, toc_entry_index: u32) -> Result<Vec<u8>> {
        let offset_and_length = &self.chunk_offset_lengths[toc_entry_index as usize];
        let offset = offset_and_length.get_offset();
        let size = offset_and_length.get_length();

        let compression_block_size = self.compression_block_size;
        let first_block_index = (offset / compression_block_size as u64) as usize;
        let last_block_index = ((align_u64(offset + size, compression_block_size as u64) - 1)
            / compression_block_size as u64) as usize;

        let blocks = &self.compression_blocks[first_block_index..=last_block_index];
        let aes_key = if self.container_flags.contains(EIoContainerFlags::Encrypted) {
            Some(
                self.config
                    .aes_keys
                    .get(&self.encryption_key_guid)
                    .with_context(|| {
                        format!(
                            "container is encrypted but no AES key for {:?} supplied",
                            self.encryption_key_guid
                        )
                    })?,
            )
        } else {
            None
        };

        use aes::cipher::BlockDecrypt;

        let mut max_buffer = 0;
        for b in blocks {
            max_buffer = max_buffer.max(align_usize(b.get_compressed_size() as usize, 16));
        }
        let mut data = vec![0; align_usize(size as usize, 16)];
        let mut buffer = vec![0; max_buffer];
        let mut cur = 0;
        for block in blocks {
            let compressed_size = block.get_compressed_size() as usize;
            let uncompressed_size = block.get_uncompressed_size() as usize;
            //eprintln!("{block:#?}");

            let out = &mut data[cur..];

            cas_stream.seek(SeekFrom::Start(block.get_offset()))?;

            let compression_method_index = block.get_compression_method_index() as usize;
            let compression_method = if compression_method_index == 0 {
                None
            } else {
                Some(self.compression_methods[compression_method_index - 1])
            };
            match compression_method {
                None => {
                    if let Some(key) = aes_key {
                        let out = &mut out[..align_usize(uncompressed_size, 16)];
                        cas_stream.read_exact(out)?;
                        for block in out.chunks_mut(16) {
                            key.0.decrypt_block(block.into());
                        }
                    } else {
                        cas_stream.read_exact(&mut out[..uncompressed_size])?;
                    }
                }
                Some(method) => {
                    let tmp = if let Some(key) = aes_key {
                        let tmp = &mut buffer[..align_usize(compressed_size, 16)];
                        cas_stream.read_exact(tmp)?;

                        for block in tmp.chunks_mut(16) {
                            key.0.decrypt_block(block.into());
                        }
                        &tmp[..compressed_size]
                    } else {
                        let tmp = &mut buffer[..compressed_size];
                        cas_stream.read_exact(tmp)?;
                        tmp
                    };
                    decompress(method, tmp, &mut out[..uncompressed_size])?;
                }
            }
            cur += uncompressed_size;
        }

        data.truncate(size as usize);
        Ok(data)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FPackageId(u64);
impl Readable for FPackageId {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self(s.de()?))
    }
}
impl Writeable for FPackageId {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.0)
    }
}
impl Display for FPackageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.serialize_u64(self.0)
    }
}
impl FPackageId {
    fn from_name(name: &str) -> Self {
        Self(lower_utf16_cityhash(name))
    }
}

fn lower_utf16_cityhash(s: &str) -> u64 {
    let bytes = s
        .to_ascii_lowercase()
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<u8>>();
    cityhasher::hash(bytes)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_package_id() {
        let package_id = FPackageId::from_name("/ACLPlugin/ACLAnimBoneCompressionSettings");
        let chunk_id = FIoChunkId::from_package_id(package_id, 0, EIoChunkType::ExportBundleData);
        dbg!(chunk_id);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FIoChunkId {
    id: [u8; 12],
}
impl std::fmt::Debug for FIoChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FIoChunkId")
            .field("chunk_id", &hex::encode(self.id))
            .field("chunk_type", &self.get_chunk_type())
            .finish()
    }
}
impl AsRef<[u8]> for FIoChunkId {
    fn as_ref(&self) -> &[u8] {
        &self.id
    }
}
impl TryFrom<Vec<u8>> for FIoChunkId {
    type Error = Vec<u8>;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            id: value.try_into()?,
        })
    }
}
impl FromStr for FIoChunkId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let id = hex::decode(s)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .context("expected 12 byte hex string")?;
        Ok(FIoChunkId { id })
    }
}
impl Readable for FIoChunkId {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { id: stream.de()? })
    }
}
impl Writeable for FIoChunkId {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.id)
    }
}
impl FIoChunkId {
    fn create(chunk_id: u64, chunk_index: u16, chunk_type: EIoChunkType) -> Self {
        let mut id = [0; 12];
        id[0..8].copy_from_slice(&u64::to_le_bytes(chunk_id));
        id[8..10].copy_from_slice(&u16::to_le_bytes(chunk_index));
        id[11] = chunk_type as u8;
        Self { id }
    }
    fn from_package_id(package_id: FPackageId, chunk_index: u16, chunk_type: EIoChunkType) -> Self {
        Self::create(package_id.0, chunk_index, chunk_type)
    }
    fn get_chunk_type(&self) -> EIoChunkType {
        EIoChunkType::from_repr(self.id[11]).unwrap()
    }
}
#[derive(Debug, Default, Clone, Copy)]
struct FIoContainerId(u64);
impl FIoContainerId {
    fn from_name(name: &str) -> Self {
        Self(lower_utf16_cityhash(name))
    }
}
impl Readable for FIoContainerId {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self(stream.de()?))
    }
}
impl Writeable for FIoContainerId {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.0)
    }
}
#[derive(Debug, Default, Clone, Copy)]
struct FIoOffsetAndLength {
    data: [u8; 10],
}
impl Readable for FIoOffsetAndLength {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self { data: s.de()? })
    }
}
impl Writeable for FIoOffsetAndLength {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.data)
    }
}
impl FIoOffsetAndLength {
    pub(crate) fn new(offset: u64, length: u64) -> Self {
        let mut new = Self::default();
        new.set_offset(offset);
        new.set_length(length);
        new
    }
    pub(crate) fn get_offset(&self) -> u64 {
        let d = self.data;
        u64::from_be_bytes([0, 0, 0, d[0], d[1], d[2], d[3], d[4]])
    }
    pub(crate) fn set_offset(&mut self, offset: u64) {
        let bytes = offset.to_be_bytes();
        self.data[0..5].copy_from_slice(&bytes[3..]);
    }
    pub(crate) fn get_length(&self) -> u64 {
        let d = self.data;
        u64::from_be_bytes([0, 0, 0, d[5], d[6], d[7], d[8], d[9]])
    }
    pub(crate) fn set_length(&mut self, offset: u64) {
        let bytes = offset.to_be_bytes();
        self.data[5..10].copy_from_slice(&bytes[3..]);
    }
}
#[derive(Default, Clone, Copy)]
#[repr(transparent)]
struct FIoStoreTocCompressedBlockEntry {
    data: [u8; 12],
}
impl std::fmt::Debug for FIoStoreTocCompressedBlockEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FIoStoreTocCompressedBlockEntry")
            .field("offset", &self.get_offset())
            .field("compressed_size", &self.get_compressed_size())
            .field("uncompressed_size", &self.get_uncompressed_size())
            .field(
                "compression_method_index",
                &self.get_compression_method_index(),
            )
            .finish()
    }
}
impl Readable for FIoStoreTocCompressedBlockEntry {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { data: stream.de()? })
    }
}
impl Writeable for FIoStoreTocCompressedBlockEntry {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.data)
    }
}
impl FIoStoreTocCompressedBlockEntry {
    pub(crate) fn new(
        offset: u64,
        compressed_size: u32,
        uncompressed_size: u32,
        compression_method_index: u8,
    ) -> Self {
        let mut new = Self::default();
        new.set_offset(offset);
        new.set_compressed_size(compressed_size);
        new.set_uncompressed_size(uncompressed_size);
        new.set_compression_method_index(compression_method_index);
        new
    }
    pub(crate) fn get_offset(&self) -> u64 {
        let d = self.data;
        u64::from_le_bytes([d[0], d[1], d[2], d[3], d[4], 0, 0, 0])
    }
    pub(crate) fn set_offset(&mut self, value: u64) {
        self.data[0..5].copy_from_slice(&value.to_le_bytes()[0..5]);
    }
    pub(crate) fn get_compressed_size(&self) -> u32 {
        let d = self.data;
        u32::from_le_bytes([d[5], d[6], d[7], 0])
    }
    pub(crate) fn set_compressed_size(&mut self, value: u32) {
        self.data[5..8].copy_from_slice(&value.to_le_bytes()[0..3]);
    }
    pub(crate) fn get_uncompressed_size(&self) -> u32 {
        let d = self.data;
        u32::from_le_bytes([d[8], d[9], d[10], 0])
    }
    pub(crate) fn set_uncompressed_size(&mut self, value: u32) {
        self.data[8..11].copy_from_slice(&value.to_le_bytes()[0..3]);
    }
    pub(crate) fn get_compression_method_index(&self) -> u8 {
        self.data[11]
    }
    pub(crate) fn set_compression_method_index(&mut self, value: u8) {
        self.data[11] = value;
    }
}
struct FIoChunkHash {
    data: [u8; 32],
}
impl FIoChunkHash {
    fn from_blake3(hash: &[u8; 32]) -> FIoChunkHash {
        let mut data = [0; 32];
        data[0..20].copy_from_slice(&hash[0..20]);
        Self { data }
    }
}
impl std::fmt::Debug for FIoChunkHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FIoChunkHash(")?;
        for b in self.data {
            write!(f, "{:02X}", b)?;
        }
        write!(f, ")")
    }
}
impl Readable for FIoChunkHash {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { data: stream.de()? })
    }
}
impl Writeable for FIoChunkHash {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.data)
    }
}
#[derive(Debug)]
struct FIoStoreTocEntryMeta {
    chunk_hash: FIoChunkHash,
    flags: FIoStoreTocEntryMetaFlags,
}
impl Readable for FIoStoreTocEntryMeta {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            chunk_hash: stream.de()?,
            flags: stream.de()?,
        })
    }
}
impl Writeable for FIoStoreTocEntryMeta {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.chunk_hash)?;
        s.ser(&self.flags)?;
        Ok(())
    }
}
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FGuid {
    a: u32,
    b: u32,
    c: u32,
    d: u32,
}
impl Readable for FGuid {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            a: stream.de()?,
            b: stream.de()?,
            c: stream.de()?,
            d: stream.de()?,
        })
    }
}
impl Writeable for FGuid {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.ser(&self.a)?;
        stream.ser(&self.b)?;
        stream.ser(&self.c)?;
        stream.ser(&self.d)?;
        Ok({})
    }
}
#[derive(Default, Clone, Copy)]
struct FSHAHash {
    data: [u8; 20],
}
impl Readable for FSHAHash {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { data: stream.de()? })
    }
}
impl Writeable for FSHAHash {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.data)
    }
}
impl std::fmt::Debug for FSHAHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FSHAHash(")?;
        for b in self.data {
            write!(f, "{:02X}", b)?;
        }
        write!(f, ")")
    }
}

bitflags! {
    #[derive(Debug, Default)]
    struct EIoContainerFlags: u8 {
        const Compressed = 0b0001;
        const Encrypted  = 0b0010;
        const Signed     = 0b0100;
        const Indexed    = 0b1000;
    }
    #[derive(Debug)]
    struct FIoStoreTocEntryMetaFlags: u8 {
        const Compressed = 1;
        const MemoryMapped = 2;
    }
}
impl Readable for EIoContainerFlags {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_bits(stream.de()?).context("invalid EIoContainerFlags value")
    }
}
impl Writeable for EIoContainerFlags {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.bits())
    }
}
impl Readable for FIoStoreTocEntryMetaFlags {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_bits(stream.de()?).context("invalid FIoStoreTocEntryMetaFlags value")
    }
}
impl Writeable for FIoStoreTocEntryMetaFlags {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.bits())
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, FromRepr, Serialize, Deserialize,
)]
#[repr(u8)]
enum EIoStoreTocVersion {
    #[default]
    Invalid,
    Initial,
    DirectoryIndex,
    PartitionSize,
    PerfectHash,
    PerfectHashWithOverflow,
    OnDemandMetaData,
    RemovedOnDemandMetaData,
    ReplaceIoChunkHashWithIoHash,
}
impl Readable for EIoStoreTocVersion {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_repr(stream.de()?).context("invalid EIoStoreTocVersion value")
    }
}
impl Writeable for EIoStoreTocVersion {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&(*self as u8))
    }
}

#[derive(Debug, Default)]
#[repr(C)]
struct FIoStoreTocHeader {
    toc_magic: [u8; 16],
    version: EIoStoreTocVersion,
    reserved0: u8,
    reserved1: u16,
    toc_header_size: u32,
    toc_entry_count: u32,
    toc_compressed_block_entry_count: u32,
    toc_compressed_block_entry_size: u32,
    compression_method_name_count: u32,
    compression_method_name_length: u32,
    compression_block_size: u32,
    directory_index_size: u32,
    partition_count: u32,
    container_id: FIoContainerId,
    encryption_key_guid: FGuid,
    container_flags: EIoContainerFlags,
    reserved3: u8,
    reserved4: u16,
    toc_chunk_perfect_hash_seeds_count: u32,
    partition_size: u64,
    toc_chunks_without_perfect_hash_count: u32,
    reserved7: u32,
    reserved8: [u64; 5],
}
impl FIoStoreTocHeader {
    const MAGIC: [u8; 16] = *b"-==--==--==--==-";
}

#[derive(Debug)]
struct FIoStoreTocChunkInfo {
    id: FIoChunkId,
    file_name: String,
    hash: FIoChunkHash,
    offset: u64,
    offset_on_disk: u64,
    size: u64,
    compressed_size: u64,
    num_compressed_blocks: u32,
    partition_index: i32,
    chunk_type: EIoChunkType,
    has_valid_file_name: bool,
    force_uncompressed: bool,
    is_memory_mapped: bool,
    is_compressed: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, FromRepr, AsRefStr)]
#[repr(u8)]
enum EIoChunkType {
    Invalid = 0,
    ExportBundleData = 1,
    BulkData = 2,
    OptionalBulkData = 3,
    MemoryMappedBulkData = 4,
    ScriptObjects = 5,
    ContainerHeader = 6,
    ExternalFile = 7,
    ShaderCodeLibrary = 8,
    ShaderCode = 9,
    PackageStoreEntry = 10,
    DerivedData = 11,
    EditorDerivedData = 12,
    PackageResource = 13,
    // from 4.25
    //Invalid = 0x0,
    //InstallManifest = 0x1,
    //ExportBundleData = 0x2,
    //BulkData = 0x3,
    //OptionalBulkData = 0x4,
    //MemoryMappedBulkData = 0x5,
    //LoaderGlobalMeta = 0x6,
    //LoaderInitialLoadMeta = 0x7,
    //LoaderGlobalNames = 0x8,
    //LoaderGlobalNameHashes = 0x9,
    //ContainerHeader = 0xa,
}

use crate::asset_conversion::FZenPackageContext;
use crate::container_header::EIoContainerHeaderVersion;
use crate::shader_library::rebuild_shader_library_from_io_store;
use crate::zen::FPackageFileVersion;
use directory_index::*;
use zen::get_package_name;

mod directory_index {
    use super::*;

    #[derive(Debug, Default)]
    pub struct FIoDirectoryIndexResource {
        pub(crate) mount_point: String,
        directory_entries: Vec<FIoDirectoryIndexEntry>,
        file_entries: Vec<FIoFileIndexEntry>,
        string_table: Vec<String>,
    }
    impl Readable for FIoDirectoryIndexResource {
        fn de<S: Read>(s: &mut S) -> Result<Self> {
            Ok(Self {
                mount_point: s.de()?,
                directory_entries: s.de()?,
                file_entries: s.de()?,
                string_table: s.de()?,
            })
        }
    }
    impl Writeable for FIoDirectoryIndexResource {
        fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
            if !self.file_entries.is_empty() {
                s.ser(&self.mount_point)?;
                s.ser(&self.directory_entries)?;
                s.ser(&self.file_entries)?;
                s.ser(&self.string_table)?;
            }
            Ok(())
        }
    }
    impl FIoDirectoryIndexResource {
        pub fn iter_root<F>(&self, mut visitor: F)
        where
            F: FnMut(u32, &[&str]),
        {
            if !self.directory_entries.is_empty() {
                self.iter(IdDir(0), &mut vec![], &mut visitor)
            }
        }
        pub fn iter<'s, F>(&'s self, dir_index: IdDir, stack: &mut Vec<&'s str>, visitor: &mut F)
        where
            F: FnMut(u32, &[&str]),
        {
            let dir = &self.directory_entries[dir_index.get()];
            if let Some(i) = dir.name {
                stack.push(&self.string_table[i.get()]);
            }

            let mut file_index = dir.first_file_entry;
            while let Some(i) = file_index {
                let file = &self.file_entries[i.get()];

                stack.push(&self.string_table[file.name.get()]);
                visitor(file.user_data, stack);
                stack.pop();

                file_index = file.next_file_entry;
            }

            {
                let mut dir_index = dir.first_child_entry;
                while let Some(i) = dir_index {
                    let dir = &self.directory_entries[i.get()];
                    self.iter(i, stack, visitor);
                    dir_index = dir.next_sibling_entry;
                }
            }

            if dir.name.is_some() {
                stack.pop();
            }
        }
        fn root(&self) -> IdDir {
            IdDir(0)
        }
        fn ensure_root(&mut self) -> IdDir {
            if self.directory_entries.is_empty() {
                self.directory_entries.push(FIoDirectoryIndexEntry {
                    name: None,
                    first_child_entry: None,
                    next_sibling_entry: None,
                    first_file_entry: None,
                });
            }
            self.root()
        }
        fn get_or_create_dir(&mut self, parent: IdDir, name: &str) -> IdDir {
            let mut dir_index = self.directory_entries[parent.get()].first_child_entry;
            let mut last = None;
            while let Some(i) = dir_index {
                let dir = &self.directory_entries[i.get()];
                if self.string_table[dir.name.unwrap().get()] == name {
                    return i;
                }
                last = dir_index;
                dir_index = dir.next_sibling_entry;
            }
            let new = FIoDirectoryIndexEntry {
                name: Some(self.get_or_create_name(name)),
                first_child_entry: None,
                next_sibling_entry: None,
                first_file_entry: None,
            };
            self.directory_entries.push(new);
            let id = IdDir(self.directory_entries.len() as u32 - 1);

            if let Some(last) = last {
                self.directory_entries[last.get()].next_sibling_entry = Some(id);
            } else {
                self.directory_entries[parent.get()].first_child_entry = Some(id);
            }
            id
        }
        fn get_or_create_file(&mut self, parent: IdDir, name: &str) -> IdFile {
            let mut file_index = self.directory_entries[parent.get()].first_file_entry;
            let mut last = None;
            while let Some(i) = file_index {
                let file = &self.file_entries[i.get()];
                if self.string_table[file.name.get()] == name {
                    return i;
                }
                last = file_index;
                file_index = file.next_file_entry;
            }
            let new = FIoFileIndexEntry {
                name: self.get_or_create_name(name),
                next_file_entry: None,
                user_data: 0,
            };
            self.file_entries.push(new);
            let id = IdFile(self.file_entries.len() as u32 - 1);
            if let Some(last) = last {
                self.file_entries[last.get()].next_file_entry = Some(id);
            } else {
                self.directory_entries[parent.get()].first_file_entry = Some(id);
            }
            id
        }
        fn get_or_create_name(&mut self, name: &str) -> IdName {
            IdName(
                self.string_table
                    .iter()
                    .position(|n| n == name)
                    .unwrap_or_else(|| {
                        self.string_table.push(name.to_string());
                        self.string_table.len() - 1
                    }) as u32,
            )
        }
        pub fn add_file(&mut self, path: &str, user_data: u32) {
            let mut components = path.split('/');
            let file_name = components.next_back().unwrap();
            let mut dir_index = self.ensure_root();
            for dir_name in components {
                dir_index = self.get_or_create_dir(dir_index, dir_name);
            }
            let file_index = self.get_or_create_file(dir_index, file_name);
            self.file_entries[file_index.get()].user_data = user_data;
        }
    }

    macro_rules! new_id {
        ($name:ident) => {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct $name(u32);
            impl $name {
                fn new(value: u32) -> Option<Self> {
                    (value != u32::MAX).then_some(Self(value))
                }
                fn get(self) -> usize {
                    self.0 as usize
                }
            }
            impl Readable for Option<$name> {
                fn de<S: Read>(s: &mut S) -> Result<Self> {
                    Ok($name::new(s.de()?))
                }
            }
            impl Readable for $name {
                fn de<S: Read>(s: &mut S) -> Result<Self> {
                    Ok(Self(s.de()?))
                }
            }
            impl Writeable for Option<$name> {
                fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
                    s.ser(&self.map(|id| id.0).unwrap_or(u32::MAX))
                }
            }
            impl Writeable for $name {
                fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
                    s.ser(&self.0)
                }
            }
        };
    }
    new_id!(IdFile);
    new_id!(IdDir);
    new_id!(IdName);

    #[derive(Debug)]
    struct FIoFileIndexEntry {
        name: IdName,
        next_file_entry: Option<IdFile>,
        user_data: u32,
    }
    impl Readable for FIoFileIndexEntry {
        fn de<S: Read>(s: &mut S) -> Result<Self> {
            Ok(Self {
                name: s.de()?,
                next_file_entry: s.de()?,
                user_data: s.de()?,
            })
        }
    }
    impl Writeable for FIoFileIndexEntry {
        fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
            s.ser(&self.name)?;
            s.ser(&self.next_file_entry)?;
            s.ser(&self.user_data)?;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FIoDirectoryIndexEntry {
        name: Option<IdName>,
        first_child_entry: Option<IdDir>,
        next_sibling_entry: Option<IdDir>,
        first_file_entry: Option<IdFile>,
    }
    impl Readable for FIoDirectoryIndexEntry {
        fn de<S: Read>(s: &mut S) -> Result<Self> {
            Ok(Self {
                name: s.de()?,
                first_child_entry: s.de()?,
                next_sibling_entry: s.de()?,
                first_file_entry: s.de()?,
            })
        }
    }
    impl Writeable for FIoDirectoryIndexEntry {
        fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
            s.ser(&self.name)?;
            s.ser(&self.first_child_entry)?;
            s.ser(&self.next_sibling_entry)?;
            s.ser(&self.first_file_entry)?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn test_dir() {
            let mut index = FIoDirectoryIndexResource {
                mount_point: Default::default(),
                directory_entries: Default::default(),
                file_entries: Default::default(),
                string_table: Default::default(),
            };

            let entries = HashMap::from([
                ("this/b/test2.txt".to_string(), 1),
                ("this/is/a/test1.txt".to_string(), 2),
                ("this/test2.txt".to_string(), 3),
                ("this/is/a/test2.txt".to_string(), 4),
            ]);

            for (path, data) in &entries {
                index.add_file(path, *data);
            }

            dbg!(&index);

            //for d in &dir.directory_entries {
            //    println!("");
            //}

            let mut new_map = HashMap::new();
            index.iter_root(|user_data, path| {
                new_map.insert(path.join("/"), user_data);
                println!("{} = {}", path.join("/"), user_data);
            });
            assert_eq!(entries, new_map);
        }
    }
}
