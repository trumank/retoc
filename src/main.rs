mod compact_binary;
mod container_header;
mod iostore;
mod legacy_asset;
mod manifest;
mod name_map;
mod script_objects;
mod ser;
mod zen;

use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};

use aes::cipher::KeyInit as _;
use anyhow::{bail, Context, Result};
use bitflags::bitflags;
use clap::Parser;
use rayon::prelude::*;
use ser::*;
use strum::{AsRefStr, FromRepr};
use tracing::instrument;

#[derive(Parser, Debug)]
struct ActionManifest {
    #[arg(index = 1)]
    utoc: PathBuf,
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
struct ActionExtractLegacy {
    #[arg(index = 1)]
    utoc: PathBuf,
    #[arg(index = 2)]
    output: PathBuf,
    #[arg(short, long, default_value = "false")]
    verbose: bool,
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
    /// List fils in .utoc (directory index)
    List(ActionList),
    /// Verify IO Store container
    Verify(ActionVerify),
    /// Extracts chunks (files) from .utoc
    Unpack(ActionUnpack),
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

    match args.action {
        Action::Manifest(action) => action_manifest(action, &config),
        Action::List(action) => action_list(action, &config),
        Action::Verify(action) => action_verify(action, &config),
        Action::Unpack(action) => action_unpack(action, &config),
        Action::ExtractLegacy(action) => action_extract_legacy(action, &config),
        Action::Get(action) => action_get(action, &config),
    }
}

fn action_manifest(args: ActionManifest, config: &Config) -> Result<()> {
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Serum/Serum/Content/Paks/pakchunk0-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Nuclear Nightmare/NuclearNightmare/Content/Paks/NuclearNightmare-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/The Isle/TheIsle/Content/Paks/pakchunk0-WindowsClient.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/FactoryGame-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/global.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/VisionsofManaDemo/VisionsofMana/Content/Paks/pakchunk0-WindowsNoEditor.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/AbioticFactor/AbioticFactor/Content/Paks/pakchunk0-Windows.utoc";

    let toc: Toc = BufReader::new(File::open(&args.utoc)?).de_ctx(config)?;
    let ucas = &args.utoc.with_extension("ucas");

    let entries = Arc::new(Mutex::new(vec![]));

    toc.file_map.keys().par_bridge().try_for_each_init(
        || (entries.clone(), BufReader::new(File::open(ucas).unwrap())),
        |(entries, ucas), file_name| -> Result<()> {
            let chunk_info = toc.get_chunk_info(file_name);

            if chunk_info.id.get_chunk_type() == EIoChunkType::ExportBundleData {
                let data = toc.read(ucas, toc.file_map[file_name])?;

                let package_name =
                    get_package_name(&data).with_context(|| file_name.to_string())?;

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
                            id: toc.chunk_ids[bulk as usize],
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
    std::fs::write(path, serde_json::to_vec(&manifest)?)?;

    println!("wrote {} entries to {}", manifest.oplog.entries.len(), path);

    Ok(())
}

fn action_list(args: ActionList, config: &Config) -> Result<()> {
    let iostore = iostore::open(args.utoc, config)?;

    for chunk_id in iostore.chunk_ids() {
        let file_name = iostore.file_name(chunk_id);
        println!(
            "{}  {:20}  {}",
            hex::encode(chunk_id.id),
            chunk_id.get_chunk_type().as_ref(),
            file_name.unwrap_or("-")
        );
    }
    Ok(())
}

fn action_verify(args: ActionVerify, config: &Config) -> Result<()> {
    let mut stream = BufReader::new(File::open(&args.utoc)?);
    let ucas = &args.utoc.with_extension("ucas");

    let toc: Toc = stream.de_ctx(config)?;

    // most of these don't match?!
    //let sigs = &toc.signatures.as_ref().unwrap().chunk_block_signatures;
    //let mut rdr = BufReader::new(File::open(ucas)?);
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
        || BufReader::new(File::open(ucas).unwrap()),
        |ucas, (i, meta)| -> Result<()> {
            let chunk_id = toc.chunk_ids[i];
            let data = toc.read(ucas, i as u32)?;

            let chunk_type = chunk_id.get_chunk_type();
            match chunk_type {
                EIoChunkType::ExportBundleData => {
                    let package_name = get_package_name(&data)?;
                    let package_id = FPackageId::from_name(&package_name);
                    let id = FIoChunkId::from_package_id(package_id, 0, chunk_type);
                    if id != chunk_id {
                        bail!("chunk ID mismatch")
                    }
                }
                _ => {} // TODO
            }

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

fn action_unpack(args: ActionUnpack, config: &Config) -> Result<()> {
    let mut stream = BufReader::new(File::open(&args.utoc)?);
    let ucas = &args.utoc.with_extension("ucas");

    let toc: Toc = stream.de_ctx(config)?;

    let output = args.output;

    // TODO extract entries not found in directory index
    // TODO output chunk id manifest
    toc.file_map.keys().par_bridge().try_for_each_init(
        || BufReader::new(File::open(ucas).unwrap()),
        |ucas, file_name| -> Result<()> {
            if args.verbose {
                println!("{file_name}");
            }
            let data = toc.read(ucas, toc.file_map[file_name])?;

            let path = output.join(file_name);
            let dir = path.parent().unwrap();
            std::fs::create_dir_all(dir)?;
            std::fs::write(path, &data)?;
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

fn action_extract_legacy(args: ActionExtractLegacy, config: &Config) -> Result<()> {
    let iostore = iostore::open(args.utoc, config)?;

    let output = args.output;

    let mut count = 0;
    iostore.chunk_ids().try_for_each(|chunk_id| -> Result<()> {
        if chunk_id.get_chunk_type() == EIoChunkType::ExportBundleData {
            if let Some(file_name) = iostore.file_name(chunk_id) {
                if let Some(filter) = &args.filter {
                    if !file_name.contains(filter) {
                        return Ok(());
                    }
                }
                if args.verbose {
                    println!("{file_name}");
                }
                let path = output.join(file_name);
                let dir = path.parent().unwrap();
                std::fs::create_dir_all(dir)?;
                legacy_asset::build_legacy(&*iostore, chunk_id, path)?;
                count += 1;
            }
        }
        Ok(())
    })?;

    println!(
        "unpacked {} legacy assets to {}",
        count,
        output.to_string_lossy()
    );

    Ok(())
}

fn action_get(args: ActionGet, config: &Config) -> Result<()> {
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
        assert_eq!(&res.toc_magic, b"-==--==--==--==-");
        assert_eq!(res.toc_header_size, 0x90);
        Ok(res)
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
) -> Result<Vec<String>> {
    let mut names = vec!["None".into()];
    for _ in 0..header.compression_method_name_count {
        names.push(String::from_utf8(
            stream
                .de_ctx::<Vec<u8>, _>(header.compression_method_name_length as usize)?
                .into_iter()
                .take_while(|&b| b != 0)
                .collect(),
        )?);
    }
    Ok(names)
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

struct Toc {
    header: FIoStoreTocHeader,
    chunk_ids: Vec<FIoChunkId>,
    chunk_offset_lengths: Vec<FIoOffsetAndLength>,
    chunk_perfect_hash_seeds: Vec<i32>,
    chunk_indices_without_perfect_hash: Vec<i32>,
    compression_blocks: Vec<FIoStoreTocCompressedBlockEntry>,
    compression_methods: Vec<String>,
    signatures: Option<TocSignatures>,
    chunk_metas: Vec<FIoStoreTocEntryMeta>,

    directory_index: Option<FIoDirectoryIndexResource>,
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
        stream.de_ctx(&Config::default())
    }
}
impl ReadableCtx<&Config> for Toc {
    fn de<S: Read>(stream: &mut S, config: &Config) -> Result<Self> {
        let header: FIoStoreTocHeader = stream.de()?;

        let chunk_ids = read_chunk_ids(stream, &header)?;
        let chunk_offset_lengths = read_chunk_offsets(stream, &header)?;
        let (chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash) =
            read_hash_map(stream, &header)?;
        let compression_blocks = read_compression_blocks(stream, &header)?;
        let compression_methods = read_compression_methods(stream, &header)?;

        let signatures = read_chunk_block_signatures(stream, &header)?;
        let directory_index = read_directory_index(stream, &header, config)?;
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
            let directory_index =
                FIoDirectoryIndexResource::read(&mut Cursor::new(directory_index))?;
            directory_index.iter_root(|user_data, path| {
                let path = path.join("/");
                file_map_lower.insert(path.to_ascii_lowercase(), user_data);
                file_map.insert(path.clone(), user_data);
                file_map_rev.insert(user_data, path);
            });
            Some(directory_index)
        } else {
            None
        };
        let chunk_id_map = chunk_ids
            .iter()
            .enumerate()
            .map(|(i, &chunk_id)| (chunk_id, i as u32))
            .collect();

        Ok(Toc {
            header,
            chunk_ids,
            chunk_offset_lengths,
            chunk_perfect_hash_seeds,
            chunk_indices_without_perfect_hash,
            compression_blocks,
            compression_methods,
            signatures,
            chunk_metas,

            directory_index,
            file_map,
            file_map_lower,
            file_map_rev,
            chunk_id_map,
        })
    }
}
fn align(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}
impl Toc {
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

        let compression_block_size = self.header.compression_block_size;
        let first_block_index = (offset / compression_block_size as u64) as usize;
        let last_block_index = ((align(offset + size, compression_block_size as u64) - 1)
            / compression_block_size as u64) as usize;

        let num_compressed_blocks = (1 + last_block_index - first_block_index) as u32;
        let offset_on_disk = self.compression_blocks[first_block_index].get_offset();
        let mut compressed_size = 0;
        let mut partition_index = -1;

        for block_index in first_block_index..=last_block_index {
            let compression_block = &self.compression_blocks[block_index];
            compressed_size += compression_block.get_compressed_size() as u64;
            if partition_index < 0 {
                partition_index =
                    (compression_block.get_offset() / self.header.partition_size) as i32;
            }
        }

        let id = self.chunk_ids[toc_entry_index];

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

        let compression_block_size = self.header.compression_block_size;
        let first_block_index = (offset / compression_block_size as u64) as usize;
        let last_block_index = ((align(offset + size, compression_block_size as u64) - 1)
            / compression_block_size as u64) as usize;

        let blocks = &self.compression_blocks[first_block_index..=last_block_index];

        let mut max_buffer = 0;
        let mut total_size = 0;
        for b in blocks {
            total_size += b.get_uncompressed_size() as usize;
            max_buffer = max_buffer.max(b.get_compressed_size() as usize);
        }
        let mut data = vec![0; total_size];
        let mut buffer = vec![0; max_buffer];
        let mut cur = 0;
        for block in blocks {
            let out = &mut data[cur..cur + block.get_uncompressed_size() as usize];
            //println!("{i} {block:#?}");

            cas_stream.seek(SeekFrom::Start(block.get_offset()))?;
            match block.get_compression_method_index() {
                0 => {
                    cas_stream.read_exact(out)?;
                }
                1 => {
                    let tmp = &mut buffer[..block.get_compressed_size() as usize];
                    cas_stream.read_exact(tmp)?;
                    oodle_loader::decompress().unwrap()(tmp, out);
                }
                other => todo!("{other}"),
            }
            cur += block.get_uncompressed_size() as usize;
        }
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
impl FPackageId {
    fn from_name(name: &str) -> Self {
        let bytes = name
            .to_ascii_lowercase()
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<u8>>();
        Self(cityhasher::hash(bytes))
    }
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
        //write!(f, "FIoChunkId(")?;
        //for b in self.id {
        //    write!(f, "{:02X}", b)?;
        //}
        //write!(f, ")")
        f.debug_struct("FIoChunkId")
            .field("chunk_id", &hex::encode(self.get_chunk_id()))
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
impl FIoChunkId {
    fn from_package_id(package_id: FPackageId, chunk_index: u16, chunk_type: EIoChunkType) -> Self {
        let mut id = [0; 12];
        id[0..8].copy_from_slice(&u64::to_le_bytes(package_id.0));
        id[8..10].copy_from_slice(&u16::to_le_bytes(chunk_index));
        id[11] = chunk_type as u8;
        Self { id }
    }
    fn get_chunk_type(&self) -> EIoChunkType {
        EIoChunkType::from_repr(self.id[11]).unwrap()
    }
    fn get_chunk_id(&self) -> [u8; 11] {
        self.id[0..11].try_into().unwrap()
    }
}
#[derive(Debug)]
struct FIoContainerId {
    id: u64,
}
impl Readable for FIoContainerId {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { id: stream.de()? })
    }
}
#[derive(Debug, Clone, Copy)]
struct FIoOffsetAndLength {
    data: [u8; 10],
}
impl Readable for FIoOffsetAndLength {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { data: stream.de()? })
    }
}
impl FIoOffsetAndLength {
    fn get_offset(&self) -> u64 {
        let d = self.data;
        u64::from_be_bytes([0, 0, 0, d[0], d[1], d[2], d[3], d[4]])
    }
    fn get_length(&self) -> u64 {
        let d = self.data;
        u64::from_be_bytes([0, 0, 0, d[5], d[6], d[7], d[8], d[9]])
    }
}
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
impl FIoStoreTocCompressedBlockEntry {
    fn get_offset(&self) -> u64 {
        let d = self.data;
        u64::from_le_bytes([d[0], d[1], d[2], d[3], d[4], 0, 0, 0])
    }
    fn get_compressed_size(&self) -> u32 {
        let d = self.data;
        u32::from_le_bytes([d[5], d[6], d[7], 0])
    }
    fn get_uncompressed_size(&self) -> u32 {
        let d = self.data;
        u32::from_le_bytes([d[8], d[9], d[10], 0])
    }
    fn get_compression_method_index(&self) -> u8 {
        self.data[11]
    }
}
struct FIoChunkHash {
    data: [u8; 32],
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
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
struct FSHAHash {
    data: [u8; 20],
}
impl Readable for FSHAHash {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { data: stream.de()? })
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
    #[derive(Debug)]
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
impl Readable for FIoStoreTocEntryMetaFlags {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_bits(stream.de()?).context("invalid FIoStoreTocEntryMetaFlags value")
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, FromRepr)]
#[repr(u8)]
enum EIoStoreTocVersion {
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

#[derive(Debug)]
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

use directory_index::*;
use zen::get_package_name;
mod directory_index {
    use super::*;

    #[derive(Debug)]
    pub struct FIoDirectoryIndexResource {
        mount_point: String,
        directory_entries: Vec<FIoDirectoryIndexEntry>,
        file_entries: Vec<FIoFileIndexEntry>,
        string_table: Vec<String>,
    }
    impl FIoDirectoryIndexResource {
        pub fn read<S: Read>(stream: &mut S) -> Result<Self> {
            Ok(Self {
                mount_point: stream.de()?,
                directory_entries: stream.de()?,
                file_entries: stream.de()?,
                string_table: stream.de()?,
            })
        }
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
                fn de<S: Read>(stream: &mut S) -> Result<Self> {
                    Ok($name::new(stream.de()?))
                }
            }
            impl Readable for $name {
                fn de<S: Read>(stream: &mut S) -> Result<Self> {
                    Ok(Self(stream.de()?))
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
        fn de<S: Read>(stream: &mut S) -> Result<Self> {
            Ok(Self {
                name: stream.de()?,
                next_file_entry: stream.de()?,
                user_data: stream.de()?,
            })
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
        fn de<S: Read>(stream: &mut S) -> Result<Self> {
            Ok(Self {
                name: stream.de()?,
                first_child_entry: stream.de()?,
                next_sibling_entry: stream.de()?,
                first_file_entry: stream.de()?,
            })
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
