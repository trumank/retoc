pub mod asset_conversion;
pub mod compact_binary;
pub mod compression;
pub mod container_header;
pub mod file_pool;
pub mod iostore;
pub mod iostore_writer;
pub mod legacy_asset;
pub mod logging;
pub mod manifest;
pub mod name_map;
pub mod script_objects;
pub mod ser;
pub mod shader_library;
pub mod verse_vm_types;
pub mod version;
pub mod version_heuristics;
pub mod zen;
pub mod zen_asset_conversion;

use anyhow::{Context, Result, bail};
use bitflags::bitflags;
use compression::{CompressionMethod, decompress};
use container_header::StoreEntry;
use file_pool::FilePool;
use fs_err as fs;
use ser::*;
use serde::{Deserialize, Serialize, Serializer};
use serde_with::serde_as;
use std::fmt::{Debug, Display, Formatter};
use std::path::Path;
use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};
use strum::{AsRefStr, FromRepr};
use tracing::instrument;

pub trait FileWriterTrait: Send + Sync {
    fn write_file(&self, path: String, allow_compress: bool, data: Vec<u8>) -> Result<()>;
}
pub struct FSFileWriter {
    dir: PathBuf,
}
impl FSFileWriter {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
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
pub struct ParallelPakWriter {
    pub entry_builder: repak::EntryBuilder,
    pub tx: std::sync::mpsc::SyncSender<(String, repak::PartialEntry<Vec<u8>>)>,
}
impl FileWriterTrait for ParallelPakWriter {
    fn write_file(&self, path: String, allow_compress: bool, data: Vec<u8>) -> Result<()> {
        let entry = self.entry_builder.build_entry(allow_compress, data)?;
        self.tx.send((path, entry))?;
        Ok(())
    }
}
pub struct NullFileWriter;
impl FileWriterTrait for NullFileWriter {
    fn write_file(&self, _path: String, _allow_compress: bool, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }
}

pub trait FileReaderTrait: Send + Sync {
    fn read(&self, path: &UEPath) -> Result<Vec<u8>>;
    fn read_opt(&self, path: &UEPath) -> Result<Option<Vec<u8>>>;
    fn list_files(&self) -> Result<Vec<UEPathBuf>>;
}
pub struct FSFileReader {
    dir: PathBuf,
}
impl FSFileReader {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
        Self { dir: dir.into() }
    }
}
impl FileReaderTrait for FSFileReader {
    fn read(&self, path: &UEPath) -> Result<Vec<u8>> {
        Ok(fs::read(self.dir.join(path.as_str()))?)
    }
    fn read_opt(&self, path: &UEPath) -> Result<Option<Vec<u8>>> {
        read_file_opt(self.dir.join(path.as_str()))
    }
    fn list_files(&self) -> Result<Vec<UEPathBuf>> {
        fn visit_dirs<F>(dir: &Path, cb: &mut F) -> std::io::Result<()>
        where
            F: FnMut(&fs::DirEntry),
        {
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        visit_dirs(&path, cb)?;
                    } else {
                        cb(&entry);
                    }
                }
            }
            Ok(())
        }

        let mut files = vec![];
        visit_dirs(&self.dir, &mut |file| {
            let file_path = file.path();
            let file_relative_path = file_path.strip_prefix(&self.dir).expect("Failed to strip dir prefix");
            files.push(to_ue_path(file_relative_path));
        })?;
        Ok(files)
    }
}
pub struct PakFileReader {
    pak: repak::PakReader,
    file: FilePool,
}
impl PakFileReader {
    pub fn new<P: Into<PathBuf>>(pak: P) -> Result<Self> {
        let file = FilePool::new(pak, rayon::max_num_threads())?;
        let pak = repak::PakBuilder::new().reader(file.acquire()?.file())?;
        Ok(Self { file, pak })
    }
}
impl FileReaderTrait for PakFileReader {
    fn read(&self, path: &UEPath) -> Result<Vec<u8>> {
        let mut handle = self.file.acquire()?;
        Ok(self.pak.get(path.as_str(), handle.file())?)
    }
    fn read_opt(&self, path: &UEPath) -> Result<Option<Vec<u8>>> {
        let mut handle = self.file.acquire()?;
        match self.pak.get(path.as_str(), handle.file()) {
            Ok(data) => Ok(Some(data)),
            Err(repak::Error::MissingEntry(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
    fn list_files(&self) -> Result<Vec<UEPathBuf>> {
        Ok(self.pak.files().into_iter().map(Into::into).collect())
    }
}

fn read_file_opt<P: AsRef<Path>>(path: P) -> Result<Option<Vec<u8>>> {
    match fs::read(path.as_ref()) {
        Ok(data) => Ok(Some(data)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn build_verse_cell_store(script_cells: &Vec<VerseScriptCell>) -> Arc<ZenScriptCellsStore> {
    let mut mutable_cell_store = ZenScriptCellsStore::create_empty();
    mutable_cell_store.add_vm_intrinsics();
    for additional_script_cell in script_cells {
        mutable_cell_store.add_script_cell(additional_script_cell.clone());
    }
    Arc::new(mutable_cell_store)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PackageTestMetadata {
    pub toc_version: EIoStoreTocVersion,
    pub container_header_version: EIoContainerHeaderVersion,
    pub package_file_version: Option<FPackageFileVersion>,
    pub store_entry: Option<StoreEntry>,
}

#[derive(Default)]
pub struct Config {
    pub aes_keys: HashMap<FGuid, AesKey>,
    pub container_header_version_override: Option<EIoContainerHeaderVersion>,
    pub toc_version_override: Option<EIoStoreTocVersion>,
}

#[derive(Debug, Clone)]
pub struct AesKey(aes::Aes256);
impl std::str::FromStr for AesKey {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use aes::cipher::KeyInit;
        use base64::{Engine as _, engine::general_purpose};
        let try_parse = |bytes: Vec<_>| aes::Aes256::new_from_slice(&bytes).ok().map(AesKey);
        hex::decode(s.strip_prefix("0x").unwrap_or(s))
            .ok()
            .and_then(try_parse)
            .or_else(|| general_purpose::STANDARD_NO_PAD.decode(s.trim_end_matches('=')).ok().and_then(try_parse))
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
    read_array(header.toc_entry_count as usize, stream, |s| s.de_ctx(header.version))
}

#[instrument(skip_all)]
fn read_chunk_offsets<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<Vec<FIoOffsetAndLength>> {
    stream.de_ctx(header.toc_entry_count as usize)
}

#[instrument(skip_all)]
fn read_hash_map<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<(Vec<i32>, Vec<i32>)> {
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
    let chunk_indices_without_perfect_hash = stream.de_ctx(chunks_without_perfect_hash_count as usize)?;

    Ok((chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash))
}

#[instrument(skip_all)]
fn read_compression_blocks<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<Vec<FIoStoreTocCompressedBlockEntry>> {
    stream.de_ctx(header.toc_compressed_block_entry_count as usize)
}

#[instrument(skip_all)]
fn read_compression_methods<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<Vec<CompressionMethod>> {
    let mut methods = vec![];
    for _ in 0..header.compression_method_name_count {
        let name = String::from_utf8(stream.de_ctx::<Vec<u8>, _>(header.compression_method_name_length as usize)?.into_iter().take_while(|&b| b != 0).collect())?;
        let method = CompressionMethod::from_str_ignore_case(&name).with_context(|| format!("unknown compression method: {name:?}"))?;
        methods.push(method);
    }
    Ok(methods)
}
#[instrument(skip_all)]
fn write_compression_methods<S: Write>(s: &mut S, toc: &Toc) -> Result<()> {
    for name in &toc.compression_methods {
        let buffer: Vec<u8> = name.as_ref().as_bytes().iter().copied().chain(std::iter::repeat(0)).take(32).collect();
        s.ser_no_length(&buffer)?;
    }
    Ok(())
}

#[instrument(skip_all)]
fn read_chunk_block_signatures<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<Option<TocSignatures>> {
    let is_signed = header.container_flags.contains(EIoContainerFlags::Signed);
    Ok(if is_signed {
        let size = stream.de::<u32>()? as usize;
        Some(TocSignatures {
            toc_signature: stream.de_ctx(size)?,
            block_signature: stream.de_ctx(size)?,
            chunk_block_signatures: stream.de_ctx(header.toc_compressed_block_entry_count as usize)?,
        })
    } else {
        None
    })
}

#[instrument(skip_all)]
fn read_directory_index<R: Read>(stream: &mut R, header: &FIoStoreTocHeader, config: &Config) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = stream.de_ctx(header.directory_index_size as usize)?;

    if header.container_flags.contains(EIoContainerFlags::Encrypted) {
        use aes::cipher::BlockDecrypt;

        let key = config.aes_keys.get(&header.encryption_key_guid).context("missing encryption key")?;
        for block in buf.chunks_mut(16) {
            key.0.decrypt_block(block.into());
        }
    }

    Ok(buf)
}

#[instrument(skip_all)]
fn read_meta<S: Read>(s: &mut S, header: &FIoStoreTocHeader) -> Result<Vec<FIoStoreTocEntryMeta>> {
    read_array(header.toc_entry_count as usize, s, |s| {
        let mut chunk_hash;
        let flags;
        if header.version >= EIoStoreTocVersion::ReplaceIoChunkHashWithIoHash {
            chunk_hash = FIoChunkHash([0; 32]);
            s.read_exact(&mut chunk_hash.0[0..20])?;
            flags = s.de()?;
            s.read_exact(&mut [0; 3])?;
        } else {
            chunk_hash = s.de()?;
            flags = s.de()?;
        }
        Ok(FIoStoreTocEntryMeta { chunk_hash, flags })
    })
}
#[instrument(skip_all)]
fn write_meta<S: Write>(s: &mut S, toc: &Toc) -> Result<()> {
    for meta in &toc.chunk_metas {
        if toc.version >= EIoStoreTocVersion::ReplaceIoChunkHashWithIoHash {
            s.write_all(&meta.chunk_hash.0[0..20])?;
            s.ser(&meta.flags)?;
            s.write_all(&[0; 3])?;
        } else {
            s.ser(&meta.chunk_hash)?;
            s.ser(&meta.flags)?;
        }
    }
    Ok(())
}

// UTF-8 path with '/' as separator
pub type UEPath = typed_path::Utf8UnixPath;
pub type UEPathBuf = typed_path::Utf8UnixPathBuf;
pub type UEPathComponent<'a> = typed_path::Utf8UnixComponent<'a>;

pub fn to_ue_path(path: &Path) -> UEPathBuf {
    let native_path = typed_path::Utf8NativePath::from_bytes_path(typed_path::NativePath::new(path.as_os_str().as_encoded_bytes())).expect("Path did not contain valid UTF-8 characters");
    native_path.with_encoding()
}

pub fn pak_path_to_game_path(pak_path: &UEPath) -> Option<String> {
    let mut components = pak_path.components();
    let a = components.next();
    let b = components.next();

    match (a, b) {
        (Some(UEPathComponent::Normal(_)), Some(UEPathComponent::Normal(b))) if b.eq_ignore_ascii_case("Plugins") => {
            let mut last = None;
            loop {
                match components.next() {
                    Some(UEPathComponent::Normal(c)) if c.eq_ignore_ascii_case("Content") => break last.map(|plugin| UEPath::new("/").join(plugin).join(components.as_path())),
                    Some(UEPathComponent::Normal(next)) => {
                        last = Some(next);
                    }
                    _ => break None,
                }
            }
        }
        (Some(UEPathComponent::Normal(a)), Some(UEPathComponent::Normal(b))) if a.eq_ignore_ascii_case("Engine") && b.eq_ignore_ascii_case("Content") => Some(UEPath::new("/Engine").join(components.as_path())),
        (Some(UEPathComponent::Normal(_)), Some(UEPathComponent::Normal(b))) if b.eq_ignore_ascii_case("Content") => Some(UEPath::new("/Game").join(components.as_path())),
        _ => None,
    }
    .map(|p| p.to_string())
}

#[derive(Default)]
#[allow(unused)]
pub struct Toc {
    pub config: Arc<Config>,

    // serialized members
    pub chunks: Vec<FIoChunkId>,
    pub chunk_offset_lengths: Vec<FIoOffsetAndLength>,
    pub chunk_perfect_hash_seeds: Vec<i32>,
    pub chunk_indices_without_perfect_hash: Vec<i32>,
    pub compression_blocks: Vec<FIoStoreTocCompressedBlockEntry>,
    pub compression_methods: Vec<CompressionMethod>,
    pub signatures: Option<TocSignatures>,
    pub chunk_metas: Vec<FIoStoreTocEntryMeta>,

    // serialized in header
    pub version: EIoStoreTocVersion,
    pub container_id: FIoContainerId,
    pub compression_block_size: u32,
    pub partition_size: u64,
    pub partition_count: u32,
    pub encryption_key_guid: FGuid,
    pub container_flags: EIoContainerFlags,

    // transient indexes
    directory_index: FIoDirectoryIndexResource,
    pub file_map: HashMap<String, u32>,
    file_map_lower: HashMap<String, u32>,
    file_map_rev: HashMap<u32, String>,
    chunk_id_map: HashMap<FIoChunkId, u32>,
}
#[allow(unused)]
pub struct TocSignatures {
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
        let (chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash) = read_hash_map(stream, &header)?;
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
        let directory_index = if !directory_index.is_empty() { FIoDirectoryIndexResource::de(&mut Cursor::new(directory_index))? } else { FIoDirectoryIndexResource::default() };
        directory_index.iter_root(|user_data, path| {
            let path = path.join("/");
            file_map_lower.insert(path.to_ascii_lowercase(), user_data);
            file_map.insert(path.clone(), user_data);
            file_map_rev.insert(user_data, path);
        });
        let chunk_id_map = chunk_ids.iter().enumerate().map(|(i, &chunk_id)| (chunk_id, i as u32)).collect();

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
        let mut container_flags = EIoContainerFlags::empty();

        container_flags |= EIoContainerFlags::Indexed;
        let mut directory_index_buffer = vec![];
        self.directory_index.ser(&mut Cursor::new(&mut directory_index_buffer))?;
        // TODO encrypt directory index

        let header = FIoStoreTocHeader {
            toc_magic: FIoStoreTocHeader::MAGIC,
            version: self.version,
            reserved0: 0,
            reserved1: 0,
            toc_header_size: std::mem::size_of::<FIoStoreTocHeader>() as u32,
            toc_entry_count: self.chunks.len() as u32,
            toc_compressed_block_entry_count: self.compression_blocks.len() as u32,
            toc_compressed_block_entry_size: std::mem::size_of::<FIoStoreTocCompressedBlockEntry>() as u32,
            compression_method_name_count: self.compression_methods.len() as u32,
            compression_method_name_length: 32,
            compression_block_size: self.compression_block_size,
            directory_index_size: directory_index_buffer.len() as u32,
            partition_count: 1,
            container_id: self.container_id,
            encryption_key_guid: Default::default(),
            container_flags,
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
        write_compression_methods(s, self)?;
        s.ser_no_length(&directory_index_buffer)?;
        write_meta(s, self)?;

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
        if let Ok(parsed_number) = right.parse::<i32>()
            && parsed_number >= 0
            && parsed_number.to_string() == right
        {
            name_without_number = left;
            name_number = parsed_number + 1; // stored as 1 more than the actual number
        }
    }
    (name_without_number, name_number)
}

impl Toc {
    pub fn new() -> Self {
        Self::default()
    }
    /// get absolute path (including mount point) for given chunk ID if has one
    pub fn file_name(&self, chunk_id: FIoChunkId) -> Option<String> {
        self.chunk_id_map
            .get(&chunk_id.with_version(self.version))
            .and_then(|index| self.file_map_rev.get(index))
            .map(|path| UEPath::new(&self.directory_index.mount_point).join(path).to_string())
    }
    #[allow(unused)]
    pub fn get_chunk_info(&self, file_name: &str) -> FIoStoreTocChunkInfo {
        let toc_entry_index = self.file_map[file_name] as usize;
        let meta = &self.chunk_metas[toc_entry_index];
        let offset_and_length = &self.chunk_offset_lengths[toc_entry_index];

        let mut hash = FIoChunkHash([0; 32]);
        // copy only first 20 bytes for some reason
        hash.0[..20].copy_from_slice(&meta.chunk_hash.0[..20]);

        let offset = offset_and_length.get_offset();
        let size = offset_and_length.get_length();

        let compression_block_size = self.compression_block_size;
        let first_block_index = (offset / compression_block_size as u64) as usize;
        let last_block_index = ((align_u64(offset + size, compression_block_size as u64) - 1) / compression_block_size as u64) as usize;

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
    #[allow(unused)]
    fn get_chunk_id_entry_index(&self, chunk_id: FIoChunkId) -> Result<u32> {
        self.chunk_id_map.get(&chunk_id).copied().with_context(|| "container does not contain entry for {chunk_id}")
    }
    pub fn read<C: Read + Seek>(&self, cas_stream: &mut C, toc_entry_index: u32) -> Result<Vec<u8>> {
        let offset_and_length = &self.chunk_offset_lengths[toc_entry_index as usize];
        let offset = offset_and_length.get_offset();
        let size = offset_and_length.get_length();

        let compression_block_size = self.compression_block_size;
        let first_block_index = (offset / compression_block_size as u64) as usize;
        let last_block_index = ((align_u64(offset + size, compression_block_size as u64) - 1) / compression_block_size as u64) as usize;

        let blocks = &self.compression_blocks[first_block_index..=last_block_index];
        let aes_key = if self.container_flags.contains(EIoContainerFlags::Encrypted) {
            Some(self.config.aes_keys.get(&self.encryption_key_guid).with_context(|| format!("container is encrypted but no AES key for {:?} supplied", self.encryption_key_guid))?)
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
            let compression_method = if compression_method_index == 0 { None } else { Some(self.compression_methods[compression_method_index - 1]) };
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FPackageId(pub u64);
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
impl FromStr for FPackageId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(FPackageId(s.parse()?))
    }
}

fn lower_utf16_cityhash(s: &str) -> u64 {
    let bytes = s.to_ascii_lowercase().encode_utf16().flat_map(u16::to_le_bytes).collect::<Vec<u8>>();
    cityhasher::hash(bytes)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_package_id() {
        let package_id = FPackageId::from_name("/ACLPlugin/ACLAnimBoneCompressionSettings");
        let _chunk_id = FIoChunkId::from_package_id(package_id, 0, EIoChunkType::ExportBundleData);
        // dbg!(chunk_id);
    }
}

pub use chunk_id::{FIoChunkId, FIoChunkIdRaw};
mod chunk_id {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct FIoChunkIdRaw {
        pub id: [u8; 12],
    }
    impl std::fmt::Debug for FIoChunkIdRaw {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FIoChunkIdRaw").field("chunk_id", &hex::encode(self.id)).finish()
        }
    }
    impl Readable for FIoChunkIdRaw {
        fn de<S: Read>(s: &mut S) -> Result<Self> {
            Ok(Self { id: s.de()? })
        }
    }
    impl Writeable for FIoChunkIdRaw {
        fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
            s.ser(&self.id)
        }
    }
    impl FromStr for FIoChunkIdRaw {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
            let id = hex::decode(s).ok().and_then(|bytes| bytes.try_into().ok()).context("expected 12 byte hex string")?;
            Ok(FIoChunkIdRaw { id })
        }
    }
    impl AsRef<[u8]> for FIoChunkIdRaw {
        fn as_ref(&self) -> &[u8] {
            &self.id
        }
    }
    impl TryFrom<Vec<u8>> for FIoChunkIdRaw {
        type Error = Vec<u8>;

        fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
            Ok(Self { id: value.try_into()? })
        }
    }

    #[derive(Clone, Copy)]
    pub struct FIoChunkId {
        id: [u8; 12],
    }
    impl std::cmp::Eq for FIoChunkId {}
    impl PartialEq for FIoChunkId {
        fn eq(&self, other: &Self) -> bool {
            self.get_raw() == other.get_raw()
        }
    }
    impl std::cmp::PartialOrd for FIoChunkId {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl std::cmp::Ord for FIoChunkId {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.get_raw().cmp(&other.get_raw())
        }
    }
    impl std::hash::Hash for FIoChunkId {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.get_raw().hash(state)
        }
    }
    impl std::fmt::Debug for FIoChunkId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            use std::fmt::Write;
            let mut buf = String::new();
            for b in &self.id[..11] {
                write!(&mut buf, "{b:02x}").unwrap();
            }
            // special case last byte and only print hex value if we know it
            if self.id[11] >> 6 & 1 != 0 {
                // has version so get raw byte value
                let is_new = self.id[11] >> 7 != 0; // read version bit
                write!(&mut buf, "{:02x}", self.get_chunk_type().value(is_new)).unwrap();
            } else {
                // version info unknown so raw byte value unknown
                write!(&mut buf, "??").unwrap();
            }
            f.debug_struct("FIoChunkId").field("chunk_id", &buf).field("chunk_type", &self.get_chunk_type()).finish()
        }
    }
    impl ReadableCtx<EIoStoreTocVersion> for FIoChunkId {
        fn de<S: Read>(s: &mut S, version: EIoStoreTocVersion) -> Result<Self> {
            let raw: FIoChunkIdRaw = s.de()?;
            Ok(Self::from_raw(raw, version))
        }
    }
    impl Writeable for FIoChunkId {
        fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
            s.ser(&self.get_raw())
        }
    }
    impl FIoChunkId {
        pub fn from_raw(raw: FIoChunkIdRaw, version: EIoStoreTocVersion) -> Self {
            let mut id: [u8; 12] = raw.id;
            let is_new = version > EIoStoreTocVersion::PerfectHash;
            id[11] = EIoChunkType::new(id[11], is_new) as u8;
            id[11] |= (is_new as u8) << 7; // set bit to is_new
            id[11] |= 1 << 6; // set bit to indicate has version
            Self { id }
        }
        pub fn with_version(self, version: EIoStoreTocVersion) -> Self {
            let mut id: [u8; 12] = self.id;
            let is_new = version > EIoStoreTocVersion::PerfectHash;
            id[11] |= (is_new as u8) << 7; // set bit to is_new
            id[11] |= 1 << 6; // set bit to indicate has version
            Self { id }
        }
        pub fn create(chunk_id: u64, chunk_index: u16, chunk_type: EIoChunkType) -> Self {
            let mut id = [0; 12];
            id[0..8].copy_from_slice(&u64::to_le_bytes(chunk_id));
            id[8..10].copy_from_slice(&u16::to_le_bytes(chunk_index));
            id[11] = chunk_type as u8;
            Self { id }
        }
        pub fn from_package_id(package_id: FPackageId, chunk_index: u16, chunk_type: EIoChunkType) -> Self {
            Self::create(package_id.0, chunk_index, chunk_type)
        }
        pub fn create_shader_code_chunk_id(shader_hash: &FSHAHash) -> Self {
            let mut id = [0; 12];
            id[0..11].copy_from_slice(&shader_hash.0[0..11]);
            id[11] = EIoChunkType::ShaderCode as u8;
            Self { id }
        }
        pub fn create_shader_library_chunk_id(shader_library_name: &str, shader_format_name: &str) -> Self {
            let name = format!("{shader_library_name}-{shader_format_name}");
            let hash = lower_utf16_cityhash(&name);
            Self::create(hash, 0, EIoChunkType::ShaderCodeLibrary)
        }
        pub fn get_chunk_id(&self) -> u64 {
            u64::from_le_bytes(self.id[0..8].try_into().unwrap())
        }
        pub fn get_chunk_type(&self) -> EIoChunkType {
            EIoChunkType::from_repr(self.id[11] & 0b11_1111).unwrap()
        }
        pub fn get_raw(&self) -> FIoChunkIdRaw {
            let mut id = self.id;
            if id[11] >> 6 & 1 == 0 {
                panic!("no version info, cannot convert to raw");
            }
            let is_new = id[11] >> 7 != 0;
            id[11] = self.get_chunk_type().value(is_new);
            FIoChunkIdRaw { id }
        }
        pub fn get_package_id(&self) -> FPackageId {
            FPackageId(u64::from_le_bytes(self.id[0..8].try_into().unwrap()))
        }
    }
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FIoContainerId(pub u64);
impl FIoContainerId {
    pub fn from_name(name: &str) -> Self {
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
pub struct FIoOffsetAndLength {
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
pub struct FIoStoreTocCompressedBlockEntry {
    data: [u8; 12],
}
impl std::fmt::Debug for FIoStoreTocCompressedBlockEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FIoStoreTocCompressedBlockEntry")
            .field("offset", &self.get_offset())
            .field("compressed_size", &self.get_compressed_size())
            .field("uncompressed_size", &self.get_uncompressed_size())
            .field("compression_method_index", &self.get_compression_method_index())
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
    pub(crate) fn new(offset: u64, compressed_size: u32, uncompressed_size: u32, compression_method_index: u8) -> Self {
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
pub struct FIoChunkHash(pub [u8; 32]);
impl FIoChunkHash {
    pub fn from_blake3(hash: &[u8; 32]) -> FIoChunkHash {
        let mut data = [0; 32];
        data[0..20].copy_from_slice(&hash[0..20]);
        Self(data)
    }
}
impl std::fmt::Debug for FIoChunkHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FIoChunkHash(")?;
        for b in self.0 {
            write!(f, "{:02X}", b)?;
        }
        write!(f, ")")
    }
}
impl Readable for FIoChunkHash {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self(stream.de()?))
    }
}
impl Writeable for FIoChunkHash {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.0)
    }
}
#[derive(Debug)]
pub struct FIoStoreTocEntryMeta {
    pub chunk_hash: FIoChunkHash,
    pub flags: FIoStoreTocEntryMetaFlags,
}
impl Readable for FIoStoreTocEntryMeta {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { chunk_hash: stream.de()?, flags: stream.de()? })
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
pub struct FGuid {
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
        Ok(())
    }
}
#[serde_as]
#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct FSHAHash(#[serde_as(as = "serde_with::hex::Hex")] [u8; 20]);
impl Readable for FSHAHash {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self(stream.de()?))
    }
}
impl Writeable for FSHAHash {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.0)
    }
}
impl std::fmt::Debug for FSHAHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FSHAHash(")?;
        for b in self.0 {
            write!(f, "{:02X}", b)?;
        }
        write!(f, ")")
    }
}

bitflags! {
    #[derive(Debug, Default)]
    pub struct EIoContainerFlags: u8 {
        const Compressed = 0b0001;
        const Encrypted  = 0b0010;
        const Signed     = 0b0100;
        const Indexed    = 0b1000;
    }
    #[derive(Debug)]
    pub struct FIoStoreTocEntryMetaFlags: u8 {
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, FromRepr, clap::ValueEnum, Serialize, Deserialize)]
#[repr(u8)]
#[clap(rename_all = "verbatim")]
pub enum EIoStoreTocVersion {
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
pub struct FIoStoreTocHeader {
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
#[allow(unused)]
pub struct FIoStoreTocChunkInfo {
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
pub enum EIoChunkType {
    Invalid,
    ExportBundleData,
    BulkData,
    OptionalBulkData,
    MemoryMappedBulkData,
    ScriptObjects,
    ContainerHeader,
    ExternalFile,
    ShaderCodeLibrary,
    ShaderCode,
    PackageStoreEntry,
    DerivedData,
    EditorDerivedData,
    PackageResource,

    // old chunk types
    InstallManifest,
    LoaderGlobalMeta,
    LoaderInitialLoadMeta,
    LoaderGlobalNames,
    LoaderGlobalNameHashes,
}
impl EIoChunkType {
    pub fn new(value: u8, is_new: bool) -> Self {
        use EIoChunkType::*;
        if is_new {
            match value {
                0 => Invalid,
                1 => ExportBundleData,
                2 => BulkData,
                3 => OptionalBulkData,
                4 => MemoryMappedBulkData,
                5 => ScriptObjects,
                6 => ContainerHeader,
                7 => ExternalFile,
                8 => ShaderCodeLibrary,
                9 => ShaderCode,
                10 => PackageStoreEntry,
                11 => DerivedData,
                12 => EditorDerivedData,
                13 => PackageResource,
                _ => panic!("invalid chunk type for version >= UE5: {value}"),
            }
        } else {
            match value {
                0 => Invalid,
                1 => InstallManifest,
                2 => ExportBundleData,
                3 => BulkData,
                4 => OptionalBulkData,
                5 => MemoryMappedBulkData,
                6 => LoaderGlobalMeta,
                7 => LoaderInitialLoadMeta,
                8 => LoaderGlobalNames,
                9 => LoaderGlobalNameHashes,
                10 => ContainerHeader,
                11 => ShaderCodeLibrary,
                12 => ShaderCode,
                _ => panic!("invalid chunk type for version < UE5: {value}"),
            }
        }
    }
    pub fn value(self, is_new: bool) -> u8 {
        use EIoChunkType::*;
        if is_new {
            match self {
                Invalid => 0,
                ExportBundleData => 1,
                BulkData => 2,
                OptionalBulkData => 3,
                MemoryMappedBulkData => 4,
                ScriptObjects => 5,
                ContainerHeader => 6,
                ExternalFile => 7,
                ShaderCodeLibrary => 8,
                ShaderCode => 9,
                PackageStoreEntry => 10,
                DerivedData => 11,
                EditorDerivedData => 12,
                PackageResource => 13,
                _ => panic!("invalid chunk type for version >= UE5: {self:?}"),
            }
        } else {
            match self {
                Invalid => 0,
                InstallManifest => 1,
                ExportBundleData => 2,
                BulkData => 3,
                OptionalBulkData => 4,
                MemoryMappedBulkData => 5,
                LoaderGlobalMeta => 6,
                LoaderInitialLoadMeta => 7,
                LoaderGlobalNames => 8,
                LoaderGlobalNameHashes => 9,
                ContainerHeader => 10,
                ShaderCodeLibrary => 11,
                ShaderCode => 12,
                _ => panic!("invalid chunk type for version < UE5: {self:?}"),
            }
        }
    }
}

use crate::container_header::EIoContainerHeaderVersion;
use crate::zen::{FPackageFileVersion, VerseScriptCell, ZenScriptCellsStore};
use directory_index::*;

mod directory_index {
    use typed_path::Utf8Component as _;

    use super::*;

    #[derive(Debug, Default)]
    pub struct FIoDirectoryIndexResource {
        pub(crate) mount_point: UEPathBuf,
        directory_entries: Vec<FIoDirectoryIndexEntry>,
        file_entries: Vec<FIoFileIndexEntry>,
        string_table: Vec<String>,
    }
    impl Readable for FIoDirectoryIndexResource {
        fn de<S: Read>(s: &mut S) -> Result<Self> {
            Ok(Self {
                mount_point: s.de::<String>()?.into(),
                directory_entries: s.de()?,
                file_entries: s.de()?,
                string_table: s.de()?,
            })
        }
    }
    impl Writeable for FIoDirectoryIndexResource {
        fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
            if !self.file_entries.is_empty() {
                s.ser(&self.mount_point.as_str())?;
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
            IdName(self.string_table.iter().position(|n| n == name).unwrap_or_else(|| {
                self.string_table.push(name.to_string());
                self.string_table.len() - 1
            }) as u32)
        }
        pub fn add_file(&mut self, path: &UEPath, user_data: u32) {
            let mut components = path.components();
            let file_name = components.next_back().unwrap();
            let mut dir_index = self.ensure_root();
            for dir_name in components {
                dir_index = self.get_or_create_dir(dir_index, dir_name.as_str());
            }
            let file_index = self.get_or_create_file(dir_index, file_name.as_str());
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

            let entries = HashMap::from([("this/b/test2.txt".to_string(), 1), ("this/is/a/test1.txt".to_string(), 2), ("this/test2.txt".to_string(), 3), ("this/is/a/test2.txt".to_string(), 4)]);

            for (path, data) in &entries {
                index.add_file(UEPath::new(path), *data);
            }

            // dbg!(&index);

            //for d in &dir.directory_entries {
            //    println!("");
            //}

            let mut new_map = HashMap::new();
            index.iter_root(|user_data, path| {
                new_map.insert(path.join("/"), user_data);
                // println!("{} = {}", path.join("/"), user_data);
            });
            assert_eq!(entries, new_map);
        }
    }
}
