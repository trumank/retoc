mod compact_binary;
mod legacy_asset;
mod manifest;
mod name_map;
mod script_objects;
mod ser;
mod zen;

use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use bitflags::bitflags;
use clap::Parser;
use rayon::prelude::*;
use ser::*;
use strum::FromRepr;
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
enum Action {
    /// Extract manifest from .utoc
    Manifest(ActionManifest),
    /// List fils in .utoc (directory index)
    List(ActionList),
    /// Verify IO Store container
    Verify(ActionVerify),
    /// Extracts chunks (files) from .utoc
    Unpack(ActionUnpack),
}

#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    action: Action,
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.action {
        Action::Manifest(action) => action_manifest(action),
        Action::List(action) => action_list(action),
        Action::Verify(action) => action_verify(action),
        Action::Unpack(action) => action_unpack(action),
    }
}

fn action_manifest(args: ActionManifest) -> Result<()> {
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Serum/Serum/Content/Paks/pakchunk0-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Nuclear Nightmare/NuclearNightmare/Content/Paks/NuclearNightmare-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/The Isle/TheIsle/Content/Paks/pakchunk0-WindowsClient.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/FactoryGame-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/global.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/VisionsofManaDemo/VisionsofMana/Content/Paks/pakchunk0-WindowsNoEditor.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/AbioticFactor/AbioticFactor/Content/Paks/pakchunk0-Windows.utoc";

    let toc: Toc = BufReader::new(File::open(&args.utoc)?).ser()?;
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

fn action_list(args: ActionList) -> Result<()> {
    let toc: Toc = BufReader::new(File::open(&args.utoc)?).ser()?;
    for f in toc.file_map.keys() {
        println!("{f}");
    }
    Ok(())
}

fn action_verify(args: ActionVerify) -> Result<()> {
    let mut stream = BufReader::new(File::open(&args.utoc)?);
    let ucas = &args.utoc.with_extension("ucas");

    let toc: Toc = stream.ser()?;

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
            let data = toc.read(ucas, i as u32)?;
            let hash = blake3::hash(&data);

            println!(
                "{:>10} {:?} {} {:?}",
                i,
                hex::encode(meta.chunk_hash.data),
                hex::encode(hash.as_bytes()),
                meta.flags
            );

            if meta.chunk_hash.data[..20] != hash.as_bytes()[..20] {
                bail!("hash mismatch for chunk #{i}")
            }

            Ok(())
        },
    )?;

    Ok(())
}

fn action_unpack(args: ActionUnpack) -> Result<()> {
    let mut stream = BufReader::new(File::open(&args.utoc)?);
    let ucas = &args.utoc.with_extension("ucas");

    let toc: Toc = stream.ser()?;

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

impl Readable for FIoStoreTocHeader {}
impl ReadableBase for FIoStoreTocHeader {
    #[instrument(skip_all, name = "FIoStoreTocHeader")]
    fn ser<R: Read>(stream: &mut R) -> Result<Self> {
        let res = FIoStoreTocHeader {
            toc_magic: stream.ser()?,
            version: stream.ser()?,
            reserved0: stream.ser()?,
            reserved1: stream.ser()?,
            toc_header_size: stream.ser()?,
            toc_entry_count: stream.ser()?,
            toc_compressed_block_entry_count: stream.ser()?,
            toc_compressed_block_entry_size: stream.ser()?,
            compression_method_name_count: stream.ser()?,
            compression_method_name_length: stream.ser()?,
            compression_block_size: stream.ser()?,
            directory_index_size: stream.ser()?,
            partition_count: stream.ser()?,
            container_id: stream.ser()?,
            encryption_key_guid: stream.ser()?,
            container_flags: stream.ser()?,
            reserved3: stream.ser()?,
            reserved4: stream.ser()?,
            toc_chunk_perfect_hash_seeds_count: stream.ser()?,
            partition_size: stream.ser()?,
            toc_chunks_without_perfect_hash_count: stream.ser()?,
            reserved7: stream.ser()?,
            reserved8: stream.ser()?,
        };
        assert_eq!(&res.toc_magic, b"-==--==--==--==-");
        assert_eq!(res.toc_header_size, 0x90);
        Ok(res)
    }
}

#[instrument(skip_all)]
fn read_chunk_ids<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<Vec<FIoChunkId>> {
    stream.ser_ctx(header.toc_entry_count as usize)
}

#[instrument(skip_all)]
fn read_chunk_offsets<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoOffsetAndLength>> {
    stream.ser_ctx(header.toc_entry_count as usize)
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
    let chunk_perfect_hash_seeds = stream.ser_ctx(perfect_hash_seeds_count as usize)?;
    let chunk_indices_without_perfect_hash =
        stream.ser_ctx(chunks_without_perfect_hash_count as usize)?;

    Ok((chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash))
}

#[instrument(skip_all)]
fn read_compression_blocks<R: Read>(
    stream: &mut R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoStoreTocCompressedBlockEntry>> {
    stream.ser_ctx(header.toc_compressed_block_entry_count as usize)
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
                .ser_ctx::<Vec<u8>, _>(header.compression_method_name_length as usize)?
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
        let size = stream.ser::<u32>()? as usize;
        Some(TocSignatures {
            toc_signature: stream.ser_ctx(size)?,
            block_signature: stream.ser_ctx(size)?,
            chunk_block_signatures: stream
                .ser_ctx(header.toc_compressed_block_entry_count as usize)?,
        })
    } else {
        None
    })
}

#[instrument(skip_all)]
fn read_directory_index<R: Read>(stream: &mut R, header: &FIoStoreTocHeader) -> Result<Vec<u8>> {
    stream.ser_ctx(header.directory_index_size as usize)
}

#[instrument(skip_all)]
fn read_meta<S: Read>(
    stream: &mut S,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoStoreTocEntryMeta>> {
    read_array(header.toc_entry_count as usize, stream, |s| {
        let res = s.ser()?;
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
}
struct TocSignatures {
    toc_signature: Vec<u8>,
    block_signature: Vec<u8>,
    chunk_block_signatures: Vec<FSHAHash>,
}
impl Readable for Toc {}
impl ReadableBase for Toc {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        let header = stream.ser()?;

        let chunk_ids = read_chunk_ids(stream, &header)?;
        let chunk_offset_lengths = read_chunk_offsets(stream, &header)?;
        let (chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash) =
            read_hash_map(stream, &header)?;
        let compression_blocks = read_compression_blocks(stream, &header)?;
        let compression_methods = read_compression_methods(stream, &header)?;

        let signatures = read_chunk_block_signatures(stream, &header)?;
        let directory_index = read_directory_index(stream, &header)?; // TODO decrypt
        let chunk_metas = read_meta(stream, &header)?;

        // build indexes
        let mut chunk_id_to_index: HashMap<FIoChunkId, u32> = Default::default();
        for (chunk_index, &chunk_id) in chunk_ids.iter().enumerate() {
            chunk_id_to_index.insert(chunk_id, chunk_index as u32);
        }

        let mut file_map: HashMap<String, u32> = Default::default();
        let mut file_map_lower: HashMap<String, u32> = Default::default();
        let directory_index = if !directory_index.is_empty() {
            let directory_index =
                FIoDirectoryIndexResource::read(&mut Cursor::new(directory_index))?;
            directory_index.iter_root(|user_data, path| {
                let path = path.join("/");
                file_map_lower.insert(path.to_ascii_lowercase(), user_data);
                file_map.insert(path, user_data);
            });
            Some(directory_index)
        } else {
            None
        };

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
impl Readable for FIoChunkId {}
impl ReadableBase for FIoChunkId {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { id: stream.ser()? })
    }
}
impl FIoChunkId {
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
impl Readable for FIoContainerId {}
impl ReadableBase for FIoContainerId {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self { id: stream.ser()? })
    }
}
#[derive(Debug, Clone, Copy)]
struct FIoOffsetAndLength {
    data: [u8; 10],
}
impl Readable for FIoOffsetAndLength {}
impl ReadableBase for FIoOffsetAndLength {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            data: stream.ser()?,
        })
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
impl Readable for FIoStoreTocCompressedBlockEntry {}
impl ReadableBase for FIoStoreTocCompressedBlockEntry {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            data: stream.ser()?,
        })
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
impl Readable for FIoChunkHash {}
impl ReadableBase for FIoChunkHash {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            data: stream.ser()?,
        })
    }
}
#[derive(Debug)]
struct FIoStoreTocEntryMeta {
    chunk_hash: FIoChunkHash,
    flags: FIoStoreTocEntryMetaFlags,
}
impl Readable for FIoStoreTocEntryMeta {}
impl ReadableBase for FIoStoreTocEntryMeta {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            chunk_hash: stream.ser()?,
            flags: stream.ser()?,
        })
    }
}
#[derive(Debug)]
struct FGuid {
    a: u32,
    b: u32,
    c: u32,
    d: u32,
}
impl Readable for FGuid {}
impl ReadableBase for FGuid {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            a: stream.ser()?,
            b: stream.ser()?,
            c: stream.ser()?,
            d: stream.ser()?,
        })
    }
}
struct FSHAHash {
    data: [u8; 20],
}
impl Readable for FSHAHash {}
impl ReadableBase for FSHAHash {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(Self {
            data: stream.ser()?,
        })
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
impl Readable for EIoContainerFlags {}
impl ReadableBase for EIoContainerFlags {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_bits(stream.ser()?).context("invalid EIoContainerFlags value")
    }
}
impl Readable for FIoStoreTocEntryMetaFlags {}
impl ReadableBase for FIoStoreTocEntryMetaFlags {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_bits(stream.ser()?).context("invalid FIoStoreTocEntryMetaFlags value")
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
impl Readable for EIoStoreTocVersion {}
impl ReadableBase for EIoStoreTocVersion {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_repr(stream.ser()?).context("invalid EIoStoreTocVersion value")
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, FromRepr)]
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
                mount_point: stream.ser()?,
                directory_entries: stream.ser()?,
                file_entries: stream.ser()?,
                string_table: stream.ser()?,
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
            impl Readable for Option<$name> {}
            impl ReadableBase for Option<$name> {
                fn ser<S: Read>(stream: &mut S) -> Result<Self> {
                    Ok($name::new(stream.ser()?))
                }
            }
            impl Readable for $name {}
            impl ReadableBase for $name {
                fn ser<S: Read>(stream: &mut S) -> Result<Self> {
                    Ok(Self(stream.ser()?))
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
    impl Readable for FIoFileIndexEntry {}
    impl ReadableBase for FIoFileIndexEntry {
        fn ser<S: Read>(stream: &mut S) -> Result<Self> {
            Ok(Self {
                name: stream.ser()?,
                next_file_entry: stream.ser()?,
                user_data: stream.ser()?,
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
    impl Readable for FIoDirectoryIndexEntry {}
    impl ReadableBase for FIoDirectoryIndexEntry {
        fn ser<S: Read>(stream: &mut S) -> Result<Self> {
            Ok(Self {
                name: stream.ser()?,
                first_child_entry: stream.ser()?,
                next_sibling_entry: stream.ser()?,
                first_file_entry: stream.ser()?,
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
