use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::Result;
use bitflags::bitflags;
use byteorder::{ReadBytesExt, LE};
use strum::FromRepr;
use tracing::instrument;

fn align(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

fn main() -> Result<()> {
    let path_ucas = "/home/truman/.local/share/Steam/steamapps/common/AbioticFactor/AbioticFactor/Content/Paks/pakchunk0-Windows.ucas";

    let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/AbioticFactor/AbioticFactor/Content/Paks/pakchunk0-Windows.utoc";
    let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Serum/Serum/Content/Paks/pakchunk0-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Nuclear Nightmare/NuclearNightmare/Content/Paks/NuclearNightmare-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/The Isle/TheIsle/Content/Paks/pakchunk0-WindowsClient.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/VisionsofManaDemo/VisionsofMana/Content/Paks/pakchunk0-WindowsNoEditor.utoc";
    let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/FactoryGame-Windows.utoc";
    let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/global.utoc";

    let mut ucas_stream = BufReader::new(File::open(Path::new(path_utoc).with_extension("ucas"))?);

    let mut stream = BufReader::new(File::open(path_utoc)?);

    ser_hex::read("trace.json", &mut stream, |s| read(s, ucas_stream))?;
    //read(stream, ucas_stream)?;

    Ok(())
}

#[instrument(skip_all)]
fn read_array<R: Read, T, F>(len: usize, mut stream: R, mut f: F) -> Result<Vec<T>>
where
    F: FnMut(&mut R) -> Result<T>,
{
    let mut array = Vec::with_capacity(len);
    for _ in 0..len {
        array.push(f(&mut stream)?);
    }
    Ok(array)
}

#[instrument(skip_all)]
fn read<R: Read, U: Read + Seek>(mut stream: R, mut ucas_stream: U) -> Result<()> {
    let header = read_header(&mut stream)?;
    dbg!(&header);

    //let total_toc_size = len - 0x90 /* sizeof(FIoStoreTocHeader) */;
    //let toc_meta_size = header.toc_entry_count as u64 *  1 /* sizeof(FIoStoreTocEntryMeta) */;

    //let default_toc_size = total_toc_size - (header.directory_index_size as u64 + toc_meta_size);
    //let toc_size = default_toc_size;

    // START TOC READ
    let chunk_ids = read_chunk_ids(&mut stream, &header)?;

    let chunk_offset_lengths = read_chunk_offsets(&mut stream, &header)?;

    let (chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash) =
        read_hash_map(&mut stream, &header)?;

    let compression_blocks = read_compression_blocks(&mut stream, &header)?;
    let compression_methods = read_compression_methods(&mut stream, &header)?;
    dbg!(&header.compression_method_name_length);

    // TODO decrypt
    read_chunk_block_signatures(&mut stream, &header)?;
    let directory_index = read_directory_index(&mut stream, &header)?;
    let chunk_metas = read_meta(&mut stream, &header)?;
    // END TOC READ

    // build index
    let mut chunk_id_to_index: HashMap<FIoChunkId, u32> = Default::default();
    for chunk_index in 0..chunk_ids.len() {
        chunk_id_to_index.insert(chunk_ids[chunk_index], chunk_index as u32);
    }

    let mut file_map: HashMap<String, u32> = Default::default();
    let directory_index = if !directory_index.is_empty() {
        let directory_index = FIoDirectoryIndexResource::read(Cursor::new(directory_index))?;
        directory_index.iter_root(|user_data, path| {
            let path = path.join("/");
            //println!("{}", path);
            file_map.insert(path, user_data);
        });
        Some(directory_index)
    } else {
        None
    };

    let toc = Toc {
        header,
        chunk_ids,
        chunk_offset_lengths,
        chunk_perfect_hash_seeds,
        chunk_indices_without_perfect_hash,
        compression_blocks,
        compression_methods,
        chunk_metas,

        directory_index,
        file_map,
    };

    let output = Path::new("global");

    for file_name in toc.file_map.keys() {
        //"FactoryGame/Content/FactoryGame/Interface/UI/InGame/OutputSlotData_Struct.uasset"
        let chunk_info = toc.get_chunk_info(file_name);
        dbg!(&chunk_info);

        let path = output.join(file_name);
        let dir = path.parent().unwrap();

        println!("{file_name}");
        let data = toc.read(&mut ucas_stream, toc.file_map[file_name])?;

        std::fs::create_dir_all(dir)?;
        std::fs::write(path, data)?;
    }

    //let data = toc.read(&mut ucas_stream, 0)?;
    //std::fs::write("giga.bin", data)?;

    //for block in toc.compression_blocks {
    //    println!("{block:#?}");
    //}
    //dbg!(file_map);

    Ok(())
}

#[instrument(skip_all)]
fn read_header<R: Read>(mut stream: R) -> Result<FIoStoreTocHeader> {
    let mut toc_magic = [0; 16];
    stream.read_exact(&mut toc_magic)?;
    assert_eq!(&toc_magic, b"-==--==--==--==-");

    let version = EIoStoreTocVersion::from_repr(stream.read_u8()?).unwrap();
    let reserved0 = stream.read_u8()?;
    let reserved1 = stream.read_u16::<LE>()?;
    let toc_header_size = stream.read_u32::<LE>()?;
    let toc_entry_count = stream.read_u32::<LE>()?;
    let toc_compressed_block_entry_count = stream.read_u32::<LE>()?;
    let toc_compressed_block_entry_size = stream.read_u32::<LE>()?;
    let compression_method_name_count = stream.read_u32::<LE>()?;
    let compression_method_name_length = stream.read_u32::<LE>()?;
    let compression_block_size = stream.read_u32::<LE>()?;
    let directory_index_size = stream.read_u32::<LE>()?;
    let partition_count = stream.read_u32::<LE>()?;
    let container_id = FIoContainerId {
        id: stream.read_u64::<LE>()?,
    };
    let encryption_key_guid = FGuid {
        a: stream.read_u32::<LE>()?,
        b: stream.read_u32::<LE>()?,
        c: stream.read_u32::<LE>()?,
        d: stream.read_u32::<LE>()?,
    };
    let container_flags = EIoContainerFlags::from_bits(stream.read_u8()?).unwrap();
    let reserved3 = stream.read_u8()?;
    let reserved4 = stream.read_u16::<LE>()?;
    let toc_chunk_perfect_hash_seeds_count = stream.read_u32::<LE>()?;
    let partition_size = stream.read_u64::<LE>()?;
    let toc_chunks_without_perfect_hash_count = stream.read_u32::<LE>()?;
    let reserved7 = stream.read_u32::<LE>()?;
    let mut reserved8 = [0; 5];
    for r in &mut reserved8 {
        *r = stream.read_u64::<LE>()?;
    }

    assert_eq!(toc_header_size, 0x90);

    Ok(FIoStoreTocHeader {
        toc_magic,
        version,
        reserved0,
        reserved1,
        toc_header_size,
        toc_entry_count,
        toc_compressed_block_entry_count,
        toc_compressed_block_entry_size,
        compression_method_name_count,
        compression_method_name_length,
        compression_block_size,
        directory_index_size,
        partition_count,
        container_id,
        encryption_key_guid,
        container_flags,
        reserved3,
        reserved4,
        toc_chunk_perfect_hash_seeds_count,
        partition_size,
        toc_chunks_without_perfect_hash_count,
        reserved7,
        reserved8,
    })
}

#[instrument(skip_all)]
fn read_chunk_ids<R: Read>(mut stream: R, header: &FIoStoreTocHeader) -> Result<Vec<FIoChunkId>> {
    let mut chunk_ids = vec![];
    for _ in 0..header.toc_entry_count {
        let mut id = [0; 12];
        stream.read_exact(&mut id)?;
        chunk_ids.push(FIoChunkId { id });
    }
    Ok(chunk_ids)
}

#[instrument(skip_all)]
fn read_chunk_offsets<R: Read>(
    mut stream: R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoOffsetAndLength>> {
    let mut offsets = vec![];
    for _ in 0..header.toc_entry_count {
        let mut data = [0; 10];
        stream.read_exact(&mut data)?;
        offsets.push(FIoOffsetAndLength { data });
    }
    Ok(offsets)
}

#[instrument(skip_all)]
fn read_hash_map<R: Read>(
    mut stream: R,
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
    let chunk_perfect_hash_seeds =
        read_array(perfect_hash_seeds_count as usize, &mut stream, |s| {
            Ok(s.read_i32::<LE>()?)
        })?;
    let chunk_indices_without_perfect_hash = read_array(
        chunks_without_perfect_hash_count as usize,
        &mut stream,
        |s| Ok(s.read_i32::<LE>()?),
    )?;

    Ok((chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash))
}

#[instrument(skip_all)]
fn read_compression_blocks<R: Read>(
    mut stream: R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoStoreTocCompressedBlockEntry>> {
    read_array(
        header.toc_compressed_block_entry_count as usize,
        &mut stream,
        |s| {
            let mut data = [0; 12];
            s.read_exact(&mut data)?;
            Ok(FIoStoreTocCompressedBlockEntry { data })
        },
    )
}

#[instrument(skip_all)]
fn read_compression_methods<R: Read>(
    mut stream: R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<String>> {
    let mut names = vec!["None".into()];
    for _ in 0..header.compression_method_name_count {
        let mut buf = vec![0; header.compression_method_name_length as usize];
        stream.read_exact(&mut buf)?;
        let nul = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        names.push(String::from_utf8(buf[..nul].to_vec())?);
    }
    Ok(names)
}

#[instrument(skip_all)]
fn read_chunk_block_signatures<R: Read>(mut stream: R, header: &FIoStoreTocHeader) -> Result<()> {
    let is_signed = header.container_flags.contains(EIoContainerFlags::Signed);
    if is_signed {
        let hash_size = stream.read_u32::<LE>()? as usize;
        let mut toc_signature = vec![0; hash_size];
        stream.read_exact(&mut toc_signature)?;
        let mut block_signature = vec![0; hash_size];
        stream.read_exact(&mut block_signature)?;

        let chunk_block_signatures = read_array(
            header.toc_compressed_block_entry_count as usize,
            &mut stream,
            |s| {
                let mut data = [0; 20];
                s.read_exact(&mut data)?;
                Ok(FSHAHash { data })
            },
        )?;
        //dbg!(chunk_block_signatures);
    }
    Ok(())
}

#[instrument(skip_all)]
fn read_directory_index<R: Read>(mut stream: R, header: &FIoStoreTocHeader) -> Result<Vec<u8>> {
    let mut directory_index = vec![0; header.directory_index_size as usize];
    stream.read_exact(&mut directory_index)?;
    Ok(directory_index)
}

#[instrument(skip_all)]
fn read_meta<R: Read>(stream: R, header: &FIoStoreTocHeader) -> Result<Vec<FIoStoreTocEntryMeta>> {
    read_array(header.toc_entry_count as usize, stream, |s| {
        let mut data = [0; 32];
        s.read_exact(&mut data)?;
        let flags = FIoStoreTocEntryMetaFlags::from_bits(s.read_u8()?).unwrap();
        if header.version >= EIoStoreTocVersion::ReplaceIoChunkHashWithIoHash {
            s.read_exact(&mut [0; 3])?;
        }
        Ok(FIoStoreTocEntryMeta {
            chunk_hash: FIoChunkHash { data },
            flags,
        })
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
    chunk_metas: Vec<FIoStoreTocEntryMeta>,

    directory_index: Option<FIoDirectoryIndexResource>,
    file_map: HashMap<String, u32>,
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
    fn read<U: Read + Seek>(&self, mut ucas_stream: U, toc_entry_index: u32) -> Result<Vec<u8>> {
        let offset_and_length = &self.chunk_offset_lengths[toc_entry_index as usize];
        let offset = offset_and_length.get_offset();
        let size = offset_and_length.get_length();

        let compression_block_size = self.header.compression_block_size;
        let first_block_index = (offset / compression_block_size as u64) as usize;
        let last_block_index = ((align(offset + size, compression_block_size as u64) - 1)
            / compression_block_size as u64) as usize;

        let mut data = vec![];
        for i in first_block_index..=last_block_index {
            let block = &self.compression_blocks[i];
            println!("{i} {block:#?}");

            let mut buffer = vec![0; block.get_compressed_size() as usize];
            ucas_stream.seek(SeekFrom::Start(block.get_offset()))?;
            ucas_stream.read_exact(&mut buffer)?;
            let decomp = match block.get_compression_method_index() {
                0 => buffer,
                1 => {
                    let mut decomp = vec![0; block.get_uncompressed_size() as usize];
                    oodle_loader::decompress().unwrap()(&buffer, &mut decomp);
                    decomp
                }
                other => todo!("{other}"),
            };
            data.extend_from_slice(&decomp);
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
        write!(f, "FIoChunkId(")?;
        for b in self.id {
            write!(f, "{:02X}", b)?;
        }
        write!(f, ")")
    }
}
impl FIoChunkId {
    fn get_chunk_type(&self) -> EIoChunkType {
        EIoChunkType::from_repr(self.id[11]).unwrap()
    }
}
#[derive(Debug)]
struct FIoContainerId {
    id: u64,
}
#[derive(Debug, Clone, Copy)]
struct FIoOffsetAndLength {
    data: [u8; 10],
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
#[derive(Debug)]
struct FIoStoreTocEntryMeta {
    chunk_hash: FIoChunkHash,
    flags: FIoStoreTocEntryMetaFlags,
}
#[derive(Debug)]
struct FGuid {
    a: u32,
    b: u32,
    c: u32,
    d: u32,
}
struct FSHAHash {
    data: [u8; 20],
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
    //LatestPlusOne,
    //Latest = LatestPlusOne - 1
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

#[derive(Debug, FromRepr)]
#[repr(u8)]
enum EIoChunkType {
    Invalid = 0x0,
    InstallManifest = 0x1,
    ExportBundleData = 0x2,
    BulkData = 0x3,
    OptionalBulkData = 0x4,
    MemoryMappedBulkData = 0x5,
    LoaderGlobalMeta = 0x6,
    LoaderInitialLoadMeta = 0x7,
    LoaderGlobalNames = 0x8,
    LoaderGlobalNameHashes = 0x9,
    ContainerHeader = 0xa,
}

fn read_string<S: Read>(mut stream: S) -> Result<String> {
    let len = stream.read_i32::<LE>()?;
    if len < 0 {
        let chars = read_array((-len) as usize, &mut stream, |r| Ok(r.read_u16::<LE>()?))?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(String::from_utf16(&chars[..length]).unwrap())
    } else {
        let mut chars = vec![0; len as usize];
        stream.read_exact(&mut chars)?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(String::from_utf8_lossy(&chars[..length]).into_owned())
    }
}

use directory_index::*;
mod directory_index {
    use std::u32;

    use super::*;

    #[derive(Debug)]
    pub struct FIoDirectoryIndexResource {
        mount_point: String,
        directory_entries: Vec<FIoDirectoryIndexEntry>,
        file_entries: Vec<FIoFileIndexEntry>,
        string_table: Vec<String>,
    }
    impl FIoDirectoryIndexResource {
        pub fn read<S: Read>(mut stream: S) -> Result<Self> {
            Ok(Self {
                mount_point: read_string(&mut stream)?,
                directory_entries: read_array(
                    stream.read_u32::<LE>()? as usize,
                    &mut stream,
                    |s| FIoDirectoryIndexEntry::read(s),
                )?,
                file_entries: read_array(stream.read_u32::<LE>()? as usize, &mut stream, |s| {
                    FIoFileIndexEntry::read(s)
                })?,
                string_table: read_array(stream.read_u32::<LE>()? as usize, &mut stream, |s| {
                    read_string(s)
                })?,
            })
        }
        pub fn iter_root<F>(&self, mut visitor: F)
        where
            F: FnMut(u32, &[&str]),
        {
            self.iter(0, &mut vec![], &mut visitor)
        }
        pub fn iter<'s, F>(&'s self, dir_index: u32, stack: &mut Vec<&'s str>, visitor: &mut F)
        where
            F: FnMut(u32, &[&str]),
        {
            let dir = &self.directory_entries[dir_index as usize];
            if dir.name != u32::MAX {
                stack.push(&self.string_table[dir.name as usize]);
            }

            let mut file_index = dir.first_file_entry;
            while file_index != u32::MAX {
                let file = &self.file_entries[file_index as usize];

                stack.push(&self.string_table[file.name as usize]);
                visitor(file.user_data, stack);
                stack.pop();

                file_index = file.next_file_entry;
            }

            {
                let mut dir_index = dir.first_child_entry;
                while dir_index != u32::MAX {
                    let dir = &self.directory_entries[dir_index as usize];
                    self.iter(dir_index, stack, visitor);
                    dir_index = dir.next_sibling_entry;
                }
            }

            if dir.name != u32::MAX {
                stack.pop();
            }
        }
    }

    #[derive(Debug)]
    struct FIoFileIndexEntry {
        name: u32,
        next_file_entry: u32,
        user_data: u32,
    }
    impl FIoFileIndexEntry {
        fn read<S: Read>(mut stream: S) -> Result<Self> {
            Ok(Self {
                name: stream.read_u32::<LE>()?,
                next_file_entry: stream.read_u32::<LE>()?,
                user_data: stream.read_u32::<LE>()?,
            })
        }
    }

    #[derive(Debug)]
    struct FIoDirectoryIndexEntry {
        name: u32,
        first_child_entry: u32,
        next_sibling_entry: u32,
        first_file_entry: u32,
    }
    impl FIoDirectoryIndexEntry {
        fn read<S: Read>(mut stream: S) -> Result<Self> {
            Ok(Self {
                name: stream.read_u32::<LE>()?,
                first_child_entry: stream.read_u32::<LE>()?,
                next_sibling_entry: stream.read_u32::<LE>()?,
                first_file_entry: stream.read_u32::<LE>()?,
            })
        }
    }
}
