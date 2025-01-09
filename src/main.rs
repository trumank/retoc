mod script_objects;
mod compact_binary;

use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Result};
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
    let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/FactoryGame-Windows.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/Satisfactory/FactoryGame/Content/Paks/global.utoc";
    //let path_utoc = "/home/truman/.local/share/Steam/steamapps/common/VisionsofManaDemo/VisionsofMana/Content/Paks/pakchunk0-WindowsNoEditor.utoc";

    let ucas_stream = BufReader::new(File::open(Path::new(path_utoc).with_extension("ucas"))?);

    let mut stream = BufReader::new(File::open(path_utoc)?);

    ser_hex::read("trace.json", &mut stream, |s| read(s, ucas_stream))?;
    //read(stream, ucas_stream)?;

    Ok(())
}

trait ReadableBase {
    fn ser<S: Read>(stream: &mut S) -> Result<Self>
    where
        Self: Sized;
}
trait Readable: ReadableBase {}
trait ReadableCtx<C> {
    fn ser<S: Read>(stream: &mut S, ctx: C) -> Result<Self>
    where
        Self: Sized;
}

impl<T> ReadExt for T where T: Read {}
trait ReadExt: Read {
    fn ser<T: ReadableBase>(&mut self) -> Result<T>
    where
        Self: Sized,
    {
        T::ser(self)
    }
    fn ser_ctx<T: ReadableCtx<C>, C>(&mut self, ctx: C) -> Result<T>
    where
        Self: Sized,
    {
        T::ser(self, ctx)
    }
}

impl<const N: usize> Readable for [u8; N] {}
impl<const N: usize> ReadableBase for [u8; N] {
    #[instrument(skip_all, name = "read_fixed_slice")]
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        let mut buf = [0; N];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
}
impl<const N: usize, T: Readable + Default + Copy> Readable for [T; N] {}
impl<const N: usize, T: Readable + Default + Copy> ReadableBase for [T; N] {
    #[instrument(skip_all, name = "read_fixed_slice")]
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        let mut buf = [Default::default(); N];
        for i in buf.iter_mut() {
            *i = stream.ser()?;
        }
        Ok(buf)
    }
}

impl Readable for String {}
impl ReadableBase for String {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        let len = stream.read_i32::<LE>()?;
        if len < 0 {
            let chars = read_array((-len) as usize, stream, |r| Ok(r.read_u16::<LE>()?))?;
            let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
            Ok(String::from_utf16(&chars[..length]).unwrap())
        } else {
            let mut chars = vec![0; len as usize];
            stream.read_exact(&mut chars)?;
            let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
            Ok(String::from_utf8_lossy(&chars[..length]).into_owned())
        }
    }
}

impl<T: Readable> Readable for Vec<T> {}
impl<T: Readable> ReadableBase for Vec<T> {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        read_array(stream.read_u32::<LE>()? as usize, stream, T::ser)
    }
}
impl<T: Readable> ReadableCtx<usize> for Vec<T> {
    fn ser<S: Read>(stream: &mut S, ctx: usize) -> Result<Self> {
        read_array(ctx, stream, T::ser)
    }
}
impl ReadableCtx<usize> for Vec<u8> {
    fn ser<S: Read>(stream: &mut S, ctx: usize) -> Result<Self> {
        let mut buf = vec![0; ctx];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl ReadableBase for u8 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u8()?)
    }
}
impl ReadableBase for i8 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i8()?)
    }
}
impl ReadableBase for u16 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u16::<LE>()?)
    }
}
impl ReadableBase for i16 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i16::<LE>()?)
    }
}
impl ReadableBase for u32 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u32::<LE>()?)
    }
}
impl ReadableBase for i32 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i32::<LE>()?)
    }
}
impl ReadableBase for u64 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u64::<LE>()?)
    }
}
impl ReadableBase for i64 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i64::<LE>()?)
    }
}
// impl Readable for u8 {} special cased for optimized Vec<u8> serialization
impl Readable for i8 {}
impl Readable for u16 {}
impl Readable for i16 {}
impl Readable for u32 {}
impl Readable for i32 {}
impl Readable for u64 {}
impl Readable for i64 {}

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
    let header = stream.ser()?;
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

    //for chunk in toc.chunk_ids {
    //    dbg!(chunk);
    //}

    //for file_name in toc.file_map.keys() {
    //    //"FactoryGame/Content/FactoryGame/Interface/UI/InGame/OutputSlotData_Struct.uasset"
    //    let chunk_info = toc.get_chunk_info(file_name);
    //    dbg!(&chunk_info);

    //    let path = output.join(file_name);
    //    let dir = path.parent().unwrap();

    //    println!("{file_name}");
    //    let data = toc.read(&mut ucas_stream, toc.file_map[file_name])?;

    //    std::fs::create_dir_all(dir)?;
    //    std::fs::write(path, data)?;
    //}

    //let data = toc.read(&mut ucas_stream, 0)?;
    //std::fs::write("giga.bin", data)?;

    //for block in toc.compression_blocks {
    //    println!("{block:#?}");
    //}
    //dbg!(file_map);

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
fn read_chunk_ids<R: Read>(mut stream: R, header: &FIoStoreTocHeader) -> Result<Vec<FIoChunkId>> {
    stream.ser_ctx(header.toc_entry_count as usize)
}

#[instrument(skip_all)]
fn read_chunk_offsets<R: Read>(
    mut stream: R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoOffsetAndLength>> {
    stream.ser_ctx(header.toc_entry_count as usize)
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
    let chunk_perfect_hash_seeds = stream.ser_ctx(perfect_hash_seeds_count as usize)?;
    let chunk_indices_without_perfect_hash =
        stream.ser_ctx(chunks_without_perfect_hash_count as usize)?;

    Ok((chunk_perfect_hash_seeds, chunk_indices_without_perfect_hash))
}

#[instrument(skip_all)]
fn read_compression_blocks<R: Read>(
    mut stream: R,
    header: &FIoStoreTocHeader,
) -> Result<Vec<FIoStoreTocCompressedBlockEntry>> {
    stream.ser_ctx(header.toc_compressed_block_entry_count as usize)
}

#[instrument(skip_all)]
fn read_compression_methods<R: Read>(
    mut stream: R,
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
fn read_chunk_block_signatures<R: Read>(mut stream: R, header: &FIoStoreTocHeader) -> Result<()> {
    let is_signed = header.container_flags.contains(EIoContainerFlags::Signed);
    if is_signed {
        let size = stream.ser::<u32>()? as usize;
        let toc_signature: Vec<u8> = stream.ser_ctx(size)?;
        let block_signature: Vec<u8> = stream.ser_ctx(size)?;

        let chunk_block_signatures: Vec<FSHAHash> =
            stream.ser_ctx(header.toc_compressed_block_entry_count as usize)?;
        //dbg!(chunk_block_signatures);
    }
    Ok(())
}

#[instrument(skip_all)]
fn read_directory_index<R: Read>(mut stream: R, header: &FIoStoreTocHeader) -> Result<Vec<u8>> {
    stream.ser_ctx(header.directory_index_size as usize)
}

#[instrument(skip_all)]
fn read_meta<R: Read>(stream: R, header: &FIoStoreTocHeader) -> Result<Vec<FIoStoreTocEntryMeta>> {
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
        //write!(f, "FIoChunkId(")?;
        //for b in self.id {
        //    write!(f, "{:02X}", b)?;
        //}
        //write!(f, ")")
        f.debug_struct("FIoChunkId")
            .field("chunk_id", &self.get_chunk_id())
            .field("chunk_index", &self.get_chunk_index())
            .field("chunk_type", &self.get_chunk_type())
            .finish()
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
    fn get_chunk_id(&self) -> u64 {
        u64::from_le_bytes(self.id[0..8].try_into().unwrap())
    }
    fn get_chunk_index(&self) -> u16 {
        u16::from_le_bytes(self.id[8..10].try_into().unwrap())
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
            while let Some(dir_name) = components.next() {
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
