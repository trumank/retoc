use crate::chunk_id::FIoChunkIdRaw;
use crate::compression::{compress, decompress, CompressionMethod};
use crate::iostore::IoStoreTrait;
use crate::iostore_writer::IoStoreWriter;
use crate::logging::*;
use crate::ser::{ReadExt, Readable, WriteExt, Writeable};
use crate::zen::FZenPackageHeader;
use crate::{EIoChunkType, EIoStoreTocVersion, FIoChunkId, FSHAHash, UEPath};
use anyhow::{anyhow, bail, Context as _};
use key_mutex::Empty;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::cmp::{min, Ordering};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use strum::{Display, FromRepr};

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
struct FIoStoreShaderMapEntry
{
    shader_indices_offset: u32,
    num_shaders: u32,
}
impl Readable for FIoStoreShaderMapEntry {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self {
            shader_indices_offset: s.de()?,
            num_shaders: s.de()?,
        })
    }
}
impl Writeable for FIoStoreShaderMapEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.shader_indices_offset)?;
        s.ser(&self.num_shaders)?;
        Ok(())
    }
}

#[derive(Copy, Clone, Default, PartialEq, Eq)]
struct FIoStoreShaderCodeEntry {
    packed: u64,
}
impl Readable for FIoStoreShaderCodeEntry {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self{packed: s.de()?})
    }
}
impl Writeable for FIoStoreShaderCodeEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.packed)?;
        Ok(())
    }
}
impl FIoStoreShaderCodeEntry {
    const SHADER_FREQUENCY_BITS: u64 = 4;
    const SHADER_FREQUENCY_SHIFT: u64 = 0;
    const SHADER_FREQUENCY_MASK: u64 = (1 << Self::SHADER_FREQUENCY_BITS) - 1;
    const SHADER_GROUP_INDEX_SHIFT: u64 = Self::SHADER_FREQUENCY_SHIFT + Self::SHADER_FREQUENCY_BITS;
    const SHADER_GROUP_INDEX_BITS: u64 = 30;
    const SHADER_GROUP_INDEX_MASK: u64 = (1 << Self::SHADER_GROUP_INDEX_BITS) - 1;
    const SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_SHIFT: u64 = Self::SHADER_GROUP_INDEX_SHIFT + Self::SHADER_GROUP_INDEX_BITS;
    const SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_BITS: u64 = 30;
    const SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_MASK: u64 = (1 << Self::SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_BITS) - 1;
    fn shader_frequency(self) -> u8 { ((self.packed >> Self::SHADER_FREQUENCY_SHIFT) & Self::SHADER_FREQUENCY_MASK) as u8 }
    fn shader_group_index(self) -> usize { ((self.packed >> Self::SHADER_GROUP_INDEX_SHIFT) & Self::SHADER_GROUP_INDEX_MASK) as usize }
    fn shader_uncompressed_offset_in_group(self) -> usize { ((self.packed >> Self::SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_SHIFT) & Self::SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_MASK) as usize }
    fn new(shader_group_index: usize, shader_uncompressed_offset_in_group: usize, shader_frequency: u8) -> Self {
        let packed: u64 = ((shader_frequency as u64 & Self::SHADER_FREQUENCY_MASK) << Self::SHADER_FREQUENCY_SHIFT) |
            ((shader_group_index as u64 & Self::SHADER_GROUP_INDEX_MASK) << Self::SHADER_GROUP_INDEX_SHIFT) |
            ((shader_uncompressed_offset_in_group as u64 & Self::SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_MASK) << Self::SHADER_UNCOMPRESSED_OFFSET_IN_GROUP_SHIFT);
        Self{packed}
    }
}
impl Debug for FIoStoreShaderCodeEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FIoStoreShaderCodeEntry")
            .field("packed", &self.packed)
            .field("shader_frequency", &self.shader_frequency())
            .field("shader_group_index", &self.shader_group_index())
            .field("shader_uncompressed_offset_in_group", &self.shader_uncompressed_offset_in_group())
            .finish()
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
struct FIoStoreShaderGroupEntry {
    shader_indices_offset: u32,
    num_shaders: u32,
    uncompressed_size: u32,
    // If uncompressed_size == compressed_size group is not compressed
    compressed_size: u32,
}
impl Readable for FIoStoreShaderGroupEntry {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self {
            shader_indices_offset: s.de()?,
            num_shaders: s.de()?,
            uncompressed_size: s.de()?,
            compressed_size: s.de()?,
        })
    }
}
impl Writeable for FIoStoreShaderGroupEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.shader_indices_offset)?;
        s.ser(&self.num_shaders)?;
        s.ser(&self.uncompressed_size)?;
        s.ser(&self.compressed_size)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, FromRepr)]
#[repr(u32)]
enum EIoStoreShaderLibraryVersion {
    Initial = 1,
}

#[derive(Debug, Clone, Default)]
struct FIoStoreShaderCodeArchiveHeader {
    shader_map_hashes: Vec<FSHAHash>,
    shader_hashes: Vec<FSHAHash>,
    // Referred to as ShaderGroupIoHashes in UE
    shader_group_chunk_ids: Vec<FIoChunkIdRaw>,
    shader_map_entries: Vec<FIoStoreShaderMapEntry>,
    shader_entries: Vec<FIoStoreShaderCodeEntry>,
    shader_group_entries: Vec<FIoStoreShaderGroupEntry>,
    shader_indices: Vec<u32>,
}
impl FIoStoreShaderCodeArchiveHeader {
    fn deserialize<S: Read>(s: &mut S, _version: EIoStoreShaderLibraryVersion) -> anyhow::Result<Self> {

        let shader_map_hashes: Vec<FSHAHash> = s.de()?;
        let shader_hashes: Vec<FSHAHash> = s.de()?;
        let shader_group_chunk_ids: Vec<FIoChunkIdRaw> = s.de()?;
        let shader_map_entries: Vec<FIoStoreShaderMapEntry> = s.de()?;
        let shader_entries: Vec<FIoStoreShaderCodeEntry> = s.de()?;
        let shader_group_entries: Vec<FIoStoreShaderGroupEntry> = s.de()?;
        let shader_indices: Vec<u32> = s.de()?;

        Ok(Self{
            shader_map_hashes,
            shader_hashes,
            shader_group_chunk_ids,
            shader_map_entries,
            shader_entries,
            shader_group_entries,
            shader_indices,
        })
    }
    fn serialize<S: Write>(&self, s: &mut S, _version: EIoStoreShaderLibraryVersion) -> anyhow::Result<()> {

        s.ser(&self.shader_map_hashes)?;
        s.ser(&self.shader_hashes)?;
        s.ser(&self.shader_group_chunk_ids)?;
        s.ser(&self.shader_map_entries)?;
        s.ser(&self.shader_entries)?;
        s.ser(&self.shader_group_entries)?;
        s.ser(&self.shader_indices)?;
        Ok(())
    }
}

fn determine_likely_compression_method_for_shader_code(shader_group_data: &[u8]) -> CompressionMethod {

    // Check compression stream headers for known magic values
    if shader_group_data.len() >= 4 && shader_group_data[1..4] == [0xB5, 0x2F, 0xFD] {
        CompressionMethod::Zstd
    } else if shader_group_data.first().is_some_and(|f| [0x78, 0x58].contains(f)) {
        CompressionMethod::Zlib
    } else if shader_group_data.first().is_some_and(|f| [0x8C].contains(f)) {
        CompressionMethod::Oodle
    } else {
        // no magic values found so assume LZ4 which does not have reliable magic
        CompressionMethod::LZ4
    }
}

fn decompress_shader_code_with_method(shader_group_data: &[u8], compression_method: CompressionMethod, uncompressed_size: usize) -> anyhow::Result<Vec<u8>> {

    // Sanity check against empty compressed data chunks
    if shader_group_data.is_empty() {
        bail!("Invalid shader group compressed data");
    }
    let mut result_uncompressed_data: Vec<u8> = vec![0; uncompressed_size];
    decompress(compression_method, shader_group_data, &mut result_uncompressed_data)?;
    Ok(result_uncompressed_data)
}

fn decompress_shader_code(shader_group_data: &[u8], compression_method: &mut Option<CompressionMethod>, uncompressed_size: usize) -> anyhow::Result<Vec<u8>> {

    if compression_method.is_none() {
        // compression method unknown so try to determine it
        let likely_compression = determine_likely_compression_method_for_shader_code(shader_group_data);

        let mut compression_to_try = [
            CompressionMethod::Zlib,
            CompressionMethod::Zstd,
            CompressionMethod::LZ4,
            CompressionMethod::Oodle,
        ];

        // move likely compression to front
        compression_to_try.sort_by_key(|&c| c != likely_compression);

        // try compression methods until one succeeds
        let mut decompressed = None;
        for compression in compression_to_try {
            let result = decompress_shader_code_with_method(shader_group_data, compression, uncompressed_size);
            if let Ok(d) = result {
                *compression_method = Some(compression);
                decompressed = Some(d);
                break;
            }
        }
        decompressed.context("Failed to find decompression method for shader")
    } else {
        // compression method already known so use it
        decompress_shader_code_with_method(shader_group_data, compression_method.unwrap(), uncompressed_size)
    }
}

fn compress_shader(shader_data: &[u8], compression_method: CompressionMethod) -> anyhow::Result<Vec<u8>> {
    let mut compression_buffer: Vec<u8> = Vec::with_capacity(shader_data.len());
    compress(compression_method, shader_data, Cursor::new(&mut compression_buffer))?;
    Ok(compression_buffer)
}

#[derive(Debug, Clone)]
pub(crate) struct IoStoreShaderCodeArchive {
    version: EIoStoreShaderLibraryVersion,
    header: FIoStoreShaderCodeArchiveHeader,
    compression_method: Option<CompressionMethod>,
    shaders_code: Vec<Vec<u8>>,
    total_shader_code_size: usize,
}
impl IoStoreShaderCodeArchive {
    // Reads full IoStore shader code archive
    pub(crate) fn read(store_access: &dyn IoStoreTrait, library_chunk_id: FIoChunkId) -> anyhow::Result<IoStoreShaderCodeArchive> {

        // Read shader library header raw data
        let shader_library_header_data = store_access.read(library_chunk_id)?;
        let mut shader_library_reader = Cursor::new(&shader_library_header_data);

        // Deserialize the shader library header and version
        let zen_shader_library_version_raw: u32 = shader_library_reader.de()?;
        let zen_shader_library_version = EIoStoreShaderLibraryVersion::from_repr(zen_shader_library_version_raw)
            .ok_or_else(|| { anyhow!("Unknown shader library version: {}", zen_shader_library_version_raw) })?;
        let shader_library_header = FIoStoreShaderCodeArchiveHeader::deserialize(&mut shader_library_reader, zen_shader_library_version)?;
        let mut compression_method: Option<CompressionMethod> = None;

        // Read and decompress individual shader groups belonging to this library, and extract shader code from them
        let mut decompressed_shaders: Vec<Vec<u8>> = vec![Vec::new(); shader_library_header.shader_entries.len()];
        let mut total_shader_code_size: usize = 0;

        for shader_group_index in 0..shader_library_header.shader_group_entries.len() {

            let shader_group_chunk_id = shader_library_header.shader_group_chunk_ids[shader_group_index];
            let shader_group_entry = shader_library_header.shader_group_entries[shader_group_index];

            // Read shader group chunk
            let mut shader_group_data = store_access.read_raw(shader_group_chunk_id)?;

            // Decompress the shader group chunk if it's compressed size does not match it's uncompressed size
            if shader_group_entry.compressed_size != shader_group_entry.uncompressed_size {

                // Establish which compression method is used for this shader library. The entire library must use the same compression method
                shader_group_data = decompress_shader_code(&shader_group_data, &mut compression_method, shader_group_entry.uncompressed_size as usize)?;

                if shader_group_data.len() != shader_group_entry.uncompressed_size as usize {
                    bail!("Invalid amount of uncompressed data from decompress_shader_group_chunk: Expected {}, got {}", shader_group_entry.uncompressed_size, shader_group_data.len());
                }
            }

            // Extract shader indices and their offsets from the shader group
            let mut shader_id_and_offset: Vec<(usize, usize)> = Vec::with_capacity(shader_group_entry.num_shaders as usize);
            for i in 0..shader_group_entry.num_shaders {

                // Resolve the actual shader index and it's shader entry
                let shader_indices_index = (shader_group_entry.shader_indices_offset + i) as usize;
                let shader_index = shader_library_header.shader_indices[shader_indices_index] as usize;
                let shader_entry = shader_library_header.shader_entries[shader_index];

                // Make sure that this shader actually belongs to this group
                if shader_entry.shader_group_index() != shader_group_index {
                    bail!("Shader {} has conflicting group index: shader points at group {}, but group {} claims that it contains the shader",
                        shader_index, shader_entry.shader_group_index(), shader_group_index);
                }
                shader_id_and_offset.push((shader_index, shader_entry.shader_uncompressed_offset_in_group()))
            }

            // Sort shaders based on their offsets. This is needed to be able to calculate their sizes by looking at the offset of the next shader
            // This is generally not necessary because UnrealPak always lays out shaders sequentially already, but it does not hurt to double check and not rely on that assumption
            shader_id_and_offset.sort_by_key(|(_, shader_group_offset)| { *shader_group_offset });

            // Copy the decompressed shader data for all shaders except the last one
            for i in 0..(shader_id_and_offset.len() - 1) {
                let (shader_index, shader_start_offset) = shader_id_and_offset[i];
                let (_, shader_end_offset) = shader_id_and_offset[i + 1];

                decompressed_shaders[shader_index] = shader_group_data[shader_start_offset..shader_end_offset].to_vec();
                total_shader_code_size += decompressed_shaders[shader_index].len();
            }

            // Copy the shader data for the last shader. It's end offset is the size of the shader group
            if !shader_id_and_offset.is_empty() {
                let (shader_index, shader_start_offset) = *shader_id_and_offset.last().unwrap();
                let shader_end_offset = shader_group_entry.uncompressed_size as usize;

                decompressed_shaders[shader_index] = shader_group_data[shader_start_offset..shader_end_offset].to_vec();
                total_shader_code_size += decompressed_shaders[shader_index].len();
            }
        }

        // Make sure that we have no shaders left with no shader code assigned
        for (shader_index, decompressed_shader) in decompressed_shaders.iter().enumerate() {
            if decompressed_shader.is_empty() {
                let shader_entry = shader_library_header.shader_entries[shader_index];
                bail!("Shader at index {} (frequency: {}, shader group index: {}, offset in group: {}) was not found in any shader group",
                    shader_index, shader_entry.shader_frequency(), shader_entry.shader_group_index(), shader_entry.shader_uncompressed_offset_in_group());
            }
        }

        Ok(IoStoreShaderCodeArchive {
            version: zen_shader_library_version,
            header: shader_library_header,
            compression_method,
            shaders_code: decompressed_shaders,
            total_shader_code_size,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct FShaderMapEntry
{
    shader_indices_offset: u32,
    num_shaders: u32,
    first_preload_index: u32,
    num_preload_entries: u32,
}
impl Readable for FShaderMapEntry {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self{
            shader_indices_offset: s.de()?,
            num_shaders: s.de()?,
            first_preload_index: s.de()?,
            num_preload_entries: s.de()?,
        })
    }
}
impl Writeable for FShaderMapEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.shader_indices_offset)?;
        s.ser(&self.num_shaders)?;
        s.ser(&self.first_preload_index)?;
        s.ser(&self.num_preload_entries)?;
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
struct FShaderCodeEntry {
    // Relative to the end of the shader library header
    offset: u64,
    size: u32,
    uncompressed_size: u32,
    frequency: u8,
}
impl Readable for FShaderCodeEntry {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self{
            offset: s.de()?,
            size: s.de()?,
            uncompressed_size: s.de()?,
            frequency: s.de()?,
        })
    }
}
impl Writeable for FShaderCodeEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.offset)?;
        s.ser(&self.size)?;
        s.ser(&self.uncompressed_size)?;
        s.ser(&self.frequency)?;
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
struct FFileCachePreloadEntry {
    // Relative to the end of the shader library header
    offset: i64,
    size: i64,
}
impl Readable for FFileCachePreloadEntry {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self{
            offset: s.de()?,
            size: s.de()?,
        })
    }
}
impl Writeable for FFileCachePreloadEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.offset)?;
        s.ser(&self.size)?;
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
struct FShaderLibraryHeader {
    shader_map_hashes: Vec<FSHAHash>,
    shader_hashes: Vec<FSHAHash>,
    shader_map_entries: Vec<FShaderMapEntry>,
    shader_entries: Vec<FShaderCodeEntry>,
    preload_entries: Vec<FFileCachePreloadEntry>,
    shader_indices: Vec<u32>,
}
impl FShaderLibraryHeader {
    fn serialize<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.shader_map_hashes)?;
        s.ser(&self.shader_hashes)?;
        s.ser(&self.shader_map_entries)?;
        s.ser(&self.shader_entries)?;
        s.ser(&self.preload_entries)?;
        s.ser(&self.shader_indices)?;
        Ok(())
    }
    fn deserialize<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self{
            shader_map_hashes: s.de()?,
            shader_hashes: s.de()?,
            shader_map_entries: s.de()?,
            shader_entries: s.de()?,
            preload_entries: s.de()?,
            shader_indices: s.de()?,
        })
    }
}

#[derive(Debug, Copy, Clone, Display, FromRepr)]
#[repr(u8)]
enum EShaderFrequency {
    Vertex = 0,
    Mesh = 1,
    Amplification = 2,
    Pixel = 3,
    Geometry = 4,
    Compute = 5,
    RayGen = 6,
    RayMiss = 7,
    RayHitGroup = 8,
    RayCallable = 9,
}

struct WriteShaderCodeResult {
    shader_code_buffer: Vec<u8>,
    shader_regions: Vec<(i64, usize, bool)>,
    shader_map_regions: Vec<(i64, usize)>,
    total_shared_shaders: usize,
    total_unique_shaders: usize,
    total_detached_shaders: usize,
}

// Lays out shader code to match its likely order of access into a single file, using shader maps to group shaders that are accessed together close to each other
fn layout_write_shader_code(shader_library: &IoStoreShaderCodeArchive, compress_shaders: bool) -> anyhow::Result<WriteShaderCodeResult> {
    // Calculate how many maps reference each shader. Shaders that are only referenced by one shader map can be put next to each other and preloaded as one region
    let total_shaders = shader_library.header.shader_entries.len();
    let mut shader_reference_count: Vec<u32> = vec![0; total_shaders];

    for shader_map_index in 0..shader_library.header.shader_map_entries.len() {
        let shader_map_entry = shader_library.header.shader_map_entries[shader_map_index];

        for i in 0..shader_map_entry.num_shaders {
            let shader_indices_index = (shader_map_entry.shader_indices_offset + i) as usize;
            let shader_index = shader_library.header.shader_indices[shader_indices_index] as usize;
            shader_reference_count[shader_index] += 1;
        }
    }

    // Write shaders depending on how many shader maps they appeared in
    let mut shader_code_buffer: Vec<u8> = Vec::with_capacity(shader_library.total_shader_code_size);
    let mut shader_code_writer = Cursor::new(&mut shader_code_buffer);
    let mut shader_file_regions: Vec<(i64, usize, bool)> = vec![(-1, 0, false); total_shaders];
    let mut shader_map_file_regions: Vec<(i64, usize)> = vec![(-1, 0); shader_library.header.shader_map_entries.len()];

    let mut total_shared_shaders: usize = 0;
    let mut total_unique_shaders: usize = 0;
    let mut total_detached_shaders: usize = 0;

    let (tx, rx) = std::sync::mpsc::sync_channel(10);

    enum Message<'a> {
        StartMap,
        EndMap {
            shader_map_index: usize,
        },
        Compress {
            shader_index: usize,
            unique: bool,
        },
        Write {
            shader_index: usize,
            data: std::borrow::Cow<'a, [u8]>,
            unique: bool,
        },
    }

    pariter::scope(|s| -> anyhow::Result<()> {
        s.spawn(|_| -> anyhow::Result<()> {
            // Write shaders that are considered "Shared" first, e.g. shaders have been seen in multiple shader maps
            for (shader_index, ref_count) in shader_reference_count.iter().enumerate() {
                if *ref_count > 1 {
                    tx.send(Message::Compress { shader_index, unique: false })?;
                    total_shared_shaders += 1;
                }
            }

            // Write shaders that only belong to a single shader map now, e.g. "Unique" shaders
            for shader_map_index in 0..shader_library.header.shader_map_entries.len() {

                let shader_map_entry = shader_library.header.shader_map_entries[shader_map_index];

                // Send signal to mark current stream position
                tx.send(Message::StartMap).unwrap();

                for i in 0..shader_map_entry.num_shaders {
                    let shader_indices_index = (shader_map_entry.shader_indices_offset + i) as usize;
                    let shader_index = shader_library.header.shader_indices[shader_indices_index] as usize;

                    // Only consider this shader if this is the only shader map that referenced it
                    if shader_reference_count[shader_index] == 1 {
                        tx.send(Message::Compress { shader_index, unique: true })?;
                        total_unique_shaders += 1;
                    }
                }

                // Send signal to mark current stream position and populate shader map entry with offsets
                tx.send(Message::EndMap { shader_map_index }).unwrap();
            }

            // Write shaders that belong to no shader map. Generally such shaders should not exist, but we should still handle and preserve them just in case
            // I refer to them as "detached" shaders. Generally such shaders cannot be loaded in runtime because runtime operates on shader maps during serialization and not singular shaders
            for (shader_index, ref_count) in shader_reference_count.iter().enumerate() {
                if *ref_count == 0 {
                    tx.send(Message::Compress { shader_index, unique: false })?;
                    total_detached_shaders += 1;
                }
            }

            // close channel signaling end
            drop(tx);

            Ok(())
        });

        use pariter::IteratorExt as _;

        let mut shader_map_start_offset = 0;

        rx.into_iter().parallel_map_scoped(s, |message| {
            match message {
                Message::Compress { shader_index, unique } => {
                    let shader_uncompressed_size = shader_library.shaders_code[shader_index].len();

                    let shader_uncompressed_data = &shader_library.shaders_code[shader_index];

                    let mut data = shader_uncompressed_data.into();

                    // Compress shader if we are allowed to and compression method is known
                    if compress_shaders && shader_library.compression_method.is_some() {
                        let shader_compressed_data = compress_shader(shader_uncompressed_data, shader_library.compression_method.unwrap())?;
                        let shader_compressed_size = shader_compressed_data.len();

                        // Sometimes compression can result in larger size for some small shaders, so only write compressed data if it's actually smaller than uncompressed size
                        if shader_compressed_size < shader_uncompressed_size {
                            data = shader_compressed_data.into();
                        }
                    }

                    Ok(Message::Write {
                        shader_index,
                        data,
                        unique,
                    })
                },
                _ => Ok(message) // pass through
            }
        }).try_for_each(|message: anyhow::Result<Message>| -> anyhow::Result<()> {
            let stream_position = shader_code_writer.stream_position()?;
            match message? {
                Message::StartMap => {
                    shader_map_start_offset = stream_position;
                }
                Message::EndMap { shader_map_index } => {
                    // Track the position and size of the shader map exclusive shaders, so we can create a single preload dependency for them later
                    let shader_map_end_offset = stream_position;
                    let shader_map_total_size = (shader_map_end_offset - shader_map_start_offset) as usize;
                    shader_map_file_regions[shader_map_index] = (shader_map_start_offset as i64, shader_map_total_size);
                }
                Message::Write { shader_index, data, unique } => {
                    shader_file_regions[shader_index] = (stream_position as i64, data.len(), unique);
                    shader_code_writer.write_all(&data)?;
                }
                Message::Compress { .. } => unreachable!(),
            }
            Ok(())
        })?;

        Ok(())

    }).unwrap()?;

    // Make sure that all shaders have been written into the file
    if let Some(missing_index) = shader_file_regions.iter().position(|s| s.0 < 0) {
        bail!("Did not write shader code at index {} into the shader code archive", missing_index);
    }
    Ok(WriteShaderCodeResult{
        shader_code_buffer,
        shader_regions: shader_file_regions,
        shader_map_regions: shader_map_file_regions,
        total_unique_shaders,
        total_shared_shaders,
        total_detached_shaders,
    })
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct ShaderMapToPackageNameListEntry {
    #[serde(rename = "ShaderMapHash")]
    pub(crate) shader_map_hash: FSHAHash,
    #[serde(rename = "Assets")]
    pub(crate) package_names: Vec<String>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct ShaderAssetInfoFileRoot {
    #[serde(rename = "ShaderCodeToAssets")]
    pub(crate) shader_code_to_assets: Vec<ShaderMapToPackageNameListEntry>,
}

// Resolves shader library filename (with optional path) into a shader asset info filename associated with that library
pub(crate) fn get_shader_asset_info_filename_from_library_filename(shader_library_filename: &str) -> anyhow::Result<String> {

    let library_name_without_extension = UEPath::new(shader_library_filename).file_stem().ok_or_else(|| { anyhow!("Failed to retrieve filename from path") })?;
    let prefix_separator_index = library_name_without_extension.find('-').ok_or_else(|| { anyhow!("Invalid shader library filename, does not have a library name and format separator") })?;
    let library_name_and_format = &library_name_without_extension[prefix_separator_index + 1..];

    let asset_info_filename = format!("ShaderAssetInfo-{library_name_and_format}.assetinfo.json");

    if let Some(parent_path) = UEPath::new(shader_library_filename).parent() {
        return Ok(parent_path.join(asset_info_filename).to_string());
    }
    Ok(asset_info_filename)
}

fn build_shader_asset_metadata_from_io_store_packages(store_access: &dyn IoStoreTrait, shader_map_hashes: &HashSet<FSHAHash>, log: &Log) -> (usize, ShaderAssetInfoFileRoot) {
    let container_header_version = store_access.container_header_version();

    // Make sure container header version is actually available first
    if container_header_version.is_empty() {
        log!(log, "WARNING: Skipping shader asset metadata build for shader library because container header version is not present");
        return (0, ShaderAssetInfoFileRoot::default())
    }
    let mut shader_map_hash_to_package_names: HashMap<FSHAHash, Vec<String>> = HashMap::new();
    let mut total_package_references: usize = 0;

    // Iterate all packages that reference any shader maps in this library and track their names
    for package_info in store_access.packages() {
        let package_id = package_info.id();
        if let Some(package_store_entry) = package_info.container().package_store_entry(package_id) {

            // Filter shader map hashes to only the ones actually contained in this shader library
            let referenced_shader_map_hashes: Vec<FSHAHash> = package_store_entry.shader_map_hashes
                .iter().filter(|x| { shader_map_hashes.contains(x) }).cloned().collect();

            // Skip this package if it actually does not reference any shader maps from this shader library. this saves us time reading and parsing its data
            if referenced_shader_map_hashes.is_empty() {
                continue
            }

            // Skip this package if we could not actually read its data from the container
            let package_data_buffer = store_access.read(FIoChunkId::from_package_id(package_id, 0, EIoChunkType::ExportBundleData));
            if package_data_buffer.is_err() {
                let error_message = package_data_buffer.unwrap_err().to_string();
                log!(log, "WARNING: Skipping reference to shader maps {referenced_shader_map_hashes:?} from package {package_id:?} because it's data could not be read: {error_message}");
                continue;
            }

            // Skip this package if we could not resolve package name from it's serialized data
            let package_header = FZenPackageHeader::get_package_name(&mut Cursor::new(&package_data_buffer.unwrap()), container_header_version.unwrap());
            if package_header.is_err() {
                let error_message = package_header.unwrap_err().to_string();
                log!(log, "WARNING: Skipping reference to shader maps {referenced_shader_map_hashes:?} from package {package_id:?} because it failed to parse as a valid asset: {error_message}");
                continue;
            }
            let package_name = package_header.unwrap();
            total_package_references += 1;

            // Associate this package name with all shader map hashes contained in it
            for shader_map_hash in referenced_shader_map_hashes {
                shader_map_hash_to_package_names.entry(shader_map_hash).or_default().push(package_name.clone());
            }
        }
    }

    // Sort to get a predictable order in the resulting JSON file
    let mut referenced_shader_map_hashes: Vec<FSHAHash> = shader_map_hash_to_package_names.keys().cloned().collect();
    referenced_shader_map_hashes.sort();

    // Create the resulting JSON file structure
    let mut shader_asset_info = ShaderAssetInfoFileRoot::default();
    for shader_map_hash in &referenced_shader_map_hashes {
        shader_asset_info.shader_code_to_assets.push(ShaderMapToPackageNameListEntry{
            shader_map_hash: *shader_map_hash,
            package_names: shader_map_hash_to_package_names.get(shader_map_hash).unwrap().clone(),
        })
    }
    (total_package_references, shader_asset_info)
}

// Returns the file contents of the built shader library on success. Second file contents are that of asset metadata for the shader library
pub(crate) fn rebuild_shader_library_from_io_store(store_access: &dyn IoStoreTrait, library_chunk_id: FIoChunkId, log: &Log, compress_shaders: bool) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {

    // Read IoStore shader library
    let io_store_shader_library = IoStoreShaderCodeArchive::read(store_access, library_chunk_id)?;

    // Retrieve the library name. Right now it is used only for stats, but in the future it can be used to reassemble shader libraries squashed into multiple containers
    let library_name = store_access.chunk_path(library_chunk_id).and_then(|x| {
        std::path::Path::new(&x).with_extension("").file_name().map(|y| { y.to_string_lossy().to_string() })
    }).ok_or_else(|| { anyhow!("Failed to retrieve IoStore shader library name for shader library chunk {:?}", library_chunk_id) })?;

    // Write shader code into the shared buffer
    let shader_code = layout_write_shader_code(&io_store_shader_library, compress_shaders)?;

    // Create shader library from the IoStore shader archive
    let mut shader_library = FShaderLibraryHeader {
        // Copy shader hashes and shader map hashes directly, they are unchanged
        shader_hashes: io_store_shader_library.header.shader_hashes.clone(),
        shader_map_hashes: io_store_shader_library.header.shader_map_hashes.clone(),
        ..Default::default()
    };

    // Create shader code entries from shader code entries in the IoStore library
    shader_library.shader_entries.reserve(io_store_shader_library.header.shader_entries.len());
    for shader_index in 0..io_store_shader_library.header.shader_entries.len() {

        let shader_frequency = io_store_shader_library.header.shader_entries[shader_index].shader_frequency();
        let uncompressed_shader_code_size = io_store_shader_library.shaders_code[shader_index].len();
        let (shader_code_offset, compressed_shader_code_size, _) = shader_code.shader_regions[shader_index];

        shader_library.shader_entries.push(FShaderCodeEntry{
            offset: shader_code_offset as u64,
            size: compressed_shader_code_size as u32,
            uncompressed_size: uncompressed_shader_code_size as u32,
            frequency: shader_frequency,
        })
    }

    // Copy the shader indices, since we are not changing what shaders belong to which shader maps
    shader_library.shader_indices = io_store_shader_library.header.shader_indices.clone();

    // Create shader map entries from IoStore shader map entries. They need minimal changes other than writing preload dependencies
    shader_library.shader_map_entries.reserve(io_store_shader_library.header.shader_map_entries.len());
    for shader_map_index in 0..io_store_shader_library.header.shader_map_entries.len() {

        let shader_map_entry = io_store_shader_library.header.shader_map_entries[shader_map_index];

        let first_preload_dependency_index = shader_library.preload_entries.len();
        let mut num_preload_dependencies: usize = 0;

        // If we have any unique shaders for this shader map, write them all in one preload entry
        let (shader_map_start_offset, shader_map_size) = shader_code.shader_map_regions[shader_map_index];
        if shader_map_size > 0 {
            shader_library.preload_entries.push(FFileCachePreloadEntry{
                offset: shader_map_start_offset,
                size: shader_map_size as i64
            });
            num_preload_dependencies += 1;
        }

        // Write preload entries for any shared shaders referenced by this shader map
        for i in 0..shader_map_entry.num_shaders {

            let shader_indices_index = (shader_map_entry.shader_indices_offset + i) as usize;
            let shader_index = io_store_shader_library.header.shader_indices[shader_indices_index] as usize;
            let (shader_start_offset, shader_compressed_size, is_shader_unique) = shader_code.shader_regions[shader_index];

            // If this shared is not unique (if it is unique, it is part of this shader map, and already has a preload dependency), add a preload dependency for it
            if !is_shader_unique {
                shader_library.preload_entries.push(FFileCachePreloadEntry{
                    offset: shader_start_offset,
                    size: shader_compressed_size as i64
                });
                num_preload_dependencies += 1;
            }
        }

        // Create the shader map entry now
        shader_library.shader_map_entries.push(FShaderMapEntry{
            shader_indices_offset: shader_map_entry.shader_indices_offset,
            num_shaders: shader_map_entry.num_shaders,
            first_preload_index: first_preload_dependency_index as u32,
            num_preload_entries: num_preload_dependencies as u32,
        })
    }

    // Serialize shader library now by serializing the header and then appending the shader code after it
    let mut result_shader_library_buffer: Vec<u8> = Vec::new();
    let mut result_library_writer = Cursor::new(&mut result_shader_library_buffer);

    // UE4.24 and above is 2, name or what changed unknown
    let shader_library_version_loose = 2;
    result_library_writer.ser(&shader_library_version_loose)?;
    // Serialize shader library header
    FShaderLibraryHeader::serialize(&shader_library, &mut result_library_writer)?;
    // Serialize shader code
    result_library_writer.write_all(&shader_code.shader_code_buffer)?;

    // Resolve asset names referencing the shader maps contained in this library
    let contained_shader_map_hashes: HashSet<FSHAHash> = shader_library.shader_map_hashes.iter().cloned().collect();
    let (total_package_references, shader_asset_info) = build_shader_asset_metadata_from_io_store_packages(store_access, &contained_shader_map_hashes, log);
    let result_shader_asset_metadata_buffer = serde_json::to_vec_pretty(&shader_asset_info)?;

    // Print shader library statistics to stdout if allowed
    if log.allow_stdout() {
        let compression_ratio = f64::round((io_store_shader_library.total_shader_code_size as f64 / shader_code.shader_code_buffer.len() as f64) * 100.0f64) as i64;
        log!(log, "Shader Library {} statistics: Shared Shaders: {}; Unique Shaders: {}; Detached Shaders: {}; Shader Maps: {} (referenced by {} packages), Uncompressed Size: {}MB, Compressed Size: {}MB, Compression Ratio: {}%",
            library_name.clone(), shader_code.total_shared_shaders, shader_code.total_unique_shaders, shader_code.total_detached_shaders, shader_library.shader_map_entries.len(), total_package_references,
             io_store_shader_library.total_shader_code_size / 1024 / 1024, shader_code.shader_code_buffer.len() / 1024 / 1024, compression_ratio,
        );
    }
    Ok((result_shader_library_buffer, result_shader_asset_metadata_buffer))
}

fn is_raytracing_shader_frequency(shader_frequency: u8) -> bool {
    shader_frequency == EShaderFrequency::RayGen as u8 || shader_frequency == EShaderFrequency::RayMiss as u8 ||
        shader_frequency == EShaderFrequency::RayHitGroup as u8 || shader_frequency == EShaderFrequency::RayCallable as u8
}

fn build_io_store_shader_code_archive_header(shader_library: &FShaderLibraryHeader, shader_format_name: &str, container_version: EIoStoreTocVersion, max_uncompressed_shader_group_size: usize) -> FIoStoreShaderCodeArchiveHeader {

    let mut shader_to_referencing_shader_maps: HashMap<usize, Vec<usize>> = HashMap::with_capacity(shader_library.shader_hashes.len());

    // Figure out which shader maps each shader belongs to
    for shader_map_index in 0..shader_library.shader_map_hashes.len() {
        let shader_map_entry = shader_library.shader_map_entries[shader_map_index].clone();

        for i in 0..shader_map_entry.num_shaders {
            let shader_indices_index = (shader_map_entry.shader_indices_offset + i) as usize;
            let shader_index = shader_library.shader_indices[shader_indices_index] as usize;
            shader_to_referencing_shader_maps.entry(shader_index).or_default().push(shader_map_index);
        }
    }

    // Sort shader map indices in natural order to keep them deterministic
    for shader_index in 0..shader_library.shader_hashes.len() {
        shader_to_referencing_shader_maps.entry(shader_index).or_default().sort();
    }

    // Sort shader indices by the number of shader maps that reference them, and then by shader map IDs, and then by shader indices for determinism
    let mut sorted_shader_indices: Vec<usize> = shader_to_referencing_shader_maps.keys().cloned().collect();
    sorted_shader_indices.sort_by(|a, b| {
        let shader_maps_a = shader_to_referencing_shader_maps.get(a).unwrap();
        let shader_maps_b = shader_to_referencing_shader_maps.get(b).unwrap();

        // Sort by the number of shader maps first
        if shader_maps_a.len() != shader_maps_b.len() {
            return shader_maps_a.len().cmp(&shader_maps_b.len());
        }
        // Sort by the array contents otherwise, and if they are the same, by shader index as a fallback
        shader_maps_a.cmp(shader_maps_b).then(a.cmp(b))
    });

    // Split into streaks of shaders that are referenced by the same set of shader maps
    let mut current_shader_group: Vec<usize> = Vec::new();
    let mut last_shader_map_set_seen: Vec<usize> = Vec::new();
    let mut shader_groups: Vec<Vec<usize>> = Vec::new();

    for shader_index in sorted_shader_indices {
        let referencing_shader_maps = shader_to_referencing_shader_maps.get(&shader_index).unwrap();

        if current_shader_group.is_empty() {

            current_shader_group.push(shader_index);
            last_shader_map_set_seen = referencing_shader_maps.clone();
        }
        else if &last_shader_map_set_seen != referencing_shader_maps {

            shader_groups.push(current_shader_group.clone());

            current_shader_group.clear();
            current_shader_group.push(shader_index);
            last_shader_map_set_seen = referencing_shader_maps.clone();
        }
        else {
            current_shader_group.push(shader_index);
        }
    }
    // Add the last group
    if !current_shader_group.is_empty() {
        shader_groups.push(current_shader_group.clone());
    }

    // Split each shader group into non-raytracing and raytracing shaders if requested
    let separate_raytracing_shaders = shader_format_name == "PCD3D_SM5";
    if separate_raytracing_shaders {
        shader_groups = shader_groups.iter().cloned().flat_map(|x| {

            let mut non_raytracing_shaders: Vec<usize> = Vec::with_capacity(x.len());
            let mut raytracing_shaders: Vec<usize> = Vec::new();

            for shader_index in &x {
                let shader_frequency = shader_library.shader_entries[*shader_index].frequency;
                if is_raytracing_shader_frequency(shader_frequency) {
                    raytracing_shaders.push(*shader_index);
                } else {
                    non_raytracing_shaders.push(*shader_index);
                }
            }

            // Only use the split up groups if both groups are not empty, otherwise use the original group
            if !non_raytracing_shaders.is_empty() && !raytracing_shaders.is_empty() {
                return vec![non_raytracing_shaders, raytracing_shaders].into_iter()
            }
            vec![x].into_iter()
        }).collect();
    }

    // Utility function to sort shader indices
    fn sort_shaders_ascending(shader_index_a: usize, shader_index_b: usize, shader_library: &FShaderLibraryHeader) -> Ordering {
        let shader_code_entry_a = shader_library.shader_entries[shader_index_a].clone();
        let shader_code_entry_b = shader_library.shader_entries[shader_index_b].clone();

        // Sort by uncompressed size descending, then by compressed size descending, then by frequency descending, and then by offset descending
        shader_code_entry_a.uncompressed_size.cmp(&shader_code_entry_b.uncompressed_size)
            .then(shader_code_entry_a.size.cmp(&shader_code_entry_b.size))
            .then(shader_code_entry_a.frequency.cmp(&shader_code_entry_b.frequency))
            .then(shader_code_entry_a.offset.cmp(&shader_code_entry_b.offset))
    }

    // Now, split the shader groups by their size, ensuring that no group is larger than maximum group size
    shader_groups = shader_groups.iter().cloned().flat_map(|x| {

        // Calculate current group size
        let mut group_size: usize = 0;
        for shader_index in &x {
            group_size += shader_library.shader_entries[*shader_index].uncompressed_size as usize;
        }
        // Do not split up groups that are under the size limit or only contain one shader
        if group_size <= max_uncompressed_shader_group_size || x.len() == 1 {
            return vec![x].into_iter();
        }

        let num_new_groups = min(group_size / max_uncompressed_shader_group_size + 1, x.len());

        // Sort the shaders in the descending order to make the splitting easier
        let mut sorted_shaders = x.clone();
        sorted_shaders.sort_by(|a, b| {
            sort_shaders_ascending(*b, *a, shader_library)
        });
        let mut new_shader_groups: Vec<Vec<usize>> = vec![Vec::new(); num_new_groups];
        let mut new_shader_group_sizes: Vec<usize> = vec![0; num_new_groups];

        // Add each shader in the smallest group currently present
        for shader_index in sorted_shaders {

            // Find the index of the smallest shader group currently present
            let mut smallest_new_group_index: usize = 0;
            for new_shader_group_index in 1..num_new_groups {
                if new_shader_group_sizes[new_shader_group_index] < new_shader_group_sizes[smallest_new_group_index] {
                    smallest_new_group_index = new_shader_group_index;
                }
            }
            let shader_size = shader_library.shader_entries[shader_index].uncompressed_size as usize;
            // Add the shader into that group
            new_shader_groups[smallest_new_group_index].push(shader_index);
            new_shader_group_sizes[smallest_new_group_index] += shader_size;
        }
        new_shader_groups.into_iter()
    }).collect();

    // Final step, sort shaders in each shader group ascending
    for shader_group in &mut shader_groups {
        shader_group.sort_by(|a, b| {
            sort_shaders_ascending(*a, *b, shader_library)
        });
    }

    // Convert shader map indices without touching their indices offsets, since we copy the original offsets there is no reason to change them
    let shader_map_entries: Vec<FIoStoreShaderMapEntry> = shader_library.shader_map_entries.iter().map(|x| {
        FIoStoreShaderMapEntry{shader_indices_offset: x.shader_indices_offset, num_shaders: x.num_shaders}
    }).collect();

    // We have the resulting shader group contents, we can create the IO store shader library header now
    let mut io_store_library_header = FIoStoreShaderCodeArchiveHeader{
        shader_map_hashes: shader_library.shader_map_hashes.clone(),
        shader_hashes: shader_library.shader_hashes.clone(),
        shader_group_chunk_ids: Vec::new(),
        shader_map_entries,
        // Write dummy shader entries that we will fill in later
        shader_entries: vec![FIoStoreShaderCodeEntry::default(); shader_library.shader_hashes.len()],
        shader_group_entries: Vec::new(),
        shader_indices: shader_library.shader_indices.clone(),
    };

    // Finds an existing index sequence in shader indices matching the provided array, or adds a new one
    fn find_or_add_sequence_in_shader_indices(library_header: &mut FIoStoreShaderCodeArchiveHeader, indices: &Vec<usize>) -> usize {
        let first_new_shader_index = indices[0];
        let max_indices_index = library_header.shader_indices.len() - indices.len() + 1;

        // Attempt to find an existing sequence that matches
        for indices_index in 0..max_indices_index {

            // If the first shader index does not match, continue to the next one
            if library_header.shader_indices[indices_index] as usize != first_new_shader_index {
                continue;
            }

            // Compare the rest of the indices
            let found_rest_of_sequence = indices
                .iter()
                .zip(library_header.shader_indices[indices_index..].iter())
                .skip(1)
                .all(|(a, b)| *a == *b as usize);

            // if the rest of the indices did not ma tch, continue
            if !found_rest_of_sequence {
                continue;
            }
            // We found the existing sequence otherwise!
            return indices_index;
        }

        // Create a new sequence otherwise
        let new_indices_index = library_header.shader_indices.len();
        library_header.shader_indices.reserve(indices.len());

        for shader_index in indices {
            library_header.shader_indices.push(*shader_index as u32);
        }
        new_indices_index
    }

    // Build resulting shader group entries and their chunk IDs
    io_store_library_header.shader_group_entries.reserve(shader_groups.len());
    io_store_library_header.shader_group_chunk_ids.reserve(shader_groups.len());

    for (shader_group_index, shader_group) in shader_groups.iter().enumerate() {
        let mut group_hasher = Sha1::new();
        let mut uncompressed_group_size: usize = 0;

        // Populate shader entries and calculate the resulting hash of the group
        for shader_index in shader_group {
            let shader_code_entry = shader_library.shader_entries[*shader_index].clone();
            let shader_hash = shader_library.shader_hashes[*shader_index];
            io_store_library_header.shader_entries[*shader_index] = FIoStoreShaderCodeEntry::new(shader_group_index, uncompressed_group_size, shader_code_entry.frequency);

            group_hasher.update(shader_hash.0);
            group_hasher.update(shader_code_entry.uncompressed_size.to_le_bytes());
            uncompressed_group_size += shader_code_entry.uncompressed_size as usize;
        }
        // Add the name of the shader library format into the group hash
        group_hasher.update(shader_format_name.as_bytes());
        let shader_hash = FSHAHash(group_hasher.finalize().into());

        // Store shader indices into the global array, or find an existing entry
        let group_indices_offset = find_or_add_sequence_in_shader_indices(&mut io_store_library_header, shader_group);

        // Prime uncompressed size, but leave compressed size as zero. it will be written later
        io_store_library_header.shader_group_entries.push(FIoStoreShaderGroupEntry{
            shader_indices_offset: group_indices_offset as u32,
            num_shaders: shader_group.len() as u32,
            uncompressed_size: uncompressed_group_size as u32,
            compressed_size: 0,
        });
        io_store_library_header.shader_group_chunk_ids.push(FIoChunkId::create_shader_code_chunk_id(&shader_hash).with_version(container_version).get_raw());
    }
    io_store_library_header
}

pub(crate) fn read_shader_asset_info(shader_asset_metadata_buffer: &[u8], package_name_to_shader_maps: &mut HashMap<String, Vec<FSHAHash>>) -> anyhow::Result<()> {
    let shader_asset_info: ShaderAssetInfoFileRoot = serde_json::from_slice(shader_asset_metadata_buffer)?;

    for shader_map_entry in &shader_asset_info.shader_code_to_assets {
        for package_name in &shader_map_entry.package_names {
            package_name_to_shader_maps.entry(package_name.clone()).or_default().push(shader_map_entry.shader_map_hash);
        }
    }
    Ok(())
}

pub(crate) fn write_io_store_library(store_writer: &mut IoStoreWriter, raw_shader_library_buffer: &Vec<u8>, shader_library_path: &UEPath, log: &Log) -> anyhow::Result<()> {

    let mut shader_library_reader = Cursor::new(raw_shader_library_buffer);

    // Read shader library header
    let shader_library_version_loose: i32 = shader_library_reader.de()?;
    if shader_library_version_loose != 2 {
        bail!("Unknown shader library file version {}. Current version is 2", shader_library_version_loose);
    }
    let shader_library_header = FShaderLibraryHeader::deserialize(&mut shader_library_reader)?;
    // Shader library code offsets are relative to the end of the shader library header
    let shader_library_code_start_offset = shader_library_reader.stream_position()?;
    let total_library_compressed_size = raw_shader_library_buffer.len() - shader_library_code_start_offset as usize;

    // Figure out the name of the format of this shader library
    let library_filename = UEPath::new(shader_library_path).file_stem().ok_or_else(|| { anyhow!("Failed to retrieve file stem from shader library path") })?;
    let shader_library_name_separator_index = library_filename.find('-').ok_or_else(|| { anyhow!("Failed to derive shader library name from shader library filename") })?;
    let shader_format_separator_index = library_filename.rfind('-').ok_or_else(|| { anyhow!("Failed to derive format name from shader library filename") })?;

    // Note that this splitting logic is actually wrong, shader format will end up being part of the library name, ahd shader format will end up being a name of the shader platform
    // However, we have to follow the wrong logic in UnrealPak to get matching shader chunk IDs. If it ever gets fixed in UE, this logic will need to be changed.
    // Shader library names are like this: ShaderArchive-Global-PCD3D_SM6-PCD3D_SM6.ushaderbytecode
    // Logic above gives "Global-PCD3D_SM6" as library name and "PCD3D_SM6" as shader format.
    // However, actual library name is "Global", actual shader format name is "PCD3D" and shader platform is "PCD3D_SM6"
    let shader_library_name = library_filename[shader_library_name_separator_index + 1..shader_format_separator_index].to_string();
    let shader_format_name = library_filename[shader_format_separator_index + 1..].to_string();

    // Create IoStore shader library header to split shaders into groups
    let max_shader_group_size = 1024 * 1024; // from UE source, can be adjusted per game using r.ShaderCodeLibrary.MaxShaderGroupSize CVar
    let mut io_store_library_header = build_io_store_shader_code_archive_header(&shader_library_header, &shader_format_name, store_writer.container_version(), max_shader_group_size);

    // Cached compression method for this shader library
    let mut compression_method: Option<CompressionMethod> = None;
    let mut total_compressed_groups_size: usize = 0;
    let mut total_library_uncompressed_size: usize = 0;

    // Create shader chunks for each shader group from the library
    for shader_group_index in 0..io_store_library_header.shader_group_entries.len() {

        let shader_group_entry = io_store_library_header.shader_group_entries[shader_group_index];
        let mut shader_group_chunk_buffer: Vec<u8> = Vec::new();

        // Read and decompress each individual shader contained within this group
        for i in 0..shader_group_entry.num_shaders {
            let shader_indices_index = (shader_group_entry.shader_indices_offset + i) as usize;
            let shader_index = io_store_library_header.shader_indices[shader_indices_index] as usize;

            // Resolve the offset of the shader code into the shader library and seek there
            let shader_code_entry = shader_library_header.shader_entries[shader_index].clone();
            let shader_code_offset = shader_library_code_start_offset + shader_code_entry.offset;
            shader_library_reader.seek(SeekFrom::Start(shader_code_offset))?;

            // Read the shader code into the temporary buffer
            let mut uncompressed_shader_code: Vec<u8> = vec![0; shader_code_entry.size as usize];
            shader_library_reader.read_exact(&mut uncompressed_shader_code)?;

            // Decompress the shader code if it's size is actually different from the uncompressed size
            if shader_code_entry.size != shader_code_entry.uncompressed_size {
                uncompressed_shader_code = decompress_shader_code(&uncompressed_shader_code, &mut compression_method, shader_code_entry.uncompressed_size as usize)?;
            }

            // Write the shader code into the uncompressed chunk buffer. Make sure shader code offset matches it's actual placement
            let expected_shader_code_offset = io_store_library_header.shader_entries[shader_index].shader_uncompressed_offset_in_group();
            let actual_shader_code_offset = shader_group_chunk_buffer.len();
            if actual_shader_code_offset != expected_shader_code_offset {
                bail!("Shader code placement inside of the group did not match it's expected placement from the library header. Expected shader code to be at offset {}, but it's actual placement is at offset {}",
                    expected_shader_code_offset, actual_shader_code_offset);
            }
            shader_group_chunk_buffer.append(&mut uncompressed_shader_code);
            total_library_uncompressed_size += shader_code_entry.uncompressed_size as usize;
        }

        // Make sure the uncompressed size matches the expected size written into the header
        let actual_uncompressed_group_size = shader_group_chunk_buffer.len();
        let expected_uncompressed_group_size = shader_group_entry.uncompressed_size as usize;
        if actual_uncompressed_group_size != expected_uncompressed_group_size {
            bail!("Expected uncompressed group size to be {} as written into the header, but after the actual shader code placement uncompressed size was {}", expected_uncompressed_group_size, actual_uncompressed_group_size);
        }

        // If we know the compression method for shaders, compress this groups content with it
        if compression_method.is_some() {
            let compressed_group_data = compress_shader(&shader_group_chunk_buffer, compression_method.unwrap())?;

            // Only take the compressed data over the decompressed data if it is actually smaller
            if compressed_group_data.len() < actual_uncompressed_group_size {
                shader_group_chunk_buffer = compressed_group_data;
            }
        }

        // Now that we know compressed group size, write it into the shader group header
        let compressed_group_size = shader_group_chunk_buffer.len();
        io_store_library_header.shader_group_entries[shader_group_index].compressed_size = compressed_group_size as u32;
        let shader_group_chunk_id = io_store_library_header.shader_group_chunk_ids[shader_group_index];

        // Write the shader code chunk for this group into the container. Note that shader chunks do not have filenames
        store_writer.write_chunk_raw(shader_group_chunk_id, None, &shader_group_chunk_buffer)?;
        total_compressed_groups_size += compressed_group_size;
    }

    // Create a new chunk for the shader library header
    let mut io_store_shader_library_buffer: Vec<u8> = Vec::new();
    let mut io_store_shader_library_writer = Cursor::new(&mut io_store_shader_library_buffer);

    // Write the version and then the contents of the shader code archive
    let io_store_library_version = EIoStoreShaderLibraryVersion::Initial;
    let io_store_library_version_raw: u32 = io_store_library_version as u32;
    io_store_shader_library_writer.ser(&io_store_library_version_raw)?;
    FIoStoreShaderCodeArchiveHeader::serialize(&io_store_library_header, &mut io_store_shader_library_writer, io_store_library_version)?;

    // Write the shader library header chunk using the provided filename
    let shader_library_chunk_id = FIoChunkId::create_shader_library_chunk_id(&shader_library_name, &shader_format_name);
    store_writer.write_chunk(shader_library_chunk_id, Some(shader_library_path), &io_store_shader_library_buffer)?;

    // Write statistics
    if log.allow_stdout() {
        let recompression_ratio = f64::round((total_library_compressed_size as f64 / total_compressed_groups_size as f64) * 100.0f64) as i64;
        let compression_ratio = f64::round((total_library_uncompressed_size as f64 / total_compressed_groups_size as f64) * 100.0f64) as i64;
        log!(log, "Shader Library {} statistics: Shader Groups: {}, Shader Maps: {}, Uncompressed Size: {}MB, Original Compressed Size: {}MB, Total Group Compressed Size: {}MB, Recompression Ratio: {}%, Total Compression Ratio: {}%",
            UEPath::new(shader_library_path).file_stem().unwrap().to_string(),
            io_store_library_header.shader_group_entries.len(), io_store_library_header.shader_map_entries.len(),
            total_library_uncompressed_size / 1024 / 1024, total_library_compressed_size / 1024 / 1024,
            total_compressed_groups_size / 1024 / 1024, recompression_ratio, compression_ratio);
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;
    use std::io::BufReader;

    #[test]
    fn test_read_container_shader_library() -> anyhow::Result<()> {

        let mut stream = BufReader::new(fs::File::open(
            "tests/UE5.4/ShaderArchive-Global-PCD3D_SM6-PCD3D_SM6.ushaderbytecode",
        )?);

        let shader_library_header = ser_hex::read("out/read_container_shader_library.trace.json", &mut stream, |x| {
            let library_version: u32 = x.de()?;
            assert_eq!(library_version, 1, "expected shader library header version to be initial");
            FIoStoreShaderCodeArchiveHeader::deserialize(x, EIoStoreShaderLibraryVersion::Initial)
        })?;
        //dbg!(shader_library_header.shader_group_entries);

        assert_eq!(shader_library_header.shader_group_entries.len(), 535);
        shader_library_header.shader_entries.iter().for_each(|x| {
            assert!(x.shader_group_index() < shader_library_header.shader_group_entries.len(), "Invalid shader group index out of bounds: {} limit {}",
                x.shader_group_index(), shader_library_header.shader_map_entries.len());

            let uncompressed_group_size = shader_library_header.shader_group_entries[x.shader_group_index()].uncompressed_size as usize;
            assert!(x.shader_uncompressed_offset_in_group() < uncompressed_group_size, "Invalid shader offset in shader group: {} with size {}",
                x.shader_uncompressed_offset_in_group(), uncompressed_group_size);
        });
        Ok(())
    }

    #[test]
    fn test_zen_shader_library_identity_conversion() -> anyhow::Result<()> {

        // Note that for legacy library only the header is included into the repository, shader code is omitted since it is irrelevant for this test and is very large
        let mut legacy_shader_library_reader = BufReader::new(fs::File::open(
            "tests/UE5.4/ShaderArchive-NuclearNightmare-PCD3D_SM6-PCD3D_SM6.ushaderbytecode",
        )?);
        let legacy_library_version: u32 = legacy_shader_library_reader.de()?;
        assert_eq!(legacy_library_version, 2, "expected legacy shader library header version to be 2");
        let legacy_shader_library = FShaderLibraryHeader::deserialize(&mut legacy_shader_library_reader)?;

        let mut zen_shader_library_reader = BufReader::new(fs::File::open(
            "tests/UE5.4/ShaderArchive-NuclearNightmare-PCD3D_SM6-PCD3D_SM6.uzenshaderbytecode",
        )?);
        let zen_library_version: u32 = zen_shader_library_reader.de()?;
        assert_eq!(zen_library_version, 1, "expected zen shader library header version to be initial");
        let original_zen_shader_library = FIoStoreShaderCodeArchiveHeader::deserialize(&mut zen_shader_library_reader, EIoStoreShaderLibraryVersion::Initial)?;

        // Check the data that is completely unchanged from legacy to zen lib first
        assert_eq!(legacy_shader_library.shader_hashes, original_zen_shader_library.shader_hashes);
        assert_eq!(legacy_shader_library.shader_map_hashes, original_zen_shader_library.shader_map_hashes);
        assert_eq!(legacy_shader_library.shader_indices, original_zen_shader_library.shader_indices);

        // Convert the legacy shader library back into zen
        let max_shader_group_size = 1024 * 1024;
        let converted_zen_shader_library = build_io_store_shader_code_archive_header(&legacy_shader_library, "PCD3D_SM6", EIoStoreTocVersion::OnDemandMetaData, max_shader_group_size);

        // Compare original zen shader library and converted zen shader library
        assert_eq!(converted_zen_shader_library.shader_hashes, original_zen_shader_library.shader_hashes);
        assert_eq!(converted_zen_shader_library.shader_map_hashes, original_zen_shader_library.shader_map_hashes);
        assert_eq!(converted_zen_shader_library.shader_indices, original_zen_shader_library.shader_indices);
        assert_eq!(converted_zen_shader_library.shader_entries, original_zen_shader_library.shader_entries);
        assert_eq!(converted_zen_shader_library.shader_map_entries, original_zen_shader_library.shader_map_entries);
        assert_eq!(converted_zen_shader_library.shader_group_chunk_ids.len(), original_zen_shader_library.shader_group_chunk_ids.len());
        assert_eq!(converted_zen_shader_library.shader_group_entries.len(), original_zen_shader_library.shader_group_entries.len());

        for shader_group_index in 0..converted_zen_shader_library.shader_group_entries.len() {
            assert_eq!(converted_zen_shader_library.shader_group_entries[shader_group_index].num_shaders, original_zen_shader_library.shader_group_entries[shader_group_index].num_shaders);
            assert_eq!(converted_zen_shader_library.shader_group_entries[shader_group_index].shader_indices_offset, original_zen_shader_library.shader_group_entries[shader_group_index].shader_indices_offset);
            assert_eq!(converted_zen_shader_library.shader_group_entries[shader_group_index].uncompressed_size, original_zen_shader_library.shader_group_entries[shader_group_index].uncompressed_size);
            // Compressed sizes can be different because Oodle compression is not deterministic
        }

        Ok(())
    }
}
