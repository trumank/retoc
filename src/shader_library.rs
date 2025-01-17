use crate::compression::{decompress, CompressionMethod};
use crate::container_header::EIoContainerHeaderVersion;
use crate::iostore::IoStoreTrait;
use crate::ser::{ReadExt, Readable, WriteExt, Writeable};
use crate::{EIoStoreTocVersion, FIoChunkId, FSHAHash};
use anyhow::{anyhow, bail};
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, Write};
use std::ops::Deref;
use strum::FromRepr;

#[derive(Debug, Copy, Clone, Default)]
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

#[derive(Copy, Clone, Default)]
struct FIoStoreShaderCodeEntry {
    packed: u64,
}
impl Readable for FIoStoreShaderCodeEntry {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {
        Ok(Self{packed: s.de()?})
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

#[derive(Debug, Copy, Clone, Default)]
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
    shader_group_chunk_ids: Vec<FIoChunkId>,
    shader_map_entries: Vec<FIoStoreShaderMapEntry>,
    shader_entries: Vec<FIoStoreShaderCodeEntry>,
    shader_group_entries: Vec<FIoStoreShaderGroupEntry>,
    shader_indices: Vec<u32>,
}
impl FIoStoreShaderCodeArchiveHeader {
    fn deserialize<S: Read>(s: &mut S, _version: EIoStoreShaderLibraryVersion) -> anyhow::Result<Self> {

        let shader_map_hashes: Vec<FSHAHash> = s.de()?;
        let shader_hashes: Vec<FSHAHash> = s.de()?;
        let shader_group_chunk_ids: Vec<FIoChunkId> = s.de()?;
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
}

fn decompress_shader_group_chunk(shader_group_data: &Vec<u8>, container_version: EIoStoreTocVersion, container_header_version: Option<EIoContainerHeaderVersion>, uncompressed_size: usize) -> anyhow::Result<Vec<u8>> {

    // Sanity check against empty compressed data chunks
    if shader_group_data.is_empty() {
        bail!("Invalid shader group compressed data");
    }
    let mut result_uncompressed_data: Vec<u8> = vec![0; uncompressed_size];

    // There is no indication of what compression algorithm is used, however, starting with UE5.3, it is always oodle
    let is_compression_always_oodle = container_version >= EIoStoreTocVersion::OnDemandMetaData ||
        (container_header_version.is_some() && container_header_version.unwrap() >= EIoContainerHeaderVersion::NoExportInfo);

    // If we know for sure that this is oodle, decompress with oodle
    if is_compression_always_oodle {
        decompress(CompressionMethod::Oodle, shader_group_data, &mut result_uncompressed_data)?;
        return Ok(result_uncompressed_data)
    }
    // Otherwise, it can be any compression format UE supports. However, by default it is always Oodle, and it is known that to change it an engine patch is necessary
    // The only known game to have used non-oodle shader compression in UE5.2 is Satisfactory U8, where it used Zlib instead
    // We can determine if it's Zlib by checking if it starts with 0x78 or 0x58. Otherwise, we assume oodle
    let is_zlib_marker = shader_group_data[0] == 0x78 || shader_group_data[0] == 0x58;
    if is_zlib_marker {
        decompress(CompressionMethod::Zlib, shader_group_data, &mut result_uncompressed_data)?;
        return Ok(result_uncompressed_data)
    }

    // Assume Oodle by default. This can be changed to account for other algorithms if games using them are discovered
    decompress(CompressionMethod::Oodle, shader_group_data, &mut result_uncompressed_data)?;
    Ok(result_uncompressed_data)
}

#[derive(Debug, Clone)]
pub(crate) struct IoStoreShaderCodeArchive {
    pub(crate) version: EIoStoreShaderLibraryVersion,
    pub(crate) header: FIoStoreShaderCodeArchiveHeader,
    pub(crate) shaders_code: Vec<Vec<u8>>,
    pub(crate) total_shader_code_size: usize,
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

        // Read and decompress individual shader groups belonging to this library, and extract shader code from them
        let mut decompressed_shaders: Vec<Vec<u8>> = vec![Vec::new(); shader_library_header.shader_entries.len()];
        let mut total_shader_code_size: usize = 0;

        for shader_group_index in 0..shader_library_header.shader_group_entries.len() {

            let shader_group_chunk_id = shader_library_header.shader_group_chunk_ids[shader_group_index];
            let shader_group_entry = shader_library_header.shader_group_entries[shader_group_index].clone();

            // Read shader group chunk
            let mut shader_group_data = store_access.read(shader_group_chunk_id)?;

            // Decompress the shader group chunk if it's compressed size does not match it's uncompressed size
            if shader_group_entry.compressed_size != shader_group_entry.uncompressed_size {
                let container_version = store_access.container_file_version().ok_or_else(|| { anyhow!("Failed to retrieve container file version") })?;
                let container_header_version = store_access.container_header_version();
                shader_group_data = decompress_shader_group_chunk(&shader_group_data, container_version, container_header_version, shader_group_entry.uncompressed_size as usize)?;
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
                let shader_entry = shader_library_header.shader_entries[shader_index].clone();

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
                let (shader_index, shader_start_offset) = shader_id_and_offset[i].clone();
                let (_, shader_end_offset) = shader_id_and_offset[i + 1].clone();

                decompressed_shaders[shader_index] = shader_group_data[shader_start_offset..shader_end_offset].to_vec();
                total_shader_code_size += decompressed_shaders[shader_index].len();
            }

            // Copy the shader data for the last shader. It's end offset is the size of the shader group
            if !shader_id_and_offset.is_empty() {
                let (shader_index, shader_start_offset) = shader_id_and_offset.last().unwrap().clone();
                let shader_end_offset = shader_group_entry.uncompressed_size as usize;

                decompressed_shaders[shader_index] = shader_group_data[shader_start_offset..shader_end_offset].to_vec();
                total_shader_code_size += decompressed_shaders[shader_index].len();
            }
        }

        // Make sure that we have no shaders left with no shader code assigned
        for shader_index in 0..shader_library_header.shader_entries.len() {
            if decompressed_shaders[shader_index].is_empty() {
                let shader_entry = shader_library_header.shader_entries[shader_index].clone();
                bail!("Shader at index {} (frequency: {}, shader group index: {}, offset in group: {}) was not found in any shader group",
                    shader_index, shader_entry.shader_frequency(), shader_entry.shader_group_index(), shader_entry.shader_uncompressed_offset_in_group());
            }
        }

        Ok(IoStoreShaderCodeArchive {
            version: zen_shader_library_version,
            header: shader_library_header,
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
impl Writeable for FShaderMapEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.shader_indices_offset)?;
        s.ser(&self.num_shaders)?;
        s.ser(&self.first_preload_index)?;
        s.ser(&self.num_preload_entries)?;
        Ok({})
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
impl Writeable for FShaderCodeEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.offset)?;
        s.ser(&self.size)?;
        s.ser(&self.uncompressed_size)?;
        s.ser(&self.frequency)?;
        Ok({})
    }
}
#[derive(Debug, Clone, Default)]
struct FFileCachePreloadEntry {
    // Relative to the end of the shader library header
    offset: i64,
    size: i64,
}
impl Writeable for FFileCachePreloadEntry {
    fn ser<S: Write>(&self, s: &mut S) -> anyhow::Result<()> {
        s.ser(&self.offset)?;
        s.ser(&self.size)?;
        Ok({})
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
        Ok({})
    }
}

struct WriteShaderCodeResult {
    shader_code_buffer: Vec<u8>,
    shader_regions: Vec<(i64, usize, bool)>,
    shader_map_regions: Vec<(i64, usize)>,
}

// Lays out shader code to match its likely order of access into a single file, using shader maps to group shaders that are accessed together close to each other
fn layout_write_shader_code(shader_library: &IoStoreShaderCodeArchive) -> anyhow::Result<WriteShaderCodeResult> {
    // Calculate how many maps reference each shader. Shaders that are only referenced by one shader map can be put next to each other and preloaded as one region
    let total_shaders = shader_library.header.shader_entries.len();
    let mut shader_reference_count: Vec<u32> = vec![0; total_shaders];

    for shader_map_index in 0..shader_library.header.shader_map_entries.len() {
        let shader_map_entry = shader_library.header.shader_map_entries[shader_map_index].clone();

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

    fn write_shader<S: Write + Seek>(writer: &mut S, shader_file_regions: &mut Vec<(i64, usize, bool)>, shader_library: &IoStoreShaderCodeArchive, shader_index: usize, unique: bool) -> anyhow::Result<()> {
        let shader_offset = writer.stream_position()? as i64;
        let shader_compressed_size = shader_library.shaders_code[shader_index].len();

        // TODO: Should we try and compress the shaders using the same compression method that IoStore shader library uses?
        shader_file_regions[shader_index] = (shader_offset, shader_compressed_size, unique);
        writer.write(&shader_library.shaders_code[shader_index])?;
        Ok({})
    };

    // Write shaders that are considered "Shared" first, e.g. shaders have been seen in multiple shader maps
    for shader_index in 0..total_shaders {
        if shader_reference_count[shader_index] > 1 {
            write_shader(&mut shader_code_writer, &mut shader_file_regions, shader_library, shader_index, false)?;
        }
    }

    // Write shaders that only belong to a single shader map now, e.g. "Unique" shaders
    for shader_map_index in 0..shader_library.header.shader_map_entries.len() {

        let shader_map_entry = shader_library.header.shader_map_entries[shader_map_index].clone();
        let shader_map_start_offset = shader_code_writer.stream_position()?;

        for i in 0..shader_map_entry.num_shaders {
            let shader_indices_index = (shader_map_entry.shader_indices_offset + i) as usize;
            let shader_index = shader_library.header.shader_indices[shader_indices_index] as usize;

            // Only consider this shader if this is the only shader map that referenced it
            if shader_reference_count[shader_index] == 1 {
                write_shader(&mut shader_code_writer, &mut shader_file_regions, shader_library, shader_index, true)?;
            }
        }

        // Track the position and size of the shader map exclusive shaders, so we can create a single preload dependency for them later
        let shader_map_end_offset = shader_code_writer.stream_position()?;
        let shader_map_total_size = (shader_map_end_offset - shader_map_start_offset) as usize;
        shader_map_file_regions[shader_map_index] = (shader_map_start_offset as i64, shader_map_total_size);
    }

    // Write shaders that belong to no shader map, e.g. "Inline" shaders
    for shader_index in 0..total_shaders {
        if shader_reference_count[shader_index] == 0 {
            write_shader(&mut shader_code_writer, &mut shader_file_regions, shader_library, shader_index, false)?;
        }
    }

    // Make sure that all shaders have been written into the file
    for shader_index in 0..total_shaders {
        if shader_file_regions[shader_index].0 < 0 {
            bail!("Did not write shader code at index {} into the shader code archive", shader_index);
        }
    }
    Ok(WriteShaderCodeResult{
        shader_code_buffer,
        shader_regions: shader_file_regions,
        shader_map_regions: shader_map_file_regions
    })
}

// Returns the file contents of the built shader library on success
pub(crate) fn rebuild_shader_library_from_io_store(store_access: &dyn IoStoreTrait, library_chunk_id: FIoChunkId) -> anyhow::Result<Vec<u8>> {

    // Read IoStore shader library
    let io_store_shader_library = IoStoreShaderCodeArchive::read(store_access, library_chunk_id)?;

    // Write shader code into the shared buffer
    let shader_code = layout_write_shader_code(&io_store_shader_library)?;

    // Create shader library from the IoStore shader archive
    let mut shader_library: FShaderLibraryHeader = FShaderLibraryHeader::default();

    // Copy shader hashes and shader map hashes directly, they are unchanged
    shader_library.shader_hashes = io_store_shader_library.header.shader_hashes.clone();
    shader_library.shader_map_hashes = io_store_shader_library.header.shader_map_hashes.clone();

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

        let shader_map_entry = io_store_shader_library.header.shader_map_entries[shader_map_index].clone();

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
    result_library_writer.write(&shader_code.shader_code_buffer)?;

    Ok(result_shader_library_buffer)
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
}
