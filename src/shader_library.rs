use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read};
use anyhow::{anyhow, bail};
use strum::FromRepr;
use crate::{EIoStoreTocVersion, FIoChunkId, FSHAHash};
use crate::compression::{decompress, CompressionMethod};
use crate::container_header::EIoContainerHeaderVersion;
use crate::iostore::IoStoreTrait;
use crate::ser::{ReadExt, Readable};

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
    let mut result_uncompressed_data: Vec<u8> = Vec::with_capacity(uncompressed_size);

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

// Returns the file contents of the reassembled shader library on success
pub(crate) fn reassemble_shader_library_from_io_store(store_access: &dyn IoStoreTrait, library_chunk_id: FIoChunkId) -> anyhow::Result<Vec<u8>> {

    // Read shader library header raw data
    let shader_library_header_data = store_access.read(library_chunk_id)?;
    let mut shader_library_reader = Cursor::new(&shader_library_header_data);

    // Deserialize the shader library header and version
    let shader_library_version_raw: u32 = shader_library_reader.de()?;
    let shader_library_version = EIoStoreShaderLibraryVersion::from_repr(shader_library_version_raw)
        .ok_or_else(|| { anyhow!("Unknown shader library version: {}", shader_library_version_raw) })?;
    let shader_library_header = FIoStoreShaderCodeArchiveHeader::deserialize(&mut shader_library_reader, shader_library_version)?;

    // Read and decompress individual shader groups belonging to this library
    let decompressed_shader_groups: Vec<Vec<u8>> = Vec::with_capacity(shader_library_header.shader_group_entries.len());

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
        }


    }

    Ok(Vec::new())
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::io::BufReader;
    use super::*;

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
