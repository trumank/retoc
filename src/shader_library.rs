use std::fmt::{Debug, Formatter};
use std::io::Read;
use crate::{FIoChunkId, FSHAHash};
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
impl Readable for FIoStoreShaderCodeArchiveHeader {
    fn de<S: Read>(s: &mut S) -> anyhow::Result<Self> {

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
            FIoStoreShaderCodeArchiveHeader::de(x)
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
