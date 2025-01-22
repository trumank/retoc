use std::{
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

use crate::{
    ser::*, CompressionMethod, EIoStoreTocVersion, FIoChunkHash, FIoChunkId, FIoContainerId,
    FIoOffsetAndLength, FIoStoreTocCompressedBlockEntry, FIoStoreTocEntryMeta,
    FIoStoreTocEntryMetaFlags, Toc,
};
use anyhow::{bail, Context, Result};
use fs_err as fs;
use crate::container_header::{EIoContainerHeaderVersion, StoreEntry};

pub(crate) struct IoStoreWriter {
    toc_path: PathBuf,
    toc_stream: BufWriter<fs::File>,
    cas_stream: BufWriter<fs::File>,
    toc: Toc,
}

impl IoStoreWriter {
    pub(crate) fn new<P: AsRef<Path>>(
        toc_path: P,
        toc_version: EIoStoreTocVersion,
        container_header_version: EIoContainerHeaderVersion,
        mount_point: String,
    ) -> Result<Self> {
        let toc_path = toc_path.as_ref().to_path_buf();
        let name = toc_path.file_stem().unwrap().to_string_lossy();
        let toc_stream = BufWriter::new(fs::File::create(&toc_path)?);
        let cas_stream = BufWriter::new(fs::File::create(toc_path.with_extension("ucas"))?);

        let mut toc = Toc::new();
        toc.compression_block_size = 0x10000;
        toc.version = toc_version;
        toc.container_id = FIoContainerId::from_name(&name);
        toc.directory_index.mount_point = mount_point;
        toc.partition_size = u64::MAX;

        Ok(Self {
            toc_path,
            toc_stream,
            cas_stream,
            toc,
        })
    }
    pub(crate) fn write_chunk(
        &mut self,
        chunk_id: FIoChunkId,
        path: Option<&str>,
        data: &[u8],
    ) -> Result<()> {
        if let Some(path) = path {
            let index = &mut self.toc.directory_index;
            let relative_path = path.strip_prefix(&index.mount_point).with_context(|| {
                format!(
                    "mount point {} does not contain path {path:?}",
                    index.mount_point
                )
            })?;
            index.add_file(relative_path, self.toc.chunks.len() as u32);
        }

        let mut offset = self.cas_stream.stream_position()?;

        let start_block = self.toc.compression_blocks.len();

        let mut hasher = blake3::Hasher::new();
        for block in data.chunks(self.toc.compression_block_size as usize) {
            self.cas_stream.write_all(block)?;
            hasher.update(block);
            let compressed_size = block.len() as u32;
            let uncompressed_size = block.len() as u32;
            let compression_method_index = 0; // "None"
            self.toc
                .compression_blocks
                .push(FIoStoreTocCompressedBlockEntry::new(
                    offset,
                    compressed_size,
                    uncompressed_size,
                    compression_method_index,
                ));
            offset += compressed_size as u64;
        }
        let hash = hasher.finalize();
        let meta = FIoStoreTocEntryMeta {
            chunk_hash: FIoChunkHash::from_blake3(hash.as_bytes()),
            flags: FIoStoreTocEntryMetaFlags::empty(),
        };

        let offset_and_length = FIoOffsetAndLength::new(
            start_block as u64 * self.toc.compression_block_size as u64,
            data.len() as u64,
        );

        self.toc.chunks.push(chunk_id);
        self.toc.chunk_offset_lengths.push(offset_and_length);
        self.toc.chunk_metas.push(meta);

        Ok(())
    }

    // TODO @trumank: Use StoreEntry provided
    pub(crate) fn write_package_chunk(&mut self, chunk_id: FIoChunkId, path: Option<&str>, data: &[u8], store_entry: &StoreEntry) -> Result<()> {
        bail!("Also need to write store entry");
        self.write_chunk(chunk_id, path, data)
    }
    pub(crate) fn container_header_version(&self) -> EIoContainerHeaderVersion {
        // TODO @trumank implement
        todo!("Truman send help")
    }
    pub(crate) fn finalize(mut self) -> Result<()> {
        self.toc_stream.ser(&self.toc)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use fs_err as fs;

    #[test]
    fn test_write_container() -> Result<()> {
        let mut writer = IoStoreWriter::new(
            "new.utoc",
            EIoStoreTocVersion::PerfectHashWithOverflow,
            EIoContainerHeaderVersion::OptionalSegmentPackages,
            "../../..".to_string(),
        )?;

        let data = fs::read("script_objects.bin")?;
        writer.write_chunk(
            FIoChunkId {
                id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5],
            },
            Some("asdf/asdf/dasf/script_objects.bin"),
            &data,
        )?;
        writer.finalize()?;
        Ok(())
    }
}
