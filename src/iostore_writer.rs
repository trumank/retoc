use std::{
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

use crate::{
    align_usize,
    chunk_id::FIoChunkIdRaw,
    container_header::{EIoContainerHeaderVersion, FIoContainerHeader, StoreEntry},
    EIoChunkType, FPackageId, UEPath, UEPathBuf,
};
use crate::{
    ser::*, EIoStoreTocVersion, FIoChunkHash, FIoChunkId, FIoContainerId, FIoOffsetAndLength,
    FIoStoreTocCompressedBlockEntry, FIoStoreTocEntryMeta, FIoStoreTocEntryMetaFlags, Toc,
};
use anyhow::{Context, Result};
use fs_err as fs;

pub(crate) struct IoStoreWriter {
    toc_path: PathBuf,
    toc_stream: BufWriter<fs::File>,
    cas_stream: BufWriter<fs::File>,
    toc: Toc,
    container_header: Option<FIoContainerHeader>,
}

impl IoStoreWriter {
    pub(crate) fn new<P: AsRef<Path>>(
        toc_path: P,
        toc_version: EIoStoreTocVersion,
        container_header_version: Option<EIoContainerHeaderVersion>,
        mount_point: UEPathBuf,
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

        let container_header =
            container_header_version.map(|v| FIoContainerHeader::new(v, toc.container_id));

        Ok(Self {
            toc_path,
            toc_stream,
            cas_stream,
            toc,
            container_header,
        })
    }
    pub(crate) fn write_chunk_raw(
        &mut self,
        chunk_id_raw: FIoChunkIdRaw,
        path: Option<&UEPath>,
        data: &[u8],
    ) -> Result<()> {
        self.write_chunk(
            FIoChunkId::from_raw(chunk_id_raw, self.toc.version),
            path,
            data,
        )
    }
    pub(crate) fn write_chunk(
        &mut self,
        chunk_id: FIoChunkId,
        path: Option<&UEPath>,
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

        self.toc
            .chunks
            .push(chunk_id.with_version(self.toc.version));
        self.toc.chunk_offset_lengths.push(offset_and_length);
        self.toc.chunk_metas.push(meta);

        Ok(())
    }

    pub(crate) fn write_package_chunk(
        &mut self,
        chunk_id: FIoChunkId,
        path: Option<&UEPath>,
        data: &[u8],
        store_entry: &StoreEntry,
    ) -> Result<()> {
        let container_header = self
            .container_header
            .as_mut()
            .expect("FIoContainerHeader is required to write package chunks");
        container_header.add_package(FPackageId(chunk_id.get_chunk_id()), store_entry.clone());
        self.write_chunk(chunk_id, path, data)
    }
    pub(crate) fn add_localized_package(
        &mut self,
        package_culture: &str,
        source_package_name: &str,
        localized_package_id: FPackageId,
    ) -> Result<()> {
        let container_header = self
            .container_header
            .as_mut()
            .expect("FIoContainerHeader is required to add localized packages");
        container_header.add_localized_package(
            package_culture,
            source_package_name,
            localized_package_id,
        )
    }
    pub(crate) fn add_package_redirect(
        &mut self,
        source_package_name: &str,
        redirect_package_id: FPackageId,
    ) -> Result<()> {
        let container_header = self
            .container_header
            .as_mut()
            .expect("FIoContainerHeader is required to add package redirects");
        container_header.add_package_redirect(source_package_name, redirect_package_id)
    }
    pub(crate) fn container_version(&self) -> EIoStoreTocVersion {
        self.toc.version
    }
    pub(crate) fn container_header_version(&self) -> EIoContainerHeaderVersion {
        self.container_header.as_ref().unwrap().version
    }
    pub(crate) fn finalize(mut self) -> Result<()> {
        if let Some(container_header) = &self.container_header {
            let mut chunk_buffer = vec![];
            container_header.ser(&mut chunk_buffer)?;
            // container header is always aligned for AES for some reason
            chunk_buffer.resize(align_usize(chunk_buffer.len(), 16), 0);

            let chunk_id = FIoChunkId::create(
                container_header.container_id.0,
                0,
                EIoChunkType::ContainerHeader,
            );
            self.write_chunk(chunk_id, None, &chunk_buffer)?;
        }
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
        fs::create_dir("out").ok();
        let mut writer = IoStoreWriter::new(
            "out/new.utoc",
            EIoStoreTocVersion::PerfectHashWithOverflow,
            Some(EIoContainerHeaderVersion::OptionalSegmentPackages),
            "../../..".into(),
        )?;

        let data = fs::read("tests/UE5.3/ScriptObjects.bin")?;
        writer.write_chunk_raw(
            FIoChunkIdRaw {
                id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5],
            },
            Some(UEPath::new("../../../asdf/asdf/dasf/script_objects.bin")),
            &data,
        )?;
        writer.finalize()?;
        Ok(())
    }
}
