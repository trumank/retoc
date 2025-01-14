use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

use crate::{ser::*, FIoChunkId, FIoOffsetAndLength, FIoStoreTocCompressedBlockEntry, Toc};
use anyhow::Result;

pub(crate) struct IoStoreWriter {
    toc_path: PathBuf,
    cas_stream: BufWriter<File>,
    toc: Toc,
}
impl IoStoreWriter {
    pub(crate) fn new<P: AsRef<Path>>(toc_path: P) -> Result<Self> {
        let toc_path = toc_path.as_ref().to_path_buf();
        let cas_stream = BufWriter::new(File::create(toc_path.with_extension(".ucas"))?);

        let mut toc = Toc::new();
        toc.header.compression_block_size = 0x10000;

        Ok(Self {
            toc_path,
            cas_stream,
            toc,
        })
    }
    pub(crate) fn write_chunk<P: AsRef<str>>(
        &mut self,
        chunk_id: FIoChunkId,
        file_name: Option<P>,
        data: &[u8],
    ) -> Result<()> {
        // write data to CAS
        let offset = self.cas_stream.stream_position()?;

        let offset_and_length = FIoOffsetAndLength::new(offset, data.len() as u64);

        for block in data.chunks(self.toc.header.compression_block_size as usize) {
            self.cas_stream.write_all(block)?;
            let compressed_size = data.len() as u32;
            let uncompressed_size = data.len() as u32;
            let compression_method_index = 0; // none
            self.toc.compression_blocks.push(FIoStoreTocCompressedBlockEntry::new(
                offset,
                compressed_size,
                uncompressed_size,
                compression_method_index,
            ));
        }

        self.toc.chunks.push(chunk_id);
        self.toc.chunk_offset_lengths.push(offset_and_length);
        //self.toc.chunk_metas: Vec<FIoStoreTocEntryMeta>,

        //self.toc.chunk_perfect_hash_seeds: Vec<i32>,
        //self.toc.chunk_indices_without_perfect_hash: Vec<i32>,
        //self.toc.compression_methods: Vec<String>,
        //self.toc.signatures: Option<TocSignatures>,

        Ok(())
    }
}
