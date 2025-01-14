use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
};

use crate::{ser::*, FIoChunkId, Toc};
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
        Ok(Self {
            toc_path,
            cas_stream,
            toc: Toc::new(),
        })
    }
    pub(crate) fn write_chunk<P: AsRef<str>>(
        &mut self,
        chunk_id: FIoChunkId,
        file_name: Option<P>,
        data: &[u8],
    ) -> Result<()> {
        self.toc.chunks.push(chunk_id);
        //self.toc.chunk_offset_lengths: Vec<FIoOffsetAndLength>,
        //self.toc.compression_blocks: Vec<FIoStoreTocCompressedBlockEntry>,
        //self.toc.chunk_metas: Vec<FIoStoreTocEntryMeta>,

        //self.toc.chunk_perfect_hash_seeds: Vec<i32>,
        //self.toc.chunk_indices_without_perfect_hash: Vec<i32>,
        //self.toc.compression_methods: Vec<String>,
        //self.toc.signatures: Option<TocSignatures>,

        Ok(())
    }
}
