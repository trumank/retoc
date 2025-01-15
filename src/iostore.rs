use std::{
    ffi::OsStr,
    io::BufReader,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use fs_err as fs;
use anyhow::{Context as _, Result};

use crate::{
    container_header::{FIoContainerHeader, StoreEntryRef},
    ser::*,
    Config, EIoChunkType, FIoChunkId, FPackageId, Toc,
};

pub fn open<P: AsRef<Path>>(path: P, config: Arc<Config>) -> Result<Box<dyn IoStoreTrait>> {
    Ok(if path.as_ref().is_dir() {
        Box::new(IoStoreBackend::open(path, config)?)
    } else {
        Box::new(IoStoreContainer::open(path, config)?)
    })
}

pub trait IoStoreTrait {
    fn read(&self, chunk_id: FIoChunkId) -> Result<Vec<u8>>;
    fn has_chunk_id(&self, chunk_id: FIoChunkId) -> bool;
    fn chunks(&self) -> Box<dyn Iterator<Item = ChunkInfo> + '_>;
    fn file_name(&self, chunk_id: FIoChunkId) -> Option<&str>;
    fn package_store_entry(&self, package_id: FPackageId) -> Option<StoreEntryRef>;
}

pub struct ChunkInfo<'a> {
    id: FIoChunkId,
    container: &'a IoStoreContainer,
}
impl ChunkInfo<'_> {
    pub fn id(&self) -> FIoChunkId {
        self.id
    }
    pub fn container(&self) -> &IoStoreContainer {
        self.container
    }
    pub fn file_name(&self) -> Option<&str> {
        self.container.file_name(self.id)
    }
    pub fn read(&self) -> Result<Vec<u8>> {
        self.container.read(self.id)
    }
}

struct IoStoreBackend {
    containers: Vec<Box<dyn IoStoreTrait>>,
}
impl IoStoreBackend {
    pub fn new() -> Result<Self> {
        Ok(Self { containers: vec![] })
    }
    pub fn open<P: AsRef<Path>>(dir: P, config: Arc<Config>) -> Result<Self> {
        let mut containers: Vec<Box<dyn IoStoreTrait>> = vec![];
        for entry in fs::read_dir(dir.as_ref())? {
            let entry = entry?;
            let path = entry.path();
            if path.extension() == Some(OsStr::new("utoc")) {
                containers.push(Box::new(IoStoreContainer::open(path, config.clone())?));
            }
        }
        Ok(Self { containers })
    }
}
impl IoStoreTrait for IoStoreBackend {
    fn read(&self, chunk_id: FIoChunkId) -> Result<Vec<u8>> {
        self.containers
            .iter()
            .find(|c| c.has_chunk_id(chunk_id))
            .with_context(|| format!("{chunk_id:?} not found in any containers"))?
            .read(chunk_id)
    }
    fn has_chunk_id(&self, chunk_id: FIoChunkId) -> bool {
        self.containers.iter().any(|c| c.has_chunk_id(chunk_id))
    }
    fn chunks(&self) -> Box<dyn Iterator<Item = ChunkInfo> + '_> {
        Box::new(self.containers.iter().flat_map(|c| c.chunks()))
    }
    fn file_name(&self, chunk_id: FIoChunkId) -> Option<&str> {
        self.containers.iter().find_map(|c| c.file_name(chunk_id))
    }
    fn package_store_entry(&self, package_id: FPackageId) -> Option<StoreEntryRef> {
        self.containers
            .iter()
            .find_map(|c| c.package_store_entry(package_id))
    }
}

pub struct IoStoreContainer {
    name: Option<String>,
    path: PathBuf,
    toc: Toc,
    cas: Arc<Mutex<BufReader<fs::File>>>,

    container_header: Option<FIoContainerHeader>,
}
impl IoStoreContainer {
    pub fn open<P: AsRef<Path>>(toc_path: P, config: Arc<Config>) -> Result<Self> {
        let path = toc_path.as_ref().to_path_buf();
        let toc: Toc = BufReader::new(fs::File::open(&path)?).de_ctx(config)?;
        let cas = BufReader::new(fs::File::open(toc_path.as_ref().with_extension("ucas"))?);

        let mut container = Self {
            name: path.file_stem().map(|f| f.to_string_lossy().into()),
            path,
            toc,
            cas: Arc::new(Mutex::new(cas)),

            container_header: None,
        };

        // TODO avoid linear search for header
        // TODO populate header lazily?
        let header_chunk = container
            .chunks()
            .find(|info| info.id().get_chunk_type() == EIoChunkType::ContainerHeader);
        if let Some(header_chunk) = header_chunk {
            let data = container.read(header_chunk.id())?;
            let header = FIoContainerHeader::de(&mut std::io::Cursor::new(data))?;
            container.container_header = Some(header);
        }

        Ok(container)
    }
    pub fn container_path(&self) -> &Path {
        self.path.as_ref()
    }
    pub fn container_name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}
impl IoStoreTrait for IoStoreContainer {
    fn read(&self, chunk_id: FIoChunkId) -> Result<Vec<u8>> {
        self.toc.read(
            &mut *self.cas.lock().unwrap(),
            self.toc.chunk_id_map[&chunk_id],
        )
    }
    fn has_chunk_id(&self, chunk_id: FIoChunkId) -> bool {
        self.toc.chunk_id_map.contains_key(&chunk_id)
    }
    fn chunks(&self) -> Box<dyn Iterator<Item = ChunkInfo> + '_> {
        Box::new(self.toc.chunks.iter().map(|&id| ChunkInfo {
            id,
            container: self,
        }))
    }
    fn file_name(&self, chunk_id: FIoChunkId) -> Option<&str> {
        self.toc
            .chunk_id_map
            .get(&chunk_id)
            .and_then(|index| self.toc.file_map_rev.get(index))
            .map(String::as_str)
    }
    fn package_store_entry(&self, package_id: FPackageId) -> Option<StoreEntryRef> {
        self.container_header
            .as_ref()
            .and_then(|header| header.get_store_entry(package_id))
    }
}
