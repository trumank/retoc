use std::{
    ffi::OsStr,
    io::BufReader,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use fs_err as fs;

use crate::{
    container_header::{EIoContainerHeaderVersion, FIoContainerHeader, StoreEntry},
    ser::*,
    Config, EIoChunkType, EIoStoreTocVersion, FIoChunkId, FPackageId, Toc,
};

macro_rules! indent_println {
    ($indent:expr, $($arg:tt)*) => {
        println!("{:width$}{}", "", format!($($arg)*), width = 2 * $indent);
    }
}

pub fn open<P: AsRef<Path>>(path: P, config: Arc<Config>) -> Result<Box<dyn IoStoreTrait>> {
    Ok(if path.as_ref().is_dir() {
        Box::new(IoStoreBackend::open(path, config)?)
    } else {
        Box::new(IoStoreContainer::open(path, config)?)
    })
}

/// Return an object that can be sorted by to achieve container priority.
/// Higher priority should Cmp higher
fn sort_container_name(full_name: &str) -> (bool, u32, &str) {
    let mut base_name = full_name;

    let mut chunk_version = 0;
    if let Some(name) = base_name.strip_suffix("_P") {
        base_name = name;
        chunk_version = 1;
        if let Some((name, version)) = base_name.rsplit_once("_") {
            if let Ok(version) = version.parse::<u32>() {
                base_name = name;
                chunk_version = version + 2;
            }
        }
    }

    // special case global to always sort highest
    (full_name == "global", chunk_version, base_name)
}

pub trait IoStoreTrait {
    fn container_name(&self) -> &str;
    fn container_file_version(&self) -> Option<EIoStoreTocVersion>;
    fn container_header_version(&self) -> Option<EIoContainerHeaderVersion>;
    fn print_info(&self, depth: usize);

    fn read(&self, chunk_id: FIoChunkId) -> Result<Vec<u8>>;
    fn has_chunk_id(&self, chunk_id: FIoChunkId) -> bool;
    fn chunks(&self) -> Box<dyn Iterator<Item = ChunkInfo> + '_>;
    fn packages(&self) -> Box<dyn Iterator<Item = PackageInfo> + '_>;
    fn child_containers(&self) -> Box<dyn Iterator<Item = &dyn IoStoreTrait> + '_>;
    /// Get absolute path (including mount point) if it has one
    fn chunk_path(&self, chunk_id: FIoChunkId) -> Option<String>;
    fn package_store_entry(&self, package_id: FPackageId) -> Option<StoreEntry>;
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
    pub fn path(&self) -> Option<String> {
        self.container.chunk_path(self.id)
    }
    pub fn read(&self) -> Result<Vec<u8>> {
        self.container.read(self.id)
    }
}

pub struct PackageInfo<'a> {
    id: FPackageId,
    container: &'a IoStoreContainer,
}
impl PackageInfo<'_> {
    pub fn id(&self) -> FPackageId {
        self.id
    }
    pub fn container(&self) -> &IoStoreContainer {
        self.container
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
        // Validate that all containers are of the same version
        let mut previous_container_version: Option<EIoStoreTocVersion> = None;
        let mut previous_container_name: String = String::new();
        let mut previous_header_container_version: Option<EIoContainerHeaderVersion> = None;
        let mut previous_header_container_name: String = String::new();

        for container in &containers {
            let this_container_version = container.container_file_version().unwrap();
            let this_container_name = container.container_name().to_string();

            // Check that container Table Of Contents version matches the previous container
            if previous_container_version.is_none() {
                previous_container_name = this_container_name.clone();
                previous_container_version = Some(this_container_version);
            }
            if this_container_version != previous_container_version.unwrap() {
                bail!("Cannot create composite container for containers of different versions: Container {} and {} have different versions {:?} and {:?}",
                    previous_container_name, this_container_name, previous_container_version.unwrap(), this_container_version);
            }

            // Check that container header version matches the previous container
            if let Some(this_container_header_version) = container.container_header_version() {
                if previous_header_container_version.is_none() {
                    previous_header_container_name = this_container_name.clone();
                    previous_header_container_version = Some(this_container_header_version);
                }
                if this_container_header_version != previous_header_container_version.unwrap() {
                    bail!("Cannot create composite container for containers of different header versions: Container {} and {} have different versions {:?} and {:?}",
                     previous_header_container_name, this_container_name, previous_header_container_version.unwrap(), this_container_header_version);
                }
            }
        }

        containers.sort_by(|a, b| {
            sort_container_name(b.container_name()).cmp(&sort_container_name(a.container_name()))
        });
        Ok(Self { containers })
    }
}
impl IoStoreTrait for IoStoreBackend {
    fn container_name(&self) -> &str {
        "VIRTUAL"
    }
    fn container_file_version(&self) -> Option<EIoStoreTocVersion> {
        self.containers
            .first()
            .and_then(|x| x.container_file_version())
    }
    fn container_header_version(&self) -> Option<EIoContainerHeaderVersion> {
        // Some containers might not have a container header, so take the first container with a header
        self.containers
            .iter()
            .find_map(|x| x.container_header_version())
    }
    fn print_info(&self, mut depth: usize) {
        indent_println!(depth, "{}", self.container_name());
        depth += 1;

        if self.child_containers().count() != 0 {
            indent_println!(depth, "child containers ({}):", self.containers.len());
            for container in self.child_containers() {
                container.print_info(depth + 1);
            }
        }
    }
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
    fn packages(&self) -> Box<dyn Iterator<Item = PackageInfo> + '_> {
        Box::new(self.containers.iter().flat_map(|c| c.packages()))
    }
    fn child_containers(&self) -> Box<dyn Iterator<Item = &dyn IoStoreTrait> + '_> {
        Box::new(self.containers.iter().map(Box::as_ref))
    }
    fn chunk_path(&self, chunk_id: FIoChunkId) -> Option<String> {
        self.containers.iter().find_map(|c| c.chunk_path(chunk_id))
    }
    fn package_store_entry(&self, package_id: FPackageId) -> Option<StoreEntry> {
        self.containers
            .iter()
            .find_map(|c| c.package_store_entry(package_id))
    }
}

pub struct IoStoreContainer {
    name: String,
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
            name: path
                .file_stem()
                .context("failed to get container name")?
                .to_string_lossy()
                .into(),
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
}
impl IoStoreTrait for IoStoreContainer {
    fn container_name(&self) -> &str {
        &self.name
    }
    fn container_file_version(&self) -> Option<EIoStoreTocVersion> {
        Some(self.toc.version)
    }
    fn container_header_version(&self) -> Option<EIoContainerHeaderVersion> {
        self.container_header.as_ref().map(|x| x.version)
    }
    fn print_info(&self, mut depth: usize) {
        indent_println!(depth, "{}", self.container_name());
        depth += 1;

        indent_println!(depth, "container_flags: {:?}", self.toc.container_flags);
        indent_println!(depth, "version: {:?}", self.toc.version);
        let mount_point = &self.toc.directory_index.mount_point;
        if !mount_point.is_empty() {
            indent_println!(depth, "mount_point: {}", mount_point);
        }
        indent_println!(depth, "chunks: {}", self.toc.chunks.len());
        indent_println!(depth, "packages: {}", self.packages().count());
        // assumes header has already been parsed
        indent_println!(
            depth,
            "has_container_header: {}",
            self.container_header.is_some()
        );
        indent_println!(
            depth,
            "compression_methods: {:?}",
            self.toc.compression_methods
        );
    }
    fn read(&self, chunk_id: FIoChunkId) -> Result<Vec<u8>> {
        let index = *self.toc.chunk_id_map.get(&chunk_id).with_context(|| {
            format!("container {:?} does not contain {:?}", self.name, chunk_id)
        })?;
        self.toc.read(&mut *self.cas.lock().unwrap(), index)
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
    fn packages(&self) -> Box<dyn Iterator<Item = PackageInfo> + '_> {
        Box::new(
            self.container_header
                .iter()
                .flat_map(|header| header.package_ids())
                .map(|&id| PackageInfo {
                    id,
                    container: self,
                }),
        )
    }
    fn child_containers(&self) -> Box<dyn Iterator<Item = &dyn IoStoreTrait> + '_> {
        Box::new(std::iter::empty())
    }
    fn chunk_path(&self, chunk_id: FIoChunkId) -> Option<String> {
        self.toc.file_name(chunk_id)
    }
    fn package_store_entry(&self, package_id: FPackageId) -> Option<StoreEntry> {
        self.container_header
            .as_ref()
            .and_then(|header| header.get_store_entry(package_id))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sort_container() {
        let mut containers = [
            "pakchunk0-Windows",
            "pakchunk10-Windows",
            "pakchunk11-Windows",
            "pakchunk12-Windows",
            "pakchunk13-Windows",
            "pakchunk14-Windows",
            "pakchunk15-Windows",
            "global",
            "pakchunk16-Windows",
            "pakchunk17-Windows",
            "pakchunk1optional-Windows",
            "pakchunk1-Windows",
            "pakchunk8-Windows_1_P",
            "pakchunk3-Windows",
            "pakchunk8-Windows_P",
            "pakchunk6-Windows",
            "pakchunk0optional-Windows",
            "pakchunk9-Windows",
            "pakchunk8-Windows_0_P",
            "pakchunk4-Windows",
            "pakchunk2-Windows",
            "pakchunk5-Windows",
            "pakchunk7-Windows",
        ];
        containers.sort_by(|a, b| sort_container_name(b).cmp(&sort_container_name(a)));
        for container in containers {
            eprintln!("{:?}", sort_container_name(container));
        }
        assert_eq!(
            containers,
            [
                "global",
                "pakchunk8-Windows_1_P",
                "pakchunk8-Windows_0_P",
                "pakchunk8-Windows_P",
                "pakchunk9-Windows",
                "pakchunk7-Windows",
                "pakchunk6-Windows",
                "pakchunk5-Windows",
                "pakchunk4-Windows",
                "pakchunk3-Windows",
                "pakchunk2-Windows",
                "pakchunk1optional-Windows",
                "pakchunk17-Windows",
                "pakchunk16-Windows",
                "pakchunk15-Windows",
                "pakchunk14-Windows",
                "pakchunk13-Windows",
                "pakchunk12-Windows",
                "pakchunk11-Windows",
                "pakchunk10-Windows",
                "pakchunk1-Windows",
                "pakchunk0optional-Windows",
                "pakchunk0-Windows",
            ]
        );
    }
}
