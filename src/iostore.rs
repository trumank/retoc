use std::{
    collections::HashSet,
    ffi::OsStr,
    io::{BufReader, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use fs_err as fs;

use crate::{
    chunk_id::FIoChunkIdRaw,
    container_header::{EIoContainerHeaderVersion, FIoContainerHeader, StoreEntry},
    file_pool::FilePool,
    script_objects::ZenScriptObjects,
    ser::*,
    Config, EIoChunkType, EIoStoreTocVersion, FIoChunkHash, FIoChunkId, FPackageId, Toc,
};

macro_rules! indent_println {
    ($indent:expr, $($arg:tt)*) => {
        println!("{:width$}{}", "", format!($($arg)*), width = 2 * $indent);
    }
}

struct UniqueIterator<I, T> {
    inner: I,
    encountered: HashSet<T>,
}

impl<I, T> UniqueIterator<I, T> {
    fn new(inner: I) -> Self {
        Self {
            inner,
            encountered: HashSet::new(),
        }
    }
}

impl<I: Iterator> Iterator for UniqueIterator<I, I::Item>
where
    I::Item: std::hash::Hash + std::cmp::Eq + Copy,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.inner.next();
            if let Some(next) = next {
                if self.encountered.insert(next) {
                    return Some(next);
                }
            } else {
                return None;
            }
        }
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

pub trait IoStoreTrait: Send + Sync {
    fn container_name(&self) -> &str;
    fn container_file_version(&self) -> Option<EIoStoreTocVersion>;
    fn container_header_version(&self) -> Option<EIoContainerHeaderVersion>;
    fn print_info(&self, depth: usize);

    fn read(&self, chunk_id: FIoChunkId) -> Result<Vec<u8>>;
    fn read_raw(&self, chunk_id_raw: FIoChunkIdRaw) -> Result<Vec<u8>>;
    fn has_chunk_id(&self, chunk_id: FIoChunkId) -> bool;
    fn has_chunk_id_raw(&self, chunk_id_raw: FIoChunkIdRaw) -> bool;
    fn chunks(&self) -> Box<dyn Iterator<Item = ChunkInfo> + Send + '_>;
    fn chunks_all(&self) -> Box<dyn Iterator<Item = ChunkInfo> + Send + '_>;
    fn packages(&self) -> Box<dyn Iterator<Item = PackageInfo> + Send + '_>;
    fn packages_all(&self) -> Box<dyn Iterator<Item = PackageInfo> + Send + '_>;
    fn child_containers(&self) -> Box<dyn Iterator<Item = &dyn IoStoreTrait> + '_>;
    /// Get absolute path (including mount point) if it has one
    fn chunk_path(&self, chunk_id: FIoChunkId) -> Option<String>;
    fn package_store_entry(&self, package_id: FPackageId) -> Option<StoreEntry>;
    fn lookup_package_redirect(&self, source_package_id: FPackageId) -> Option<FPackageId>;

    fn load_script_objects(&self) -> Result<ZenScriptObjects> {
        if self.container_file_version().unwrap() > EIoStoreTocVersion::PerfectHash {
            let script_objects_data =
                self.read(FIoChunkId::create(0, 0, EIoChunkType::ScriptObjects))?;
            ZenScriptObjects::deserialize_new(&mut Cursor::new(script_objects_data))
        } else {
            let script_objects_data = self.read(FIoChunkId::create(
                0,
                0,
                EIoChunkType::LoaderInitialLoadMeta,
            ))?;
            let names = self.read(FIoChunkId::create(0, 0, EIoChunkType::LoaderGlobalNames))?;
            ZenScriptObjects::deserialize_old(&mut Cursor::new(script_objects_data), &names)
        }
    }
}

#[derive(Clone, Copy)]
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
    fn toc_index(&self) -> u32 {
        *self.container.toc.chunk_id_map.get(&self.id).unwrap()
    }
    pub fn hash(&self) -> &FIoChunkHash {
        &self.container.toc.chunk_metas[self.toc_index() as usize].chunk_hash
    }
    pub fn read(&self) -> Result<Vec<u8>> {
        self.container.read(self.id)
    }
}
impl std::cmp::Eq for ChunkInfo<'_> {}
impl std::cmp::PartialEq for ChunkInfo<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}
impl std::hash::Hash for ChunkInfo<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

#[derive(Clone, Copy)]
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
impl std::cmp::Eq for PackageInfo<'_> {}
impl std::cmp::PartialEq for PackageInfo<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}
impl std::hash::Hash for PackageInfo<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
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
    fn read(&self, mut chunk_id: FIoChunkId) -> Result<Vec<u8>> {
        if let Some(version) = self.container_file_version() {
            chunk_id = chunk_id.with_version(version);
        }
        self.containers
            .iter()
            .find(|c| c.has_chunk_id(chunk_id))
            .with_context(|| format!("{chunk_id:?} not found in any containers"))?
            .read(chunk_id)
    }
    fn read_raw(&self, chunk_id_raw: FIoChunkIdRaw) -> Result<Vec<u8>> {
        self.containers
            .iter()
            .find(|c| c.has_chunk_id_raw(chunk_id_raw))
            .with_context(|| format!("{chunk_id_raw:?} not found in any containers"))?
            .read_raw(chunk_id_raw)
    }
    fn has_chunk_id(&self, chunk_id: FIoChunkId) -> bool {
        self.containers.iter().any(|c| c.has_chunk_id(chunk_id))
    }
    fn has_chunk_id_raw(&self, chunk_id_raw: FIoChunkIdRaw) -> bool {
        self.containers
            .iter()
            .any(|c| c.has_chunk_id_raw(chunk_id_raw))
    }
    fn chunks(&self) -> Box<dyn Iterator<Item = ChunkInfo> + Send + '_> {
        Box::new(UniqueIterator::new(self.chunks_all()))
    }
    fn chunks_all(&self) -> Box<dyn Iterator<Item = ChunkInfo> + Send + '_> {
        Box::new(self.containers.iter().flat_map(|c| c.chunks_all()))
    }
    fn packages(&self) -> Box<dyn Iterator<Item = PackageInfo> + Send + '_> {
        Box::new(self.containers.iter().flat_map(|c| c.packages()))
    }
    fn packages_all(&self) -> Box<dyn Iterator<Item = PackageInfo> + Send + '_> {
        Box::new(UniqueIterator::new(
            self.containers.iter().flat_map(|c| c.packages()),
        ))
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
    fn lookup_package_redirect(&self, source_package_id: FPackageId) -> Option<FPackageId> {
        self.containers
            .iter()
            .find_map(|c| c.lookup_package_redirect(source_package_id))
    }
}

pub struct IoStoreContainer {
    name: String,
    path: PathBuf,
    toc: Toc,
    cas: FilePool,

    container_header: Option<FIoContainerHeader>,
}
impl IoStoreContainer {
    pub fn open<P: AsRef<Path>>(toc_path: P, config: Arc<Config>) -> Result<Self> {
        let path = toc_path.as_ref().to_path_buf();
        let toc: Toc = BufReader::new(fs::File::open(&path)?).de_ctx(config.clone())?;
        let cas = FilePool::new(path.with_extension("ucas"), rayon::max_num_threads())?;

        let mut container = Self {
            name: path
                .file_stem()
                .context("failed to get container name")?
                .to_string_lossy()
                .into(),
            path,
            toc,
            cas,

            container_header: None,
        };

        // TODO avoid linear search for header
        // TODO populate header lazily?
        let header_chunk = container
            .chunks()
            .find(|info| info.id().get_chunk_type() == EIoChunkType::ContainerHeader);
        if let Some(header_chunk) = header_chunk {
            let chunk_id = header_chunk.id();
            let data = container.read(chunk_id)?;
            match FIoContainerHeader::deserialize(
                &mut std::io::Cursor::new(&data),
                config.container_header_version_override,
            ) {
                Ok(header) => {
                    container.container_header = Some(header);
                }
                Err(err) => {
                    eprintln!("Failed to parse ContainerHeader ({chunk_id:?}). Package metadata will be unavailable: {err:?}");
                }
            }
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
        if !mount_point.as_str().is_empty() {
            indent_println!(depth, "mount_point: {}", mount_point);
        }
        indent_println!(depth, "chunks: {}", self.toc.chunks.len());
        indent_println!(depth, "packages: {}", self.packages().count());
        // assumes header has already been parsed
        indent_println!(
            depth,
            "container_header_version: {:?}",
            self.container_header.as_ref().map(|h| h.version)
        );
        indent_println!(
            depth,
            "compression_methods: {:?}",
            self.toc.compression_methods
        );
    }
    fn read(&self, chunk_id: FIoChunkId) -> Result<Vec<u8>> {
        let chunk_id = chunk_id.with_version(self.toc.version);
        let index = *self.toc.chunk_id_map.get(&chunk_id).with_context(|| {
            format!("container {:?} does not contain {:?}", self.name, chunk_id)
        })?;
        let mut file_lock = self.cas.acquire()?;
        self.toc
            .read(&mut file_lock.file(), index)
            .with_context(|| format!("Failed to read chunk {chunk_id:?}"))
    }
    fn read_raw(&self, chunk_id_raw: FIoChunkIdRaw) -> Result<Vec<u8>> {
        self.read(FIoChunkId::from_raw(chunk_id_raw, self.toc.version))
    }
    fn has_chunk_id(&self, chunk_id: FIoChunkId) -> bool {
        self.toc
            .chunk_id_map
            .contains_key(&chunk_id.with_version(self.toc.version))
    }
    fn has_chunk_id_raw(&self, chunk_id_raw: FIoChunkIdRaw) -> bool {
        self.has_chunk_id(FIoChunkId::from_raw(chunk_id_raw, self.toc.version))
    }
    fn chunks(&self) -> Box<dyn Iterator<Item = ChunkInfo> + Send + '_> {
        // chunks should already be unique in individual containers
        self.chunks_all()
    }
    fn chunks_all(&self) -> Box<dyn Iterator<Item = ChunkInfo> + Send + '_> {
        Box::new(self.toc.chunks.iter().map(|&id| ChunkInfo {
            id,
            container: self,
        }))
    }
    fn packages(&self) -> Box<dyn Iterator<Item = PackageInfo> + Send + '_> {
        // packages should already be unique in individual containers
        self.packages_all()
    }
    fn packages_all(&self) -> Box<dyn Iterator<Item = PackageInfo> + Send + '_> {
        Box::new(
            self.container_header
                .iter()
                .flat_map(|header| header.package_ids())
                .map(|id| PackageInfo {
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
    fn lookup_package_redirect(&self, source_package_id: FPackageId) -> Option<FPackageId> {
        self.container_header
            .as_ref()
            .and_then(|header| header.lookup_package_redirect(source_package_id))
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
