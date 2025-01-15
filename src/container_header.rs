use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek as _, SeekFrom},
    marker::PhantomData,
};

use anyhow::{bail, Context as _, Result};
use strum::FromRepr;
use tracing::instrument;

use crate::{
    name_map::{FMappedName, FNameMap},
    ser::*,
    FIoContainerId, FPackageId, FSHAHash, ReadExt,
};

#[derive(Debug)]
pub(crate) struct FIoContainerHeader {
    container_id: FIoContainerId,
    package_ids: Vec<FPackageId>,
    store_entries: StoreEntries,
    optional_segment_package_ids: Vec<FPackageId>,
    optional_segment_store_entries: Vec<u8>,
    redirect_name_map: FNameMap,
    localized_packages: Vec<FIoContainerHeaderLocalizedPackage>,
    package_redirects: Vec<FIoContainerHeaderPackageRedirect>,

    package_entry_map: HashMap<FPackageId, u32>,
}
impl Readable for FIoContainerHeader {
    #[instrument(skip_all, name = "FIoContainerHeader")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let signature: u32 = s.de()?;
        if signature != 0x496f436e {
            bail!("invalid ContainerHeader signature")
        }

        let version: EIoContainerHeaderVersion = s.de()?;

        let container_id = s.de()?;
        let package_ids: Vec<_> = s.de()?;
        let store_entries = s.de_ctx((version, package_ids.len()))?;
        let optional_segment_package_ids = s.de()?;
        let optional_segment_store_entries = s.de()?;
        let redirect_name_map = s.de()?;
        let localized_packages = s.de()?;
        let package_redirects = s.de()?;

        // TODO SoftPackageReferences

        let package_entry_map = package_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i as u32))
            .collect();

        Ok(Self {
            container_id,
            package_ids,
            store_entries,
            optional_segment_package_ids,
            optional_segment_store_entries,
            redirect_name_map,
            localized_packages,
            package_redirects,
            package_entry_map,
        })
    }
}
impl FIoContainerHeader {
    pub(crate) fn get_store_entry(&self, package_id: FPackageId) -> Option<StoreEntryRef> {
        // TODO handle redirects?
        let index = *self.package_entry_map.get(&package_id)?;
        Some(self.store_entries.get_ref(index))
    }
    pub(crate) fn package_ids(&self) -> &[FPackageId] {
        &self.package_ids
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, FromRepr)]
#[repr(u32)]
enum EIoContainerHeaderVersion {
    Initial = 0,
    LocalizedPackages = 1,
    OptionalSegmentPackages = 2,
    NoExportInfo = 3,
    SoftPackageReferences = 4,
}
impl Readable for EIoContainerHeaderVersion {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let value = s.de()?;
        Self::from_repr(value).with_context(|| format!("invalid EIoStoreTocVersion value: {value}"))
    }
}

#[derive(Debug)]
struct FIoContainerHeaderLocalizedPackage {
    source_package_id: FPackageId,
    source_package_name: FMappedName,
}
impl Readable for FIoContainerHeaderLocalizedPackage {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            source_package_id: s.de()?,
            source_package_name: s.de()?,
        })
    }
}

#[derive(Debug)]
struct FIoContainerHeaderPackageRedirect {
    source_package_id: FPackageId,
    target_package_id: FPackageId,
    source_package_name: FMappedName,
}
impl Readable for FIoContainerHeaderPackageRedirect {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            source_package_id: s.de()?,
            target_package_id: s.de()?,
            source_package_name: s.de()?,
        })
    }
}

#[derive(Debug)]
pub(crate) struct StoreEntryRef<'a> {
    pub(crate) imported_packages: &'a [FPackageId],
    pub(crate) shader_map_hashes: &'a [FSHAHash],
}
#[derive(Debug, Default)]
pub(crate) struct StoreEntry {
    pub(crate) imported_packages: Vec<FPackageId>,
    pub(crate) shader_map_hashes: Vec<FSHAHash>,
}
impl StoreEntryRef<'_> {
    pub(crate) fn to_owned(&self) -> StoreEntry {
        StoreEntry {
            imported_packages: self.imported_packages.to_vec(),
            shader_map_hashes: self.shader_map_hashes.to_vec(),
        }
    }
}

#[derive(Debug)]
struct StoreEntries {
    imported_packages: HashMap<u32, Vec<FPackageId>>,
    shader_map_hashes: HashMap<u32, Vec<FSHAHash>>,
}
impl StoreEntries {
    fn get_ref(&self, index: u32) -> StoreEntryRef {
        StoreEntryRef {
            imported_packages: self
                .imported_packages
                .get(&index)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            shader_map_hashes: self
                .shader_map_hashes
                .get(&index)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        }
    }
}
impl ReadableCtx<(EIoContainerHeaderVersion, usize)> for StoreEntries {
    fn de<S: Read>(
        s: &mut S,
        (version, package_count): (EIoContainerHeaderVersion, usize),
    ) -> Result<Self> {
        let buffer: Vec<u8> = s.de()?;
        let mut cur = Cursor::new(buffer);

        let mut imported_packages = HashMap::new();
        let mut shader_map_hashes = HashMap::new();

        let entries: Vec<FFilePackageStoreEntry> =
            read_array(package_count, &mut cur, |s| s.de_ctx(version))?;
        for (i, entry) in entries.iter().enumerate() {
            let offset = i * std::mem::size_of::<FFilePackageStoreEntry>();

            let num = entry.imported_packages.array_num as usize;
            if num != 0 {
                let offset = offset
                    + entry.imported_packages.offset_to_data_from_this as usize
                    + std::mem::offset_of!(FFilePackageStoreEntry, imported_packages);
                cur.seek(SeekFrom::Start(offset as u64))?;
                let array: Vec<_> = cur.de_ctx(num)?;
                imported_packages.insert(i as u32, array);
            }

            let num = entry.shader_map_hashes.array_num as usize;
            if num != 0 {
                let offset = offset
                    + entry.shader_map_hashes.offset_to_data_from_this as usize
                    + std::mem::offset_of!(FFilePackageStoreEntry, shader_map_hashes);
                cur.seek(SeekFrom::Start(offset as u64))?;
                let array: Vec<_> = cur.de_ctx(num)?;
                shader_map_hashes.insert(i as u32, array);
            }
        }
        Ok(Self {
            imported_packages,
            shader_map_hashes,
        })
    }
}

#[derive(Debug)]
#[repr(C)]
struct TFilePackageStoreEntryCArrayView<T> {
    array_num: u32,
    offset_to_data_from_this: u32,
    _phantom: PhantomData<T>,
}
impl<T> Readable for TFilePackageStoreEntryCArrayView<T> {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            array_num: s.de()?,
            offset_to_data_from_this: s.de()?,
            _phantom: Default::default(),
        })
    }
}

#[derive(Debug)]
#[repr(C)] // Needed to determine the number of package store entries
struct FFilePackageStoreEntry {
    imported_packages: TFilePackageStoreEntryCArrayView<FPackageId>,
    shader_map_hashes: TFilePackageStoreEntryCArrayView<FSHAHash>,
}
impl ReadableCtx<EIoContainerHeaderVersion> for FFilePackageStoreEntry {
    #[instrument(skip_all, name = "FFilePackageStoreEntry ")]
    fn de<S: Read>(s: &mut S, version: EIoContainerHeaderVersion) -> Result<Self> {
        if version == EIoContainerHeaderVersion::Initial {
            todo!()
        } else {
            if version < EIoContainerHeaderVersion::NoExportInfo {
                // TODO
                let export_count: u32 = s.de()?;
                let export_bundle_count: u32 = s.de()?;
            }
            Ok(Self {
                imported_packages: s.de()?,
                shader_map_hashes: s.de()?,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use fs_err as fs;
    use std::io::BufReader;

    #[test]
    fn test_container_header() -> Result<()> {
        let mut stream = BufReader::new(fs::File::open("containerheader.bin")?);

        let header = ser_hex::read("trace.json", &mut stream, FIoContainerHeader::de)?;
        dbg!(header.store_entries.shader_map_hashes);

        Ok(())
    }
}
