use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek as _, SeekFrom, Write},
    marker::PhantomData,
};

use anyhow::{bail, Context as _, Result};
use strum::FromRepr;
use tracing::instrument;

use crate::name_map::EMappedNameType;
use crate::{
    name_map::{FMappedName, FNameMap},
    ser::*,
    FIoContainerId, FPackageId, FSHAHash, ReadExt,
};

#[derive(Debug, PartialEq)]
pub(crate) struct FIoContainerHeader {
    pub(crate) version: EIoContainerHeaderVersion,
    pub(crate) container_id: FIoContainerId,
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
        if signature != Self::MAGIC {
            bail!("invalid ContainerHeader signature")
        }

        let version: EIoContainerHeaderVersion = s.de()?;

        let container_id = s.de()?;
        let package_ids: Vec<_> = s.de()?;
        let store_entries = StoreEntries::deserialize(s, version, package_ids.len())?;
        let optional_segment_package_ids = s.de()?;
        let optional_segment_store_entries = s.de()?;
        let redirect_name_map = FNameMap::deserialize(s, EMappedNameType::Container)?;
        let localized_packages = s.de()?;
        let package_redirects = s.de()?;

        if version >= EIoContainerHeaderVersion::SoftPackageReferences {
            todo!("soft package references")
        }

        let package_entry_map = package_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i as u32))
            .collect();

        Ok(Self {
            version,
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
impl Writeable for FIoContainerHeader {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&Self::MAGIC)?;
        s.ser(&self.version)?;
        s.ser(&self.container_id)?;
        s.ser(&self.package_ids)?;
        self.store_entries.serialize(s, self.version)?;
        s.ser(&self.optional_segment_package_ids)?;
        s.ser(&self.optional_segment_store_entries)?;
        self.redirect_name_map.serialize(s)?;
        s.ser(&self.localized_packages)?;
        s.ser(&self.package_redirects)?;

        if self.version >= EIoContainerHeaderVersion::SoftPackageReferences {
            todo!("soft package references")
        }

        Ok(())
    }
}
impl FIoContainerHeader {
    const MAGIC: u32 = 0x496f436e;

    pub(crate) fn new(version: EIoContainerHeaderVersion, container_id: FIoContainerId) -> Self {
        Self {
            version,
            container_id,
            package_ids: vec![],
            store_entries: StoreEntries::default(),
            optional_segment_package_ids: vec![],
            optional_segment_store_entries: vec![],
            redirect_name_map: FNameMap::default(),
            localized_packages: vec![],
            package_redirects: vec![],
            package_entry_map: HashMap::new(),
        }
    }

    pub(crate) fn add_package(&mut self, package_id: FPackageId, store_entry: StoreEntry) {
        self.package_ids.push(package_id);
        self.store_entries.entries.push(store_entry);
    }

    pub(crate) fn get_store_entry(&self, package_id: FPackageId) -> Option<StoreEntry> {
        // TODO handle redirects?
        let index = *self.package_entry_map.get(&package_id)?;
        Some(self.store_entries.get(index))
    }
    pub(crate) fn package_ids(&self) -> &[FPackageId] {
        &self.package_ids
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, FromRepr)]
#[repr(u32)]
pub(crate) enum EIoContainerHeaderVersion {
    Initial = 0,
    LocalizedPackages = 1,
    OptionalSegmentPackages = 2,
    NoExportInfo = 3,
    SoftPackageReferences = 4,
}
impl Readable for EIoContainerHeaderVersion {
    #[instrument(skip_all, name = "EIoContainerHeaderVersion")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let value = s.de()?;
        Self::from_repr(value).with_context(|| format!("invalid EIoStoreTocVersion value: {value}"))
    }
}
impl Writeable for EIoContainerHeaderVersion {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&(*self as u32))
    }
}

#[derive(Debug, PartialEq)]
struct FIoContainerHeaderLocalizedPackage {
    source_package_id: FPackageId,
    source_package_name: FMappedName,
}
impl Readable for FIoContainerHeaderLocalizedPackage {
    #[instrument(skip_all, name = "FIoContainerHeaderLocalizedPackage")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            source_package_id: s.de()?,
            source_package_name: s.de()?,
        })
    }
}
impl Writeable for FIoContainerHeaderLocalizedPackage {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.source_package_id)?;
        s.ser(&self.source_package_name)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
struct FIoContainerHeaderPackageRedirect {
    source_package_id: FPackageId,
    target_package_id: FPackageId,
    source_package_name: FMappedName,
}
impl Readable for FIoContainerHeaderPackageRedirect {
    #[instrument(skip_all, name = "FIoContainerHeaderPackageRedirect")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            source_package_id: s.de()?,
            target_package_id: s.de()?,
            source_package_name: s.de()?,
        })
    }
}
impl Writeable for FIoContainerHeaderPackageRedirect {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.source_package_id)?;
        s.ser(&self.target_package_id)?;
        s.ser(&self.source_package_name)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct StoreEntry {
    pub(crate) export_counts: PackageExportCounts,
    pub(crate) imported_packages: Vec<FPackageId>,
    pub(crate) shader_map_hashes: Vec<FSHAHash>,
}

#[derive(Debug, Default, PartialEq)]
struct StoreEntries {
    entries: Vec<StoreEntry>,
}
impl StoreEntries {
    fn get(&self, index: u32) -> StoreEntry {
        self.entries[index as usize].clone()
    }
    #[instrument(skip_all, name = "StoreEntries")]
    fn deserialize<S: Read>(
        s: &mut S,
        version: EIoContainerHeaderVersion,
        package_count: usize,
    ) -> Result<Self> {
        let buffer: Vec<u8> = s.de()?;
        let mut cur = Cursor::new(buffer);

        let (member_offset, entry_size) = if version >= EIoContainerHeaderVersion::NoExportInfo {
            (0, 16)
        } else {
            (8, 24)
        };

        let entries = read_array(package_count, &mut cur, |s| {
            FFilePackageStoreEntry::deserialize(s, version)
        })?;

        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(i, entry)| -> Result<StoreEntry> {
                let offset = i * entry_size; // sizeof(FFilePackageStoreEntry)

                let num = entry.imported_packages.array_num as usize;
                let imported_packages = if num != 0 {
                    let offset = offset
                        + member_offset
                        + entry.imported_packages.offset_to_data_from_this as usize
                        + 0; // offset_of(FFilePackageStoreEntry::imported_packages)
                    cur.seek(SeekFrom::Start(offset as u64))?;
                    cur.de_ctx(num)?
                } else {
                    vec![]
                };

                let num = entry.shader_map_hashes.array_num as usize;
                let shader_map_hashes = if num != 0 {
                    let offset = offset
                        + member_offset
                        + entry.shader_map_hashes.offset_to_data_from_this as usize
                        + 8; // offset_of(FFilePackageStoreEntry::shader_map_hashes)
                    cur.seek(SeekFrom::Start(offset as u64))?;
                    cur.de_ctx(num)?
                } else {
                    vec![]
                };
                Ok(StoreEntry {
                    export_counts: entry.export_counts,
                    imported_packages,
                    shader_map_hashes,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { entries })
    }
    #[instrument(skip_all, name = "StoreEntries")]
    fn serialize<S: Write>(&self, s: &mut S, version: EIoContainerHeaderVersion) -> Result<()> {
        let mut buffer: Vec<u8> = vec![];
        let mut cur = Cursor::new(&mut buffer);

        let (member_offset, entry_size) = if version >= EIoContainerHeaderVersion::NoExportInfo {
            (0, 16)
        } else {
            (8, 24)
        };

        // calculate end of entries to start writing arrays
        let mut array_offset = self.entries.len() * entry_size;

        for entry in &self.entries {
            let mut ser_entry = FFilePackageStoreEntry {
                export_counts: entry.export_counts.clone(),
                ..Default::default()
            };

            // save entry to calculate offsets and restore later
            let entry_offset = cur.position() as usize;

            // start writing arrays
            cur.set_position(array_offset as u64);

            if !entry.imported_packages.is_empty() {
                let offset = cur.position() as usize - entry_offset - member_offset - 0;
                ser_entry.imported_packages.offset_to_data_from_this = offset as u32;
                ser_entry.imported_packages.array_num = entry.imported_packages.len() as u32;
                cur.ser_no_length(&entry.imported_packages)?;
            }
            if !entry.shader_map_hashes.is_empty() {
                let offset = cur.position() as usize - entry_offset - member_offset - 8;
                ser_entry.shader_map_hashes.offset_to_data_from_this = offset as u32;
                ser_entry.shader_map_hashes.array_num = entry.shader_map_hashes.len() as u32;
                cur.ser_no_length(&entry.shader_map_hashes)?;
            }

            // advance array_offset
            array_offset = cur.position() as usize;

            // reset cursor and write entry
            cur.set_position(entry_offset as u64);
            ser_entry.serialize(&mut cur, version)?;
        }

        s.ser::<Vec<u8>>(&buffer)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
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
impl<T> Writeable for TFilePackageStoreEntryCArrayView<T> {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.array_num)?;
        s.ser(&self.offset_to_data_from_this)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct FFilePackageStoreEntry {
    // version < EIoContainerHeaderVersion::NoExportInfo
    export_counts: PackageExportCounts,

    imported_packages: TFilePackageStoreEntryCArrayView<FPackageId>,
    shader_map_hashes: TFilePackageStoreEntryCArrayView<FSHAHash>,
}
impl FFilePackageStoreEntry {
    #[instrument(skip_all, name = "FFilePackageStoreEntry")]
    fn deserialize<S: Read>(s: &mut S, version: EIoContainerHeaderVersion) -> Result<Self> {
        if version == EIoContainerHeaderVersion::Initial {
            todo!()
        } else {
            let export_counts = if version < EIoContainerHeaderVersion::NoExportInfo {
                s.de()?
            } else {
                Default::default()
            };
            Ok(Self {
                export_counts,
                imported_packages: s.de()?,
                shader_map_hashes: s.de()?,
            })
        }
    }
    #[instrument(skip_all, name = "FFilePackageStoreEntry")]
    fn serialize<S: Write>(&self, s: &mut S, version: EIoContainerHeaderVersion) -> Result<()> {
        if version == EIoContainerHeaderVersion::Initial {
            todo!()
        } else {
            if version < EIoContainerHeaderVersion::NoExportInfo {
                s.ser(&self.export_counts)?;
            }
            s.ser(&self.imported_packages)?;
            s.ser(&self.shader_map_hashes)?;
            Ok(())
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct PackageExportCounts {
    pub(crate) export_count: i32,
    pub(crate) export_bundle_count: i32,
}
impl Readable for PackageExportCounts {
    #[instrument(skip_all, name = "PackageExportCounts")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            export_count: s.de()?,
            export_bundle_count: s.de()?,
        })
    }
}
impl Writeable for PackageExportCounts {
    #[instrument(skip_all, name = "PackageExportCounts")]
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.export_count)?;
        s.ser(&self.export_bundle_count)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use fs_err as fs;
    use std::io::BufReader;

    #[test]
    fn test_container_header() -> Result<()> {
        let stream = BufReader::new(fs::File::open("tests/UE5.3/ContainerHeader_1.bin")?);

        let header: FIoContainerHeader =
            ser_hex::TraceStream::new("out/container_header.trace.json", stream).de()?;
        dbg!(&header.store_entries.entries);

        let mut out_cur = Cursor::new(vec![]);
        out_cur.ser(&header)?;
        out_cur.set_position(0);

        let header2: FIoContainerHeader = out_cur.de()?;

        assert_eq!(header, header2);

        Ok(())
    }
}
