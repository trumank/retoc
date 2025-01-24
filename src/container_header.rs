use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek as _, SeekFrom, Write},
    marker::PhantomData,
};

use anyhow::{Context as _, Result};
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
        let version: EIoContainerHeaderVersion;
        let container_id;
        if signature == Self::MAGIC {
            version = s.de()?;
            container_id = s.de()?;
        } else {
            version = EIoContainerHeaderVersion::Initial;
            let mut id = [0; 8];
            id[0..4].copy_from_slice(&signature.to_le_bytes());
            id[4..8].copy_from_slice(&s.de::<[u8; 4]>()?);
            container_id = FIoContainerId(u64::from_le_bytes(id));
        }

        let mut new = Self::new(version, container_id);

        if version == EIoContainerHeaderVersion::Initial {
            let _package_count: u32 = s.de()?;

            //let unknown: u32 = s.de()?; // ff7r2?

            let names: Vec<u8> = s.de()?;
            assert_eq!(names.len(), 0, "impl names");
            let name_hashes: Vec<u8> = s.de()?;
            assert_eq!(name_hashes.len(), 8, "impl names");
        }

        new.package_ids = s.de()?;
        new.store_entries = StoreEntries::deserialize(s, version, new.package_ids.len())?;

        if version > EIoContainerHeaderVersion::Initial {
            new.optional_segment_package_ids = s.de()?;
            new.optional_segment_store_entries = s.de()?;
            new.redirect_name_map = FNameMap::deserialize(s, EMappedNameType::Container)?;
            new.localized_packages = s.de()?;
            new.package_redirects = s.de()?;
        } else {
            // FCulturePackageMap = TMap<FString, TArray<TPair<FPackageId, FPackageId>>>;
            let culture_package_name: u32 = s.de()?;
            assert_eq!(culture_package_name, 0, "impl culture package map");

            // TArray<TPair<FPackageId, FPackageId>> PackageRedirects;
            let package_redirects: u32 = s.de()?;
            assert_eq!(package_redirects, 0, "impl package redirects");
        }

        if version >= EIoContainerHeaderVersion::SoftPackageReferences {
            todo!("soft package references")
        }

        new.package_entry_map = new
            .package_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i as u32))
            .collect();

        Ok(new)
    }
}
impl Writeable for FIoContainerHeader {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        if self.version > EIoContainerHeaderVersion::Initial {
            s.ser(&Self::MAGIC)?;
            s.ser(&self.version)?;
        }
        s.ser(&self.container_id)?;

        if self.version == EIoContainerHeaderVersion::Initial {
            s.ser(&(self.package_ids.len() as u32))?;

            //let unknown: u32 = s.de()?; // ff7r2?

            s.ser(&0u32)?; // TODO names
            s.ser(&8u32)?; // TODO names
            s.ser(&0xC1640000u64)?; // TODO names
        }

        s.ser(&self.package_ids)?;
        self.store_entries.serialize(s, self.version)?;

        if self.version > EIoContainerHeaderVersion::Initial {
            s.ser(&self.optional_segment_package_ids)?;
            s.ser(&self.optional_segment_store_entries)?;
            self.redirect_name_map.serialize(s)?;
            s.ser(&self.localized_packages)?;
            s.ser(&self.package_redirects)?;
        } else {
            s.ser(&0u32)?; // TODO culture_package_name
            s.ser(&0u32)?; // TODO package_redirects
        }

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
    // version == EIoContainerHeaderVersion::NoExportInfo
    pub(crate) export_bundles_size: u64,
    pub(crate) load_order: u32,

    // version < EIoContainerHeaderVersion::NoExportInfo
    pub(crate) export_count: i32,
    pub(crate) export_bundle_count: i32,

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
        } else if version > EIoContainerHeaderVersion::Initial {
            (8, 24)
        } else {
            (24, 32)
        };

        let entries = read_array(package_count, &mut cur, |s| {
            FFilePackageStoreEntry::deserialize(s, version)
        })?;

        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(i, entry)| -> Result<StoreEntry> {
                let offset = i * entry_size; // sizeof(FFilePackageStoreEntry)

                let mut new = StoreEntry {
                    export_bundles_size: entry.export_bundles_size,
                    load_order: entry.load_order,

                    export_count: entry.export_count,
                    export_bundle_count: entry.export_bundle_count,

                    ..Default::default()
                };

                let num = entry.imported_packages.array_num as usize;
                new.imported_packages = if num != 0 {
                    let offset = offset
                        + member_offset
                        + entry.imported_packages.offset_to_data_from_this as usize
                        + 0; // offset_of(FFilePackageStoreEntry::imported_packages)
                    cur.seek(SeekFrom::Start(offset as u64))?;
                    cur.de_ctx(num)?
                } else {
                    vec![]
                };

                if version > EIoContainerHeaderVersion::Initial {
                    let num = entry.shader_map_hashes.array_num as usize;
                    new.shader_map_hashes = if num != 0 {
                        let offset = offset
                            + member_offset
                            + entry.shader_map_hashes.offset_to_data_from_this as usize
                            + 8; // offset_of(FFilePackageStoreEntry::shader_map_hashes)
                        cur.seek(SeekFrom::Start(offset as u64))?;
                        cur.de_ctx(num)?
                    } else {
                        vec![]
                    };
                }

                Ok(new)
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
        } else if version > EIoContainerHeaderVersion::Initial {
            (8, 24)
        } else {
            (24, 32)
        };

        // calculate end of entries to start writing arrays
        let mut array_offset = self.entries.len() * entry_size;

        for entry in &self.entries {
            let mut ser_entry = FFilePackageStoreEntry {
                export_bundles_size: entry.export_bundles_size,
                load_order: entry.load_order,

                export_count: entry.export_count,
                export_bundle_count: entry.export_bundle_count,

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
            if version > EIoContainerHeaderVersion::Initial {
                if !entry.shader_map_hashes.is_empty() {
                    let offset = cur.position() as usize - entry_offset - member_offset - 8;
                    ser_entry.shader_map_hashes.offset_to_data_from_this = offset as u32;
                    ser_entry.shader_map_hashes.array_num = entry.shader_map_hashes.len() as u32;
                    cur.ser_no_length(&entry.shader_map_hashes)?;
                }
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
    // version == EIoContainerHeaderVersion::NoExportInfo
    export_bundles_size: u64,
    load_order: u32,

    // version < EIoContainerHeaderVersion::NoExportInfo
    export_count: i32,
    export_bundle_count: i32,

    imported_packages: TFilePackageStoreEntryCArrayView<FPackageId>,
    shader_map_hashes: TFilePackageStoreEntryCArrayView<FSHAHash>,
}
impl FFilePackageStoreEntry {
    #[instrument(skip_all, name = "FFilePackageStoreEntry")]
    fn deserialize<S: Read>(s: &mut S, version: EIoContainerHeaderVersion) -> Result<Self> {
        let mut entry = Self::default();

        if version == EIoContainerHeaderVersion::Initial {
            entry.export_bundles_size = s.de()?;
        }
        if version < EIoContainerHeaderVersion::NoExportInfo {
            entry.export_count = s.de()?;
            entry.export_bundle_count = s.de()?;
        }
        if version == EIoContainerHeaderVersion::Initial {
            entry.load_order = s.de()?;
            let _pad: u32 = s.de()?;
        }
        entry.imported_packages = s.de()?;
        if version > EIoContainerHeaderVersion::Initial {
            entry.shader_map_hashes = s.de()?;
        };
        Ok(entry)
    }
    #[instrument(skip_all, name = "FFilePackageStoreEntry")]
    fn serialize<S: Write>(&self, s: &mut S, version: EIoContainerHeaderVersion) -> Result<()> {
        if version == EIoContainerHeaderVersion::Initial {
            s.ser(&self.export_bundles_size)?;
        }
        if version < EIoContainerHeaderVersion::NoExportInfo {
            s.ser(&self.export_count)?;
            s.ser(&self.export_bundle_count)?;
        }
        if version == EIoContainerHeaderVersion::Initial {
            s.ser(&self.load_order)?;
            s.ser(&0u32)?;
        }
        s.ser(&self.imported_packages)?;
        if version > EIoContainerHeaderVersion::Initial {
            s.ser(&self.shader_map_hashes)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use fs_err as fs;
    use std::io::BufReader;

    #[test]
    fn test_container_header_new() -> Result<()> {
        let stream = BufReader::new(fs::File::open("tests/UE5.3/ContainerHeader_1.bin")?);

        let header: FIoContainerHeader =
            ser_hex::TraceStream::new("out/container_header.trace.json", stream).de()?;
        //dbg!(&header.store_entries.entries);

        let mut out_cur = Cursor::new(vec![]);
        out_cur.ser(&header)?;
        out_cur.set_position(0);

        let header2: FIoContainerHeader = out_cur.de()?;

        assert_eq!(header, header2);

        Ok(())
    }

    #[test]
    fn test_container_header_initial() -> Result<()> {
        let mut stream = BufReader::new(fs::File::open("tests/UE4.27/ContainerHeader_1.bin")?);

        let header: FIoContainerHeader = stream.de()?;

        let mut out_cur = Cursor::new(vec![]);
        out_cur.ser(&header)?;
        out_cur.set_position(0);

        let header2: FIoContainerHeader = out_cur.de()?;

        assert_eq!(header, header2);

        Ok(())
    }
}
