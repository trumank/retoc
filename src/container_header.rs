use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Seek as _, SeekFrom, Write},
    marker::PhantomData,
};
use strum::FromRepr;
use tracing::instrument;

use crate::name_map::{read_name_batch_parts, write_name_batch_parts, EMappedNameType};
use crate::{
    name_map::{FMappedName, FNameMap},
    ser::*,
    FIoContainerId, FPackageId, FSHAHash, ReadExt,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct FIoContainerHeader {
    pub(crate) version: EIoContainerHeaderVersion,
    pub(crate) container_id: FIoContainerId,
    packages: StoreEntries,
    optional_segment_package_ids: Vec<FPackageId>,
    optional_segment_store_entries: Vec<u8>,
    redirect_name_map: FNameMap,
    localized_packages: Vec<FIoContainerHeaderLocalizedPackage>,
    package_redirects: Vec<FIoContainerHeaderPackageRedirect>,
    // Legacy UE4 culture map (also known as localized package map) and package redirects (without source package name information)
    legacy_culture_package_map: FCulturePackageMap,
    legacy_package_redirects: Vec<LegacyContainerHeaderPackageRedirect>,
    // HashSet for IDs of the localized packages, since they only need to be added once
    localized_source_package_ids: HashSet<FPackageId>,
    // Package redirect lookup table, from source package ID to the redirected package ID
    package_redirect_lookup: HashMap<FPackageId, FPackageId>,
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

            let names_buffer: Vec<u8> = s.de()?;
            let _name_hashes_buffer: Vec<u8> = s.de()?;
            let names = read_name_batch_parts(&names_buffer)?;

            // Create local name map for this container. This map should always be empty in legacy UE4 containers
            new.redirect_name_map = FNameMap::create_from_names(EMappedNameType::Container, names);
        }

        new.packages = StoreEntries::deserialize(s, version)?;

        if version > EIoContainerHeaderVersion::Initial {
            new.optional_segment_package_ids = s.de()?;
            new.optional_segment_store_entries = s.de()?;
            new.redirect_name_map = FNameMap::deserialize(s, EMappedNameType::Container)?;
            new.localized_packages = s.de()?;
            new.package_redirects = s.de()?;

            // Populate Source Package IDs of localized packages from the list we just read
            new.localized_source_package_ids = new
                .package_redirects
                .iter()
                .map(|x| x.source_package_id)
                .collect();

            // Populate package redirects lookup from the package redirect list
            new.package_redirect_lookup
                .reserve(new.package_redirects.len());
            for redirect_entry in &new.package_redirects {
                new.package_redirect_lookup.insert(
                    redirect_entry.source_package_id,
                    redirect_entry.target_package_id,
                );
            }
        } else {
            new.legacy_culture_package_map = s.de()?;
            new.legacy_package_redirects = s.de()?;

            // Populate package redirects lookup from the legacy package redirect list
            new.package_redirect_lookup
                .reserve(new.legacy_package_redirects.len());
            for redirect_entry in &new.legacy_package_redirects {
                new.package_redirect_lookup.insert(
                    redirect_entry.source_package_id,
                    redirect_entry.target_package_id,
                );
            }
        }

        if version >= EIoContainerHeaderVersion::SoftPackageReferences {
            todo!("soft package references")
        }

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
            s.ser(&(self.packages.0.len() as u32))?;

            //let unknown: u32 = s.de()?; // ff7r2?

            // Serialize container local name map. This map is generally empty in legacy UE4 containers because there are no fields that write to it
            let (names_buffer, name_hashes_buffer) =
                write_name_batch_parts(&self.redirect_name_map.copy_raw_names())?;
            s.ser(&names_buffer)?;
            s.ser(&name_hashes_buffer)?;
        }

        self.packages.serialize(s, self.version)?;

        if self.version > EIoContainerHeaderVersion::Initial {
            s.ser(&self.optional_segment_package_ids)?;
            s.ser(&self.optional_segment_store_entries)?;
            self.redirect_name_map.serialize(s)?;
            s.ser(&self.localized_packages)?;
            s.ser(&self.package_redirects)?;
        } else {
            s.ser(&self.legacy_culture_package_map)?;
            s.ser(&self.legacy_package_redirects)?;
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
            packages: StoreEntries::default(),
            optional_segment_package_ids: vec![],
            optional_segment_store_entries: vec![],
            redirect_name_map: FNameMap::default(),
            localized_packages: vec![],
            package_redirects: vec![],
            legacy_culture_package_map: FCulturePackageMap::default(),
            legacy_package_redirects: vec![],
            localized_source_package_ids: HashSet::new(),
            package_redirect_lookup: HashMap::new(),
        }
    }

    pub(crate) fn add_package(&mut self, package_id: FPackageId, store_entry: StoreEntry) {
        self.packages.0.insert(package_id, store_entry);
    }

    pub(crate) fn add_localized_package(
        &mut self,
        package_culture: &str,
        source_package_name: &str,
        localized_package_id: FPackageId,
    ) -> Result<()> {
        let source_package_id = FPackageId::from_name(source_package_name);

        // New style localized packages do not track the localized package IDs, they only track the list of packages that are localized. Actual Package IDs for localized packages
        // are derived in runtime from package names. So we only need to create a single entry in the localized packages for each package
        if self.version > EIoContainerHeaderVersion::Initial {
            if !self
                .localized_source_package_ids
                .contains(&source_package_id)
            {
                let source_package_mapped_name = self.redirect_name_map.store(source_package_name);

                self.localized_source_package_ids
                    .insert(source_package_id);
                self.localized_packages
                    .push(FIoContainerHeaderLocalizedPackage {
                        source_package_id,
                        source_package_name: source_package_mapped_name,
                    });
            }
        } else {
            // Old style localized packages. They track individual packages and their localized variants for each culture
            // Key in the culture package map is the culture name, values are mappings of source package ID to localized package ID
            let culture_localized_packages = self
                .legacy_culture_package_map
                .0
                .entry(package_culture.to_string())
                .or_default();
            culture_localized_packages.push((source_package_id, localized_package_id));
        }
        Ok(())
    }

    pub(crate) fn add_package_redirect(
        &mut self,
        source_package_name: &str,
        redirect_package_id: FPackageId,
    ) -> Result<()> {
        let source_package_id = FPackageId::from_name(source_package_name);

        // New style redirects track the package name as well as it's package ID
        if self.version > EIoContainerHeaderVersion::Initial {
            let source_package_name = self.redirect_name_map.store(source_package_name);

            self.package_redirects
                .push(FIoContainerHeaderPackageRedirect {
                    source_package_id,
                    source_package_name,
                    target_package_id: redirect_package_id,
                });
            self.package_redirect_lookup
                .insert(source_package_id, redirect_package_id);
        } else {
            // Old style redirects only track bare source package ID and redirect package ID
            self.legacy_package_redirects
                .push(LegacyContainerHeaderPackageRedirect {
                    source_package_id,
                    target_package_id: redirect_package_id,
                });
            self.package_redirect_lookup
                .insert(source_package_id, redirect_package_id);
        }
        Ok(())
    }

    pub(crate) fn lookup_package_redirect(
        &self,
        source_package_id: FPackageId,
    ) -> Option<FPackageId> {
        self.package_redirect_lookup
            .get(&source_package_id)
            .cloned()
    }

    pub(crate) fn get_store_entry(&self, package_id: FPackageId) -> Option<StoreEntry> {
        self.packages.get(package_id)
    }
    pub(crate) fn package_ids(
        &self,
    ) -> std::iter::Copied<std::collections::btree_map::Keys<'_, FPackageId, StoreEntry>> {
        self.packages.0.keys().copied()
    }
}

#[derive(
    Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, FromRepr, Serialize, Deserialize,
)]
#[repr(u32)]
pub(crate) enum EIoContainerHeaderVersion {
    Initial = 0,
    LocalizedPackages = 1,
    OptionalSegmentPackages = 2,
    NoExportInfo = 3,
    #[default]
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

// Used for UE4.27 package redirects that do not provide a source package name
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct LegacyContainerHeaderPackageRedirect {
    source_package_id: FPackageId,
    target_package_id: FPackageId,
}
impl Readable for LegacyContainerHeaderPackageRedirect {
    #[instrument(skip_all, name = "LegacyContainerHeaderPackageRedirect")]
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            source_package_id: s.de()?,
            target_package_id: s.de()?,
        })
    }
}
impl Writeable for LegacyContainerHeaderPackageRedirect {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&self.source_package_id)?;
        s.ser(&self.target_package_id)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct StoreEntries(BTreeMap<FPackageId, StoreEntry>);
impl StoreEntries {
    fn get(&self, package_id: FPackageId) -> Option<StoreEntry> {
        self.0.get(&package_id).cloned()
    }
    #[instrument(skip_all, name = "StoreEntries")]
    fn deserialize<S: Read>(s: &mut S, version: EIoContainerHeaderVersion) -> Result<Self> {
        let package_ids: Vec<FPackageId> = s.de()?;

        let buffer: Vec<u8> = s.de()?;
        let mut cur = Cursor::new(buffer);

        let (member_offset, entry_size) = if version >= EIoContainerHeaderVersion::NoExportInfo {
            (0, 16)
        } else if version > EIoContainerHeaderVersion::Initial {
            (8, 24)
        } else {
            (24, 32)
        };

        let entries = read_array(package_ids.len(), &mut cur, |s| {
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
                        + entry.imported_packages.offset_to_data_from_this as usize; // offset_of(FFilePackageStoreEntry::imported_packages)
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

        Ok(Self(BTreeMap::from_iter(
            package_ids.into_iter().zip(entries.into_iter()),
        )))
    }
    #[instrument(skip_all, name = "StoreEntries")]
    fn serialize<S: Write>(&self, s: &mut S, version: EIoContainerHeaderVersion) -> Result<()> {
        s.ser(&(self.0.len() as u32))?;
        for package_id in self.0.keys() {
            s.ser(package_id)?;
        }

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
        let mut array_offset = self.0.len() * entry_size;

        for entry in self.0.values() {
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
                let offset = cur.position() as usize - entry_offset - member_offset;
                ser_entry.imported_packages.offset_to_data_from_this = offset as u32;
                ser_entry.imported_packages.array_num = entry.imported_packages.len() as u32;
                cur.ser_no_length(&entry.imported_packages)?;
            }
            if version > EIoContainerHeaderVersion::Initial && !entry.shader_map_hashes.is_empty() {
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

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct FCulturePackageMap(BTreeMap<String, Vec<(FPackageId, FPackageId)>>);
impl Readable for FCulturePackageMap {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        let culture_package_map_len: u32 = s.de()?;
        let mut culture_package_map = BTreeMap::new();
        for _ in 0..culture_package_map_len {
            let key: String = s.de()?;
            let value: Vec<(FPackageId, FPackageId)> =
                read_array(s.de::<u32>()? as usize, s, |s| Ok((s.de()?, s.de()?)))?;
            culture_package_map.insert(key, value);
        }
        Ok(Self(culture_package_map))
    }
}
impl Writeable for FCulturePackageMap {
    fn ser<S: Write>(&self, s: &mut S) -> Result<()> {
        s.ser(&(self.0.len() as u32))?;
        for (key, value) in &self.0 {
            s.ser(key)?;
            s.ser(&(value.len() as u32))?;
            for (a, b) in value {
                s.ser(a)?;
                s.ser(b)?;
            }
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
