use std::io::Read;

use anyhow::Result;
use tracing::instrument;

use crate::{
    name_map::{FMinimalName, FNameMap},
    ReadExt, Readable,
};

pub(crate) fn get_package_name(data: &[u8]) -> Result<String> {
    let header: FZenPackageHeader = std::io::Cursor::new(data).ser()?;
    Ok(header.name_map.get(header.summary.name).to_string())
}

// just enough to get PackageName
pub(crate) struct FPackageFileSummary {
    pub(crate) tag: u32,
    pub(crate) file_version_ue4: u32,
    pub(crate) file_version_ue5: Option<u32>,
    pub(crate) total_header_size: u32,
    pub(crate) package_name: String,
}
impl Readable for FPackageFileSummary {
    #[instrument(skip_all, name = "FPackageFileSummary")]
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        let tag = s.ser()?;
        assert_eq!(tag, 0x9e2a83c1);

        let legacy_file_version: i32 = dbg!(s.ser()?);

        if legacy_file_version != -4 {
            let _legacy_ue3_version: i32 = s.ser()?;
        }
        let file_version_ue4 = s.ser()?;
        assert_eq!(file_version_ue4, 0);
        let file_version_ue5 = if legacy_file_version <= -8 {
            Some(s.ser()?)
        } else {
            None
        };
        assert_eq!(file_version_ue5, Some(0));
        let _file_version_licensee_ue: u32 = s.ser()?;
        if legacy_file_version <= -2 {
            let custom_versions: u32 = s.ser()?; // empty (unversioned)
            assert_eq!(custom_versions, 0);
        }

        let total_header_size = s.ser()?;
        let package_name = s.ser()?;

        Ok(Self {
            tag,
            file_version_ue4,
            file_version_ue5,
            total_header_size,
            package_name,
        })
    }
}

pub(crate) struct FZenPackageSummary {
    has_versioning_info: u32,
    header_size: u32,
    name: FMinimalName,
    package_flags: u32,
    cooked_header_size: u32,
    imported_public_export_hashes_offset: i32,
    import_map_offset: i32,
    export_map_offset: i32,
    export_bundle_entries_offset: i32,
    dependency_bundle_headers_offset: i32,
    dependency_bundle_entries_offset: i32,
    imported_package_names_offset: i32,
}
impl Readable for FZenPackageSummary {
    #[instrument(skip_all, name = "FZenPackageSummary")]
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            has_versioning_info: s.ser()?,
            header_size: s.ser()?,
            name: s.ser()?,
            package_flags: s.ser()?,
            cooked_header_size: s.ser()?,
            imported_public_export_hashes_offset: s.ser()?,
            import_map_offset: s.ser()?,
            export_map_offset: s.ser()?,
            export_bundle_entries_offset: s.ser()?,
            dependency_bundle_headers_offset: s.ser()?,
            dependency_bundle_entries_offset: s.ser()?,
            imported_package_names_offset: s.ser()?,
        })
    }
}

pub(crate) struct FZenPackageHeader {
    summary: FZenPackageSummary,
    name_map: FNameMap,
}
impl Readable for FZenPackageHeader {
    #[instrument(skip_all, name = "FZenPackageHeader")]
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            summary: s.ser()?,
            name_map: s.ser()?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{fs::File, io::BufReader};

    #[test]
    fn test_zen() -> Result<()> {
        let mut stream = BufReader::new(File::open(
            //"zen_out/AbioticFactor/Content/Audio/Abiotic_Dialog_NarrativeNPC.uasset",
            "bad.uasset",
        )?);

        let header = ser_hex::read("trace.json", &mut stream, FZenPackageHeader::ser)?;
        header.name_map.get(header.summary.name);

        //dbg!(field);

        Ok(())
    }
}
