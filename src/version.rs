use crate::{
    EIoStoreTocVersion,
    container_header::EIoContainerHeaderVersion,
    zen::{EUnrealEngineObjectUE4Version, EUnrealEngineObjectUE5Version, FPackageFileVersion},
};

use EngineVersion::*;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
#[clap(rename_all = "verbatim")]
pub(crate) enum EngineVersion {
    UE4_25,
    UE4_26,
    UE4_27,
    UE5_0,
    UE5_1,
    UE5_2,
    UE5_3,
    UE5_4,
    UE5_5,
}

impl EngineVersion {
    pub(crate) fn toc_version(self) -> EIoStoreTocVersion {
        use EIoStoreTocVersion::*;
        match self {
            UE4_25 => DirectoryIndex,
            UE4_26 => DirectoryIndex,
            UE4_27 => PartitionSize,
            UE5_0 => PerfectHashWithOverflow,
            UE5_1 => PerfectHashWithOverflow,
            UE5_2 => PerfectHashWithOverflow,
            UE5_3 => PerfectHashWithOverflow,
            UE5_4 => OnDemandMetaData,
            UE5_5 => ReplaceIoChunkHashWithIoHash,
        }
    }
    pub(crate) fn container_header_version(self) -> EIoContainerHeaderVersion {
        use EIoContainerHeaderVersion::*;
        match self {
            UE4_25 => Initial,
            UE4_26 => Initial,
            UE4_27 => Initial,
            UE5_0 => LocalizedPackages,
            UE5_1 => OptionalSegmentPackages,
            UE5_2 => OptionalSegmentPackages,
            UE5_3 => NoExportInfo,
            UE5_4 => NoExportInfo,
            UE5_5 => SoftPackageReferences,
        }
    }
    pub(crate) fn object_ue4_version(self) -> EUnrealEngineObjectUE4Version {
        use EUnrealEngineObjectUE4Version::*;
        match self {
            UE4_25 => AddedPackageOwner,
            UE4_26 => CorrectLicenseeFlag,
            UE4_27 => CorrectLicenseeFlag,
            _ => unreachable!(),
        }
    }
    pub(crate) fn object_ue5_version(self) -> EUnrealEngineObjectUE5Version {
        use EUnrealEngineObjectUE5Version::*;
        match self {
            UE5_0 => LargeWorldCoordinates,
            UE5_1 => AddSoftObjectPathList,
            UE5_2 => DataResources,
            UE5_3 => DataResources,
            UE5_4 => PropertyTagCompleteTypeName,
            UE5_5 => AssetRegistryPackageBuildDependencies,
            _ => unreachable!(),
        }
    }
    pub(crate) fn package_file_version(self) -> FPackageFileVersion {
        if self < UE5_0 {
            FPackageFileVersion::create_ue4(self.object_ue4_version())
        } else {
            FPackageFileVersion::create_ue5(self.object_ue5_version())
        }
    }
}
