use crate::{
    container_header::EIoContainerHeaderVersion, zen::EUnrealEngineObjectUE5Version,
    EIoStoreTocVersion,
};

use strum::{IntoStaticStr, VariantArray};

use EngineVersion::*;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IntoStaticStr, VariantArray)]
pub(crate) enum EngineVersion {
    UE5_0,
    UE5_1,
    UE5_2,
    UE5_3,
    UE5_4,
    UE5_5,
}
impl clap::ValueEnum for EngineVersion {
    fn value_variants<'a>() -> &'a [Self] {
        Self::VARIANTS
    }
    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        let name: &'static str = self.into();
        Some(clap::builder::PossibleValue::new(name))
    }
}

impl EngineVersion {
    pub(crate) fn toc_version(self) -> EIoStoreTocVersion {
        use EIoStoreTocVersion::*;
        match self {
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
            UE5_0 => LocalizedPackages,
            UE5_1 => OptionalSegmentPackages,
            UE5_2 => OptionalSegmentPackages,
            UE5_3 => NoExportInfo,
            UE5_4 => NoExportInfo,
            UE5_5 => SoftPackageReferences,
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
        }
    }
}
