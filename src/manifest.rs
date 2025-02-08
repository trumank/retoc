use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::chunk_id::FIoChunkIdRaw;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PackageStoreManifest {
    pub(crate) oplog: OpLog,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OpLog {
    pub(crate) entries: Vec<Op>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Op {
    pub(crate) packagestoreentry: PackageStoreEntry,
    pub(crate) packagedata: Vec<ChunkData>,
    pub(crate) bulkdata: Vec<ChunkData>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PackageStoreEntry {
    pub(crate) packagename: String,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ChunkData {
    #[serde_as(as = "serde_with::hex::Hex")]
    pub(crate) id: FIoChunkIdRaw,
    pub(crate) filename: String,
}
