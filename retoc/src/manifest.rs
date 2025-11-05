use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::chunk_id::FIoChunkIdRaw;

#[derive(Debug, Serialize, Deserialize)]
pub struct PackageStoreManifest {
    pub oplog: OpLog,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpLog {
    pub entries: Vec<Op>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Op {
    pub packagestoreentry: PackageStoreEntry,
    pub packagedata: Vec<ChunkData>,
    pub bulkdata: Vec<ChunkData>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PackageStoreEntry {
    pub packagename: String,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkData {
    #[serde_as(as = "serde_with::hex::Hex")]
    pub id: FIoChunkIdRaw,
    pub filename: String,
}
