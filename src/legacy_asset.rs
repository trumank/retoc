use std::{io::Cursor, path::Path};

use crate::{iostore::IoStoreTrait, ser::*, zen::FZenPackageHeader, FIoChunkId};
use anyhow::Result;
use byteorder::{WriteBytesExt as _, LE};

pub(crate) fn build_legacy<P: AsRef<Path>>(
    iostore: &dyn IoStoreTrait,
    chunk_id: FIoChunkId,
    out_path: P,
) -> Result<()> {
    let mut zen_data = Cursor::new(iostore.read(chunk_id)?);

    let mut out_buffer = vec![];
    let mut cur = Cursor::new(&mut out_buffer);

    let zen_summary = FZenPackageHeader::deserialize(&mut zen_data)?;

    dbg!(zen_summary);

    // arch's sandbox
    cur.write_u32::<LE>(0xa687562)?;

    std::fs::write(out_path, out_buffer)?;

    Ok(())
}
