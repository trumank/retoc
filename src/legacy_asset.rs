use std::{
    io::{Cursor, Read, Seek},
    path::Path,
};

use crate::{ser::*, zen::FZenPackageHeader, Toc};
use anyhow::Result;
use byteorder::{WriteBytesExt as _, LE};

pub(crate) fn build_legacy<C: Read + Seek, P: AsRef<Path>>(
    toc: &Toc,
    cas: &mut C,
    toc_entry_index: u32,
    out_path: P,
) -> Result<()> {
    let mut zen_data = Cursor::new(toc.read(cas, toc_entry_index)?);

    let mut out_buffer = vec![];
    let mut cur = Cursor::new(&mut out_buffer);

    let zen_summary = FZenPackageHeader::ser(&mut zen_data)?;

    dbg!(zen_summary);

    // arch's sandbox
    cur.write_u32::<LE>(0xa687562)?;

    std::fs::write(out_path, out_buffer)?;

    Ok(())
}
