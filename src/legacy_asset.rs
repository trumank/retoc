use std::io::{Cursor, Read, Seek};

use crate::{ser::*, zen::FZenPackageHeader, Toc};
use anyhow::Result;
use byteorder::{WriteBytesExt as _, LE};

pub(crate) fn build_legacy<C: Read + Seek>(
    toc: &Toc,
    cas: &mut C,
    toc_entry_index: u32,
) -> Result<()> {
    let mut zen_data = Cursor::new(toc.read(cas, toc_entry_index)?);

    let mut out_buffer = vec![];
    let mut cur = Cursor::new(&mut out_buffer);

    let zen_summary = FZenPackageHeader::ser(&mut zen_data)?;

    cur.write_u32::<LE>(1234)?;

    Ok(())
}
