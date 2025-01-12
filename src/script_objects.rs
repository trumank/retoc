use std::io::Read;

use anyhow::Result;
use tracing::instrument;

use crate::{
    name_map::{FMinimalName, FNameMap},
    read_array,
    ser::*,
};

#[instrument(skip_all)]
fn read_script_objects<S: Read>(s: &mut S) -> Result<()> {
    let global_name_map: FNameMap = s.de()?;
    let num_script_objects: u32 = s.de()?;

    let script_objects = read_array(num_script_objects as usize, s, FScriptObjectEntry::read)?;

    for s in script_objects {
        //println!(
        //    "{}:",
        //    global_name_map.header_bytes[s.object_name.index.value as usize & 0xffffff]
        //);
        //println!("{}:", s.object_name.index.value as usize & 0xffffff);
        println!("{}:", global_name_map.get(s.object_name));
        println!("  global_index:    {:?}", s.global_index.get());
        println!("  outer_index:     {:?}", s.outer_index.get());
        println!("  cdo_class_index: {:?}", s.cdo_class_index.get());
    }

    //println!("{:#?}", script_objects);

    Ok(())
}

#[derive(Debug)]
struct FScriptObjectEntry {
    //mapped: FMappedName,
    object_name: FMinimalName,
    global_index: FPackageObjectIndex,
    outer_index: FPackageObjectIndex,
    cdo_class_index: FPackageObjectIndex,
}
impl FScriptObjectEntry {
    #[instrument(skip_all, name = "FScriptObjectEntry")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            //mapped: FMappedName::read(s)?,
            object_name: s.de()?,
            global_index: FPackageObjectIndex::read(s)?,
            outer_index: FPackageObjectIndex::read(s)?,
            cdo_class_index: FPackageObjectIndex::read(s)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct FPackageObjectIndex {
    type_and_id: u64,
}
impl FPackageObjectIndex {
    #[instrument(skip_all, name = "FPackageObjectIndex")]
    fn read<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self {
            type_and_id: s.de()?,
        })
    }
    fn get(self) -> Option<u64> {
        (self.type_and_id != u64::MAX).then_some(self.type_and_id)
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::BufReader};

    use super::*;
    #[test]
    fn test_read_script_objects() -> Result<()> {
        let mut stream = BufReader::new(File::open("giga.bin")?);

        ser_hex::read("trace.json", &mut stream, |s| read_script_objects(s))?;

        Ok(())
    }
}
