use std::io::Read;

use anyhow::{Context as _, Result};
use indexmap::IndexMap;
use serde::Serialize;
use serde_with::serde_as;
use strum::FromRepr;
use tracing::instrument;

use crate::{ReadExt, Readable};

struct Ctx<R: Read> {
    inner: R,
    read: usize,
}
impl<R: Read> Ctx<R> {
    fn new(inner: R) -> Self {
        Self { inner, read: 0 }
    }
}
impl<R: Read> Read for Ctx<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf).inspect(|size| self.read += size)
    }
}

#[derive(Debug, Clone, Copy, FromRepr)]
#[repr(u8)]
enum ECbFieldType {
    None = 0x00,
    Null = 0x01,
    Object = 0x02,
    UniformObject = 0x03,
    Array = 0x04,
    UniformArray = 0x05,
    Binary = 0x06,
    String = 0x07,
    IntegerPositive = 0x08,
    IntegerNegative = 0x09,
    Float32 = 0x0a,
    Float64 = 0x0b,
    BoolFalse = 0x0c,
    BoolTrue = 0x0d,
    ObjectAttachment = 0x0e,
    BinaryAttachment = 0x0f,
    Hash = 0x10,
    Uuid = 0x11,
    DateTime = 0x12,
    TimeSpan = 0x13,
    ObjectId = 0x14,
    CustomById = 0x1e,
    CustomByName = 0x1f,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    struct ECbFieldTypeFlags : u8 {
        const Type = 0b1_1111;
        const Reserved = 0x20;
        const HasFieldType = 0x40;
        const HasFieldName = 0x80;
    }
}
impl Readable for ECbFieldTypeFlags {
    #[instrument(skip_all, name = "ECbFieldType")]
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Self::from_bits(stream.de::<u8>()?).context("invalid ECbFieldType")
    }
}
impl ECbFieldTypeFlags {
    fn get_type(self) -> ECbFieldType {
        ECbFieldType::from_repr(self.bits() & 0b1_1111).unwrap()
    }
    fn has_field_name(self) -> bool {
        self.contains(ECbFieldTypeFlags::HasFieldName)
    }
    fn has_field_type(self) -> bool {
        self.contains(ECbFieldTypeFlags::HasFieldType)
    }
}

#[derive(Debug, Clone, Serialize)]
struct Field {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(flatten)]
    value: FieldValue,
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum FieldValue {
    Null,
    Object(IndexMap<String, FieldValue>),
    UniformObject(IndexMap<String, FieldValue>),
    Array(Vec<FieldValue>),
    UniformArray(Vec<FieldValue>),
    //Binary,
    String(String),
    //IntegerPositive,
    //IntegerNegative,
    //Float32,
    //Float64,
    //BoolFalse,
    //BoolTrue,
    //ObjectAttachment,
    BinaryAttachment([u8; 20]),
    //Hash,
    //Uuid,
    //DateTime,
    //TimeSpan,
    ObjectId(#[serde_as(as = "serde_with::hex::Hex")] [u8; 12]),
    //CustomById,
    //CustomByName,
}

#[instrument(skip_all)]
fn read_string<S: Read>(stream: &mut S) -> Result<String> {
    let size = read_var_uint(stream)? as usize;
    Ok(String::from_utf8(stream.de_ctx(size)?)?)
}

#[instrument(skip_all)]
fn read_compact_binary<S: Read>(stream: &mut S) -> Result<Field> {
    read_field(&mut Ctx::new(stream), ECbFieldTypeFlags::HasFieldType)
}
#[instrument(skip_all)]
fn read_field<S: Read>(stream: &mut Ctx<S>, mut tag: ECbFieldTypeFlags) -> Result<Field> {
    if tag.has_field_type() {
        tag = stream.de()?;
    }
    let name = if tag.has_field_name() {
        Some(read_string(stream)?)
    } else {
        None
    };

    Ok(Field {
        name,
        value: match tag.get_type() {
            //None = 0x00,
            ECbFieldType::Null => FieldValue::Null,
            ECbFieldType::Object => {
                let size = varint::read_var_uint(stream)? as usize;
                let mut fields = IndexMap::new();
                if size > 0 {
                    let start = stream.read;
                    while stream.read < start + size {
                        let field = read_field(stream, ECbFieldTypeFlags::HasFieldType)?;
                        fields.insert(field.name.unwrap(), field.value);
                    }
                }
                FieldValue::Object(fields)
            }
            ECbFieldType::UniformObject => {
                let size = varint::read_var_uint(stream)? as usize;
                let mut fields = IndexMap::new();
                if size > 0 {
                    let start = stream.read;
                    let tag: ECbFieldTypeFlags = stream.de()?;
                    while stream.read < start + size {
                        let field = read_field(stream, tag)?;
                        fields.insert(field.name.unwrap(), field.value);
                    }
                }
                FieldValue::UniformObject(fields)
            }
            ECbFieldType::Array => {
                let size = varint::read_var_uint(stream)?;
                let count = varint::read_var_uint(stream)?;
                let mut fields = vec![];
                for _ in 0..count {
                    fields.push(read_field(stream, ECbFieldTypeFlags::HasFieldType)?.value);
                }
                FieldValue::Array(fields)
            }
            ECbFieldType::UniformArray => {
                let size = varint::read_var_uint(stream)?;
                let count = varint::read_var_uint(stream)?;
                let tag: ECbFieldTypeFlags = stream.de()?;
                let mut fields = vec![];
                for _ in 0..count {
                    fields.push(read_field(stream, tag)?.value);
                }
                FieldValue::UniformArray(fields)
            }
            //Binary = 0x06,
            ECbFieldType::String => FieldValue::String(read_string(stream)?),
            //ECbFieldType::IntegerPositive = 0x08,
            //ECbFieldType::IntegerNegative = 0x09,
            //ECbFieldType::Float32 = 0x0a,
            //ECbFieldType::Float64 = 0x0b,
            //ECbFieldType::BoolFalse = 0x0c,
            //ECbFieldType::BoolTrue = 0x0d,
            //ECbFieldType::ObjectAttachment = 0x0e,
            ECbFieldType::BinaryAttachment => {
                FieldValue::BinaryAttachment(stream.de::<[u8; 20]>()?)
            }
            //ECbFieldType::Hash = 0x10,
            //ECbFieldType::Uuid = 0x11,
            //ECbFieldType::DateTime = 0x12,
            //ECbFieldType::TimeSpan = 0x13,
            ECbFieldType::ObjectId => FieldValue::ObjectId(stream.de::<[u8; 12]>()?),
            //ECbFieldType::CustomById = 0x1e,
            //ECbFieldType::CustomByName = 0x1f,
            _ => todo!("{tag:?}"),
        },
    })
}

#[cfg(test)]
mod test {
    use super::*;

    use fs_err as fs;
    use std::io::BufReader;

    #[test]
    fn test_compact_binary() -> Result<()> {
        let mut stream = BufReader::new(fs::File::open("asdf/out/packagestore.manifest")?);

        let mut field = ser_hex::read("trace.json", &mut stream, read_compact_binary)?;

        //dbg!(field);
        fn sort_by_key_ref<T, F, K>(a: &mut [T], key: F)
        where
            F: Fn(&T) -> &K,
            K: ?Sized + Ord,
        {
            a.sort_by(|x, y| key(x).cmp(key(y)));
        }

        match &mut field.value {
            FieldValue::UniformObject(ref mut vec) => match &mut vec[0] {
                FieldValue::UniformObject(ref mut index_map) => match &mut index_map["entries"] {
                    FieldValue::UniformArray(vec) => {
                        sort_by_key_ref(vec, |op| match op {
                            FieldValue::Object(index_map) => {
                                match &index_map["packagestoreentry"] {
                                    FieldValue::UniformObject(index_map) => {
                                        match &index_map["packagename"] {
                                            FieldValue::String(string) => string,
                                            _ => unreachable!(),
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            _ => unreachable!(),
                        });
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }

        fs::write("packagestore.json", serde_json::to_vec(&field)?)?;

        Ok(())
    }
}

use varint::*;
mod varint {
    use super::*;

    #[instrument(skip_all)]
    pub fn read_var_uint<S: Read>(stream: &mut S) -> Result<u64> {
        let lead: u8 = stream.de()?;
        let byte_count = lead.leading_ones();

        let mut value = (lead & (0xff >> byte_count)) as u64;
        for _ in 0..byte_count {
            value <<= 8;
            value |= stream.de::<u8>()? as u64;
        }
        Ok(value)
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn test_var_uint() {
            let buf = [0xe1, 0x23, 0x45, 0x67];
            let mut cur = std::io::Cursor::new(&buf);
            assert_eq!(0x1234567, read_var_uint(&mut cur).unwrap());
        }
    }
}
