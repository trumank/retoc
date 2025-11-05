use crate::ser::{ReadExt, Readable, Utf8String, WriteExt, Writeable};
use crate::zen::FPackageIndex;
use anyhow::anyhow;
use std::io::{Read, Write};
use strum::FromRepr;

#[derive(Debug, Copy, Clone, PartialEq, Eq, FromRepr)]
#[repr(u8)]
enum EVerseEncodedValueType {
    None,
    Cell,
    Object,
    Char,
    Char32,
    Float,
    Int,
}
impl Writeable for EVerseEncodedValueType {
    fn ser<S: Write>(&self, stream: &mut S) -> anyhow::Result<()> {
        stream.ser(&(*self as u8))?;
        Ok(())
    }
}
impl Readable for EVerseEncodedValueType {
    fn de<S: Read>(stream: &mut S) -> anyhow::Result<Self> {
        Self::from_repr(stream.de()?).ok_or_else(|| anyhow!("Unknown encoded verse value type"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum VValue {
    None,
    Cell(FPackageIndex),
    Object(FPackageIndex),
    Char(u8),
    Char32(u32),
    Float(f64),
    Int(i32),
}
impl Writeable for VValue {
    fn ser<S: Write>(&self, stream: &mut S) -> anyhow::Result<()> {
        match self {
            VValue::None => {
                stream.ser(&EVerseEncodedValueType::None)?;
                Ok(())
            }
            VValue::Cell(cell_package_index) => {
                stream.ser(&EVerseEncodedValueType::Cell)?;
                stream.ser(cell_package_index)?;
                Ok(())
            }
            VValue::Object(object_package_index) => {
                stream.ser(&EVerseEncodedValueType::Object)?;
                stream.ser(object_package_index)?;
                Ok(())
            }
            VValue::Char(char_value) => {
                stream.ser(&EVerseEncodedValueType::Char)?;
                stream.ser(char_value)?;
                Ok(())
            }
            VValue::Char32(char32_value) => {
                stream.ser(&EVerseEncodedValueType::Char32)?;
                stream.ser(char32_value)?;
                Ok(())
            }
            VValue::Float(float_value) => {
                stream.ser(&EVerseEncodedValueType::Float)?;
                stream.ser(float_value)?;
                Ok(())
            }
            VValue::Int(int_value) => {
                stream.ser(&EVerseEncodedValueType::Int)?;
                stream.ser(int_value)?;
                Ok(())
            }
        }
    }
}
impl Readable for VValue {
    fn de<S: Read>(stream: &mut S) -> anyhow::Result<Self> {
        let encoded_value_type: EVerseEncodedValueType = stream.de()?;
        match encoded_value_type {
            EVerseEncodedValueType::None => Ok(VValue::None),
            EVerseEncodedValueType::Cell => {
                let cell_package_index: FPackageIndex = stream.de()?;
                Ok(VValue::Cell(cell_package_index))
            }
            EVerseEncodedValueType::Object => {
                let object_package_index: FPackageIndex = stream.de()?;
                Ok(VValue::Object(object_package_index))
            }
            EVerseEncodedValueType::Char => {
                let char_value: u8 = stream.de()?;
                Ok(VValue::Char(char_value))
            }
            EVerseEncodedValueType::Char32 => {
                let char32_value: u32 = stream.de()?;
                Ok(VValue::Char32(char32_value))
            }
            EVerseEncodedValueType::Float => {
                let float_value: f64 = stream.de()?;
                Ok(VValue::Float(float_value))
            }
            EVerseEncodedValueType::Int => {
                let int_value: i32 = stream.de()?;
                Ok(VValue::Int(int_value))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VNameValueMapEntry {
    pub(crate) name: Utf8String,
    pub(crate) value: VValue,
}
impl Writeable for VNameValueMapEntry {
    fn ser<S: Write>(&self, stream: &mut S) -> anyhow::Result<()> {
        stream.ser(&self.name)?;
        stream.ser(&self.value)?;
        Ok(())
    }
}
impl Readable for VNameValueMapEntry {
    fn de<S: Read>(stream: &mut S) -> anyhow::Result<Self> {
        Ok(Self { name: stream.de()?, value: stream.de()? })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VPackage {
    pub(crate) name: FPackageIndex,
    pub(crate) root_path: FPackageIndex,
    pub(crate) definitions: Vec<VNameValueMapEntry>,
    pub(crate) associated_u_package: VValue,
}
impl Writeable for VPackage {
    fn ser<S: Write>(&self, stream: &mut S) -> anyhow::Result<()> {
        stream.ser(&self.name)?;
        stream.ser(&self.root_path)?;
        stream.ser(&self.definitions)?;
        stream.ser(&self.associated_u_package)?;
        Ok(())
    }
}
impl Readable for VPackage {
    fn de<S: Read>(stream: &mut S) -> anyhow::Result<Self> {
        Ok(Self {
            name: stream.de()?,
            root_path: stream.de()?,
            definitions: stream.de()?,
            associated_u_package: stream.de()?,
        })
    }
}
