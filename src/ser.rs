use anyhow::Result;
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use std::{io::Read, io::Write};
use tracing::instrument;

pub trait Readable {
    fn de<S: Read>(stream: &mut S) -> Result<Self>
    where
        Self: Sized;
    fn de_vec<S: Read>(len: usize, stream: &mut S) -> Result<Vec<Self>>
    where
        Self: Sized,
    {
        read_array(len, stream, Self::de)
    }
    fn de_array<S: Read, const N: usize>(stream: &mut S) -> Result<[Self; N]>
    where
        Self: Sized + Copy + Default,
    {
        let mut buf = [Default::default(); N];
        for i in buf.iter_mut() {
            *i = Self::de(stream)?;
        }
        Ok(buf)
    }
}
pub trait Writeable {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()>;
    fn ser_array<S: Write, T: AsRef<[Self]>>(this: T, stream: &mut S) -> Result<()>
    where
        Self: Sized,
    {
        for i in this.as_ref() {
            Self::ser(i, stream)?;
        }
        Ok(())
    }
}
pub trait ReadableCtx<C> {
    fn de<S: Read>(stream: &mut S, ctx: C) -> Result<Self>
    where
        Self: Sized;
}

impl<T> ReadExt for T where T: Read {}
pub trait ReadExt: Read {
    #[instrument(skip_all)]
    fn de<T: Readable>(&mut self) -> Result<T>
    where
        Self: Sized,
    {
        T::de(self)
    }
    #[instrument(skip_all)]
    fn de_ctx<T: ReadableCtx<C>, C>(&mut self, ctx: C) -> Result<T>
    where
        Self: Sized,
    {
        T::de(self, ctx)
    }
}
impl<T> WriteExt for T where T: Write {}
pub trait WriteExt: Write {
    #[instrument(skip_all)]
    fn ser<T: Writeable>(&mut self, value: &T) -> Result<()>
    where
        Self: Sized,
    {
        value.ser(self)
    }
    /// Serialize &[T] without length prefix
    #[instrument(skip_all)]
    fn ser_no_length<T: Writeable, S: AsRef<[T]>>(&mut self, value: &S) -> Result<()>
    where
        Self: Sized,
    {
        T::ser_array(value.as_ref(), self)
    }
}

impl<const N: usize, T: Readable + Default + Copy> Readable for [T; N] {
    #[instrument(skip_all, name = "read_fixed_slice")]
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        T::de_array(stream)
    }
}
impl<const N: usize, T: Writeable> Writeable for [T; N] {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        T::ser_array(self, stream)
    }
}

impl Readable for String {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        read_string_data(s.de()?, s)
    }
}
impl Writeable for String {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        write_string(stream, self)
    }
}
impl Writeable for &str {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        write_string(stream, self)
    }
}

impl<T: Readable> Readable for Vec<T> {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        T::de_vec(stream.read_u32::<LE>()? as usize, stream)
    }
}
impl<T: Readable> ReadableCtx<usize> for Vec<T> {
    fn de<S: Read>(stream: &mut S, ctx: usize) -> Result<Self> {
        T::de_vec(ctx, stream)
    }
}
impl<T: Writeable> Writeable for Vec<T> {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        stream.write_u32::<LE>(self.len() as u32)?;
        T::ser_array(self, stream)
    }
}

impl Readable for bool {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u32::<LE>()? != 0)
    }
}
impl Writeable for bool {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_u32::<LE>(if *self { 1 } else { 0 })?)
    }
}
impl Readable for u8 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u8()?)
    }
    fn de_vec<S: Read>(len: usize, stream: &mut S) -> Result<Vec<Self>>
    where
        Self: Sized,
    {
        let mut buf = vec![0; len];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
    fn de_array<S: Read, const N: usize>(stream: &mut S) -> Result<[Self; N]>
    where
        Self: Sized + Copy + Default,
    {
        let mut buf = [0; N];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
}
impl Writeable for u8 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_u8(*self)?)
    }
    fn ser_array<S: Write, T: AsRef<[Self]>>(this: T, stream: &mut S) -> Result<()>
    where
        Self: Sized,
    {
        Ok(stream.write_all(this.as_ref())?)
    }
}
impl Readable for i8 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i8()?)
    }
}
impl Writeable for i8 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_i8(*self)?)
    }
}
impl Readable for u16 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u16::<LE>()?)
    }
}
impl Writeable for u16 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_u16::<LE>(*self)?)
    }
}
impl Readable for i16 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i16::<LE>()?)
    }
}
impl Writeable for i16 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_i16::<LE>(*self)?)
    }
}
impl Readable for u32 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u32::<LE>()?)
    }
}
impl Writeable for u32 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_u32::<LE>(*self)?)
    }
}
impl Readable for i32 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i32::<LE>()?)
    }
}
impl Writeable for i32 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_i32::<LE>(*self)?)
    }
}
impl Readable for u64 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u64::<LE>()?)
    }
}
impl Writeable for u64 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_u64::<LE>(*self)?)
    }
}
impl Readable for i64 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i64::<LE>()?)
    }
}
impl Writeable for i64 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_i64::<LE>(*self)?)
    }
}
impl Readable for f32 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_f32::<LE>()?)
    }
}
impl Writeable for f32 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_f32::<LE>(*self)?)
    }
}
impl Readable for f64 {
    fn de<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_f64::<LE>()?)
    }
}
impl Writeable for f64 {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        Ok(stream.write_f64::<LE>(*self)?)
    }
}

#[instrument(skip_all)]
pub fn read_array<S: Read, T, F>(len: usize, stream: &mut S, mut f: F) -> Result<Vec<T>>
where
    F: FnMut(&mut S) -> Result<T>,
{
    let mut array = Vec::with_capacity(len);
    for _ in 0..len {
        array.push(f(stream)?);
    }
    Ok(array)
}

#[instrument(skip_all)]
pub fn read_string_data<S: Read>(len: i32, stream: &mut S) -> Result<String> {
    if len < 0 {
        let chars = read_array((-len) as usize, stream, |r| Ok(r.read_u16::<LE>()?))?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(String::from_utf16(&chars[..length]).unwrap())
    } else {
        let mut chars = vec![0; len as usize];
        stream.read_exact(&mut chars)?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(String::from_utf8_lossy(&chars[..length]).into_owned())
    }
}

/// Write string data only. Do not write length prefix
pub fn write_string_data<S: Write>(stream: &mut S, value: &str) -> Result<()> {
    if value.is_empty() {
    } else if value.is_ascii() {
        stream.write_all(value.as_bytes())?;
        stream.write_u8(0)?;
    } else {
        let chars: Vec<u16> = value.encode_utf16().collect();
        for c in chars {
            stream.write_u16::<LE>(c)?;
        }
        stream.write_u16::<LE>(0)?;
    }
    Ok(())
}

pub fn serialized_string_len<S: Write>(_stream: &mut S, value: &str) -> Result<i32> {
    Ok(if value.is_empty() {
        0
    } else if value.is_ascii() {
        value.len() as i32 + 1
    } else {
        -(value.encode_utf16().count() as i32 + 1)
    })
}

pub fn write_string<S: Write>(stream: &mut S, value: &str) -> Result<()> {
    if value.is_empty() {
        stream.write_u32::<LE>(0)?;
    } else if value.is_ascii() {
        stream.write_u32::<LE>(value.len() as u32 + 1)?;
        stream.write_all(value.as_bytes())?;
        stream.write_u8(0)?;
    } else {
        let chars: Vec<u16> = value.encode_utf16().collect();
        stream.write_i32::<LE>(-(chars.len() as i32 + 1))?;
        for c in chars {
            stream.write_u16::<LE>(c)?;
        }
        stream.write_u16::<LE>(0)?;
    }
    Ok(())
}

#[instrument(skip_all)]
pub fn read_utf8_string<S: Read>(stream: &mut S) -> Result<String> {
    let len: i32 = stream.de()?;
    let mut chars = vec![0; len as usize];
    stream.read_exact(&mut chars)?;
    Ok(String::from_utf8(chars).unwrap())
}

pub fn write_utf8_string<S: Write>(stream: &mut S, value: &str) -> Result<()> {
    let len: i32 = value.len() as i32;
    stream.ser(&len)?;
    stream.write_all(value.as_bytes())?;
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Utf8String(pub String);
impl Display for Utf8String {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl AsRef<str> for Utf8String {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl Deref for Utf8String {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Utf8String {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Readable for Utf8String {
    fn de<S: Read>(s: &mut S) -> Result<Self> {
        Ok(Self(read_utf8_string(s)?))
    }
}
impl Writeable for Utf8String {
    fn ser<S: Write>(&self, stream: &mut S) -> Result<()> {
        write_utf8_string(stream, &self.0)
    }
}
