use std::io::Read;

use anyhow::Result;
use byteorder::{ReadBytesExt, LE};
use tracing::instrument;

pub(crate) trait ReadableBase {
    fn ser<S: Read>(stream: &mut S) -> Result<Self>
    where
        Self: Sized;
}
pub(crate) trait Readable: ReadableBase {}
pub(crate) trait ReadableCtx<C> {
    fn ser<S: Read>(stream: &mut S, ctx: C) -> Result<Self>
    where
        Self: Sized;
}

impl<T> ReadExt for T where T: Read {}
pub(crate) trait ReadExt: Read {
    #[instrument(skip_all)]
    fn ser<T: ReadableBase>(&mut self) -> Result<T>
    where
        Self: Sized,
    {
        T::ser(self)
    }
    #[instrument(skip_all)]
    fn ser_ctx<T: ReadableCtx<C>, C>(&mut self, ctx: C) -> Result<T>
    where
        Self: Sized,
    {
        T::ser(self, ctx)
    }
}

impl<const N: usize> Readable for [u8; N] {}
impl<const N: usize> ReadableBase for [u8; N] {
    #[instrument(skip_all, name = "read_fixed_slice")]
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        let mut buf = [0; N];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
}
impl<const N: usize, T: Readable + Default + Copy> Readable for [T; N] {}
impl<const N: usize, T: Readable + Default + Copy> ReadableBase for [T; N] {
    #[instrument(skip_all, name = "read_fixed_slice")]
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        let mut buf = [Default::default(); N];
        for i in buf.iter_mut() {
            *i = stream.ser()?;
        }
        Ok(buf)
    }
}

impl Readable for String {}
impl ReadableBase for String {
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        read_string(s.ser()?, s)
    }
}

impl<T: Readable> Readable for Vec<T> {}
impl<T: Readable> ReadableBase for Vec<T> {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        read_array(stream.read_u32::<LE>()? as usize, stream, T::ser)
    }
}
impl<T: Readable> ReadableCtx<usize> for Vec<T> {
    fn ser<S: Read>(stream: &mut S, ctx: usize) -> Result<Self> {
        read_array(ctx, stream, T::ser)
    }
}
impl ReadableCtx<usize> for Vec<u8> {
    fn ser<S: Read>(stream: &mut S, ctx: usize) -> Result<Self> {
        let mut buf = vec![0; ctx];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl ReadableBase for u8 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u8()?)
    }
}
impl ReadableBase for i8 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i8()?)
    }
}
impl ReadableBase for u16 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u16::<LE>()?)
    }
}
impl ReadableBase for i16 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i16::<LE>()?)
    }
}
impl ReadableBase for u32 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u32::<LE>()?)
    }
}
impl ReadableBase for i32 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i32::<LE>()?)
    }
}
impl ReadableBase for u64 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u64::<LE>()?)
    }
}
impl ReadableBase for i64 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i64::<LE>()?)
    }
}
// impl Readable for u8 {} special cased for optimized Vec<u8> serialization
impl Readable for i8 {}
impl Readable for u16 {}
impl Readable for i16 {}
impl Readable for u32 {}
impl Readable for i32 {}
impl Readable for u64 {}
impl Readable for i64 {}

#[instrument(skip_all)]
pub(crate) fn read_array<S: Read, T, F>(len: usize, stream: &mut S, mut f: F) -> Result<Vec<T>>
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
pub(crate) fn read_string<S: Read>(len: i32, stream: &mut S) -> Result<String> {
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
