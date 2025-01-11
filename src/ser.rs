use std::{io::Read, io::Write};

use anyhow::Result;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use tracing::instrument;

pub(crate) trait Readable {
    fn ser<S: Read>(stream: &mut S) -> Result<Self>
    where
        Self: Sized;
    fn ser_vec<S: Read>(len: usize, stream: &mut S) -> Result<Vec<Self>>
    where
        Self: Sized,
    {
        read_array(len, stream, Self::ser)
    }
    fn ser_array<S: Read, const N: usize>(stream: &mut S) -> Result<[Self; N]>
    where
        Self: Sized + Copy + Default,
    {
        let mut buf = [Default::default(); N];
        for i in buf.iter_mut() {
            *i = Self::ser(stream)?;
        }
        Ok(buf)
    }
}
pub(crate) trait ReadableCtx<C> {
    fn ser<S: Read>(stream: &mut S, ctx: C) -> Result<Self>
    where
        Self: Sized;
}

impl<T> ReadExt for T where T: Read {}
pub(crate) trait ReadExt: Read {
    #[instrument(skip_all)]
    fn ser<T: Readable>(&mut self) -> Result<T>
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

impl<const N: usize, T: Readable + Default + Copy> Readable for [T; N] {
    #[instrument(skip_all, name = "read_fixed_slice")]
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        T::ser_array(stream)
    }
}

impl Readable for String {
    fn ser<S: Read>(s: &mut S) -> Result<Self> {
        read_string(s.ser()?, s)
    }
}

impl<T: Readable> Readable for Vec<T> {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        T::ser_vec(stream.read_u32::<LE>()? as usize, stream)
    }
}
impl<T: Readable> ReadableCtx<usize> for Vec<T> {
    fn ser<S: Read>(stream: &mut S, ctx: usize) -> Result<Self> {
        T::ser_vec(ctx, stream)
    }
}

impl Readable for u8 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u8()?)
    }
    fn ser_vec<S: Read>(len: usize, stream: &mut S) -> Result<Vec<Self>>
    where
        Self: Sized,
    {
        let mut buf = vec![0; len];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
    fn ser_array<S: Read, const N: usize>(stream: &mut S) -> Result<[Self; N]>
    where
        Self: Sized + Copy + Default,
    {
        let mut buf = [0; N];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }
}
impl Readable for i8 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i8()?)
    }
}
impl Readable for u16 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u16::<LE>()?)
    }
}
impl Readable for i16 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i16::<LE>()?)
    }
}
impl Readable for u32 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u32::<LE>()?)
    }
}
impl Readable for i32 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i32::<LE>()?)
    }
}
impl Readable for u64 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_u64::<LE>()?)
    }
}
impl Readable for i64 {
    fn ser<S: Read>(stream: &mut S) -> Result<Self> {
        Ok(stream.read_i64::<LE>()?)
    }
}

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

pub(crate) fn write_string<S: Write>(stream: &mut S, value: &str) -> Result<()> {
    if value.is_empty() || value.is_ascii() {
        stream.write_u32::<LE>(value.as_bytes().len() as u32 + 1)?;
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
