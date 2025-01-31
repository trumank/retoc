use anyhow::{bail, Result};
use std::io::{Read as _, Write};
use strum::{AsRefStr, EnumString, VariantArray};

#[derive(Debug, Clone, Copy, PartialEq, EnumString, AsRefStr, VariantArray)]
pub enum CompressionMethod {
    Zlib,
    Zstd,
    LZ4,
    Oodle,
}
impl CompressionMethod {
    pub(crate) fn from_str_ignore_case(value: &str) -> Option<Self> {
        CompressionMethod::VARIANTS
            .iter()
            .copied()
            .find(|v| v.as_ref().eq_ignore_ascii_case(value))
    }
}

pub fn compress<S: Write>(
    compression: CompressionMethod,
    input: &[u8],
    mut output: S,
) -> Result<()> {
    match compression {
        CompressionMethod::Zlib => {
            let mut encoder = flate2::write::ZlibEncoder::new(output, flate2::Compression::best());
            encoder.write_all(input)?;
            encoder.finish()?;
        }
        CompressionMethod::Zstd => {
            let buf = zstd::stream::encode_all(input, 0)?;
            output.write_all(&buf)?;
        }
        CompressionMethod::LZ4 => {
            let buf = lz4_flex::block::compress(input);
            output.write_all(&buf)?;
        }
        CompressionMethod::Oodle => {
            let buffer = oodle_loader::oodle()?.compress(
                input,
                oodle_loader::Compressor::Mermaid,
                oodle_loader::CompressionLevel::Normal,
            )?;
            output.write_all(&buffer)?;
        }
    }
    Ok(())
}

pub fn decompress(compression: CompressionMethod, input: &[u8], output: &mut [u8]) -> Result<()> {
    match compression {
        CompressionMethod::Zlib => {
            flate2::read::ZlibDecoder::new(input).read_exact(output)?;
        }
        CompressionMethod::Zstd => {
            zstd::bulk::decompress_to_buffer(input, output)?;
        }
        CompressionMethod::LZ4 => {
            lz4_flex::block::decompress_into(input, output)?;
        }
        CompressionMethod::Oodle => {
            let status = oodle_loader::oodle()?.decompress(input, output);
            if status < 0 || status as usize != output.len() {
                bail!(
                    "Oodle decompression failed: expected {} output bytes, got {}",
                    output.len(),
                    status,
                );
            }
        }
    }
    Ok(())
}
