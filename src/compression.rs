use anyhow::{bail, Result};
use std::io::{Read as _, Write};
use strum::{AsRefStr, EnumString};

#[derive(Debug, Clone, Copy, EnumString, AsRefStr)]
pub enum CompressionMethod {
    Zlib,
    Oodle,
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
