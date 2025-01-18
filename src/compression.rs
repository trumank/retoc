use anyhow::{bail, Result};
use std::io::Read as _;
use strum::{AsRefStr, EnumString};

#[derive(Debug, Clone, Copy, EnumString, AsRefStr)]
pub enum CompressionMethod {
    Zlib,
    Oodle,
}

pub fn decompress(compression: CompressionMethod, input: &[u8], output: &mut [u8]) -> Result<()> {
    match compression {
        CompressionMethod::Zlib => {
            flate2::read::ZlibDecoder::new(input).read_exact(output)?;
        }
        CompressionMethod::Oodle => {
            let status = oodle_loader::decompress().unwrap()(input, output);
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
