//! Extraction of human readable strings out of raw (decompressed) container data, in the spirit of
//! strings(1) but aware of the UTF-16 that UE uses for wide FStrings.

use std::borrow::Cow;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ScanEncoding {
    /// ASCII/UTF-8 only
    Utf8,
    /// UTF-16LE only (wide FStrings)
    Utf16,
    /// Both of the above
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringEncoding {
    Utf8,
    Utf16,
}
impl fmt::Display for StringEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Utf8 => write!(f, "utf8"),
            Self::Utf16 => write!(f, "utf16"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ScanOptions {
    /// Minimum length of a string in characters (not bytes)
    pub min_len: usize,
    pub encoding: ScanEncoding,
    /// Also scan for UTF-16 starting at odd byte offsets. UE string buffers are practically always
    /// 2 byte aligned within a chunk, so this mostly just doubles the work and the noise
    pub utf16_unaligned: bool,
    /// Accept non-ASCII characters, which is required to find e.g. Japanese text but adds a lot of
    /// noise: about a third of all byte pairs decode to a printable CJK character, so scanning
    /// binary data for UTF-16 this way returns most of the data back as "text"
    pub unicode: bool,
}
impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            min_len: 4,
            encoding: ScanEncoding::Both,
            utf16_unaligned: false,
            unicode: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FoundString<'data> {
    /// Byte offset of the string within the scanned data
    pub offset: usize,
    pub encoding: StringEncoding,
    pub text: Cow<'data, str>,
}

/// Calls `visit` for every printable string of at least `options.min_len` characters found in `data`.
/// Strings never contain control characters, so callers can safely emit them line by line.
pub fn scan<'data>(data: &'data [u8], options: &ScanOptions, mut visit: impl FnMut(FoundString<'data>)) {
    if matches!(options.encoding, ScanEncoding::Utf8 | ScanEncoding::Both) {
        scan_utf8(data, options, &mut visit);
    }
    if matches!(options.encoding, ScanEncoding::Utf16 | ScanEncoding::Both) {
        scan_utf16(data, 0, options, &mut visit);
        if options.utf16_unaligned {
            scan_utf16(data, 1, options, &mut visit);
        }
    }
}

/// Convenience wrapper collecting the results of [`scan`], sorted by offset
pub fn scan_to_vec<'data>(data: &'data [u8], options: &ScanOptions) -> Vec<FoundString<'data>> {
    let mut found = vec![];
    scan(data, options, |s| found.push(s));
    found.sort_by_key(|s| s.offset);
    found
}

fn is_printable(c: char, options: &ScanOptions) -> bool {
    // control characters (including tab and newline) terminate a string, which keeps the output free
    // of anything that would break a line based format
    !c.is_control() && (options.unicode || c.is_ascii())
}

fn scan_utf8<'data>(data: &'data [u8], options: &ScanOptions, visit: &mut impl FnMut(FoundString<'data>)) {
    let mut index = 0;
    let mut run_start = 0;
    let mut run_len = 0;

    let flush = |run_start: usize, run_end: usize, run_len: usize, visit: &mut dyn FnMut(FoundString<'data>)| {
        if run_len >= options.min_len {
            // the run was decoded from valid UTF-8, so this cannot fail
            let text = std::str::from_utf8(&data[run_start..run_end]).unwrap();
            visit(FoundString {
                offset: run_start,
                encoding: StringEncoding::Utf8,
                text: Cow::Borrowed(text),
            });
        }
    };

    while index < data.len() {
        match decode_utf8(&data[index..]) {
            Some((c, size)) if is_printable(c, options) => {
                if run_len == 0 {
                    run_start = index;
                }
                run_len += 1;
                index += size;
            }
            _ => {
                flush(run_start, index, run_len, visit);
                run_len = 0;
                index += 1;
            }
        }
    }
    flush(run_start, data.len(), run_len, visit);
}

fn scan_utf16<'data>(data: &'data [u8], align: usize, options: &ScanOptions, visit: &mut impl FnMut(FoundString<'data>)) {
    let mut index = align;
    let mut run_start = align;
    let mut run_len = 0;
    let mut run = String::new();

    let flush = |run: &mut String, run_start: usize, run_len: usize, visit: &mut dyn FnMut(FoundString<'data>)| {
        if run_len >= options.min_len {
            visit(FoundString {
                offset: run_start,
                encoding: StringEncoding::Utf16,
                text: Cow::Owned(std::mem::take(run)),
            });
        }
        run.clear();
    };

    while index + 2 <= data.len() {
        match decode_utf16(&data[index..]) {
            Some((c, size)) if is_printable(c, options) => {
                if run_len == 0 {
                    run_start = index;
                }
                run_len += 1;
                run.push(c);
                index += size;
            }
            _ => {
                flush(&mut run, run_start, run_len, visit);
                run_len = 0;
                index += 2;
            }
        }
    }
    flush(&mut run, run_start, run_len, visit);
}

/// Decodes a single UTF-8 character, returning it and the number of bytes it occupies
fn decode_utf8(data: &[u8]) -> Option<(char, usize)> {
    let first = *data.first()?;
    let (size, mut code) = match first {
        0x00..=0x7F => return Some((first as char, 1)),
        0xC2..=0xDF => (2, (first & 0x1F) as u32),
        0xE0..=0xEF => (3, (first & 0x0F) as u32),
        0xF0..=0xF4 => (4, (first & 0x07) as u32),
        _ => return None, // continuation byte, overlong two byte sequence or invalid
    };
    if data.len() < size {
        return None;
    }
    for &byte in &data[1..size] {
        if byte & 0xC0 != 0x80 {
            return None;
        }
        code = (code << 6) | (byte & 0x3F) as u32;
    }
    // reject overlong encodings, the shorter form would have been used instead
    if code < [0, 0, 0x80, 0x800, 0x1_0000][size] {
        return None;
    }
    Some((char::from_u32(code)?, size))
}

/// Decodes a single UTF-16LE character, returning it and the number of bytes it occupies
fn decode_utf16(data: &[u8]) -> Option<(char, usize)> {
    let unit = u16::from_le_bytes([*data.first()?, *data.get(1)?]);
    if let 0xD800..=0xDBFF = unit {
        // high surrogate, only valid when followed by a low surrogate
        let low = u16::from_le_bytes([*data.get(2)?, *data.get(3)?]);
        if !matches!(low, 0xDC00..=0xDFFF) {
            return None;
        }
        let code = 0x1_0000 + (((unit as u32 - 0xD800) << 10) | (low as u32 - 0xDC00));
        return Some((char::from_u32(code)?, 4));
    }
    Some((char::from_u32(unit as u32)?, 2))
}

#[cfg(test)]
mod test {
    use super::*;

    fn utf16(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(|c| c.to_le_bytes()).collect()
    }

    fn scan_strings(data: &[u8], options: &ScanOptions) -> Vec<(usize, StringEncoding, String)> {
        scan_to_vec(data, options).into_iter().map(|s| (s.offset, s.encoding, s.text.into_owned())).collect()
    }

    #[test]
    fn test_scan_utf8() {
        let options = ScanOptions {
            encoding: ScanEncoding::Utf8,
            unicode: true,
            ..Default::default()
        };

        let data = b"\x00\x01/Game/Foo\x00ab\x00\xffcombo_\xe3\x82\xa2 end";
        assert_eq!(
            scan_strings(data, &options),
            [
                (2, StringEncoding::Utf8, "/Game/Foo".to_string()),
                // "ab" is below the minimum length and dropped
                (16, StringEncoding::Utf8, "combo_ア end".to_string()),
            ]
        );

        // truncated multi byte sequences and lone continuation bytes are not printable
        assert_eq!(scan_strings(b"aaaa\xe3\x82", &options), [(0, StringEncoding::Utf8, "aaaa".to_string())]);
        assert_eq!(scan_strings(b"aaaa\x82\x82bbbb", &options), [(0, StringEncoding::Utf8, "aaaa".to_string()), (6, StringEncoding::Utf8, "bbbb".to_string())]);
        // overlong encoding of '/'
        assert_eq!(scan_strings(b"aaaa\xc0\xafaaaa", &options), [(0, StringEncoding::Utf8, "aaaa".to_string()), (6, StringEncoding::Utf8, "aaaa".to_string())]);
    }

    #[test]
    fn test_scan_utf16() {
        let options = ScanOptions {
            encoding: ScanEncoding::Utf16,
            unicode: true,
            ..Default::default()
        };

        let mut data = vec![0x00, 0x00];
        data.extend(utf16("ダメージ"));
        data.extend([0, 0, 0, 0]);
        data.extend(utf16("🎮 ok"));
        assert_eq!(scan_strings(&data, &options), [(2, StringEncoding::Utf16, "ダメージ".to_string()), (14, StringEncoding::Utf16, "🎮 ok".to_string())]);

        // a lone surrogate terminates the run
        let mut data = utf16("aaaa");
        data.extend([0x00, 0xD8]);
        data.extend(utf16("bbbb"));
        assert_eq!(scan_strings(&data, &options), [(0, StringEncoding::Utf16, "aaaa".to_string()), (10, StringEncoding::Utf16, "bbbb".to_string())]);

        // a string at an odd offset is only found when unaligned scanning is enabled. Reading it at
        // the wrong alignment still yields printable (CJK) code units, which is exactly the noise
        // that keeps unaligned scanning off by default
        let mut data = vec![0x00];
        data.extend(utf16("hello"));
        assert!(!scan_strings(&data, &options).iter().any(|(_, _, text)| text == "hello"));
        assert!(scan_strings(&data, &ScanOptions { utf16_unaligned: true, ..options }).contains(&(1, StringEncoding::Utf16, "hello".to_string())));
    }

    #[test]
    fn test_scan_both() {
        // the ANSI text reads as two printable CJK code units in UTF-16, only the terminator keeps
        // that run below the minimum length
        let mut data = b"name".to_vec();
        data.extend([0x00, 0x00]);
        data.extend(utf16("wide"));
        assert_eq!(scan_strings(&data, &ScanOptions::default()), [(0, StringEncoding::Utf8, "name".to_string()), (6, StringEncoding::Utf16, "wide".to_string())]);
    }

    #[test]
    fn test_ascii_only_by_default() {
        // most byte pairs decode to a printable CJK character, so without --unicode the UTF-16 scan
        // only accepts ASCII and leaves binary data alone
        let noise: Vec<u8> = (0..64u8).map(|i| i.wrapping_mul(37).wrapping_add(0x41)).collect();
        assert_eq!(scan_strings(&noise, &ScanOptions { encoding: ScanEncoding::Utf16, ..Default::default() }), []);
        assert!(
            !scan_strings(
                &noise,
                &ScanOptions {
                    encoding: ScanEncoding::Utf16,
                    unicode: true,
                    ..Default::default()
                }
            )
            .is_empty()
        );

        // the same applies to multi byte UTF-8
        let data = "アイテム".as_bytes();
        assert_eq!(scan_strings(data, &ScanOptions { encoding: ScanEncoding::Utf8, ..Default::default() }), []);
        assert_eq!(
            scan_strings(
                data,
                &ScanOptions {
                    encoding: ScanEncoding::Utf8,
                    unicode: true,
                    ..Default::default()
                }
            ),
            [(0, StringEncoding::Utf8, "アイテム".to_string())]
        );
    }

    #[test]
    fn test_min_len() {
        let options = ScanOptions { min_len: 8, ..Default::default() };
        assert_eq!(scan_strings(b"shortstr", &options), [(0, StringEncoding::Utf8, "shortstr".to_string())]);
        assert_eq!(scan_strings(b"shortst", &options), []);
    }
}
