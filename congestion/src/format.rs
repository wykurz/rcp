//! Binary log file format for auto-meta histogram recordings.
//!
//! See `docs/congestion_control.md` for the canonical format reference.
//! The short version:
//!
//! ```text
//!   "RCP-AUTOMETA-HIST-V1\n"  // 21 ASCII bytes (magic, includes \n)
//!   <hdr_len: u32 LE>
//!   <hdr_bytes: JSON, hdr_len bytes>
//!   loop {
//!       <rec_len: u32 LE>
//!       <unix_micros: u64 LE>
//!       <side: u8>            // 0 = Source, 1 = Destination
//!       <op: u8>              // MetadataOp discriminant
//!       <samples_count: u64 LE>
//!       <hdr_v2_blob: rec_len - 18 bytes>
//!   }
//! ```
//!
//! Multi-byte integers are little-endian. Records are length-prefixed so
//! readers can detect partial writes (process killed mid-record) and stop
//! cleanly at the last full record.

use crate::measurement::{MetadataOp, Side};

/// Magic bytes at the start of every log file. Includes a trailing
/// newline so `head -1 file.hdr` shows the version on a clean line.
pub const MAGIC: &[u8; 21] = b"RCP-AUTOMETA-HIST-V1\n";

/// The fixed (non-blob) bytes per record: u64 timestamp + u8 side + u8 op + u64 count.
pub const RECORD_FIXED_BYTES: usize = 8 + 1 + 1 + 8;

/// Maximum accepted size of the JSON header in bytes. Real headers
/// are a few hundred bytes; this cap is generous enough that any
/// realistic configuration fits, but tight enough to refuse a
/// malicious or corrupt file that claims a multi-GiB header.
pub const MAX_HEADER_BYTES: u32 = 1 << 20; // 1 MiB

/// Maximum accepted size of a single record in bytes. HDR v2 blobs
/// at the configured range and precision are ~few KiB; this cap
/// catches obvious corruption while leaving generous headroom.
pub const MAX_RECORD_BYTES: u32 = 1 << 20; // 1 MiB

/// Top-level header captured at file start. Mirrors the fields a reader
/// needs to interpret records; `auto_meta` carries a snapshot of the
/// `AutoMetaThrottleConfig` that drove the run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct LogHeader {
    pub format_version: u32,
    pub tool: String,
    pub tool_version: String,
    pub hostname: String,
    pub pid: u32,
    pub start_unix_micros: u64,
    pub snapshot_interval_micros: u64,
    pub auto_meta: AutoMetaSnapshot,
    pub hdr: HdrSnapshot,
    pub unit_labels: Vec<UnitLabel>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct AutoMetaSnapshot {
    pub initial_cwnd: u32,
    pub min_cwnd: u32,
    pub max_cwnd: u32,
    pub alpha: f64,
    pub beta: f64,
    pub increase_step: u32,
    pub decrease_step: u32,
    pub baseline_percentile: f64,
    pub current_percentile: f64,
    pub long_window_micros: u64,
    pub short_window_micros: u64,
    pub tick_interval_micros: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct HdrSnapshot {
    pub lowest_discernible_micros: u64,
    pub highest_trackable_micros: u64,
    pub significant_figures: u8,
    pub unit: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct UnitLabel {
    pub side: u8,
    pub op: u8,
    pub label: String,
}

/// One record's worth of decoded data after parsing the binary frame.
///
/// `histogram` is the deserialized HDR; `samples_count` is what the
/// writer recorded in the fixed prefix (it equals `histogram.len()`
/// for valid records and is exposed redundantly for tools that want to
/// scan a log without parsing each blob).
#[derive(Debug)]
pub struct Record {
    pub unix_micros: u64,
    pub side: Side,
    pub op: MetadataOp,
    pub samples_count: u64,
    pub histogram: hdrhistogram::Histogram<u64>,
}

/// Errors returned by the binary format reader.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("io: {0:#}")]
    Io(#[from] std::io::Error),
    #[error("bad magic: expected {MAGIC:?}, found {0:?}")]
    BadMagic([u8; 21]),
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u32),
    #[error("invalid Side discriminant {0}")]
    BadSide(u8),
    #[error("invalid MetadataOp discriminant {0}")]
    BadOp(u8),
    #[error("hdr deserialization failed: {0}")]
    Hdr(String),
    #[error("json: {0:#}")]
    Json(#[from] serde_json::Error),
    #[error("header length {0} exceeds max {MAX_HEADER_BYTES}")]
    HeaderTooLarge(u32),
    #[error("record length {0} exceeds max {MAX_RECORD_BYTES}")]
    RecordTooLarge(u32),
}

/// Errors returned by the binary format writer.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("io: {0:#}")]
    Io(#[from] std::io::Error),
    #[error("hdr serialization failed: {0}")]
    Hdr(String),
    #[error("json: {0:#}")]
    Json(#[from] serde_json::Error),
}

/// Write the file header (magic + length + JSON header) to `out`.
pub fn write_file_header<W: std::io::Write>(
    out: &mut W,
    header: &LogHeader,
) -> Result<(), WriteError> {
    out.write_all(MAGIC)?;
    let json = serde_json::to_vec_pretty(header)?;
    let len = u32::try_from(json.len()).expect("header < 4GB");
    out.write_all(&len.to_le_bytes())?;
    out.write_all(&json)?;
    Ok(())
}

/// Read and validate the file header, returning the deserialized struct.
pub fn read_file_header<R: std::io::Read>(input: &mut R) -> Result<LogHeader, ReadError> {
    let mut magic = [0u8; 21];
    input.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(ReadError::BadMagic(magic));
    }
    let mut len_bytes = [0u8; 4];
    input.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes);
    if len > MAX_HEADER_BYTES {
        return Err(ReadError::HeaderTooLarge(len));
    }
    let len = len as usize;
    let mut json = vec![0u8; len];
    input.read_exact(&mut json)?;
    let header: LogHeader = serde_json::from_slice(&json)?;
    if header.format_version != 1 {
        return Err(ReadError::UnsupportedVersion(header.format_version));
    }
    Ok(header)
}

/// Append one record to `out`, encoding `histogram` as HDR v2 binary.
///
/// The record body's fixed prefix (unix_micros + side + op + samples_count)
/// occupies [`RECORD_FIXED_BYTES`] bytes; the rest is the HDR blob.
pub fn write_record<W: std::io::Write>(
    out: &mut W,
    unix_micros: u64,
    side: Side,
    op: MetadataOp,
    histogram: &hdrhistogram::Histogram<u64>,
) -> Result<(), WriteError> {
    use hdrhistogram::serialization::{Serializer, V2Serializer};
    let mut blob: Vec<u8> = Vec::new();
    let mut serializer = V2Serializer::new();
    serializer
        .serialize(histogram, &mut blob)
        .map_err(|e| WriteError::Hdr(format!("{e:?}")))?;
    let rec_len = u32::try_from(RECORD_FIXED_BYTES + blob.len()).expect("record < 4GB");
    out.write_all(&rec_len.to_le_bytes())?;
    out.write_all(&unix_micros.to_le_bytes())?;
    out.write_all(&[side as u8])?;
    out.write_all(&[op as u8])?;
    out.write_all(&histogram.len().to_le_bytes())?;
    out.write_all(&blob)?;
    Ok(())
}

/// Read one record. Returns `Ok(None)` at EOF *or* on truncation
/// (partial-record tail): the caller treats this as "stop cleanly,
/// every prior record is valid".
pub fn read_record<R: std::io::Read>(input: &mut R) -> Result<Option<Record>, ReadError> {
    let mut len_bytes = [0u8; 4];
    match input.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(ReadError::Io(e)),
    }
    let rec_len = u32::from_le_bytes(len_bytes);
    if rec_len > MAX_RECORD_BYTES {
        return Err(ReadError::RecordTooLarge(rec_len));
    }
    let rec_len = rec_len as usize;
    if rec_len < RECORD_FIXED_BYTES {
        // Length too small to even fit the fixed prefix → corrupt or
        // truncated; bail rather than erroring so callers preserve any
        // earlier good records.
        return Ok(None);
    }
    let mut body = vec![0u8; rec_len];
    if let Err(e) = input.read_exact(&mut body) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(ReadError::Io(e));
    }
    let unix_micros = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let side = match body[8] {
        0 => Side::Source,
        1 => Side::Destination,
        x => return Err(ReadError::BadSide(x)),
    };
    let op = match body[9] {
        0 => MetadataOp::Stat,
        1 => MetadataOp::ReadLink,
        2 => MetadataOp::MkDir,
        3 => MetadataOp::RmDir,
        4 => MetadataOp::Unlink,
        5 => MetadataOp::HardLink,
        6 => MetadataOp::Symlink,
        7 => MetadataOp::Chmod,
        8 => MetadataOp::OpenCreate,
        x => return Err(ReadError::BadOp(x)),
    };
    let samples_count = u64::from_le_bytes(body[10..18].try_into().unwrap());
    let blob = &body[18..];
    let mut deserializer = hdrhistogram::serialization::Deserializer::new();
    let mut blob_cursor = std::io::Cursor::new(blob);
    let histogram: hdrhistogram::Histogram<u64> = deserializer
        .deserialize(&mut blob_cursor)
        .map_err(|e| ReadError::Hdr(format!("{e:?}")))?;
    Ok(Some(Record {
        unix_micros,
        side,
        op,
        samples_count,
        histogram,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> LogHeader {
        LogHeader {
            format_version: 1,
            tool: "rcp".to_string(),
            tool_version: "0.32.0".to_string(),
            hostname: "test-host".to_string(),
            pid: 12345,
            start_unix_micros: 1_700_000_000_000_000,
            snapshot_interval_micros: 1_000_000,
            auto_meta: AutoMetaSnapshot {
                initial_cwnd: 1,
                min_cwnd: 1,
                max_cwnd: 4096,
                alpha: 1.3,
                beta: 1.8,
                increase_step: 1,
                decrease_step: 1,
                baseline_percentile: 0.1,
                current_percentile: 0.5,
                long_window_micros: 10_000_000,
                short_window_micros: 1_000_000,
                tick_interval_micros: 50_000,
            },
            hdr: HdrSnapshot {
                lowest_discernible_micros: 1,
                highest_trackable_micros: 3_600_000_000,
                significant_figures: 3,
                unit: "microseconds".to_string(),
            },
            unit_labels: vec![
                UnitLabel {
                    side: 0,
                    op: 0,
                    label: "src-stat".into(),
                },
                UnitLabel {
                    side: 1,
                    op: 0,
                    label: "dst-stat".into(),
                },
            ],
        }
    }

    #[test]
    fn header_serde_roundtrip() {
        let h = sample_header();
        let bytes = serde_json::to_vec(&h).unwrap();
        let h2: LogHeader = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(h, h2);
    }

    #[test]
    fn header_tolerates_unknown_top_level_keys() {
        // Forward-compat: a future writer adding `extra: { ... }` must not
        // break older readers. serde's default is to error on unknown
        // fields; we explicitly need the lenient behavior.
        let json = r#"{
            "format_version": 1,
            "tool": "rcp",
            "tool_version": "0.32.0",
            "hostname": "h",
            "pid": 1,
            "start_unix_micros": 0,
            "snapshot_interval_micros": 1000000,
            "auto_meta": {
                "initial_cwnd": 1, "min_cwnd": 1, "max_cwnd": 4096,
                "alpha": 1.3, "beta": 1.8,
                "increase_step": 1, "decrease_step": 1,
                "baseline_percentile": 0.1, "current_percentile": 0.5,
                "long_window_micros": 10000000, "short_window_micros": 1000000,
                "tick_interval_micros": 50000
            },
            "hdr": {
                "lowest_discernible_micros": 1,
                "highest_trackable_micros": 3600000000,
                "significant_figures": 3, "unit": "microseconds"
            },
            "unit_labels": [],
            "extra": { "future_key": 42 }
        }"#;
        let parsed: LogHeader = serde_json::from_str(json).expect("must tolerate unknown keys");
        assert_eq!(parsed.format_version, 1);
    }

    #[test]
    fn write_and_read_file_with_two_records_roundtrips() {
        // Build a small file: header + two records + truncate. Read it back;
        // both records present, both decode cleanly.
        let mut buf: Vec<u8> = Vec::new();
        let header = sample_header();
        write_file_header(&mut buf, &header).unwrap();
        let mut h1 = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 1_000_000, 3).unwrap();
        h1.record(100).unwrap();
        h1.record(200).unwrap();
        write_record(
            &mut buf,
            1_700_000_000_500_000,
            Side::Source,
            MetadataOp::Stat,
            &h1,
        )
        .unwrap();
        let mut h2 = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 1_000_000, 3).unwrap();
        h2.record(300).unwrap();
        write_record(
            &mut buf,
            1_700_000_001_500_000,
            Side::Destination,
            MetadataOp::MkDir,
            &h2,
        )
        .unwrap();

        let mut cursor = std::io::Cursor::new(&buf);
        let parsed_header = read_file_header(&mut cursor).unwrap();
        assert_eq!(parsed_header, header);
        let r1 = read_record(&mut cursor).unwrap().expect("first record");
        assert_eq!(r1.unix_micros, 1_700_000_000_500_000);
        assert_eq!(r1.side, Side::Source);
        assert_eq!(r1.op, MetadataOp::Stat);
        assert_eq!(r1.samples_count, 2);
        let r2 = read_record(&mut cursor).unwrap().expect("second record");
        assert_eq!(r2.side, Side::Destination);
        assert_eq!(r2.op, MetadataOp::MkDir);
        assert_eq!(r2.samples_count, 1);
        // EOF after last record returns Ok(None).
        assert!(read_record(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn read_record_returns_none_on_truncated_tail() {
        // Process killed mid-write: file ends inside a record. The reader
        // must detect the truncation via the length prefix and return
        // Ok(None) so callers can recover all complete records.
        let mut buf: Vec<u8> = Vec::new();
        let header = sample_header();
        write_file_header(&mut buf, &header).unwrap();
        let mut h = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 1_000_000, 3).unwrap();
        h.record(100).unwrap();
        write_record(&mut buf, 0, Side::Source, MetadataOp::Stat, &h).unwrap();
        // Truncate: drop the last 5 bytes.
        buf.truncate(buf.len() - 5);
        let mut cursor = std::io::Cursor::new(&buf);
        let _ = read_file_header(&mut cursor).unwrap();
        // Truncation manifests on read_record: it must return Ok(None),
        // not an error.
        assert!(read_record(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn read_file_header_rejects_bad_magic() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(b"NOT-THE-RIGHT-MAGIC!!"); // 21 bytes
        let mut cursor = std::io::Cursor::new(&buf);
        assert!(read_file_header(&mut cursor).is_err());
    }

    #[test]
    fn read_file_header_rejects_excessive_length() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(MAGIC);
        // pretend the header is 100 MiB
        buf.extend_from_slice(&(100u32 * 1024 * 1024).to_le_bytes());
        // append a few bytes of "header" so the read doesn't simply EOF
        buf.extend_from_slice(b"{ \"format_version\": 1 }");
        let mut cursor = std::io::Cursor::new(&buf);
        let err = read_file_header(&mut cursor).unwrap_err();
        assert!(
            matches!(err, ReadError::HeaderTooLarge(_)),
            "expected HeaderTooLarge, got: {err:?}",
        );
    }

    #[test]
    fn read_record_rejects_excessive_length() {
        let mut buf: Vec<u8> = Vec::new();
        write_file_header(&mut buf, &sample_header()).unwrap();
        // append a record with claimed length 100 MiB
        buf.extend_from_slice(&(100u32 * 1024 * 1024).to_le_bytes());
        // some bytes; the read will hit the cap before allocating
        buf.extend_from_slice(&[0u8; 32]);
        let mut cursor = std::io::Cursor::new(&buf);
        let _ = read_file_header(&mut cursor).unwrap();
        let err = read_record(&mut cursor).unwrap_err();
        assert!(
            matches!(err, ReadError::RecordTooLarge(_)),
            "expected RecordTooLarge, got: {err:?}",
        );
    }
}
