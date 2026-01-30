use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

use crate::errors::{DriftError, Result};
use crate::events::Event;

#[derive(Debug, Clone)]
pub struct Frame {
    pub length: u32,
    pub crc32: u32,
    pub data: Vec<u8>,
}

impl Frame {
    pub fn new(data: Vec<u8>) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let crc32 = hasher.finalize();

        Self {
            length: data.len() as u32,
            crc32,
            data,
        }
    }

    pub fn verify(&self) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(&self.data);
        hasher.finalize() == self.crc32
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.length.to_le_bytes())?;
        writer.write_all(&self.crc32.to_le_bytes())?;
        writer.write_all(&self.data)?;
        Ok(())
    }

    pub fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        // Maximum frame size: 64MB (prevents DoS via untrusted length field)
        const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

        let mut length_buf = [0u8; 4];
        match reader.read_exact(&mut length_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let length = u32::from_le_bytes(length_buf);

        // Validate frame size to prevent unbounded allocations
        if length > MAX_FRAME_SIZE {
            return Err(DriftError::CorruptSegment(format!(
                "Frame size {} exceeds maximum allowed size of {} bytes",
                length, MAX_FRAME_SIZE
            )));
        }

        // Additional sanity check: reject zero-length frames
        if length == 0 {
            return Err(DriftError::CorruptSegment(
                "Invalid zero-length frame".into(),
            ));
        }

        let mut crc32_buf = [0u8; 4];
        reader.read_exact(&mut crc32_buf)?;
        let crc32 = u32::from_le_bytes(crc32_buf);

        let mut data = vec![0u8; length as usize];
        reader.read_exact(&mut data)?;

        let frame = Self {
            length,
            crc32,
            data,
        };

        if !frame.verify() {
            return Err(DriftError::CorruptSegment("CRC verification failed".into()));
        }

        Ok(Some(frame))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FramedRecord {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub event_type: u8,
    pub event: Event,
}

impl FramedRecord {
    pub fn from_event(event: Event) -> Self {
        use crate::events::EventType;

        let event_type = match event.event_type {
            EventType::Insert => 1,
            EventType::Patch => 2,
            EventType::SoftDelete => 3,
        };

        Self {
            sequence: event.sequence,
            timestamp_ms: event.timestamp.unix_timestamp() as u64 * 1000,
            event_type,
            event,
        }
    }

    pub fn to_frame(&self) -> Result<Frame> {
        let data = rmp_serde::to_vec(self)?;
        Ok(Frame::new(data))
    }

    pub fn from_frame(frame: &Frame) -> Result<Self> {
        Ok(rmp_serde::from_slice(&frame.data)?)
    }
}
