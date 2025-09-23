use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::errors::Result;
use crate::events::Event;
use crate::storage::frame::{Frame, FramedRecord};
use crate::encryption::EncryptionService;

pub struct Segment {
    path: PathBuf,
    id: u64,
    encryption_service: Option<Arc<EncryptionService>>,
}

impl Segment {
    pub fn new(path: PathBuf, id: u64) -> Self {
        Self {
            path,
            id,
            encryption_service: None,
        }
    }

    pub fn new_with_encryption(path: PathBuf, id: u64, encryption_service: Arc<EncryptionService>) -> Self {
        Self {
            path,
            id,
            encryption_service: Some(encryption_service),
        }
    }

    pub fn create(&self) -> Result<SegmentWriter> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        Ok(SegmentWriter::new(file, self.encryption_service.clone(), self.id))
    }

    pub fn open_writer(&self) -> Result<SegmentWriter> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(SegmentWriter::new(file, self.encryption_service.clone(), self.id))
    }

    pub fn open_reader(&self) -> Result<SegmentReader> {
        let file = File::open(&self.path)?;
        Ok(SegmentReader::new(file, self.encryption_service.clone(), self.id))
    }

    pub fn size(&self) -> Result<u64> {
        Ok(fs::metadata(&self.path)?.len())
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn truncate_at(&self, position: u64) -> Result<()> {
        let file = OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(position)?;
        Ok(())
    }
}

pub struct SegmentWriter {
    writer: BufWriter<File>,
    bytes_written: u64,
    encryption_service: Option<Arc<EncryptionService>>,
    segment_id: u64,
}

impl SegmentWriter {
    fn new(file: File, encryption_service: Option<Arc<EncryptionService>>, segment_id: u64) -> Self {
        let pos = file.metadata().map(|m| m.len()).unwrap_or(0);
        Self {
            writer: BufWriter::new(file),
            bytes_written: pos,
            encryption_service,
            segment_id,
        }
    }

    pub fn append_event(&mut self, event: &Event) -> Result<u64> {
        let record = FramedRecord::from_event(event.clone());
        let mut frame = record.to_frame()?;

        // Encrypt frame data if encryption service is available
        if let Some(ref encryption_service) = self.encryption_service {
            let context = format!("segment_{}", self.segment_id);
            frame.data = encryption_service.encrypt(&frame.data, &context)?;
            // Recalculate CRC after encryption
            use crc32fast::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(&frame.data);
            frame.crc32 = hasher.finalize();
            frame.length = frame.data.len() as u32;
        }

        frame.write_to(&mut self.writer)?;
        self.bytes_written += 8 + frame.data.len() as u64;
        Ok(self.bytes_written)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.writer.get_mut().sync_all()?;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

pub struct SegmentReader {
    reader: BufReader<File>,
    encryption_service: Option<Arc<EncryptionService>>,
    segment_id: u64,
}

impl SegmentReader {
    fn new(file: File, encryption_service: Option<Arc<EncryptionService>>, segment_id: u64) -> Self {
        Self {
            reader: BufReader::new(file),
            encryption_service,
            segment_id,
        }
    }

    pub fn read_all_events(&mut self) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        while let Some(event) = self.read_next_event()? {
            events.push(event);
        }
        Ok(events)
    }

    pub fn read_next_event(&mut self) -> Result<Option<Event>> {
        match Frame::read_from(&mut self.reader)? {
            Some(mut frame) => {
                // Decrypt frame data if encryption service is available
                if let Some(ref encryption_service) = self.encryption_service {
                    let context = format!("segment_{}", self.segment_id);
                    frame.data = encryption_service.decrypt(&frame.data, &context)?;
                }

                let record = FramedRecord::from_frame(&frame)?;
                Ok(Some(record.event))
            }
            None => Ok(None),
        }
    }

    pub fn verify_and_find_corruption(&mut self) -> Result<Option<u64>> {
        self.reader.seek(SeekFrom::Start(0))?;

        loop {
            let current_pos = self.reader.stream_position()?;
            match Frame::read_from(&mut self.reader) {
                Ok(Some(mut frame)) => {
                    if !frame.verify() {
                        return Ok(Some(current_pos));
                    }

                    // Try to decrypt if encryption is enabled
                    if let Some(ref encryption_service) = self.encryption_service {
                        let context = format!("segment_{}", self.segment_id);
                        match encryption_service.decrypt(&frame.data, &context) {
                            Ok(decrypted) => frame.data = decrypted,
                            Err(_) => return Ok(Some(current_pos)), // Decryption failure indicates corruption
                        }
                    }

                    match FramedRecord::from_frame(&frame) {
                        Ok(_) => {
                            let _ = self.reader.stream_position()?;
                        }
                        Err(_) => return Ok(Some(current_pos)),
                    }
                }
                Ok(None) => break,
                Err(_) => return Ok(Some(current_pos)),
            }
        }

        Ok(None)
    }
}