//! PostgreSQL Wire Protocol Codec

use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::io::{self, ErrorKind};

use super::messages::Message;

/// PostgreSQL protocol codec for tokio
#[allow(dead_code)]
pub struct PostgresCodec {
    startup_done: bool,
}

impl PostgresCodec {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            startup_done: false,
        }
    }
}

/// Decode a message from bytes
pub fn decode_message(buf: &mut BytesMut, startup_done: bool) -> io::Result<Option<Message>> {
    if !startup_done {
        // During the startup phase, we could get:
        // 1. Startup message (no type byte, starts with length)
        // 2. SSL request (no type byte, special version number)
        // 3. Password response (has type byte 'p') after auth challenge

        // First check if this might be a regular message with a type byte (like password)
        if buf.len() >= 5 {
            // Peek at the first byte to see if it's a message type
            let first_byte = buf[0];
            if first_byte == b'p' {
                // This is a password message, handle it like a regular message
                // Fall through to normal message handling below
            } else {
                // Check for startup message or SSL request
                if buf.len() < 8 {
                    return Ok(None);
                }

                let len = read_i32(buf) as usize;
                if buf.len() < len - 4 {
                    // Not enough data
                    buf.reserve(len - 4 - buf.len());
                    return Ok(None);
                }

                let version = read_i32(buf);

                if version == 80877103 {
                    // SSL request
                    return Ok(Some(Message::SSLRequest));
                }

                // Startup message
                let mut params = HashMap::new();
                while buf.remaining() > 0 {
                    let key = read_cstring(buf)?;
                    if key.is_empty() {
                        break;
                    }
                    let value = read_cstring(buf)?;
                    params.insert(key, value);
                }

                return Ok(Some(Message::StartupMessage {
                    version,
                    parameters: params,
                }));
            }
        } else {
            return Ok(None); // Not enough data
        }
    }

    // Regular message
    if buf.len() < 5 {
        return Ok(None);
    }

    let msg_type = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

    if buf.len() < len + 1 {
        // Not enough data
        buf.reserve(len + 1 - buf.len());
        return Ok(None);
    }

    // Remove type byte and length
    buf.advance(1);
    let len_without_self = read_i32(buf) as usize - 4;

    // Take the message body
    let msg_buf = buf.split_to(len_without_self);

    match msg_type {
        b'Q' => {
            // Query
            let sql = String::from_utf8(msg_buf[..msg_buf.len() - 1].to_vec())
                .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Invalid UTF-8"))?;
            Ok(Some(Message::Query { sql }))
        }

        b'X' => {
            // Terminate
            Ok(Some(Message::Terminate))
        }

        b'p' => {
            // Password message
            let password = String::from_utf8(msg_buf[..msg_buf.len() - 1].to_vec())
                .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Invalid UTF-8"))?;
            Ok(Some(Message::PasswordMessage { password }))
        }

        b'P' => {
            // Parse
            let mut cursor = msg_buf;
            let statement_name = read_cstring_from(&mut cursor)?;
            let query = read_cstring_from(&mut cursor)?;

            let param_count = cursor.get_i16() as usize;
            let mut parameter_types = Vec::with_capacity(param_count);
            for _ in 0..param_count {
                parameter_types.push(cursor.get_i32());
            }

            Ok(Some(Message::Parse {
                statement_name,
                query,
                parameter_types,
            }))
        }

        b'S' => {
            // Sync
            Ok(Some(Message::Sync))
        }

        _ => {
            // Unknown message type
            Ok(None)
        }
    }
}

/// Encode a message to bytes
pub fn encode_message(msg: &Message) -> BytesMut {
    msg.encode()
}

// Helper functions

fn read_i32(buf: &mut BytesMut) -> i32 {
    buf.get_i32()
}

fn read_cstring(buf: &mut BytesMut) -> io::Result<String> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Missing null terminator"))?;

    let s = String::from_utf8(buf[..pos].to_vec())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Invalid UTF-8"))?;

    buf.advance(pos + 1);
    Ok(s)
}

fn read_cstring_from(buf: &mut BytesMut) -> io::Result<String> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Missing null terminator"))?;

    let s = String::from_utf8(buf.split_to(pos).to_vec())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Invalid UTF-8"))?;

    buf.advance(1); // Skip null terminator
    Ok(s)
}
