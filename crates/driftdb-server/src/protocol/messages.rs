//! PostgreSQL Protocol Messages

use bytes::{BufMut, BytesMut};
use std::collections::HashMap;

/// PostgreSQL protocol messages
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Message {
    // Startup phase
    StartupMessage {
        #[allow(dead_code)]
        version: i32,
        parameters: HashMap<String, String>,
    },
    SSLRequest,

    // Authentication
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMD5Password {
        #[allow(dead_code)]
        salt: [u8; 4],
    },
    AuthenticationSASL {
        #[allow(dead_code)]
        mechanisms: Vec<String>,
    },
    AuthenticationSASLContinue {
        data: Vec<u8>,
    },
    AuthenticationSASLFinal {
        data: Vec<u8>,
    },
    PasswordMessage {
        password: String,
    },
    SASLInitialResponse {
        mechanism: String,
        data: Vec<u8>,
    },
    SASLResponse {
        data: Vec<u8>,
    },

    // Simple Query
    Query {
        sql: String,
    },

    // Extended Query
    Parse {
        #[allow(dead_code)]
        statement_name: String,
        #[allow(dead_code)]
        query: String,
        #[allow(dead_code)]
        parameter_types: Vec<i32>,
    },
    Bind {
        portal_name: String,
        statement_name: String,
        parameter_formats: Vec<i16>,
        parameters: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Execute {
        portal_name: String,
        max_rows: i32,
    },
    Describe {
        typ: u8, // 'S' for statement, 'P' for portal
        name: String,
    },
    Close {
        typ: u8, // 'S' for statement, 'P' for portal
        name: String,
    },
    Sync,
    Flush,

    // Responses
    CommandComplete {
        tag: String,
    },
    DataRow {
        values: Vec<Option<Vec<u8>>>,
    },
    EmptyQueryResponse,
    ErrorResponse {
        fields: HashMap<u8, String>,
    },
    NoticeResponse {
        fields: HashMap<u8, String>,
    },
    ReadyForQuery {
        status: u8,
    },
    RowDescription {
        fields: Vec<super::FieldDescription>,
    },
    ParameterDescription {
        types: Vec<i32>,
    },
    ParseComplete,
    BindComplete,
    CloseComplete,
    PortalSuspended,
    NoData,

    // Copy operations (not implemented yet)
    CopyInResponse,
    CopyOutResponse,
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    CopyFail {
        message: String,
    },

    // Misc
    ParameterStatus {
        name: String,
        value: String,
    },
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    Terminate,
}

impl Message {
    /// Get the message type byte
    #[allow(dead_code)]
    pub fn type_byte(&self) -> Option<u8> {
        match self {
            Message::AuthenticationOk
            | Message::AuthenticationCleartextPassword
            | Message::AuthenticationMD5Password { .. }
            | Message::AuthenticationSASL { .. }
            | Message::AuthenticationSASLContinue { .. }
            | Message::AuthenticationSASLFinal { .. } => Some(b'R'),
            Message::PasswordMessage { .. } => Some(b'p'),
            Message::SASLInitialResponse { .. } => Some(b'p'),
            Message::SASLResponse { .. } => Some(b'p'),
            Message::Query { .. } => Some(b'Q'),
            Message::Parse { .. } => Some(b'P'),
            Message::Bind { .. } => Some(b'B'),
            Message::Execute { .. } => Some(b'E'),
            Message::Describe { .. } => Some(b'D'),
            Message::Close { .. } => Some(b'C'),
            Message::Sync => Some(b'S'),
            Message::Flush => Some(b'H'),
            Message::CommandComplete { .. } => Some(b'C'),
            Message::DataRow { .. } => Some(b'D'),
            Message::EmptyQueryResponse => Some(b'I'),
            Message::ErrorResponse { .. } => Some(b'E'),
            Message::NoticeResponse { .. } => Some(b'N'),
            Message::ReadyForQuery { .. } => Some(b'Z'),
            Message::RowDescription { .. } => Some(b'T'),
            Message::ParameterDescription { .. } => Some(b't'),
            Message::ParseComplete => Some(b'1'),
            Message::BindComplete => Some(b'2'),
            Message::CloseComplete => Some(b'3'),
            Message::PortalSuspended => Some(b's'),
            Message::NoData => Some(b'n'),
            Message::ParameterStatus { .. } => Some(b'S'),
            Message::BackendKeyData { .. } => Some(b'K'),
            Message::Terminate => Some(b'X'),
            _ => None,
        }
    }

    /// Encode the message to bytes
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();

        match self {
            Message::AuthenticationOk => {
                buf.put_u8(b'R');
                buf.put_i32(8); // Length including self
                buf.put_i32(0); // Auth type: OK
            }

            Message::AuthenticationMD5Password { salt } => {
                buf.put_u8(b'R');
                buf.put_i32(12); // Length including self (4 + 4 + 4)
                buf.put_i32(5); // Auth type: MD5
                buf.put_slice(salt); // 4 byte salt
            }

            Message::CommandComplete { tag } => {
                buf.put_u8(b'C');
                let len = 4 + tag.len() + 1; // Length + tag + null
                buf.put_i32(len as i32);
                buf.put_slice(tag.as_bytes());
                buf.put_u8(0); // Null terminator
            }

            Message::DataRow { values } => {
                buf.put_u8(b'D');

                // Calculate length
                let mut len = 4 + 2; // Length itself + field count
                for val in values {
                    len += 4; // Field length
                    if let Some(v) = val {
                        len += v.len();
                    }
                }

                buf.put_i32(len as i32);
                buf.put_i16(values.len() as i16);

                for val in values {
                    if let Some(v) = val {
                        buf.put_i32(v.len() as i32);
                        buf.put_slice(v);
                    } else {
                        buf.put_i32(-1); // NULL
                    }
                }
            }

            Message::ReadyForQuery { status } => {
                buf.put_u8(b'Z');
                buf.put_i32(5); // Length including self
                buf.put_u8(*status);
            }

            Message::RowDescription { fields } => {
                buf.put_u8(b'T');

                // Calculate length
                let mut len = 4 + 2; // Length + field count
                for field in fields {
                    len += field.name.len() + 1; // Name + null
                    len += 4 + 2 + 4 + 2 + 4 + 2; // Fixed fields
                }

                buf.put_i32(len as i32);
                buf.put_i16(fields.len() as i16);

                for field in fields {
                    buf.put_slice(field.name.as_bytes());
                    buf.put_u8(0); // Null terminator
                    buf.put_i32(field.table_oid);
                    buf.put_i16(field.column_id);
                    buf.put_i32(field.type_oid);
                    buf.put_i16(field.type_size);
                    buf.put_i32(field.type_modifier);
                    buf.put_i16(field.format_code);
                }
            }

            Message::ErrorResponse { fields } => {
                buf.put_u8(b'E');

                // Calculate length
                let mut len = 4; // Length itself
                for value in fields.values() {
                    len += 1 + value.len() + 1; // Code + value + null
                }
                len += 1; // Final null

                buf.put_i32(len as i32);

                for (code, value) in fields {
                    buf.put_u8(*code);
                    buf.put_slice(value.as_bytes());
                    buf.put_u8(0);
                }
                buf.put_u8(0); // Final null
            }

            Message::ParameterStatus { name, value } => {
                buf.put_u8(b'S');
                let len = 4 + name.len() + 1 + value.len() + 1;
                buf.put_i32(len as i32);
                buf.put_slice(name.as_bytes());
                buf.put_u8(0);
                buf.put_slice(value.as_bytes());
                buf.put_u8(0);
            }

            Message::BackendKeyData {
                process_id,
                secret_key,
            } => {
                buf.put_u8(b'K');
                buf.put_i32(12); // Length including self
                buf.put_i32(*process_id);
                buf.put_i32(*secret_key);
            }

            _ => {
                // Not all messages need encoding (client->server)
            }
        }

        buf
    }
}

// Convenience constructors
impl Message {
    pub fn error(code: &str, message: &str) -> Self {
        let mut fields = HashMap::new();
        fields.insert(b'S', "ERROR".to_string());
        fields.insert(b'C', code.to_string());
        fields.insert(b'M', message.to_string());
        Message::ErrorResponse { fields }
    }

    #[allow(dead_code)]
    pub fn notice(message: &str) -> Self {
        let mut fields = HashMap::new();
        fields.insert(b'S', "NOTICE".to_string());
        fields.insert(b'M', message.to_string());
        Message::NoticeResponse { fields }
    }
}
