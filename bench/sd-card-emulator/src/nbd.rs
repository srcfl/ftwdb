use crate::control::{ConnectionRegistry, SharedCard};
use crate::model::{DeviceStatus, ErrorCode};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

const NBD_MAGIC: u64 = 0x4e42_444d_4147_4943;
const NBD_OPTIONS_MAGIC: u64 = 0x4948_4156_454f_5054;
const NBD_OPTION_REPLY_MAGIC: u64 = 0x0003_e889_0455_65a9;
const NBD_REQUEST_MAGIC: u32 = 0x2560_9513;
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x6744_6698;

const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1;
const NBD_FLAG_NO_ZEROES: u16 = 2;
const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1;
const NBD_FLAG_C_NO_ZEROES: u32 = 2;

const NBD_FLAG_HAS_FLAGS: u16 = 1;
const NBD_FLAG_READ_ONLY: u16 = 1 << 1;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_FUA: u16 = 1 << 3;

const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_LIST: u32 = 3;
const NBD_OPT_INFO: u32 = 6;
const NBD_OPT_GO: u32 = 7;

const NBD_REP_ACK: u32 = 1;
const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = (1 << 31) + 1;
const NBD_REP_ERR_INVALID: u32 = (1 << 31) + 3;
const NBD_REP_ERR_UNKNOWN: u32 = (1 << 31) + 6;

const NBD_INFO_EXPORT: u16 = 0;
const NBD_INFO_BLOCK_SIZE: u16 = 3;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_FLAG_FUA: u16 = 1;

const EXPORT_NAME: &[u8] = b"ftw-sd";
const MAX_OPTION_BYTES: usize = 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;

pub fn serve(
    address: &str,
    card: SharedCard,
    connections: Arc<ConnectionRegistry>,
    running: Arc<AtomicBool>,
) -> io::Result<()> {
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    eprintln!("NBD server listening on {address}, export ftw-sd");
    while running.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, peer)) => {
                stream.set_nodelay(true)?;
                connections.register(&stream)?;
                let card = Arc::clone(&card);
                thread::spawn(move || {
                    if let Err(error) = serve_connection(stream, card) {
                        eprintln!("NBD connection from {peer} failed: {error}");
                    }
                });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub fn serve_connection(mut stream: TcpStream, card: SharedCard) -> io::Result<()> {
    serve_stream(&mut stream, card)
}

fn serve_stream<S: Read + Write>(stream: &mut S, card: SharedCard) -> io::Result<()> {
    let Some(generation) = negotiate(&mut *stream, &card)? else {
        return Ok(());
    };
    transmission(stream, &card, generation)
}

fn negotiate<S: Read + Write>(stream: &mut S, card: &SharedCard) -> io::Result<Option<u64>> {
    write_u64(stream, NBD_MAGIC)?;
    write_u64(stream, NBD_OPTIONS_MAGIC)?;
    write_u16(stream, NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES)?;
    stream.flush()?;

    let client_flags = read_u32(stream)?;
    let known_flags = NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES;
    if client_flags & !known_flags != 0 || client_flags & NBD_FLAG_C_FIXED_NEWSTYLE == 0 {
        return Err(protocol_error("unsupported client handshake flags"));
    }
    let no_zeroes = client_flags & NBD_FLAG_C_NO_ZEROES != 0;

    loop {
        if read_u64(stream)? != NBD_OPTIONS_MAGIC {
            return Err(protocol_error("invalid NBD option magic"));
        }
        let option = read_u32(stream)?;
        let length = read_u32(stream)? as usize;
        if length > MAX_OPTION_BYTES {
            return Err(protocol_error("NBD option is too large"));
        }
        let mut data = vec![0; length];
        stream.read_exact(&mut data)?;

        match option {
            NBD_OPT_EXPORT_NAME => {
                if !valid_export_name(&data) {
                    return Err(protocol_error("unknown NBD export"));
                }
                let card = card.lock().expect("SD-card lock poisoned");
                write_u64(stream, card.size())?;
                write_u16(stream, transmission_flags(card.status()))?;
                if !no_zeroes {
                    stream.write_all(&[0; 124])?;
                }
                stream.flush()?;
                return Ok(Some(card.generation()));
            }
            NBD_OPT_ABORT => {
                if data.is_empty() {
                    option_reply(stream, option, NBD_REP_ACK, &[])?;
                } else {
                    option_reply(stream, option, NBD_REP_ERR_INVALID, b"ABORT has data")?;
                }
                return Ok(None);
            }
            NBD_OPT_LIST => {
                if !data.is_empty() {
                    option_reply(stream, option, NBD_REP_ERR_INVALID, b"LIST has data")?;
                    continue;
                }
                let mut payload = Vec::new();
                payload.extend_from_slice(&(EXPORT_NAME.len() as u32).to_be_bytes());
                payload.extend_from_slice(EXPORT_NAME);
                payload.extend_from_slice(b"FTW deterministic SD-card emulator");
                option_reply(stream, option, NBD_REP_SERVER, &payload)?;
                option_reply(stream, option, NBD_REP_ACK, &[])?;
            }
            NBD_OPT_INFO | NBD_OPT_GO => {
                let Some(request) = parse_info_request(&data) else {
                    option_reply(stream, option, NBD_REP_ERR_INVALID, b"invalid INFO/GO")?;
                    continue;
                };
                if !valid_export_name(request.name) {
                    option_reply(stream, option, NBD_REP_ERR_UNKNOWN, b"unknown export")?;
                    continue;
                }
                let card = card.lock().expect("SD-card lock poisoned");
                let mut export = Vec::with_capacity(12);
                export.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                export.extend_from_slice(&card.size().to_be_bytes());
                export.extend_from_slice(&transmission_flags(card.status()).to_be_bytes());
                option_reply(stream, option, NBD_REP_INFO, &export)?;
                if request.information.contains(&NBD_INFO_BLOCK_SIZE) {
                    let logical = u32::try_from(card.logical_block_bytes()).unwrap_or(4096);
                    let mut block_sizes = Vec::with_capacity(14);
                    block_sizes.extend_from_slice(&NBD_INFO_BLOCK_SIZE.to_be_bytes());
                    block_sizes.extend_from_slice(&512_u32.to_be_bytes());
                    block_sizes.extend_from_slice(&logical.max(4096).to_be_bytes());
                    block_sizes.extend_from_slice(&(MAX_REQUEST_BYTES as u32).to_be_bytes());
                    option_reply(stream, option, NBD_REP_INFO, &block_sizes)?;
                }
                option_reply(stream, option, NBD_REP_ACK, &[])?;
                if option == NBD_OPT_GO {
                    return Ok(Some(card.generation()));
                }
            }
            _ => option_reply(stream, option, NBD_REP_ERR_UNSUP, b"unsupported option")?,
        }
    }
}

fn transmission<S: Read + Write>(
    stream: &mut S,
    card: &SharedCard,
    generation: u64,
) -> io::Result<()> {
    loop {
        {
            let card = card.lock().expect("SD-card lock poisoned");
            if card.generation() != generation || card.status() == DeviceStatus::Offline {
                return Ok(());
            }
        }
        let Some(header) = read_request_header(stream)? else {
            return Ok(());
        };
        if header.magic != NBD_REQUEST_MAGIC {
            return Err(protocol_error("invalid NBD request magic"));
        }
        if header.length > MAX_REQUEST_BYTES {
            return Err(protocol_error("NBD request is too large"));
        }
        let mut write_data = if header.command == NBD_CMD_WRITE {
            vec![0; header.length]
        } else {
            Vec::new()
        };
        if !write_data.is_empty() {
            stream.read_exact(&mut write_data)?;
        }

        if header.command == NBD_CMD_DISC {
            return Ok(());
        }
        let result = match header.command {
            NBD_CMD_READ if header.flags == 0 => {
                let mut card = card.lock().expect("SD-card lock poisoned");
                card.read(header.offset, header.length).map(Some)
            }
            NBD_CMD_WRITE if header.flags & !NBD_CMD_FLAG_FUA == 0 => {
                let mut card = card.lock().expect("SD-card lock poisoned");
                card.write(
                    header.offset,
                    write_data,
                    header.flags & NBD_CMD_FLAG_FUA != 0,
                )
                .map(|()| None)
            }
            NBD_CMD_FLUSH if header.flags == 0 && header.length == 0 => {
                let mut card = card.lock().expect("SD-card lock poisoned");
                card.flush().map(|()| None)
            }
            _ => Err(crate::model::DeviceError::from_code(
                ErrorCode::Invalid,
                "unsupported NBD command or flags",
            )),
        };
        match result {
            Ok(data) => simple_reply(stream, header.cookie, 0, data.as_deref())?,
            Err(error) if error.disconnects() => return Ok(()),
            Err(error) => simple_reply(stream, header.cookie, error.code.errno(), None)?,
        }
    }
}

fn transmission_flags(status: DeviceStatus) -> u16 {
    let mut flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_FUA;
    if status == DeviceStatus::ReadOnly {
        flags |= NBD_FLAG_READ_ONLY;
    }
    flags
}

fn valid_export_name(name: &[u8]) -> bool {
    name.is_empty() || name == EXPORT_NAME
}

struct InfoRequest<'a> {
    name: &'a [u8],
    information: Vec<u16>,
}

fn parse_info_request(data: &[u8]) -> Option<InfoRequest<'_>> {
    if data.len() < 6 {
        return None;
    }
    let name_length = u32::from_be_bytes(data[..4].try_into().ok()?) as usize;
    let count_offset = 4_usize.checked_add(name_length)?;
    let list_offset = count_offset.checked_add(2)?;
    if list_offset > data.len() {
        return None;
    }
    let count = u16::from_be_bytes(data[count_offset..list_offset].try_into().ok()?) as usize;
    let expected = list_offset.checked_add(count.checked_mul(2)?)?;
    if expected != data.len() {
        return None;
    }
    let mut information = Vec::with_capacity(count);
    for chunk in data[list_offset..].chunks_exact(2) {
        information.push(u16::from_be_bytes(chunk.try_into().ok()?));
    }
    Some(InfoRequest {
        name: &data[4..count_offset],
        information,
    })
}

fn option_reply<S: Write>(stream: &mut S, option: u32, kind: u32, data: &[u8]) -> io::Result<()> {
    write_u64(stream, NBD_OPTION_REPLY_MAGIC)?;
    write_u32(stream, option)?;
    write_u32(stream, kind)?;
    write_u32(stream, data.len() as u32)?;
    stream.write_all(data)?;
    stream.flush()
}

fn simple_reply<S: Write>(
    stream: &mut S,
    cookie: u64,
    error: u32,
    data: Option<&[u8]>,
) -> io::Result<()> {
    write_u32(stream, NBD_SIMPLE_REPLY_MAGIC)?;
    write_u32(stream, error)?;
    write_u64(stream, cookie)?;
    if error == 0
        && let Some(data) = data
    {
        stream.write_all(data)?;
    }
    stream.flush()
}

struct RequestHeader {
    magic: u32,
    flags: u16,
    command: u16,
    cookie: u64,
    offset: u64,
    length: usize,
}

fn read_request_header<S: Read>(stream: &mut S) -> io::Result<Option<RequestHeader>> {
    let mut bytes = [0; 28];
    match stream.read(&mut bytes[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!(),
        Err(error) if is_disconnect(&error) => return Ok(None),
        Err(error) => return Err(error),
    }
    if let Err(error) = stream.read_exact(&mut bytes[1..]) {
        if is_disconnect(&error) {
            return Ok(None);
        }
        return Err(error);
    }
    Ok(Some(RequestHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        flags: u16::from_be_bytes(bytes[4..6].try_into().unwrap()),
        command: u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
        cookie: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        offset: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        length: u32::from_be_bytes(bytes[24..28].try_into().unwrap()) as usize,
    }))
}

fn is_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

fn protocol_error(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_u32<S: Read>(stream: &mut S) -> io::Result<u32> {
    let mut bytes = [0; 4];
    stream.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64<S: Read>(stream: &mut S) -> io::Result<u64> {
    let mut bytes = [0; 8];
    stream.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

fn write_u16<S: Write>(stream: &mut S, value: u16) -> io::Result<()> {
    stream.write_all(&value.to_be_bytes())
}

fn write_u32<S: Write>(stream: &mut S, value: u32) -> io::Result<()> {
    stream.write_all(&value.to_be_bytes())
}

fn write_u64<S: Write>(stream: &mut S, value: u64) -> io::Result<()> {
    stream.write_all(&value.to_be_bytes())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::config::{
        CacheProfile, DeviceProfile, FaultProfile, IoProfile, Profile, WearProfile,
    };
    use crate::model::SdCard;
    use std::fs;
    use std::os::unix::net::UnixStream;
    use std::sync::{Arc, Mutex};

    #[test]
    fn fixed_newstyle_client_can_write_flush_and_read() {
        let path = std::env::temp_dir().join(format!("ftw-sd-nbd-test-{}", std::process::id()));
        let card = Arc::new(Mutex::new(SdCard::open(&path, profile(), 42).unwrap()));
        let (mut server_stream, mut client) = UnixStream::pair().unwrap();
        let server_card = Arc::clone(&card);
        let server = thread::spawn(move || {
            serve_stream(&mut server_stream, server_card).unwrap();
        });

        assert_eq!(read_u64(&mut client).unwrap(), NBD_MAGIC);
        assert_eq!(read_u64(&mut client).unwrap(), NBD_OPTIONS_MAGIC);
        let mut flags = [0; 2];
        client.read_exact(&mut flags).unwrap();
        write_u32(
            &mut client,
            NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES,
        )
        .unwrap();
        write_u64(&mut client, NBD_OPTIONS_MAGIC).unwrap();
        write_u32(&mut client, NBD_OPT_EXPORT_NAME).unwrap();
        write_u32(&mut client, EXPORT_NAME.len() as u32).unwrap();
        client.write_all(EXPORT_NAME).unwrap();
        assert_eq!(read_u64(&mut client).unwrap(), 64 * 1024);
        client.read_exact(&mut flags).unwrap();

        send_request(&mut client, 1, NBD_CMD_WRITE, 0, 4096, &[1, 2, 3, 4]);
        read_success(&mut client, 1);
        send_request(&mut client, 2, NBD_CMD_FLUSH, 0, 0, &[]);
        read_success(&mut client, 2);
        send_request(&mut client, 3, NBD_CMD_READ, 0, 4096, &[0; 4]);
        read_success(&mut client, 3);
        let mut data = [0; 4];
        client.read_exact(&mut data).unwrap();
        assert_eq!(data, [1, 2, 3, 4]);
        send_request(&mut client, 4, NBD_CMD_DISC, 0, 0, &[]);
        drop(client);
        server.join().unwrap();
        fs::remove_file(path).unwrap();
    }

    fn send_request(
        stream: &mut impl Write,
        cookie: u64,
        command: u16,
        flags: u16,
        offset: u64,
        data: &[u8],
    ) {
        write_u32(stream, NBD_REQUEST_MAGIC).unwrap();
        write_u16(stream, flags).unwrap();
        write_u16(stream, command).unwrap();
        write_u64(stream, cookie).unwrap();
        write_u64(stream, offset).unwrap();
        write_u32(stream, data.len() as u32).unwrap();
        if command == NBD_CMD_WRITE {
            stream.write_all(data).unwrap();
        }
    }

    fn read_success(stream: &mut impl Read, cookie: u64) {
        assert_eq!(read_u32(stream).unwrap(), NBD_SIMPLE_REPLY_MAGIC);
        assert_eq!(read_u32(stream).unwrap(), 0);
        assert_eq!(read_u64(stream).unwrap(), cookie);
    }

    fn profile() -> Profile {
        let io = IoProfile {
            bandwidth_bytes_per_second: 0,
            iops: 0,
            base_latency_us: 0,
            jitter_latency_us: 0,
            spike_probability_ppm: 0,
            spike_latency_us_min: 0,
            spike_latency_us_max: 0,
        };
        Profile {
            schema_version: 1,
            name: "nbd-test".to_owned(),
            device: DeviceProfile {
                size_bytes: 64 * 1024,
                logical_block_bytes: 512,
                erase_block_bytes: 4096,
            },
            read: io.clone(),
            write: io,
            cache: CacheProfile {
                enabled: true,
                max_bytes: 4096,
                false_flush_probability_ppm: 0,
                power_loss_persist_probability_ppm: 0,
                power_loss_torn_probability_ppm: 0,
                power_loss_reorder_probability_ppm: 0,
            },
            wear: WearProfile {
                enabled: false,
                accelerated_factor: 1,
                warning_cycles: 100,
                failure_cycles: 200,
                worn_eio_probability_ppm: 0,
                read_only_after_bad_blocks: 0,
            },
            faults: FaultProfile {
                eio_probability_ppm: 0,
                torn_write_probability_ppm: 0,
                silent_corruption_probability_ppm: 0,
                power_loss_after_ops: None,
                read_only_after_ops: None,
                disappear_after_ops: None,
            },
        }
    }
}
