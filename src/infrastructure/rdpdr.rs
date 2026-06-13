use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CHANNEL_FLAG_FIRST: u32 = 0x0000_0001;
const CHANNEL_FLAG_LAST: u32 = 0x0000_0002;
const RDPDR_CTYP_CORE: u16 = 0x4472;
const PAKID_CORE_SERVER_ANNOUNCE: u16 = 0x496e;
const PAKID_CORE_CLIENTID_CONFIRM: u16 = 0x4343;
const PAKID_CORE_CLIENT_NAME: u16 = 0x434e;
const PAKID_CORE_DEVICELIST_ANNOUNCE: u16 = 0x4441;
const PAKID_CORE_DEVICE_REPLY: u16 = 0x6472;
const PAKID_CORE_DEVICE_IOREQUEST: u16 = 0x4952;
const PAKID_CORE_DEVICE_IOCOMPLETION: u16 = 0x4943;
const PAKID_CORE_SERVER_CAPABILITY: u16 = 0x5350;
const PAKID_CORE_CLIENT_CAPABILITY: u16 = 0x4350;
const PAKID_CORE_USER_LOGGEDON: u16 = 0x554c;

const RDPDR_DTYP_FILESYSTEM: u32 = 0x0000_0008;
const DEVICE_ID: u32 = 1;
const MAX_IO_SIZE: usize = 1024 * 1024;

const IRP_MJ_CREATE: u32 = 0x00;
const IRP_MJ_CLOSE: u32 = 0x02;
const IRP_MJ_READ: u32 = 0x03;
const IRP_MJ_WRITE: u32 = 0x04;
const IRP_MJ_QUERY_INFORMATION: u32 = 0x05;
const IRP_MJ_SET_INFORMATION: u32 = 0x06;
const IRP_MJ_QUERY_VOLUME_INFORMATION: u32 = 0x0a;
const IRP_MJ_DIRECTORY_CONTROL: u32 = 0x0c;
const IRP_MJ_DEVICE_CONTROL: u32 = 0x0e;
const IRP_MJ_LOCK_CONTROL: u32 = 0x11;
const IRP_MN_QUERY_DIRECTORY: u32 = 0x01;

const STATUS_SUCCESS: u32 = 0x0000_0000;
const STATUS_NO_MORE_FILES: u32 = 0x8000_0006;
const STATUS_UNSUCCESSFUL: u32 = 0xc000_0001;
const STATUS_INVALID_PARAMETER: u32 = 0xc000_000d;
const STATUS_NO_SUCH_FILE: u32 = 0xc000_000f;
const STATUS_ACCESS_DENIED: u32 = 0xc000_0022;
const STATUS_OBJECT_NAME_COLLISION: u32 = 0xc000_0035;
const STATUS_NOT_SUPPORTED: u32 = 0xc000_00bb;
const STATUS_NOT_A_DIRECTORY: u32 = 0xc000_0103;
const STATUS_DIRECTORY_NOT_EMPTY: u32 = 0xc000_0101;

const FILE_DIRECTORY_INFORMATION: u32 = 1;
const FILE_FULL_DIRECTORY_INFORMATION: u32 = 2;
const FILE_BOTH_DIRECTORY_INFORMATION: u32 = 3;
const FILE_BASIC_INFORMATION: u32 = 4;
const FILE_STANDARD_INFORMATION: u32 = 5;
const FILE_RENAME_INFORMATION: u32 = 10;
const FILE_NAMES_INFORMATION: u32 = 12;
const FILE_DISPOSITION_INFORMATION: u32 = 13;
const FILE_ALLOCATION_INFORMATION: u32 = 19;
const FILE_END_OF_FILE_INFORMATION: u32 = 20;
const FILE_ATTRIBUTE_TAG_INFORMATION: u32 = 35;

const FILE_SUPERSEDE: u32 = 0;
const FILE_OPEN: u32 = 1;
const FILE_CREATE: u32 = 2;
const FILE_OPEN_IF: u32 = 3;
const FILE_OVERWRITE: u32 = 4;
const FILE_OVERWRITE_IF: u32 = 5;
const FILE_DIRECTORY_FILE: u32 = 0x0000_0001;
const FILE_NON_DIRECTORY_FILE: u32 = 0x0000_0040;
const FILE_DELETE_ON_CLOSE: u32 = 0x0000_1000;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
const FILE_ATTRIBUTE_NORMAL: u32 = 0x0000_0080;
const GENERIC_WRITE: u32 = 0x4000_0000;
const FILE_WRITE_DATA: u32 = 0x0000_0002;
const FILE_APPEND_DATA: u32 = 0x0000_0004;

#[derive(Default)]
pub(crate) struct RdpdrProcessResult {
    pub messages: Vec<Vec<u8>>,
}

pub(crate) struct RdpdrState {
    root: PathBuf,
    name: String,
    version_major: u16,
    version_minor: u16,
    client_id: u32,
    have_server_caps: bool,
    have_client_id: bool,
    announced: bool,
    fragment_data: Vec<u8>,
    fragment_total_length: usize,
    handles: HashMap<u32, DriveHandle>,
    next_file_id: u32,
    debug: bool,
}

struct DriveHandle {
    path: PathBuf,
    file: Option<File>,
    directory_entries: Vec<DirectoryEntry>,
    directory_index: usize,
    delete_on_close: bool,
}

struct DirectoryEntry {
    name: String,
    metadata: fs::Metadata,
}

impl RdpdrState {
    pub(crate) fn new(root: PathBuf, name: String, debug: bool) -> std::io::Result<Self> {
        Ok(Self {
            root: root.canonicalize()?,
            name,
            version_major: 1,
            version_minor: 0x000c,
            client_id: 0,
            have_server_caps: false,
            have_client_id: false,
            announced: false,
            fragment_data: Vec::new(),
            fragment_total_length: 0,
            handles: HashMap::new(),
            next_file_id: 1,
            debug,
        })
    }

    pub(crate) fn process_channel_payload(&mut self, payload: &[u8]) -> RdpdrProcessResult {
        let mut result = RdpdrProcessResult::default();
        if payload.len() < 8 {
            return result;
        }
        let total_length = read_u32(payload, 0).unwrap_or(0) as usize;
        let flags = read_u32(payload, 4).unwrap_or(0);
        if flags & CHANNEL_FLAG_FIRST != 0 {
            self.fragment_data.clear();
            self.fragment_total_length = total_length.min(MAX_IO_SIZE * 2);
        }
        if self.fragment_data.len().saturating_add(payload.len() - 8) > MAX_IO_SIZE * 2 {
            self.fragment_data.clear();
            self.fragment_total_length = 0;
            return result;
        }
        self.fragment_data.extend_from_slice(&payload[8..]);
        if flags & CHANNEL_FLAG_LAST == 0 {
            return result;
        }

        let length = self.fragment_total_length.min(self.fragment_data.len());
        let message = self.fragment_data[..length].to_vec();
        self.fragment_data.clear();
        self.fragment_total_length = 0;
        if let Some(response) = self.process_message(&message) {
            result.messages.extend(response);
        }
        result
    }

    fn process_message(&mut self, message: &[u8]) -> Option<Vec<Vec<u8>>> {
        if message.len() < 4 || read_u16(message, 0)? != RDPDR_CTYP_CORE {
            return None;
        }
        let packet_id = read_u16(message, 2)?;
        if self.debug {
            eprintln!(
                "RDP DEBUG drive: packet=0x{packet_id:04x} data={}B",
                message.len()
            );
        }
        let mut responses = Vec::new();
        match packet_id {
            PAKID_CORE_SERVER_ANNOUNCE => {
                self.version_major = read_u16(message, 4)?;
                self.version_minor = read_u16(message, 6)?.min(0x000c);
                self.client_id = read_u32(message, 8)?;
                self.have_server_caps = false;
                self.have_client_id = false;
                self.announced = false;
                self.handles.clear();
                responses.push(self.client_id_confirm());
                responses.push(self.client_name());
            }
            PAKID_CORE_SERVER_CAPABILITY => {
                self.have_server_caps = true;
                responses.push(self.client_capabilities());
                if self.have_client_id && !self.announced {
                    responses.push(self.device_announce());
                    self.announced = true;
                }
            }
            PAKID_CORE_CLIENTID_CONFIRM => {
                self.version_major = read_u16(message, 4).unwrap_or(self.version_major);
                self.version_minor = read_u16(message, 6).unwrap_or(self.version_minor);
                self.client_id = read_u32(message, 8).unwrap_or(self.client_id);
                self.have_client_id = true;
                if self.have_server_caps && !self.announced {
                    responses.push(self.device_announce());
                    self.announced = true;
                }
            }
            PAKID_CORE_USER_LOGGEDON => {
                responses.push(self.device_announce());
                self.announced = true;
            }
            PAKID_CORE_DEVICE_REPLY => {
                if self.debug {
                    let status = read_u32(message, 8).unwrap_or(STATUS_UNSUCCESSFUL);
                    eprintln!("RDP DEBUG drive: device reply status=0x{status:08x}");
                }
            }
            PAKID_CORE_DEVICE_IOREQUEST => responses.push(self.process_irp(message)),
            _ => {}
        }
        Some(responses)
    }

    fn client_id_confirm(&self) -> Vec<u8> {
        let mut out = core_header(PAKID_CORE_CLIENTID_CONFIRM);
        put_u16(&mut out, self.version_major);
        put_u16(&mut out, self.version_minor);
        put_u32(&mut out, self.client_id);
        out
    }

    fn client_name(&self) -> Vec<u8> {
        let encoded = utf16_bytes("PORTIX", true);
        let mut out = core_header(PAKID_CORE_CLIENT_NAME);
        put_u32(&mut out, 1);
        put_u32(&mut out, 0);
        put_u32(&mut out, encoded.len() as u32);
        out.extend_from_slice(&encoded);
        out
    }

    fn client_capabilities(&self) -> Vec<u8> {
        let mut out = core_header(PAKID_CORE_CLIENT_CAPABILITY);
        put_u16(&mut out, 2);
        put_u16(&mut out, 0);
        put_u16(&mut out, 1); // CAP_GENERAL_TYPE
        put_u16(&mut out, 44);
        put_u32(&mut out, 2); // GENERAL_CAPABILITY_VERSION_02
        put_u32(&mut out, 0); // osType
        put_u32(&mut out, 0); // osVersion
        put_u16(&mut out, self.version_major);
        put_u16(&mut out, self.version_minor);
        put_u32(&mut out, 0x0000_3fff); // supported IRP major functions
        put_u32(&mut out, 0);
        put_u32(&mut out, 0x0000_0007); // remove, display name, user logged-on
        put_u32(&mut out, 0);
        put_u32(&mut out, 0);
        put_u32(&mut out, 0);
        put_u16(&mut out, 4); // CAP_DRIVE_TYPE
        put_u16(&mut out, 8);
        put_u32(&mut out, 2); // DRIVE_CAPABILITY_VERSION_02
        out
    }

    fn device_announce(&self) -> Vec<u8> {
        let mut dos_name = [0u8; 8];
        let name = self.name.as_bytes();
        dos_name[..name.len().min(7)].copy_from_slice(&name[..name.len().min(7)]);
        let mut device_data = self.name.as_bytes().to_vec();
        device_data.push(0);

        let mut out = core_header(PAKID_CORE_DEVICELIST_ANNOUNCE);
        put_u32(&mut out, 1);
        put_u32(&mut out, RDPDR_DTYP_FILESYSTEM);
        put_u32(&mut out, DEVICE_ID);
        out.extend_from_slice(&dos_name);
        put_u32(&mut out, device_data.len() as u32);
        out.extend_from_slice(&device_data);
        out
    }

    fn process_irp(&mut self, message: &[u8]) -> Vec<u8> {
        if message.len() < 24 {
            return io_completion(DEVICE_ID, 0, STATUS_INVALID_PARAMETER, &[]);
        }
        let device_id = read_u32(message, 4).unwrap_or(DEVICE_ID);
        let file_id = read_u32(message, 8).unwrap_or(0);
        let completion_id = read_u32(message, 12).unwrap_or(0);
        let major = read_u32(message, 16).unwrap_or(u32::MAX);
        let minor = read_u32(message, 20).unwrap_or(0);
        let input = &message[24..];
        if device_id != DEVICE_ID {
            return io_completion(device_id, completion_id, STATUS_NO_SUCH_FILE, &[]);
        }

        let (status, data) = match major {
            IRP_MJ_CREATE => self.create(input),
            IRP_MJ_CLOSE => self.close(file_id),
            IRP_MJ_READ => self.read(file_id, input),
            IRP_MJ_WRITE => self.write(file_id, input),
            IRP_MJ_QUERY_INFORMATION => self.query_information(file_id, input),
            IRP_MJ_SET_INFORMATION => self.set_information(file_id, input),
            IRP_MJ_QUERY_VOLUME_INFORMATION => self.query_volume(input),
            IRP_MJ_DIRECTORY_CONTROL if minor == IRP_MN_QUERY_DIRECTORY => {
                self.query_directory(file_id, input)
            }
            IRP_MJ_DEVICE_CONTROL | IRP_MJ_LOCK_CONTROL => {
                (STATUS_SUCCESS, 0u32.to_le_bytes().to_vec())
            }
            _ => (STATUS_NOT_SUPPORTED, Vec::new()),
        };
        if self.debug {
            eprintln!(
                "RDP DEBUG drive: IRP major=0x{major:02x} file={file_id} status=0x{status:08x} response={}B",
                data.len()
            );
        }
        io_completion(device_id, completion_id, status, &data)
    }

    fn create(&mut self, input: &[u8]) -> (u32, Vec<u8>) {
        if input.len() < 32 {
            return create_response(STATUS_INVALID_PARAMETER, 0, 0);
        }
        let desired_access = read_u32(input, 0).unwrap_or(0);
        let allocation_size = read_u64(input, 4).unwrap_or(0);
        let disposition = read_u32(input, 20).unwrap_or(FILE_OPEN);
        let options = read_u32(input, 24).unwrap_or(0);
        let path_length = read_u32(input, 28).unwrap_or(0) as usize;
        if path_length > MAX_IO_SIZE || 32 + path_length > input.len() {
            return create_response(STATUS_INVALID_PARAMETER, 0, 0);
        }
        let Some(remote_path) = decode_utf16(&input[32..32 + path_length]) else {
            return create_response(STATUS_INVALID_PARAMETER, 0, 0);
        };
        let target = match self.resolve_path(&remote_path, disposition != FILE_OPEN) {
            Ok(path) => path,
            Err(status) => return create_response(status, 0, 0),
        };
        let wants_directory = options & FILE_DIRECTORY_FILE != 0;
        let wants_file = options & FILE_NON_DIRECTORY_FILE != 0;
        let existed = target.exists();
        if existed && wants_directory && !target.is_dir() {
            return create_response(STATUS_NOT_A_DIRECTORY, 0, 0);
        }
        if existed && wants_file && target.is_dir() {
            return create_response(STATUS_ACCESS_DENIED, 0, 0);
        }

        let result = if wants_directory || (existed && target.is_dir()) {
            match disposition {
                FILE_CREATE if existed => Err(STATUS_OBJECT_NAME_COLLISION),
                FILE_OPEN | FILE_OVERWRITE if !existed => Err(STATUS_NO_SUCH_FILE),
                _ if !existed => fs::create_dir(&target).map_err(|error| map_io_error(&error)),
                _ => Ok(()),
            }
            .map(|_| None)
        } else {
            self.open_file(&target, desired_access, disposition)
                .map(Some)
        };
        let file = match result {
            Ok(file) => file,
            Err(status) => return create_response(status, 0, 0),
        };
        if allocation_size > 0 {
            if let Some(file) = &file {
                if let Err(error) = file.set_len(allocation_size) {
                    return create_response(map_io_error(&error), 0, 0);
                }
            }
        }

        let file_id = self.next_file_id;
        self.next_file_id = self.next_file_id.wrapping_add(1).max(1);
        self.handles.insert(
            file_id,
            DriveHandle {
                path: target,
                file,
                directory_entries: Vec::new(),
                directory_index: 0,
                delete_on_close: options & FILE_DELETE_ON_CLOSE != 0,
            },
        );
        let information = if existed { 1 } else { 2 };
        create_response(STATUS_SUCCESS, file_id, information)
    }

    fn open_file(&self, path: &Path, desired_access: u32, disposition: u32) -> Result<File, u32> {
        let existed = path.exists();
        if disposition == FILE_CREATE && existed {
            return Err(STATUS_OBJECT_NAME_COLLISION);
        }
        if matches!(disposition, FILE_OPEN | FILE_OVERWRITE) && !existed {
            return Err(STATUS_NO_SUCH_FILE);
        }
        let writable = desired_access & (GENERIC_WRITE | FILE_WRITE_DATA | FILE_APPEND_DATA) != 0
            || matches!(
                disposition,
                FILE_SUPERSEDE | FILE_CREATE | FILE_OPEN_IF | FILE_OVERWRITE | FILE_OVERWRITE_IF
            );
        let mut options = OpenOptions::new();
        options.read(true).write(writable);
        match disposition {
            FILE_SUPERSEDE => {
                options.create(true).truncate(true);
            }
            FILE_CREATE => {
                options.create_new(true);
            }
            FILE_OPEN_IF => {
                options.create(true);
            }
            FILE_OVERWRITE => {
                options.truncate(true);
            }
            FILE_OVERWRITE_IF => {
                options.create(true).truncate(true);
            }
            _ => {}
        }
        options.open(path).map_err(|error| map_io_error(&error))
    }

    fn close(&mut self, file_id: u32) -> (u32, Vec<u8>) {
        let Some(handle) = self.handles.remove(&file_id) else {
            return (STATUS_NO_SUCH_FILE, vec![0; 5]);
        };
        drop(handle.file);
        if handle.delete_on_close {
            let result = if handle.path.is_dir() {
                fs::remove_dir(&handle.path)
            } else {
                fs::remove_file(&handle.path)
            };
            if let Err(error) = result {
                return (map_io_error(&error), vec![0; 5]);
            }
        }
        (STATUS_SUCCESS, vec![0; 5])
    }

    fn read(&mut self, file_id: u32, input: &[u8]) -> (u32, Vec<u8>) {
        let Some(length) = read_u32(input, 0).map(|value| value as usize) else {
            return length_response(STATUS_INVALID_PARAMETER, &[]);
        };
        let Some(offset) = read_u64(input, 4) else {
            return length_response(STATUS_INVALID_PARAMETER, &[]);
        };
        if length > MAX_IO_SIZE {
            return length_response(STATUS_INVALID_PARAMETER, &[]);
        }
        let Some(file) = self
            .handles
            .get_mut(&file_id)
            .and_then(|handle| handle.file.as_mut())
        else {
            return length_response(STATUS_NO_SUCH_FILE, &[]);
        };
        if let Err(error) = file.seek(SeekFrom::Start(offset)) {
            return length_response(map_io_error(&error), &[]);
        }
        let mut bytes = vec![0; length];
        match file.read(&mut bytes) {
            Ok(read) => {
                bytes.truncate(read);
                length_response(STATUS_SUCCESS, &bytes)
            }
            Err(error) => length_response(map_io_error(&error), &[]),
        }
    }

    fn write(&mut self, file_id: u32, input: &[u8]) -> (u32, Vec<u8>) {
        let Some(length) = read_u32(input, 0).map(|value| value as usize) else {
            return write_response(STATUS_INVALID_PARAMETER, 0);
        };
        let Some(offset) = read_u64(input, 4) else {
            return write_response(STATUS_INVALID_PARAMETER, 0);
        };
        if length > MAX_IO_SIZE || input.len() < 32 + length {
            return write_response(STATUS_INVALID_PARAMETER, 0);
        }
        let Some(file) = self
            .handles
            .get_mut(&file_id)
            .and_then(|handle| handle.file.as_mut())
        else {
            return write_response(STATUS_NO_SUCH_FILE, 0);
        };
        if let Err(error) = file.seek(SeekFrom::Start(offset)) {
            return write_response(map_io_error(&error), 0);
        }
        match file.write_all(&input[32..32 + length]) {
            Ok(()) => write_response(STATUS_SUCCESS, length as u32),
            Err(error) => write_response(map_io_error(&error), 0),
        }
    }

    fn query_information(&self, file_id: u32, input: &[u8]) -> (u32, Vec<u8>) {
        let Some(class) = read_u32(input, 0) else {
            return length_response(STATUS_INVALID_PARAMETER, &[]);
        };
        let Some(handle) = self.handles.get(&file_id) else {
            return length_response(STATUS_NO_SUCH_FILE, &[]);
        };
        let metadata = match fs::metadata(&handle.path) {
            Ok(metadata) => metadata,
            Err(error) => return length_response(map_io_error(&error), &[]),
        };
        let attributes = file_attributes(&metadata);
        let size = metadata.len();
        let mut data = Vec::new();
        match class {
            FILE_BASIC_INFORMATION => {
                put_u64(&mut data, file_time(metadata.created().ok()));
                put_u64(&mut data, file_time(metadata.accessed().ok()));
                put_u64(&mut data, file_time(metadata.modified().ok()));
                put_u64(&mut data, file_time(metadata.modified().ok()));
                put_u32(&mut data, attributes);
            }
            FILE_STANDARD_INFORMATION => {
                put_u64(&mut data, size);
                put_u64(&mut data, size);
                put_u32(&mut data, 1);
                data.push(handle.delete_on_close as u8);
                data.push(metadata.is_dir() as u8);
            }
            FILE_ATTRIBUTE_TAG_INFORMATION => {
                put_u32(&mut data, attributes);
                put_u32(&mut data, 0);
            }
            _ => return length_response(STATUS_NOT_SUPPORTED, &[]),
        }
        length_response(STATUS_SUCCESS, &data)
    }

    fn set_information(&mut self, file_id: u32, input: &[u8]) -> (u32, Vec<u8>) {
        if input.len() < 32 {
            return set_response(STATUS_INVALID_PARAMETER, 0);
        }
        let class = read_u32(input, 0).unwrap_or(0);
        let length = read_u32(input, 4).unwrap_or(0) as usize;
        if length > MAX_IO_SIZE || input.len() < 32 + length {
            return set_response(STATUS_INVALID_PARAMETER, 0);
        }
        let data = &input[32..32 + length];
        let root = self.root.clone();
        let Some(handle) = self.handles.get_mut(&file_id) else {
            return set_response(STATUS_NO_SUCH_FILE, 0);
        };
        let status = match class {
            FILE_END_OF_FILE_INFORMATION | FILE_ALLOCATION_INFORMATION => {
                let Some(size) = read_u64(data, 0) else {
                    return set_response(STATUS_INVALID_PARAMETER, 0);
                };
                match handle
                    .file
                    .as_ref()
                    .ok_or(STATUS_ACCESS_DENIED)
                    .and_then(|file| file.set_len(size).map_err(|error| map_io_error(&error)))
                {
                    Ok(()) => STATUS_SUCCESS,
                    Err(status) => status,
                }
            }
            FILE_DISPOSITION_INFORMATION => {
                handle.delete_on_close = data.first().copied().unwrap_or(1) != 0;
                STATUS_SUCCESS
            }
            FILE_RENAME_INFORMATION => {
                if data.len() < 6 {
                    STATUS_INVALID_PARAMETER
                } else {
                    let replace = data[0] != 0;
                    let path_length = read_u32(data, 2).unwrap_or(0) as usize;
                    if path_length > MAX_IO_SIZE || 6 + path_length > data.len() {
                        STATUS_INVALID_PARAMETER
                    } else if let Some(remote_path) = decode_utf16(&data[6..6 + path_length]) {
                        match resolve_mapped_path(&root, &remote_path, true) {
                            Ok(target) if !replace && target.exists() => {
                                STATUS_OBJECT_NAME_COLLISION
                            }
                            Ok(target) => match fs::rename(&handle.path, &target) {
                                Ok(()) => {
                                    handle.path = target;
                                    STATUS_SUCCESS
                                }
                                Err(error) => map_io_error(&error),
                            },
                            Err(status) => status,
                        }
                    } else {
                        STATUS_INVALID_PARAMETER
                    }
                }
            }
            FILE_BASIC_INFORMATION => STATUS_SUCCESS,
            _ => STATUS_NOT_SUPPORTED,
        };
        set_response(status, length as u32)
    }

    fn query_volume(&self, input: &[u8]) -> (u32, Vec<u8>) {
        let Some(class) = read_u32(input, 0) else {
            return length_response(STATUS_INVALID_PARAMETER, &[]);
        };
        let mut data = Vec::new();
        match class {
            1 => {
                let label = utf16_bytes(&self.name, true);
                put_u64(&mut data, 0);
                put_u32(&mut data, 0x5054_5852);
                put_u32(&mut data, label.len() as u32);
                data.push(0);
                data.extend_from_slice(&label);
            }
            3 => {
                put_u64(&mut data, 262_144);
                put_u64(&mut data, 131_072);
                put_u32(&mut data, 8);
                put_u32(&mut data, 512);
            }
            4 => {
                put_u32(&mut data, 7);
                put_u32(&mut data, 0x20);
            }
            5 => {
                let fs_name = utf16_bytes("PORTIXFS", true);
                put_u32(&mut data, 0x0000_0007);
                put_u32(&mut data, 255);
                put_u32(&mut data, fs_name.len() as u32);
                data.extend_from_slice(&fs_name);
            }
            7 => {
                put_u64(&mut data, 262_144);
                put_u64(&mut data, 131_072);
                put_u64(&mut data, 131_072);
                put_u32(&mut data, 8);
                put_u32(&mut data, 512);
            }
            _ => return length_response(STATUS_NOT_SUPPORTED, &[]),
        }
        length_response(STATUS_SUCCESS, &data)
    }

    fn query_directory(&mut self, file_id: u32, input: &[u8]) -> (u32, Vec<u8>) {
        if input.len() < 32 {
            return directory_response(STATUS_INVALID_PARAMETER, &[]);
        }
        let class = read_u32(input, 0).unwrap_or(0);
        let initial = input[4] != 0;
        let path_length = read_u32(input, 5).unwrap_or(0) as usize;
        if path_length > MAX_IO_SIZE || 32 + path_length > input.len() {
            return directory_response(STATUS_INVALID_PARAMETER, &[]);
        }
        let pattern = decode_utf16(&input[32..32 + path_length]).unwrap_or_else(|| "*".to_owned());
        let root = self.root.clone();
        let Some(handle) = self.handles.get_mut(&file_id) else {
            return directory_response(STATUS_NO_SUCH_FILE, &[]);
        };
        if !handle.path.is_dir() {
            return directory_response(STATUS_NOT_A_DIRECTORY, &[]);
        }
        if initial {
            let (directory, file_pattern) = split_search_pattern(&handle.path, &pattern);
            let directory = match directory.canonicalize() {
                Ok(directory) if directory.starts_with(&root) => directory,
                Ok(_) => return directory_response(STATUS_ACCESS_DENIED, &[]),
                Err(error) => return directory_response(map_io_error(&error), &[]),
            };
            handle.directory_entries = match read_directory_entries(&directory, &file_pattern) {
                Ok(entries) => entries,
                Err(error) => return directory_response(map_io_error(&error), &[]),
            };
            handle.directory_index = 0;
        }
        let Some(entry) = handle.directory_entries.get(handle.directory_index) else {
            return directory_response(STATUS_NO_MORE_FILES, &[]);
        };
        handle.directory_index += 1;
        let name = utf16_bytes(&entry.name, false);
        let metadata = &entry.metadata;
        let attributes = file_attributes(metadata);
        let size = metadata.len();
        let mut data = Vec::new();
        put_u32(&mut data, 0); // NextEntryOffset
        put_u32(&mut data, 0); // FileIndex
        put_u64(&mut data, file_time(metadata.created().ok()));
        put_u64(&mut data, file_time(metadata.accessed().ok()));
        put_u64(&mut data, file_time(metadata.modified().ok()));
        put_u64(&mut data, file_time(metadata.modified().ok()));
        put_u64(&mut data, size);
        put_u64(&mut data, size);
        put_u32(&mut data, attributes);
        put_u32(&mut data, name.len() as u32);
        match class {
            FILE_DIRECTORY_INFORMATION => {}
            FILE_FULL_DIRECTORY_INFORMATION => put_u32(&mut data, 0),
            FILE_BOTH_DIRECTORY_INFORMATION => {
                put_u32(&mut data, 0);
                data.push(0);
                data.extend_from_slice(&[0; 24]);
            }
            FILE_NAMES_INFORMATION => {
                data.clear();
                put_u32(&mut data, 0);
                put_u32(&mut data, 0);
                put_u32(&mut data, name.len() as u32);
            }
            _ => return directory_response(STATUS_NOT_SUPPORTED, &[]),
        }
        data.extend_from_slice(&name);
        directory_response(STATUS_SUCCESS, &data)
    }

    fn resolve_path(&self, remote_path: &str, allow_missing: bool) -> Result<PathBuf, u32> {
        resolve_mapped_path(&self.root, remote_path, allow_missing)
    }
}

fn resolve_mapped_path(
    root: &Path,
    remote_path: &str,
    allow_missing: bool,
) -> Result<PathBuf, u32> {
    let normalized = remote_path.trim_matches('\0').replace('\\', "/");
    let mut relative = PathBuf::new();
    for component in Path::new(normalized.trim_start_matches('/')).components() {
        match component {
            Component::Normal(part) if part != "." => relative.push(part),
            Component::CurDir => {}
            _ => return Err(STATUS_ACCESS_DENIED),
        }
    }
    let target = root.join(relative);
    if target.exists() {
        let canonical = target
            .canonicalize()
            .map_err(|error| map_io_error(&error))?;
        if canonical.starts_with(root) {
            Ok(canonical)
        } else {
            Err(STATUS_ACCESS_DENIED)
        }
    } else if allow_missing {
        let parent = target.parent().unwrap_or(root);
        let canonical_parent = parent
            .canonicalize()
            .map_err(|error| map_io_error(&error))?;
        if canonical_parent.starts_with(root) {
            Ok(target)
        } else {
            Err(STATUS_ACCESS_DENIED)
        }
    } else {
        Err(STATUS_NO_SUCH_FILE)
    }
}

fn split_search_pattern(base: &Path, pattern: &str) -> (PathBuf, String) {
    let normalized = pattern.trim_matches('\0').replace('\\', "/");
    let relative = normalized.trim_start_matches('/');
    let path = Path::new(relative);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("*")
        .to_owned();
    let directory = path
        .parent()
        .map_or_else(|| base.to_owned(), |parent| base.join(parent));
    (directory, file_pattern)
}

fn read_directory_entries(directory: &Path, pattern: &str) -> std::io::Result<Vec<DirectoryEntry>> {
    let mut entries = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            wildcard_matches(pattern, &name).then(|| {
                entry
                    .metadata()
                    .ok()
                    .map(|metadata| DirectoryEntry { name, metadata })
            })?
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.name.to_lowercase().cmp(&right.name.to_lowercase()));
    Ok(entries)
}

fn wildcard_matches(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() || pattern == "*" || pattern == "*.*" {
        return true;
    }
    let pattern = pattern.to_lowercase();
    let name = name.to_lowercase();
    if let Some((prefix, suffix)) = pattern.split_once('*') {
        name.starts_with(prefix) && name.ends_with(suffix)
    } else {
        pattern == name
    }
}

fn file_attributes(metadata: &fs::Metadata) -> u32 {
    if metadata.is_dir() {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_NORMAL
    }
}

fn file_time(time: Option<SystemTime>) -> u64 {
    const WINDOWS_EPOCH_OFFSET: u64 = 11_644_473_600;
    time.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| {
            (duration.as_secs() + WINDOWS_EPOCH_OFFSET) * 10_000_000
                + u64::from(duration.subsec_nanos() / 100)
        })
        .unwrap_or(0)
}

fn map_io_error(error: &std::io::Error) -> u32 {
    match error.kind() {
        std::io::ErrorKind::NotFound => STATUS_NO_SUCH_FILE,
        std::io::ErrorKind::PermissionDenied => STATUS_ACCESS_DENIED,
        std::io::ErrorKind::AlreadyExists => STATUS_OBJECT_NAME_COLLISION,
        std::io::ErrorKind::DirectoryNotEmpty => STATUS_DIRECTORY_NOT_EMPTY,
        std::io::ErrorKind::InvalidInput => STATUS_INVALID_PARAMETER,
        _ => STATUS_UNSUCCESSFUL,
    }
}

fn create_response(status: u32, file_id: u32, information: u8) -> (u32, Vec<u8>) {
    let mut data = Vec::with_capacity(5);
    put_u32(&mut data, file_id);
    data.push(information);
    (status, data)
}

fn length_response(status: u32, data: &[u8]) -> (u32, Vec<u8>) {
    let mut output = Vec::with_capacity(4 + data.len());
    put_u32(&mut output, data.len() as u32);
    output.extend_from_slice(data);
    (status, output)
}

fn write_response(status: u32, length: u32) -> (u32, Vec<u8>) {
    let mut output = Vec::with_capacity(5);
    put_u32(&mut output, length);
    output.push(0);
    (status, output)
}

fn set_response(status: u32, length: u32) -> (u32, Vec<u8>) {
    (status, length.to_le_bytes().to_vec())
}

fn directory_response(status: u32, data: &[u8]) -> (u32, Vec<u8>) {
    let mut output = Vec::with_capacity(5 + data.len());
    put_u32(&mut output, data.len() as u32);
    output.extend_from_slice(data);
    if data.is_empty() {
        output.push(0);
    }
    (status, output)
}

fn core_header(packet_id: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    put_u16(&mut out, RDPDR_CTYP_CORE);
    put_u16(&mut out, packet_id);
    out
}

fn io_completion(device_id: u32, completion_id: u32, status: u32, data: &[u8]) -> Vec<u8> {
    let mut out = core_header(PAKID_CORE_DEVICE_IOCOMPLETION);
    put_u32(&mut out, device_id);
    put_u32(&mut out, completion_id);
    put_u32(&mut out, status);
    out.extend_from_slice(data);
    out
}

fn utf16_bytes(value: &str, null_terminated: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity((value.len() + usize::from(null_terminated)) * 2);
    for unit in value
        .encode_utf16()
        .chain(null_terminated.then_some(0).into_iter())
    {
        out.extend_from_slice(&unit.to_le_bytes());
    }
    out
}

fn decode_utf16(data: &[u8]) -> Option<String> {
    if data.len() % 2 != 0 {
        return None;
    }
    let units = data
        .chunks_exact(2)
        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
        .take_while(|unit| *unit != 0)
        .collect::<Vec<_>>();
    String::from_utf16(&units).ok()
}

fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        data.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_parent_path_escape() {
        let temp = tempfile::tempdir().unwrap();
        let state = RdpdrState::new(temp.path().to_owned(), "PORTIX".to_owned(), false).unwrap();
        assert_eq!(
            state.resolve_path("\\..\\secret", true),
            Err(STATUS_ACCESS_DENIED)
        );
    }

    #[test]
    fn creates_reads_and_writes_inside_mapped_root() {
        let temp = tempfile::tempdir().unwrap();
        let mut state =
            RdpdrState::new(temp.path().to_owned(), "PORTIX".to_owned(), false).unwrap();
        let path = utf16_bytes("\\hello.txt", true);
        let mut create = Vec::new();
        put_u32(&mut create, GENERIC_WRITE);
        put_u64(&mut create, 0);
        put_u32(&mut create, 0);
        put_u32(&mut create, 3);
        put_u32(&mut create, FILE_OPEN_IF);
        put_u32(&mut create, FILE_NON_DIRECTORY_FILE);
        put_u32(&mut create, path.len() as u32);
        create.extend_from_slice(&path);
        let (status, response) = state.create(&create);
        assert_eq!(status, STATUS_SUCCESS);
        let file_id = read_u32(&response, 0).unwrap();

        let mut write = Vec::new();
        put_u32(&mut write, 5);
        put_u64(&mut write, 0);
        write.extend_from_slice(&[0; 20]);
        write.extend_from_slice(b"hello");
        assert_eq!(state.write(file_id, &write).0, STATUS_SUCCESS);

        let mut read = Vec::new();
        put_u32(&mut read, 5);
        put_u64(&mut read, 0);
        assert_eq!(&state.read(file_id, &read).1[4..], b"hello");
    }

    #[test]
    fn device_announce_contains_drive_name() {
        let temp = tempfile::tempdir().unwrap();
        let state = RdpdrState::new(temp.path().to_owned(), "PORTIX".to_owned(), false).unwrap();
        let announce = state.device_announce();
        assert!(announce.windows(6).any(|bytes| bytes == b"PORTIX"));
    }

    #[test]
    fn directory_query_cannot_escape_mapped_root() {
        let temp = tempfile::tempdir().unwrap();
        let mut state =
            RdpdrState::new(temp.path().to_owned(), "PORTIX".to_owned(), false).unwrap();
        state.handles.insert(
            1,
            DriveHandle {
                path: temp.path().canonicalize().unwrap(),
                file: None,
                directory_entries: Vec::new(),
                directory_index: 0,
                delete_on_close: false,
            },
        );
        let pattern = utf16_bytes("\\..\\*", true);
        let mut input = Vec::new();
        put_u32(&mut input, FILE_DIRECTORY_INFORMATION);
        input.push(1);
        put_u32(&mut input, pattern.len() as u32);
        input.extend_from_slice(&[0; 23]);
        input.extend_from_slice(&pattern);
        assert_eq!(state.query_directory(1, &input).0, STATUS_ACCESS_DENIED);
    }
}
