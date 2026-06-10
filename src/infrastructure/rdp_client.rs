use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_rustls::rustls;

use crate::domain::rdp_profile::RdpProfile;

/// Commands that can be sent to an active RDP session.
#[derive(Debug)]
pub enum RdpCommand {
    /// Send keyboard input (scancode, is_pressed)
    KeyboardInput { scancode: u16, is_pressed: bool },
    /// Send mouse button input
    MouseInput {
        x: u16,
        y: u16,
        button: MouseButton,
        is_pressed: bool,
    },
    /// Send mouse move
    MouseMove { x: u16, y: u16 },
    /// Request current frame buffer as RGBA bytes
    RequestFrame {
        response_tx: oneshot::Sender<Vec<u8>>,
    },
    /// Disconnect
    Disconnect,
}

#[derive(Debug, Clone, Copy)]
pub enum MouseButton {
    Left,
    Right,
    Middle,
}

/// Event emitted when the RDP frame buffer is updated.
#[derive(Clone, Debug, serde::Serialize)]
pub struct RdpFrameEvent {
    pub session_id: String,
    pub width: u16,
    pub height: u16,
    /// RGBA pixel data for the full frame
    pub data: Vec<u8>,
}

/// RDP connection runtime. Handles the full lifecycle:
/// X.224 → TLS → MCS → Licensing → Capabilities → Active Session
pub struct RdpRuntime {
    pub session_id: String,
    pub profile: RdpProfile,
    pub frame_tx: broadcast::Sender<RdpFrameEvent>,
}

impl RdpRuntime {
    pub fn new(
        profile: RdpProfile,
        session_id: String,
        frame_tx: broadcast::Sender<RdpFrameEvent>,
    ) -> Self {
        Self {
            session_id,
            profile,
            frame_tx,
        }
    }

    pub async fn run(self, mut command_rx: mpsc::Receiver<RdpCommand>) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.profile.host, self.profile.port);
        let width = self.profile.width;
        let height = self.profile.height;

        // ─── TCP Connect ──────────────────────────────────────────────────
        let tcp_stream = TcpStream::connect(&addr).await.map_err(|e| {
            anyhow::anyhow!("Failed to connect to RDP server at {}: {}", addr, e)
        })?;

        // ─── X.224 Connection Request (request TLS) ───────────────────────
        let x224_cr = build_x224_connection_request(&self.profile);
        let mut stream = tcp_stream;
        stream.write_all(&x224_cr).await?;

        // ─── X.224 Connection Confirm ─────────────────────────────────────
        let mut resp_buf = vec![0u8; 8192];
        let n = stream.read(&mut resp_buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!(
                "RDP server closed connection during X.224 negotiation"
            ));
        }
        let selected_protocol = parse_x224_confirm(&resp_buf[..n])?;

        // ─── TLS Upgrade ──────────────────────────────────────────────────
        let mut tls_stream = if selected_protocol >= 1 {
            let tls_config = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();

            let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
            let server_name: rustls::pki_types::ServerName<'static> =
                self.profile.host.clone().try_into().unwrap_or_else(|_| {
                    let ip: std::net::IpAddr = self
                        .profile
                        .host
                        .parse()
                        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
                    rustls::pki_types::ServerName::IpAddress(ip.into())
                });

            let tls = connector.connect(server_name, stream).await?;
            RdpStream::Tls(tls)
        } else {
            RdpStream::Plain(stream)
        };

        // ─── MCS Connect Initial ─────────────────────────────────────────
        let mcs_ci = build_mcs_connect_initial(&self.profile);
        tls_stream.write_all(&mcs_ci).await?;

        let mut buf = vec![0u8; 16384];
        let n = tls_stream.read(&mut buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Server closed during MCS Connect"));
        }

        // ─── MCS Erect Domain + Attach User ──────────────────────────────
        tls_stream.write_all(&build_mcs_erect_domain()).await?;
        tls_stream.write_all(&build_mcs_attach_user()).await?;

        let n = tls_stream.read(&mut buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Server closed during Attach User"));
        }
        let user_channel_id = parse_attach_user_confirm(&buf[..n])?;

        // ─── Channel Joins ────────────────────────────────────────────────
        for channel in [user_channel_id, 1003u16] {
            tls_stream
                .write_all(&build_mcs_channel_join(user_channel_id, channel))
                .await?;
            let n = tls_stream.read(&mut buf).await?;
            if n == 0 {
                return Err(anyhow::anyhow!("Server closed during channel join"));
            }
        }

        // ─── Client Info PDU ──────────────────────────────────────────────
        let client_info = build_client_info_pdu(user_channel_id, &self.profile);
        tls_stream.write_all(&client_info).await?;

        // ─── Wait for Demand Active PDU ───────────────────────────────────
        let mut got_demand_active = false;
        for _ in 0..30 {
            let n = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                tls_stream.read(&mut buf),
            )
            .await??;
            if n == 0 {
                break;
            }
            if contains_demand_active(&buf[..n]) {
                got_demand_active = true;
                break;
            }
        }
        if !got_demand_active {
            return Err(anyhow::anyhow!(
                "Failed to receive Demand Active PDU. \
                 Ensure NLA is disabled on the RDP server."
            ));
        }

        // ─── Confirm Active + Synchronize sequence ────────────────────────
        let confirm_active = build_confirm_active_pdu(user_channel_id, width, height);
        tls_stream.write_all(&confirm_active).await?;

        let sync_seq = build_synchronize_sequence(user_channel_id);
        tls_stream.write_all(&sync_seq).await?;

        // ─── Frame buffer + initial frame ─────────────────────────────────
        let mut frame_buffer =
            vec![0u8; (width as usize) * (height as usize) * 4];

        let _ = self.frame_tx.send(RdpFrameEvent {
            session_id: self.session_id.clone(),
            width,
            height,
            data: frame_buffer.clone(),
        });

        // ─── Active Session Loop ─────────────────────────────────────────
        let mut read_buf = vec![0u8; 65536];
        loop {
            tokio::select! {
                read_result = tls_stream.read(&mut read_buf) => {
                    match read_result {
                        Ok(0) => return Ok(()),
                        Ok(n) => {
                            if let Some(updates) = extract_bitmap_updates(&read_buf[..n]) {
                                for update in updates {
                                    apply_bitmap_to_buffer(
                                        &mut frame_buffer, width, height, &update,
                                    );
                                }
                                let _ = self.frame_tx.send(RdpFrameEvent {
                                    session_id: self.session_id.clone(),
                                    width,
                                    height,
                                    data: frame_buffer.clone(),
                                });
                            }
                        }
                        Err(e) => return Err(anyhow::anyhow!("RDP read error: {}", e)),
                    }
                }

                cmd = command_rx.recv() => {
                    match cmd {
                        Some(RdpCommand::KeyboardInput { scancode, is_pressed }) => {
                            let pdu = build_keyboard_pdu(user_channel_id, scancode, is_pressed);
                            let _ = tls_stream.write_all(&pdu).await;
                        }
                        Some(RdpCommand::MouseMove { x, y }) => {
                            let pdu = build_mouse_move_pdu(user_channel_id, x, y);
                            let _ = tls_stream.write_all(&pdu).await;
                        }
                        Some(RdpCommand::MouseInput { x, y, button, is_pressed }) => {
                            let pdu = build_mouse_button_pdu(user_channel_id, x, y, button, is_pressed);
                            let _ = tls_stream.write_all(&pdu).await;
                        }
                        Some(RdpCommand::RequestFrame { response_tx }) => {
                            let _ = response_tx.send(frame_buffer.clone());
                        }
                        Some(RdpCommand::Disconnect) | None => {
                            let shutdown = build_shutdown_pdu(user_channel_id);
                            let _ = tls_stream.write_all(&shutdown).await;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

// ─── Stream Abstraction ──────────────────────────────────────────────────────

enum RdpStream {
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
    Plain(TcpStream),
}

impl RdpStream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Tls(s) => s.write_all(buf).await,
            Self::Plain(s) => s.write_all(buf).await,
        }
    }

    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Tls(s) => s.read(buf).await,
            Self::Plain(s) => s.read(buf).await,
        }
    }
}

// ─── TLS Certificate Verifier (accept any cert) ─────────────────────────────

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

// ─── RDP Protocol PDU Builders ───────────────────────────────────────────────

fn build_x224_connection_request(profile: &RdpProfile) -> Vec<u8> {
    let cookie = format!("Cookie: mstshash={}\r\n", profile.username);
    let cookie_bytes = cookie.as_bytes();
    // Negotiation Request: type=1, flags=0, length=8, protocols=TLS(0x01)
    let neg_req: [u8; 8] = [0x01, 0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00];
    let x224_len = 6 + cookie_bytes.len() + neg_req.len();
    let tpkt_len = 4 + x224_len;

    let mut pdu = Vec::with_capacity(tpkt_len);
    pdu.push(0x03);
    pdu.push(0x00);
    pdu.extend_from_slice(&(tpkt_len as u16).to_be_bytes());
    pdu.push((x224_len - 1) as u8);
    pdu.push(0xE0); // CR
    pdu.extend_from_slice(&[0x00, 0x00]); // DST-REF
    pdu.extend_from_slice(&[0x00, 0x00]); // SRC-REF
    pdu.push(0x00); // Class 0
    pdu.extend_from_slice(cookie_bytes);
    pdu.extend_from_slice(&neg_req);
    pdu
}

fn parse_x224_confirm(data: &[u8]) -> anyhow::Result<u8> {
    if data.len() < 11 {
        return Err(anyhow::anyhow!("X.224 confirm too short"));
    }
    if data[0] != 0x03 {
        return Err(anyhow::anyhow!("Invalid TPKT version"));
    }
    if data[5] != 0xD0 {
        return Err(anyhow::anyhow!(
            "Expected X.224 CC (0xD0), got 0x{:02X}",
            data[5]
        ));
    }
    let tpkt_len = u16::from_be_bytes([data[2], data[3]]) as usize;
    if tpkt_len > data.len() {
        return Err(anyhow::anyhow!("TPKT length mismatch"));
    }
    // Look for negotiation response type 0x02
    if tpkt_len >= 15 {
        let neg_start = tpkt_len - 8;
        if data[neg_start] == 0x02 {
            return Ok(data[neg_start + 4]);
        }
    }
    Ok(0)
}

fn build_mcs_connect_initial(profile: &RdpProfile) -> Vec<u8> {
    let width = profile.width;
    let height = profile.height;

    // Client Core Data
    let mut core = Vec::new();
    core.extend_from_slice(&0xC001u16.to_le_bytes());
    let len_pos = core.len();
    core.extend_from_slice(&0u16.to_le_bytes()); // placeholder
    core.extend_from_slice(&0x00080004u32.to_le_bytes()); // version RDP5+
    core.extend_from_slice(&width.to_le_bytes());
    core.extend_from_slice(&height.to_le_bytes());
    core.extend_from_slice(&0xCA01u16.to_le_bytes()); // colorDepth
    core.extend_from_slice(&0xAA03u16.to_le_bytes()); // SASSequence
    core.extend_from_slice(&0x00000409u32.to_le_bytes()); // keyboard layout
    core.extend_from_slice(&2600u32.to_le_bytes()); // clientBuild
    // clientName (32 bytes UTF-16)
    let mut name_buf = [0u8; 32];
    for (i, ch) in "Portix".encode_utf16().take(15).enumerate() {
        let b = ch.to_le_bytes();
        name_buf[i * 2] = b[0];
        name_buf[i * 2 + 1] = b[1];
    }
    core.extend_from_slice(&name_buf);
    core.extend_from_slice(&4u32.to_le_bytes()); // keyboardType
    core.extend_from_slice(&0u32.to_le_bytes()); // keyboardSubType
    core.extend_from_slice(&12u32.to_le_bytes()); // keyboardFunctionKey
    core.extend_from_slice(&[0u8; 64]); // imeFileName
    core.extend_from_slice(&0xCA01u16.to_le_bytes()); // postBeta2ColorDepth
    core.extend_from_slice(&1u16.to_le_bytes()); // clientProductId
    core.extend_from_slice(&0u32.to_le_bytes()); // serialNumber
    core.extend_from_slice(&24u16.to_le_bytes()); // highColorDepth (24bpp)
    core.extend_from_slice(&0x000Fu16.to_le_bytes()); // supportedColorDepths
    core.extend_from_slice(&0x0001u16.to_le_bytes()); // earlyCapabilityFlags
    core.extend_from_slice(&[0u8; 64]); // clientDigProductId
    core.push(0); // connectionType
    core.push(0); // pad
    core.extend_from_slice(&0x0001u32.to_le_bytes()); // serverSelectedProtocol=TLS
    let core_len = core.len() as u16;
    core[len_pos..len_pos + 2].copy_from_slice(&core_len.to_le_bytes());

    // Client Security Data
    let mut sec = Vec::new();
    sec.extend_from_slice(&0xC002u16.to_le_bytes());
    sec.extend_from_slice(&12u16.to_le_bytes());
    sec.extend_from_slice(&0x0000001Bu32.to_le_bytes());
    sec.extend_from_slice(&0u32.to_le_bytes());

    // Client Network Data (no virtual channels)
    let mut net = Vec::new();
    net.extend_from_slice(&0xC003u16.to_le_bytes());
    net.extend_from_slice(&8u16.to_le_bytes());
    net.extend_from_slice(&0u32.to_le_bytes());

    let user_data = [&core[..], &sec[..], &net[..]].concat();
    let gcc = build_gcc_wrapper(&user_data);
    build_mcs_ci_pdu(&gcc)
}

fn build_gcc_wrapper(user_data: &[u8]) -> Vec<u8> {
    let mut gcc = Vec::new();
    gcc.extend_from_slice(&[0x00, 0x05, 0x00, 0x14, 0x7C, 0x00, 0x01]);
    let pdu_len = user_data.len() + 14;
    per_write_length(&mut gcc, pdu_len);
    gcc.extend_from_slice(&[0x00, 0x08, 0x00, 0x10, 0x00, 0x01, 0xC0, 0x00]);
    gcc.push(0x44); gcc.push(0x75); gcc.push(0x63); gcc.push(0x61); // "Duca"
    per_write_length(&mut gcc, user_data.len());
    gcc.extend_from_slice(user_data);
    gcc
}

fn build_mcs_ci_pdu(gcc_data: &[u8]) -> Vec<u8> {
    let mut mcs = Vec::new();
    // callingDomainSelector
    mcs.push(0x04); mcs.push(0x01); mcs.push(0x01);
    // calledDomainSelector
    mcs.push(0x04); mcs.push(0x01); mcs.push(0x01);
    // upwardFlag
    mcs.push(0x01); mcs.push(0x01); mcs.push(0xFF);
    // Parameters
    mcs.extend_from_slice(&build_domain_params(34, 2, 0, 1, 0, 1, 0xFFFF, 2));
    mcs.extend_from_slice(&build_domain_params(1, 1, 1, 1, 0, 1, 0x420, 2));
    mcs.extend_from_slice(&build_domain_params(0xFFFF, 0xFC17, 0xFFFF, 1, 0, 1, 0xFFFF, 2));
    // userData
    mcs.push(0x04);
    ber_write_length(&mut mcs, gcc_data.len());
    mcs.extend_from_slice(gcc_data);

    let content_len = mcs.len();
    let mut final_pdu = Vec::new();
    final_pdu.push(0x7F); final_pdu.push(0x65);
    ber_write_length(&mut final_pdu, content_len);
    final_pdu.extend_from_slice(&mcs);

    wrap_tpkt_x224_data(&final_pdu)
}

fn build_domain_params(a: u32, b: u32, c: u32, d: u32, e: u32, f: u32, g: u32, h: u32) -> Vec<u8> {
    let mut content = Vec::new();
    for v in [a, b, c, d, e, f, g, h] {
        ber_write_int(&mut content, v);
    }
    let mut params = Vec::new();
    params.push(0x30);
    ber_write_length(&mut params, content.len());
    params.extend_from_slice(&content);
    params
}

fn ber_write_int(buf: &mut Vec<u8>, value: u32) {
    buf.push(0x02);
    if value <= 0x7F {
        buf.push(1); buf.push(value as u8);
    } else if value <= 0x7FFF {
        buf.push(2); buf.push((value >> 8) as u8); buf.push(value as u8);
    } else if value <= 0x7FFFFF {
        buf.push(3);
        buf.push((value >> 16) as u8); buf.push((value >> 8) as u8); buf.push(value as u8);
    } else {
        buf.push(4);
        buf.push((value >> 24) as u8); buf.push((value >> 16) as u8);
        buf.push((value >> 8) as u8); buf.push(value as u8);
    }
}

fn ber_write_length(buf: &mut Vec<u8>, len: usize) {
    if len < 128 {
        buf.push(len as u8);
    } else if len < 256 {
        buf.push(0x81); buf.push(len as u8);
    } else {
        buf.push(0x82); buf.push((len >> 8) as u8); buf.push(len as u8);
    }
}

fn per_write_length(buf: &mut Vec<u8>, len: usize) {
    if len < 128 {
        buf.push(len as u8);
    } else {
        buf.push(0x80 | ((len >> 8) & 0x7F) as u8);
        buf.push((len & 0xFF) as u8);
    }
}

fn wrap_tpkt_x224_data(data: &[u8]) -> Vec<u8> {
    let total = 4 + 3 + data.len();
    let mut pdu = Vec::with_capacity(total);
    pdu.push(0x03); pdu.push(0x00);
    pdu.extend_from_slice(&(total as u16).to_be_bytes());
    pdu.push(0x02); pdu.push(0xF0); pdu.push(0x80);
    pdu.extend_from_slice(data);
    pdu
}

fn build_mcs_erect_domain() -> Vec<u8> {
    wrap_tpkt_x224_data(&[0x04, 0x01, 0x00, 0x01, 0x00])
}

fn build_mcs_attach_user() -> Vec<u8> {
    wrap_tpkt_x224_data(&[0x28])
}

fn parse_attach_user_confirm(data: &[u8]) -> anyhow::Result<u16> {
    if data.len() < 11 {
        return Err(anyhow::anyhow!("Attach User Confirm too short"));
    }
    let mcs_start = 7;
    if data[mcs_start] != 0x2E {
        return Err(anyhow::anyhow!(
            "Expected Attach User Confirm (0x2E), got 0x{:02X}",
            data[mcs_start]
        ));
    }
    if data[mcs_start + 1] != 0 {
        return Err(anyhow::anyhow!("Attach User failed"));
    }
    let user_id = u16::from_be_bytes([data[mcs_start + 2], data[mcs_start + 3]]) + 1001;
    Ok(user_id)
}

fn build_mcs_channel_join(user_id: u16, channel_id: u16) -> Vec<u8> {
    let mut d = Vec::new();
    d.push(0x38);
    d.extend_from_slice(&(user_id - 1001).to_be_bytes());
    d.extend_from_slice(&channel_id.to_be_bytes());
    wrap_tpkt_x224_data(&d)
}

fn build_client_info_pdu(user_channel_id: u16, profile: &RdpProfile) -> Vec<u8> {
    let mut info = Vec::new();
    // Security header: SEC_INFO_PKT
    info.extend_from_slice(&0x00000040u32.to_le_bytes());
    // TS_INFO_PACKET
    info.extend_from_slice(&0u32.to_le_bytes()); // CodePage
    info.extend_from_slice(&0x0000_0177u32.to_le_bytes()); // Flags

    let domain = profile.domain.as_deref().unwrap_or("");
    let domain_utf16: Vec<u8> = domain.encode_utf16().flat_map(|c| c.to_le_bytes()).collect();
    let username_utf16: Vec<u8> = profile.username.encode_utf16().flat_map(|c| c.to_le_bytes()).collect();
    let password = profile.password.as_deref().unwrap_or("");
    let password_utf16: Vec<u8> = password.encode_utf16().flat_map(|c| c.to_le_bytes()).collect();

    info.extend_from_slice(&(domain_utf16.len() as u16).to_le_bytes());
    info.extend_from_slice(&(username_utf16.len() as u16).to_le_bytes());
    info.extend_from_slice(&(password_utf16.len() as u16).to_le_bytes());
    info.extend_from_slice(&0u16.to_le_bytes()); // AlternateShell
    info.extend_from_slice(&0u16.to_le_bytes()); // WorkingDir

    info.extend_from_slice(&domain_utf16); info.extend_from_slice(&[0, 0]);
    info.extend_from_slice(&username_utf16); info.extend_from_slice(&[0, 0]);
    info.extend_from_slice(&password_utf16); info.extend_from_slice(&[0, 0]);
    info.extend_from_slice(&[0, 0]); // AlternateShell null
    info.extend_from_slice(&[0, 0]); // WorkingDir null

    wrap_mcs_send_data(user_channel_id, 1003, &info)
}

fn wrap_mcs_send_data(user_id: u16, channel_id: u16, data: &[u8]) -> Vec<u8> {
    let mut mcs = Vec::new();
    mcs.push(0x64);
    mcs.extend_from_slice(&(user_id - 1001).to_be_bytes());
    mcs.extend_from_slice(&channel_id.to_be_bytes());
    mcs.push(0x70);
    let len = data.len();
    if len < 128 {
        mcs.push(len as u8);
    } else {
        mcs.push(0x80 | ((len >> 8) & 0x3F) as u8);
        mcs.push((len & 0xFF) as u8);
    }
    mcs.extend_from_slice(data);
    wrap_tpkt_x224_data(&mcs)
}

fn contains_demand_active(data: &[u8]) -> bool {
    if data.len() < 20 {
        return false;
    }
    // Look for PDUTYPE_DEMANDACTIVEPDU (0x0011) in the payload
    for i in 7..data.len().saturating_sub(1) {
        if data[i] == 0x11 && data[i + 1] == 0x00 {
            return true;
        }
    }
    false
}

fn build_confirm_active_pdu(user_channel_id: u16, width: u16, height: u16) -> Vec<u8> {
    // Minimal capability sets for Confirm Active

    // General Capability Set
    let mut general_cap = Vec::new();
    general_cap.extend_from_slice(&1u16.to_le_bytes()); // capabilitySetType = CAPSTYPE_GENERAL
    general_cap.extend_from_slice(&24u16.to_le_bytes()); // lengthCapability
    general_cap.extend_from_slice(&1u16.to_le_bytes()); // osMajorType (Windows)
    general_cap.extend_from_slice(&3u16.to_le_bytes()); // osMinorType
    general_cap.extend_from_slice(&0x0200u16.to_le_bytes()); // protocolVersion
    general_cap.extend_from_slice(&0u16.to_le_bytes()); // pad
    general_cap.extend_from_slice(&0u16.to_le_bytes()); // generalCompressionTypes
    general_cap.extend_from_slice(&0x001Du16.to_le_bytes()); // extraFlags
    general_cap.extend_from_slice(&0u16.to_le_bytes()); // updateCapabilityFlag
    general_cap.extend_from_slice(&0u16.to_le_bytes()); // remoteUnshareFlag
    general_cap.extend_from_slice(&0u16.to_le_bytes()); // generalCompressionLevel
    general_cap.extend_from_slice(&0u8.to_le_bytes()); // refreshRectSupport
    general_cap.extend_from_slice(&0u8.to_le_bytes()); // suppressOutputSupport

    // Bitmap Capability Set
    let mut bitmap_cap = Vec::new();
    bitmap_cap.extend_from_slice(&2u16.to_le_bytes()); // CAPSTYPE_BITMAP
    bitmap_cap.extend_from_slice(&28u16.to_le_bytes()); // length
    bitmap_cap.extend_from_slice(&24u16.to_le_bytes()); // preferredBitsPerPixel
    bitmap_cap.extend_from_slice(&1u16.to_le_bytes()); // receive1BitPerPixel
    bitmap_cap.extend_from_slice(&1u16.to_le_bytes()); // receive4BitsPerPixel
    bitmap_cap.extend_from_slice(&1u16.to_le_bytes()); // receive8BitsPerPixel
    bitmap_cap.extend_from_slice(&width.to_le_bytes()); // desktopWidth
    bitmap_cap.extend_from_slice(&height.to_le_bytes()); // desktopHeight
    bitmap_cap.extend_from_slice(&0u16.to_le_bytes()); // pad
    bitmap_cap.extend_from_slice(&0x0001u16.to_le_bytes()); // desktopResizeFlag
    bitmap_cap.extend_from_slice(&1u16.to_le_bytes()); // bitmapCompressionFlag
    bitmap_cap.extend_from_slice(&0u8.to_le_bytes()); // highColorFlags
    bitmap_cap.extend_from_slice(&0u8.to_le_bytes()); // drawingFlags
    bitmap_cap.extend_from_slice(&1u16.to_le_bytes()); // multipleRectangleSupport
    bitmap_cap.extend_from_slice(&0u16.to_le_bytes()); // pad

    // Order Capability Set (minimal)
    let mut order_cap = Vec::new();
    order_cap.extend_from_slice(&3u16.to_le_bytes()); // CAPSTYPE_ORDER
    order_cap.extend_from_slice(&88u16.to_le_bytes()); // length
    order_cap.extend_from_slice(&[0u8; 16]); // terminalDescriptor
    order_cap.extend_from_slice(&0u32.to_le_bytes()); // pad
    order_cap.extend_from_slice(&1u16.to_le_bytes()); // desktopSaveXGranularity
    order_cap.extend_from_slice(&20u16.to_le_bytes()); // desktopSaveYGranularity
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // pad
    order_cap.extend_from_slice(&1u16.to_le_bytes()); // maximumOrderLevel
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // numberFonts
    order_cap.extend_from_slice(&0x0022u16.to_le_bytes()); // orderFlags
    order_cap.extend_from_slice(&[0u8; 32]); // orderSupport (all zeros = no drawing orders)
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // textFlags
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // orderSupportExFlags
    order_cap.extend_from_slice(&0u32.to_le_bytes()); // pad
    order_cap.extend_from_slice(&480u32.to_le_bytes()); // desktopSaveSize
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // pad
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // pad
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // textANSICodePage
    order_cap.extend_from_slice(&0u16.to_le_bytes()); // pad

    // Input Capability Set
    let mut input_cap = Vec::new();
    input_cap.extend_from_slice(&13u16.to_le_bytes()); // CAPSTYPE_INPUT
    input_cap.extend_from_slice(&88u16.to_le_bytes()); // length
    input_cap.extend_from_slice(&0x0001u16.to_le_bytes()); // inputFlags: INPUT_FLAG_SCANCODES
    input_cap.extend_from_slice(&0u16.to_le_bytes()); // pad
    input_cap.extend_from_slice(&0x00000409u32.to_le_bytes()); // keyboardLayout
    input_cap.extend_from_slice(&4u32.to_le_bytes()); // keyboardType
    input_cap.extend_from_slice(&0u32.to_le_bytes()); // keyboardSubType
    input_cap.extend_from_slice(&12u32.to_le_bytes()); // keyboardFunctionKey
    input_cap.extend_from_slice(&[0u8; 64]); // imeFileName

    let num_caps: u16 = 4;
    let all_caps = [&general_cap[..], &bitmap_cap[..], &order_cap[..], &input_cap[..]].concat();

    // Share Control Header (Confirm Active)
    let mut pdu = Vec::new();
    // shareId (4 bytes) + originatorId (2) + lengthSourceDescriptor (2) +
    // lengthCombinedCapabilities (2) + sourceDescriptor + numCapabilities (2) + pad (2) + caps
    let source_desc = b"RDP\0";
    let combined_caps_len = 4 + all_caps.len(); // numCaps(2) + pad(2) + caps

    let share_data_len = 4 + 2 + 2 + 2 + source_desc.len() + combined_caps_len;
    let share_ctrl_len = share_data_len + 6; // shareControlHeader is 6 bytes

    // Share Control Header
    pdu.extend_from_slice(&(share_ctrl_len as u16).to_le_bytes()); // totalLength
    pdu.extend_from_slice(&0x0013u16.to_le_bytes()); // PDUTYPE_CONFIRMACTIVEPDU
    pdu.extend_from_slice(&(user_channel_id).to_le_bytes()); // PDUSource

    // Share Data
    pdu.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes()); // shareId
    pdu.extend_from_slice(&0x03EAu16.to_le_bytes()); // originatorId
    pdu.extend_from_slice(&(source_desc.len() as u16).to_le_bytes());
    pdu.extend_from_slice(&(combined_caps_len as u16).to_le_bytes());
    pdu.extend_from_slice(source_desc);
    pdu.extend_from_slice(&num_caps.to_le_bytes());
    pdu.extend_from_slice(&0u16.to_le_bytes()); // pad
    pdu.extend_from_slice(&all_caps);

    wrap_mcs_send_data(user_channel_id, 1003, &pdu)
}

fn build_synchronize_sequence(user_channel_id: u16) -> Vec<u8> {
    let mut result = Vec::new();

    // Synchronize PDU
    let mut sync = Vec::new();
    sync.extend_from_slice(&22u16.to_le_bytes()); // totalLength
    sync.extend_from_slice(&0x0017u16.to_le_bytes()); // PDUTYPE_DATAPDU
    sync.extend_from_slice(&user_channel_id.to_le_bytes());
    sync.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes()); // shareId
    sync.push(0); // pad
    sync.push(1); // streamId
    sync.extend_from_slice(&6u16.to_le_bytes()); // uncompressedLength
    sync.push(31); // pduType2 = PDUTYPE2_SYNCHRONIZE
    sync.push(0); // compressedType
    sync.extend_from_slice(&0u16.to_le_bytes()); // compressedLength
    sync.extend_from_slice(&1u16.to_le_bytes()); // messageType
    sync.extend_from_slice(&(user_channel_id).to_le_bytes()); // targetUser
    result.extend_from_slice(&wrap_mcs_send_data(user_channel_id, 1003, &sync));

    // Control Cooperate PDU
    let mut ctrl = Vec::new();
    ctrl.extend_from_slice(&26u16.to_le_bytes());
    ctrl.extend_from_slice(&0x0017u16.to_le_bytes());
    ctrl.extend_from_slice(&user_channel_id.to_le_bytes());
    ctrl.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes());
    ctrl.push(0); ctrl.push(1);
    ctrl.extend_from_slice(&8u16.to_le_bytes());
    ctrl.push(20); // PDUTYPE2_CONTROL
    ctrl.push(0);
    ctrl.extend_from_slice(&0u16.to_le_bytes());
    ctrl.extend_from_slice(&4u16.to_le_bytes()); // action = CTRLACTION_COOPERATE
    ctrl.extend_from_slice(&0u16.to_le_bytes()); // grantId
    ctrl.extend_from_slice(&0u32.to_le_bytes()); // controlId
    result.extend_from_slice(&wrap_mcs_send_data(user_channel_id, 1003, &ctrl));

    // Control Request Control PDU
    let mut req = Vec::new();
    req.extend_from_slice(&26u16.to_le_bytes());
    req.extend_from_slice(&0x0017u16.to_le_bytes());
    req.extend_from_slice(&user_channel_id.to_le_bytes());
    req.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes());
    req.push(0); req.push(1);
    req.extend_from_slice(&8u16.to_le_bytes());
    req.push(20);
    req.push(0);
    req.extend_from_slice(&0u16.to_le_bytes());
    req.extend_from_slice(&1u16.to_le_bytes()); // CTRLACTION_REQUEST_CONTROL
    req.extend_from_slice(&0u16.to_le_bytes());
    req.extend_from_slice(&0u32.to_le_bytes());
    result.extend_from_slice(&wrap_mcs_send_data(user_channel_id, 1003, &req));

    // Font List PDU
    let mut font = Vec::new();
    font.extend_from_slice(&26u16.to_le_bytes());
    font.extend_from_slice(&0x0017u16.to_le_bytes());
    font.extend_from_slice(&user_channel_id.to_le_bytes());
    font.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes());
    font.push(0); font.push(1);
    font.extend_from_slice(&8u16.to_le_bytes());
    font.push(39); // PDUTYPE2_FONTLIST
    font.push(0);
    font.extend_from_slice(&0u16.to_le_bytes());
    font.extend_from_slice(&0u16.to_le_bytes()); // numberFonts
    font.extend_from_slice(&0u16.to_le_bytes()); // totalNumFonts
    font.extend_from_slice(&0x0003u16.to_le_bytes()); // listFlags
    font.extend_from_slice(&0x0032u16.to_le_bytes()); // entrySize
    result.extend_from_slice(&wrap_mcs_send_data(user_channel_id, 1003, &font));

    result
}

// ─── Input PDU Builders ──────────────────────────────────────────────────────

fn build_keyboard_pdu(user_channel_id: u16, scancode: u16, is_pressed: bool) -> Vec<u8> {
    // Slow-path keyboard input event wrapped in Input PDU
    let mut pdu = Vec::new();
    // Share Control Header
    let total_len: u16 = 6 + 12 + 4 + 12; // header + shareData + numEvents + event
    pdu.extend_from_slice(&total_len.to_le_bytes());
    pdu.extend_from_slice(&0x0017u16.to_le_bytes()); // PDUTYPE_DATAPDU
    pdu.extend_from_slice(&user_channel_id.to_le_bytes());
    // Share Data Header
    pdu.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes());
    pdu.push(0); pdu.push(1);
    pdu.extend_from_slice(&16u16.to_le_bytes()); // uncompressedLength
    pdu.push(28); // PDUTYPE2_INPUT
    pdu.push(0);
    pdu.extend_from_slice(&0u16.to_le_bytes());
    // Input PDU Data
    pdu.extend_from_slice(&1u16.to_le_bytes()); // numEvents
    pdu.extend_from_slice(&0u16.to_le_bytes()); // pad
    // TS_INPUT_EVENT (keyboard)
    pdu.extend_from_slice(&0u32.to_le_bytes()); // eventTime
    pdu.extend_from_slice(&0x0004u16.to_le_bytes()); // messageType = INPUT_EVENT_SCANCODE
    // Keyboard flags: KBDFLAGS_DOWN=0, KBDFLAGS_RELEASE=0x8000
    let flags: u16 = if is_pressed { 0 } else { 0x8000 };
    pdu.extend_from_slice(&flags.to_le_bytes());
    pdu.extend_from_slice(&scancode.to_le_bytes()); // keyCode
    pdu.extend_from_slice(&0u16.to_le_bytes()); // pad

    wrap_mcs_send_data(user_channel_id, 1003, &pdu)
}

fn build_mouse_move_pdu(user_channel_id: u16, x: u16, y: u16) -> Vec<u8> {
    build_mouse_event_pdu(user_channel_id, 0x0800, x, y) // PTRFLAGS_MOVE
}

fn build_mouse_button_pdu(
    user_channel_id: u16,
    x: u16,
    y: u16,
    button: MouseButton,
    is_pressed: bool,
) -> Vec<u8> {
    let flags: u16 = match (button, is_pressed) {
        (MouseButton::Left, true) => 0x8000 | 0x1000,   // PTRFLAGS_DOWN | PTRFLAGS_BUTTON1
        (MouseButton::Left, false) => 0x1000,
        (MouseButton::Right, true) => 0x8000 | 0x2000,  // PTRFLAGS_DOWN | PTRFLAGS_BUTTON2
        (MouseButton::Right, false) => 0x2000,
        (MouseButton::Middle, true) => 0x8000 | 0x4000,  // PTRFLAGS_DOWN | PTRFLAGS_BUTTON3
        (MouseButton::Middle, false) => 0x4000,
    };
    build_mouse_event_pdu(user_channel_id, flags, x, y)
}

fn build_mouse_event_pdu(user_channel_id: u16, pointer_flags: u16, x: u16, y: u16) -> Vec<u8> {
    let mut pdu = Vec::new();
    let total_len: u16 = 6 + 12 + 4 + 12;
    pdu.extend_from_slice(&total_len.to_le_bytes());
    pdu.extend_from_slice(&0x0017u16.to_le_bytes());
    pdu.extend_from_slice(&user_channel_id.to_le_bytes());
    pdu.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes());
    pdu.push(0); pdu.push(1);
    pdu.extend_from_slice(&16u16.to_le_bytes());
    pdu.push(28); // PDUTYPE2_INPUT
    pdu.push(0);
    pdu.extend_from_slice(&0u16.to_le_bytes());
    // numEvents + pad
    pdu.extend_from_slice(&1u16.to_le_bytes());
    pdu.extend_from_slice(&0u16.to_le_bytes());
    // TS_INPUT_EVENT (mouse)
    pdu.extend_from_slice(&0u32.to_le_bytes()); // eventTime
    pdu.extend_from_slice(&0x8001u16.to_le_bytes()); // messageType = INPUT_EVENT_MOUSE
    pdu.extend_from_slice(&pointer_flags.to_le_bytes());
    pdu.extend_from_slice(&x.to_le_bytes());
    pdu.extend_from_slice(&y.to_le_bytes());

    wrap_mcs_send_data(user_channel_id, 1003, &pdu)
}

fn build_shutdown_pdu(user_channel_id: u16) -> Vec<u8> {
    let mut pdu = Vec::new();
    pdu.extend_from_slice(&18u16.to_le_bytes());
    pdu.extend_from_slice(&0x0017u16.to_le_bytes());
    pdu.extend_from_slice(&user_channel_id.to_le_bytes());
    pdu.extend_from_slice(&0x00_03_EA_01u32.to_le_bytes());
    pdu.push(0); pdu.push(1);
    pdu.extend_from_slice(&0u16.to_le_bytes());
    pdu.push(36); // PDUTYPE2_SHUTDOWN_REQUEST
    pdu.push(0);
    pdu.extend_from_slice(&0u16.to_le_bytes());

    wrap_mcs_send_data(user_channel_id, 1003, &pdu)
}

// ─── Bitmap Processing ───────────────────────────────────────────────────────

struct BitmapUpdate {
    x: u16,
    y: u16,
    width: u16,
    height: u16,
    bpp: u16,
    data: Vec<u8>,
}

fn extract_bitmap_updates(data: &[u8]) -> Option<Vec<BitmapUpdate>> {
    // Find TS_UPDATE_BITMAP PDU in the data stream
    // The update type 0x0001 = UPDATETYPE_BITMAP appears in the data
    if data.len() < 20 {
        return None;
    }

    let mut updates = Vec::new();
    let mut offset = 0;

    while offset + 20 < data.len() {
        // Look for bitmap update header pattern
        // After MCS + share headers, we look for updateType = 0x0001
        if offset + 2 <= data.len() && data[offset] == 0x01 && data[offset + 1] == 0x00 {
            // Potential bitmap update
            if offset + 4 > data.len() {
                break;
            }
            let num_rects = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) as usize;
            let mut rect_offset = offset + 4;

            for _ in 0..num_rects.min(64) {
                if rect_offset + 18 > data.len() {
                    break;
                }
                let dest_left = u16::from_le_bytes([data[rect_offset], data[rect_offset + 1]]);
                let dest_top = u16::from_le_bytes([data[rect_offset + 2], data[rect_offset + 3]]);
                let dest_right = u16::from_le_bytes([data[rect_offset + 4], data[rect_offset + 5]]);
                let dest_bottom = u16::from_le_bytes([data[rect_offset + 6], data[rect_offset + 7]]);
                let bmp_width = u16::from_le_bytes([data[rect_offset + 8], data[rect_offset + 9]]);
                let bmp_height = u16::from_le_bytes([data[rect_offset + 10], data[rect_offset + 11]]);
                let bpp = u16::from_le_bytes([data[rect_offset + 12], data[rect_offset + 13]]);
                let _flags = u16::from_le_bytes([data[rect_offset + 14], data[rect_offset + 15]]);
                let bmp_length = u16::from_le_bytes([data[rect_offset + 16], data[rect_offset + 17]]) as usize;

                rect_offset += 18;
                if rect_offset + bmp_length > data.len() {
                    break;
                }

                let bmp_data = data[rect_offset..rect_offset + bmp_length].to_vec();
                rect_offset += bmp_length;

                let w = dest_right.saturating_sub(dest_left) + 1;
                let h = dest_bottom.saturating_sub(dest_top) + 1;

                updates.push(BitmapUpdate {
                    x: dest_left,
                    y: dest_top,
                    width: w.max(bmp_width),
                    height: h.max(bmp_height),
                    bpp,
                    data: bmp_data,
                });
            }
            if !updates.is_empty() {
                return Some(updates);
            }
        }
        offset += 1;
    }

    if updates.is_empty() { None } else { Some(updates) }
}

fn apply_bitmap_to_buffer(
    frame_buffer: &mut [u8],
    frame_width: u16,
    _frame_height: u16,
    update: &BitmapUpdate,
) {
    let bpp = update.bpp as usize;
    let bytes_per_pixel = if bpp >= 24 { bpp / 8 } else { return }; // Only handle 24/32bpp
    let stride = frame_width as usize * 4;

    // Convert bitmap data to RGBA and write into frame buffer
    // RDP bitmaps are bottom-up by default
    let src_row_bytes = update.width as usize * bytes_per_pixel;

    for row in 0..update.height as usize {
        let dest_y = update.y as usize + row;
        let src_row = if update.height as usize > 1 {
            (update.height as usize - 1) - row
        } else {
            0
        };
        let src_offset = src_row * src_row_bytes;
        let dest_offset = dest_y * stride + (update.x as usize) * 4;

        for col in 0..update.width as usize {
            let src_px = src_offset + col * bytes_per_pixel;
            let dst_px = dest_offset + col * 4;

            if src_px + bytes_per_pixel > update.data.len() || dst_px + 4 > frame_buffer.len() {
                break;
            }

            // RDP sends BGR(A), we convert to RGBA
            frame_buffer[dst_px] = update.data[src_px + 2]; // R
            frame_buffer[dst_px + 1] = update.data[src_px + 1]; // G
            frame_buffer[dst_px + 2] = update.data[src_px]; // B
            frame_buffer[dst_px + 3] = 255; // A
        }
    }
}
