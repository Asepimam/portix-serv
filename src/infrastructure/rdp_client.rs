use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_rustls::rustls;

use crate::domain::rdp_profile::RdpProfile;
use crate::infrastructure::rdpdr::RdpdrState;

fn rdp_log_line(message: impl AsRef<str>) {
    eprintln!("{}", message.as_ref());
}

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
    /// Announce new local Unicode clipboard text to the remote session.
    SetClipboardText { text: String },
    /// Request current frame buffer as RGBA bytes (zero-copy Arc)
    RequestFrame {
        response_tx: oneshot::Sender<Arc<Vec<u8>>>,
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
    pub x: u16,
    pub y: u16,
    pub width: u16,
    pub height: u16,
    pub desktop_width: u16,
    pub desktop_height: u16,
    pub sequence: u64,
    pub full_frame: bool,
    /// RGBA pixel data for this full frame or dirty region.
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RdpClipboardEvent {
    pub session_id: String,
    pub text: String,
}

// ─── High-Performance Framebuffer ────────────────────────────────────────────

#[derive(Clone, Copy, Debug, Default)]
struct DirtyRect {
    x: u16,
    y: u16,
    width: u16,
    height: u16,
}

impl DirtyRect {
    fn area(self) -> usize {
        self.width as usize * self.height as usize
    }

    fn right(self) -> u16 {
        self.x.saturating_add(self.width)
    }

    fn bottom(self) -> u16 {
        self.y.saturating_add(self.height)
    }

    fn union(self, other: Self) -> Self {
        let left = self.x.min(other.x);
        let top = self.y.min(other.y);
        let right = self.right().max(other.right());
        let bottom = self.bottom().max(other.bottom());
        Self {
            x: left,
            y: top,
            width: right.saturating_sub(left),
            height: bottom.saturating_sub(top),
        }
    }
}

/// Double-buffered framebuffer with dirty tracking.
/// - Write buffer: actively updated by network thread
/// - Read buffer: served to Flutter via Arc swap (zero-copy)
struct Framebuffer {
    /// Active write buffer (network thread writes here)
    write_buf: Vec<u8>,
    /// Snapshot served to readers (swapped atomically)
    read_snapshot: Arc<Vec<u8>>,
    /// Whether write_buf has changes since last snapshot
    dirty: bool,
    dirty_rects: Vec<DirtyRect>,
    version: u64,
    snapshot_version: u64,
    last_request_version: u64,
    has_content: bool,
    /// Frame dimensions
    width: usize,
    height: usize,
    stride: usize, // width * 4
}

impl Framebuffer {
    fn new(width: usize, height: usize) -> Self {
        let size = width * height * 4;
        let buf = vec![0u8; size];
        Self {
            write_buf: buf.clone(),
            read_snapshot: Arc::new(buf),
            dirty: false,
            dirty_rects: Vec::with_capacity(64),
            version: 1,
            snapshot_version: 1,
            last_request_version: 0,
            has_content: false,
            width,
            height,
            stride: width * 4,
        }
    }

    /// Get the current read snapshot only if it changed since the last request.
    fn snapshot_for_request(&mut self) -> Option<Arc<Vec<u8>>> {
        if !self.has_content {
            return None;
        }
        if self.last_request_version == self.version {
            return None;
        }
        if self.dirty {
            self.read_snapshot = Arc::new(self.write_buf.clone());
            self.snapshot_version = self.version;
            self.dirty = false;
        }
        self.last_request_version = self.version;
        Some(self.read_snapshot.clone())
    }

    /// Clear the framebuffer
    fn clear(&mut self) {
        self.write_buf.fill(0);
        self.mark_dirty(DirtyRect {
            x: 0,
            y: 0,
            width: self.width as u16,
            height: self.height as u16,
        });
    }

    fn is_mostly_black(&self) -> bool {
        if !self.has_content || self.write_buf.is_empty() {
            return false;
        }

        let mut sampled = 0usize;
        let mut black = 0usize;
        for pixel in self.write_buf.chunks_exact(4).step_by(64) {
            sampled += 1;
            if pixel[0] < 8 && pixel[1] < 8 && pixel[2] < 8 {
                black += 1;
            }
        }
        sampled > 0 && black * 100 / sampled >= 98
    }

    fn mark_dirty(&mut self, rect: DirtyRect) {
        let Some(rect) = self.clamp_rect(rect) else {
            return;
        };

        self.has_content = true;
        self.dirty = true;
        self.version = self.version.wrapping_add(1).max(1);

        if self.dirty_rects.len() >= 64 {
            let merged = self
                .dirty_rects
                .iter()
                .copied()
                .fold(rect, DirtyRect::union);
            self.dirty_rects.clear();
            self.dirty_rects.push(merged);
            return;
        }

        self.dirty_rects.push(rect);
    }

    fn dirty_area(&self) -> usize {
        self.dirty_rects.iter().copied().map(DirtyRect::area).sum()
    }

    fn drain_dirty_events(
        &mut self,
        session_id: &str,
        max_regions: usize,
        bandwidth_saving: bool,
        include_data: bool,
    ) -> Vec<RdpFrameEvent> {
        if self.dirty_rects.is_empty() {
            return Vec::new();
        }

        let sequence = self.version;
        let rects = self.take_flush_rects(max_regions, bandwidth_saving);
        let full_frame = rects.len() == 1
            && rects[0].x == 0
            && rects[0].y == 0
            && rects[0].width as usize == self.width
            && rects[0].height as usize == self.height;

        rects
            .into_iter()
            .map(|rect| RdpFrameEvent {
                session_id: session_id.to_owned(),
                x: rect.x,
                y: rect.y,
                width: rect.width,
                height: rect.height,
                desktop_width: self.width as u16,
                desktop_height: self.height as u16,
                sequence,
                full_frame,
                data: if include_data {
                    self.copy_region(rect)
                } else {
                    Vec::new()
                },
            })
            .collect()
    }

    fn take_flush_rects(&mut self, max_regions: usize, bandwidth_saving: bool) -> Vec<DirtyRect> {
        if self.dirty_rects.len() <= 1 {
            return std::mem::take(&mut self.dirty_rects);
        }

        let framebuffer_area = self.width * self.height;
        let dirty_area = self.dirty_area();
        if bandwidth_saving
            && dirty_area < framebuffer_area / 2
            && self.dirty_rects.len() <= max_regions
        {
            return std::mem::take(&mut self.dirty_rects);
        }

        if self.dirty_rects.len() > max_regions || dirty_area > framebuffer_area / 2 {
            let merged = self
                .dirty_rects
                .iter()
                .copied()
                .reduce(DirtyRect::union)
                .into_iter()
                .collect();
            self.dirty_rects.clear();
            return merged;
        }

        std::mem::take(&mut self.dirty_rects)
    }

    fn clamp_rect(&self, rect: DirtyRect) -> Option<DirtyRect> {
        let x = rect.x as usize;
        let y = rect.y as usize;
        if x >= self.width || y >= self.height || rect.width == 0 || rect.height == 0 {
            return None;
        }
        let right = (x + rect.width as usize).min(self.width);
        let bottom = (y + rect.height as usize).min(self.height);
        Some(DirtyRect {
            x: x as u16,
            y: y as u16,
            width: (right - x) as u16,
            height: (bottom - y) as u16,
        })
    }

    fn copy_region(&self, rect: DirtyRect) -> Vec<u8> {
        let row_bytes = rect.width as usize * 4;
        let mut out = vec![0; row_bytes * rect.height as usize];
        for row in 0..rect.height as usize {
            let src_start = (rect.y as usize + row) * self.stride + rect.x as usize * 4;
            let dst_start = row * row_bytes;
            out[dst_start..dst_start + row_bytes]
                .copy_from_slice(&self.write_buf[src_start..src_start + row_bytes]);
        }
        out
    }
}

#[derive(Clone, Copy)]
struct FramePolicy {
    max_fps: u16,
    bandwidth_saving: bool,
    stream_pixels: bool,
}

#[derive(Clone, Copy)]
struct KeepAwakePolicy {
    enabled: bool,
    interval: Duration,
}

impl KeepAwakePolicy {
    fn from_profile(profile: &RdpProfile) -> Self {
        let enabled = profile
            .extra
            .get("portix_keep_awake")
            .or_else(|| profile.extra.get("keep_awake"))
            .map(|value| !matches!(value.as_str(), "0" | "false" | "no" | "off"))
            .unwrap_or(true);
        let interval_seconds = profile
            .extra
            .get("portix_keep_awake_interval_seconds")
            .or_else(|| profile.extra.get("keep_awake_interval_seconds"))
            .and_then(|value| value.parse::<u64>().ok())
            .map(|seconds| seconds.clamp(2, 300))
            .unwrap_or(30);

        Self {
            enabled,
            interval: Duration::from_secs(interval_seconds),
        }
    }
}

#[derive(Clone, Copy)]
struct AutoUnlockPolicy {
    enabled: bool,
    initial_delay: Duration,
    wake_delay: Duration,
}

impl AutoUnlockPolicy {
    fn from_profile(profile: &RdpProfile) -> Self {
        let enabled = profile
            .extra
            .get("portix_auto_unlock")
            .or_else(|| profile.extra.get("auto_unlock"))
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"));
        Self {
            enabled,
            initial_delay: Duration::from_secs(2),
            wake_delay: Duration::from_millis(900),
        }
    }
}

enum AutoUnlockState {
    Disabled,
    CheckBlank { deadline: Instant },
    WakeSent { deadline: Instant },
    Done,
}

#[derive(Clone)]
struct DriveRedirectionPolicy {
    root: Option<PathBuf>,
    name: String,
}

impl DriveRedirectionPolicy {
    fn from_profile(profile: &RdpProfile) -> Self {
        let root = profile
            .extra
            .get("portix_drive_path")
            .or_else(|| profile.extra.get("drive_path"))
            .filter(|value| !value.trim().is_empty())
            .map(|value| PathBuf::from(value.trim()))
            .filter(|path| path.is_dir());
        let name = profile
            .extra
            .get("portix_drive_name")
            .or_else(|| profile.extra.get("drive_name"))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("PORTIX")
            .chars()
            .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
            .take(7)
            .collect::<String>();

        Self {
            root,
            name: if name.is_empty() {
                "PORTIX".to_owned()
            } else {
                name
            },
        }
    }

    fn enabled(&self) -> bool {
        self.root.is_some()
    }
}

impl FramePolicy {
    fn from_profile(profile: &RdpProfile) -> Self {
        let max_fps = profile
            .extra
            .get("portix_fps")
            .or_else(|| profile.extra.get("fps"))
            .and_then(|value| value.parse::<u16>().ok())
            .map(|fps| fps.clamp(15, 60))
            .unwrap_or(60);
        let bandwidth_saving = profile
            .extra
            .get("bandwidth_saving")
            .or_else(|| profile.extra.get("portix_bandwidth_saving"))
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"));
        let stream_pixels_value = profile
            .extra
            .get("stream_pixels")
            .or_else(|| profile.extra.get("portix_stream_pixels"))
            .cloned()
            .or_else(|| std::env::var("PORTIX_RDP_STREAM_PIXELS").ok());
        let stream_pixels = stream_pixels_value
            .as_deref()
            .is_some_and(|value| matches!(value, "1" | "true" | "yes" | "on"));
        Self {
            max_fps,
            bandwidth_saving,
            stream_pixels,
        }
    }
}

struct FramePacer {
    policy: FramePolicy,
    last_emit: Instant,
    last_input: Instant,
}

impl FramePacer {
    fn new(policy: FramePolicy) -> Self {
        let now = Instant::now();
        Self {
            policy,
            last_emit: now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
            last_input: now,
        }
    }

    fn record_input(&mut self) {
        self.last_input = Instant::now();
    }

    fn should_flush(&mut self, dirty_area: usize, framebuffer_area: usize) -> bool {
        if dirty_area == 0 {
            return false;
        }
        let now = Instant::now();
        let active_input = now.duration_since(self.last_input) < Duration::from_millis(250);
        let fps = if self.policy.bandwidth_saving {
            15
        } else if active_input && self.policy.max_fps >= 60 {
            60
        } else {
            let _large_damage = dirty_area > framebuffer_area / 3;
            30.min(self.policy.max_fps)
        };
        let interval = Duration::from_micros(1_000_000 / fps as u64);
        if now.duration_since(self.last_emit) >= interval {
            self.last_emit = now;
            true
        } else {
            false
        }
    }
}

#[derive(Default)]
struct RdpDebugStats {
    enabled: bool,
    last_log: Option<Instant>,
    read_bytes: usize,
    pdus: usize,
    bitmap_rects: usize,
    compressed_rects: usize,
    uncompressed_rects: usize,
    bpp15_rects: usize,
    bpp16_rects: usize,
    bpp24_rects: usize,
    bpp32_rects: usize,
    applied_rects: usize,
    skipped_rects: usize,
    emitted_events: usize,
    emitted_bytes: usize,
    frame_requests: usize,
    frame_hits: usize,
    frame_empty: usize,
    keep_awake_events: usize,
    rle_partial: usize,
    rle_invalid: usize,
    size_mismatch: usize,
    mismatch_samples: Vec<String>,
    fastpath_orders: usize,
}

impl RdpDebugStats {
    fn new(profile: &RdpProfile) -> Self {
        Self {
            enabled: std::env::var("PORTIX_RDP_DEBUG")
                .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
                || profile
                    .extra
                    .get("portix_debug")
                    .or_else(|| profile.extra.get("debug"))
                    .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on")),
            ..Self::default()
        }
    }

    fn log_connect(&self, message: impl AsRef<str>) {
        if self.enabled {
            rdp_log_line(format!("RDP DEBUG connect: {}", message.as_ref()));
        }
    }

    fn tick(&mut self, session_id: &str) {
        if !self.enabled {
            return;
        }

        let now = Instant::now();
        let last_log = self.last_log.get_or_insert(now);
        if now.duration_since(*last_log) < Duration::from_secs(1) {
            return;
        }

        let elapsed = now.duration_since(*last_log).as_secs_f64().max(0.001);
        rdp_log_line(format!(
            "RDP DEBUG session={} read={:.1}KB/s pdus={:.1}/s bitmap_rects={} comp={} raw={} bpp15={} bpp16={} bpp24={} bpp32={} applied={} skipped={} rle_partial={} rle_invalid={} size_mismatch={} mismatch_samples=[{}] orders={} emitted={:.1}/s emitted={:.1}KB/s frame_req={} hits={} empty={} keep_awake={}",
            session_id,
            self.read_bytes as f64 / 1024.0 / elapsed,
            self.pdus as f64 / elapsed,
            self.bitmap_rects,
            self.compressed_rects,
            self.uncompressed_rects,
            self.bpp15_rects,
            self.bpp16_rects,
            self.bpp24_rects,
            self.bpp32_rects,
            self.applied_rects,
            self.skipped_rects,
            self.rle_partial,
            self.rle_invalid,
            self.size_mismatch,
            self.mismatch_samples.join("; "),
            self.fastpath_orders,
            self.emitted_events as f64 / elapsed,
            self.emitted_bytes as f64 / 1024.0 / elapsed,
            self.frame_requests,
            self.frame_hits,
            self.frame_empty,
            self.keep_awake_events,
        ));

        *last_log = now;
        self.read_bytes = 0;
        self.pdus = 0;
        self.bitmap_rects = 0;
        self.compressed_rects = 0;
        self.uncompressed_rects = 0;
        self.bpp15_rects = 0;
        self.bpp16_rects = 0;
        self.bpp24_rects = 0;
        self.bpp32_rects = 0;
        self.applied_rects = 0;
        self.skipped_rects = 0;
        self.emitted_events = 0;
        self.emitted_bytes = 0;
        self.frame_requests = 0;
        self.frame_hits = 0;
        self.frame_empty = 0;
        self.keep_awake_events = 0;
        self.rle_partial = 0;
        self.rle_invalid = 0;
        self.size_mismatch = 0;
        self.mismatch_samples.clear();
        self.fastpath_orders = 0;
    }

    fn record_size_mismatch(&mut self, update: &BitmapUpdate<'_>) {
        self.size_mismatch += 1;
        if self.enabled && self.mismatch_samples.len() < 8 {
            self.mismatch_samples.push(format!(
                "@{},{} dst={}x{} bmp={}x{} bpp={} comp={} len={}",
                update.x,
                update.y,
                update.width,
                update.height,
                update.bmp_width,
                update.bmp_height,
                update.bpp,
                update.compressed,
                update.data.len()
            ));
        }
    }
}

#[derive(Default)]
struct RdpRuntimeCaches {
    bitmap: BitmapCache,
    glyph: GlyphCache,
    surface: SurfaceCache,
}

impl RdpRuntimeCaches {
    fn total_entries(&self) -> usize {
        self.bitmap.entries.len() + self.glyph.entries.len() + self.surface.entries.len()
    }
}

#[derive(Default)]
struct BitmapCache {
    entries: HashMap<u64, Arc<[u8]>>,
}

#[derive(Default)]
struct GlyphCache {
    entries: HashMap<u64, Arc<[u8]>>,
}

#[derive(Default)]
struct SurfaceCache {
    entries: HashMap<u64, Arc<[u8]>>,
}

/// RDP connection runtime. Handles the full lifecycle:
/// X.224 → TLS → MCS → Licensing → Capabilities → Active Session
pub struct RdpRuntime {
    pub session_id: String,
    pub profile: RdpProfile,
    pub frame_tx: broadcast::Sender<RdpFrameEvent>,
    pub clipboard_tx: broadcast::Sender<RdpClipboardEvent>,
    pub status_tx: broadcast::Sender<crate::domain::events::ConnectionStatusEvent>,
}

impl RdpRuntime {
    pub fn new(
        profile: RdpProfile,
        session_id: String,
        frame_tx: broadcast::Sender<RdpFrameEvent>,
        clipboard_tx: broadcast::Sender<RdpClipboardEvent>,
        status_tx: broadcast::Sender<crate::domain::events::ConnectionStatusEvent>,
    ) -> Self {
        Self {
            session_id,
            profile,
            frame_tx,
            clipboard_tx,
            status_tx,
        }
    }

    pub async fn run(self, mut command_rx: mpsc::Receiver<RdpCommand>) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.profile.host, self.profile.port);
        let width = self.profile.width;
        let height = self.profile.height;
        let frame_policy = FramePolicy::from_profile(&self.profile);
        let keep_awake_policy = KeepAwakePolicy::from_profile(&self.profile);
        let auto_unlock_policy = AutoUnlockPolicy::from_profile(&self.profile);
        let drive_policy = DriveRedirectionPolicy::from_profile(&self.profile);
        let mut debug_stats = RdpDebugStats::new(&self.profile);
        debug_stats.log_connect(format!(
            "target={addr} requested_size={}x{} fps={} stream_pixels={} keep_awake={} keep_awake_interval={}s auto_unlock={} drive={} drive_name={}",
            width,
            height,
            self.profile
                .extra
                .get("portix_fps")
                .or_else(|| self.profile.extra.get("fps"))
                .map_or("default", String::as_str),
            frame_policy.stream_pixels,
            keep_awake_policy.enabled,
            keep_awake_policy.interval.as_secs(),
            auto_unlock_policy.enabled,
            drive_policy.enabled(),
            drive_policy.name,
        ));

        // Allow up to 3 connection attempts (initial + 2 reconnects for post-login reactivation)
        let max_attempts = 3;
        let mut attempt = 0;
        let mut shared_frame: Option<Arc<Mutex<Framebuffer>>> = None;
        let mut frame_pacer = FramePacer::new(frame_policy);
        let caches = RdpRuntimeCaches::default();
        let _ = caches.total_entries();

        'connection: loop {
            attempt += 1;
            if attempt > max_attempts {
                return Err(anyhow::anyhow!("RDP: max reconnection attempts exceeded"));
            }
            if attempt > 1 {
                rdp_log_line(format!(
                    "RDP: reconnecting (attempt {}/{})",
                    attempt, max_attempts
                ));
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }

            // ─── TCP Connect ──────────────────────────────────────────────────
            let tcp_stream =
                tokio::time::timeout(Duration::from_secs(8), TcpStream::connect(&addr))
                    .await
                    .map_err(|_| anyhow::anyhow!("Timed out connecting to RDP server at {}", addr))?
                    .map_err(|e| {
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

            // Parse the response. If TLS negotiation was rejected, retry without it.
            let (selected_protocol, need_reconnect) = match parse_x224_confirm(&resp_buf[..n]) {
                Ok(proto) => (proto, false),
                Err(_) => {
                    // Server may not support TLS negotiation.
                    // Reconnect and try without negotiation request.
                    drop(stream);
                    let tcp_stream2 =
                        tokio::time::timeout(Duration::from_secs(8), TcpStream::connect(&addr))
                            .await
                            .map_err(|_| anyhow::anyhow!("Timed out reconnecting to {}", addr))?
                            .map_err(|e| anyhow::anyhow!("Reconnect failed: {}", e))?;
                    stream = tcp_stream2;
                    let plain_cr = build_x224_connection_request_plain(&self.profile);
                    stream.write_all(&plain_cr).await?;
                    let n2 = stream.read(&mut resp_buf).await?;
                    if n2 == 0 {
                        return Err(anyhow::anyhow!(
                            "Server closed connection during plain X.224 negotiation"
                        ));
                    }
                    let proto = parse_x224_confirm(&resp_buf[..n2])?;
                    (proto, true)
                }
            };
            debug_stats.log_connect(format!(
                "selected_protocol={} transport={}",
                selected_protocol,
                if selected_protocol >= 1 && !need_reconnect {
                    "tls"
                } else {
                    "plain"
                }
            ));

            // ─── TLS Upgrade (only if server selected TLS) ───────────────────
            let mut tls_stream = if selected_protocol >= 1 && !need_reconnect {
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
                RdpStream::Tls(Box::new(tls))
            } else {
                RdpStream::Plain(stream)
            };

            // ─── MCS Connect Initial ─────────────────────────────────────────
            let mcs_ci = build_mcs_connect_initial(&self.profile, selected_protocol);
            tls_stream.write_all(&mcs_ci).await?;

            let mut buf = vec![0u8; 16384];
            let n = tls_stream.read(&mut buf).await?;
            if n == 0 {
                return Err(anyhow::anyhow!("Server closed during MCS Connect"));
            }
            debug_stats.log_connect(format!("mcs_connect_response={}B", n));
            let cliprdr_channel_id = parse_server_static_channel_id(&buf[..n], 0);
            let rdpdr_channel_id = drive_policy
                .enabled()
                .then(|| parse_server_static_channel_id(&buf[..n], 1))
                .flatten();
            // Parse encryption level from SC_SECURITY block.
            // encryptionLevel=0 means no RDP security layer — Client Info PDU
            // must NOT include the SEC_INFO_PKT security header in that case.
            let server_encryption_level = parse_server_encryption_level(&buf[..n]);
            debug_stats.log_connect(format!(
                "cliprdr_channel_id={} rdpdr_channel_id={} encryption_level={}",
                cliprdr_channel_id
                    .map(|channel| channel.to_string())
                    .unwrap_or_else(|| "unavailable".to_owned()),
                rdpdr_channel_id
                    .map(|channel| channel.to_string())
                    .unwrap_or_else(|| "unavailable".to_owned()),
                server_encryption_level,
            ));

            // ─── MCS Erect Domain + Attach User ──────────────────────────────
            tls_stream.write_all(&build_mcs_erect_domain()).await?;
            tls_stream.write_all(&build_mcs_attach_user()).await?;

            let n = tls_stream.read(&mut buf).await?;
            if n == 0 {
                return Err(anyhow::anyhow!("Server closed during Attach User"));
            }
            let user_channel_id = parse_attach_user_confirm(&buf[..n])?;
            debug_stats.log_connect(format!("user_channel_id={user_channel_id}"));

            // ─── Channel Joins ────────────────────────────────────────────────
            let mut channels = vec![user_channel_id, 1003u16];
            if let Some(channel_id) = cliprdr_channel_id {
                channels.push(channel_id);
            }
            if let Some(channel_id) = rdpdr_channel_id {
                channels.push(channel_id);
            }
            for channel in channels {
                tls_stream
                    .write_all(&build_mcs_channel_join(user_channel_id, channel))
                    .await?;
                let n = tls_stream.read(&mut buf).await?;
                if n == 0 {
                    return Err(anyhow::anyhow!("Server closed during channel join"));
                }
            }

            // ─── Client Info PDU ──────────────────────────────────────────────
            let client_info = build_client_info_pdu(user_channel_id, &self.profile, server_encryption_level);
            tls_stream.write_all(&client_info).await?;

            // ─── Licensing + Wait for Demand Active PDU ───────────────────────
            // After Client Info PDU the server sends one of:
            //   1. A licensing PDU sequence (Server License Request → Platform
            //      Challenge → optionally New License / Upgrade License / No
            //      License Required), followed by the Demand Active PDU.
            //   2. The Demand Active PDU directly (xrdp no-license mode).
            //
            // We handle the licensing exchange here so the Demand Active never
            // gets swallowed by an unprocessed licensing packet.
            let got_demand_active = wait_for_demand_active(
                &mut tls_stream,
                &mut buf,
                user_channel_id,
                &debug_stats,
            )
            .await?;

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
            let fb = shared_frame.get_or_insert_with(|| {
                Arc::new(Mutex::new(Framebuffer::new(
                    width as usize,
                    height as usize,
                )))
            });

            if attempt > 1 {
                fb.lock().clear();
            }

            if attempt == 1 {
                let _ = self
                    .status_tx
                    .send(crate::domain::events::ConnectionStatusEvent {
                        session_id: self.session_id.clone(),
                        status: crate::domain::session::ConnectionStatus::Connected,
                        message: Some("connected".to_owned()),
                    });

                fb.lock().mark_dirty(DirtyRect {
                    x: 0,
                    y: 0,
                    width,
                    height,
                });
                let initial_events = fb.lock().drain_dirty_events(
                    &self.session_id,
                    1,
                    false,
                    frame_policy.stream_pixels,
                );
                for event in initial_events {
                    debug_stats.emitted_events += 1;
                    debug_stats.emitted_bytes += event.data.len();
                    let _ = self.frame_tx.send(event);
                }
            }

            // ─── Active Session Loop (optimized) ─────────────────────────────
            let shared_frame_ref = fb.clone();
            let mut pdu_buf = BytesMut::with_capacity(256 * 1024);
            let mut read_buf = vec![0u8; 128 * 1024];
            let mut frame_tick = tokio::time::interval(Duration::from_millis(8));
            frame_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut keep_awake_tick = tokio::time::interval(keep_awake_policy.interval);
            keep_awake_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            keep_awake_tick.tick().await;
            let mut auto_unlock_tick = tokio::time::interval(Duration::from_millis(100));
            auto_unlock_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            auto_unlock_tick.tick().await;
            let mut decode_scratch = DecodeScratch::default();
            let mut fastpath_fragments = FastPathFragmentState::default();
            let mut clipboard_state = ClipboardChannelState {
                debug: debug_stats.enabled,
                ..ClipboardChannelState::default()
            };
            let mut rdpdr_state =
                drive_policy.root.as_ref().and_then(|root| {
                    match RdpdrState::new(
                        root.clone(),
                        drive_policy.name.clone(),
                        debug_stats.enabled,
                    ) {
                        Ok(state) => Some(state),
                        Err(error) => {
                            rdp_log_line(format!(
                                "RDP drive: unable to map {}: {error}",
                                root.display()
                            ));
                            None
                        }
                    }
                });
            let mut pending_cmd: Option<RdpCommand> = None;
            let mut auto_unlock_state = if auto_unlock_policy.enabled {
                AutoUnlockState::CheckBlank {
                    deadline: Instant::now() + auto_unlock_policy.initial_delay,
                }
            } else {
                AutoUnlockState::Disabled
            };

            loop {
                tokio::select! {
                    biased;

                    read_result = tls_stream.read(&mut read_buf) => {
                        match read_result {
                            Ok(0) => {
                                rdp_log_line(format!(
                                    "RDP: server closed connection (attempt {})",
                                    attempt
                                ));
                                continue 'connection;
                            }
                            Ok(n) => {
                                debug_stats.read_bytes += n;
                                pdu_buf.extend_from_slice(&read_buf[..n]);

                                // Process all complete PDUs and advance in-place.
                                loop {
                                    let pdu_len = match get_pdu_length(&pdu_buf) {
                                        Some(len) if pdu_buf.len() >= len => len,
                                        _ => break,
                                    };

                                    let updates = if pdu_buf[0] == 0x03 {
                                        if is_demand_active_pdu(&pdu_buf[..pdu_len]) {
                                            let ca = build_confirm_active_pdu(user_channel_id, width, height);
                                            let _ = tls_stream.write_all(&ca).await;
                                            let ss = build_synchronize_sequence(user_channel_id);
                                            let _ = tls_stream.write_all(&ss).await;
                                            None
                                        } else if let Some(clip_channel_id) = cliprdr_channel_id
                                            && server_mcs_channel_id(&pdu_buf[..pdu_len])
                                                == Some(clip_channel_id)
                                        {
                                            let result = process_cliprdr_packet(
                                                &pdu_buf[..pdu_len],
                                                user_channel_id,
                                                clip_channel_id,
                                                &mut clipboard_state,
                                            );
                                            for response in result.responses {
                                                tls_stream.write_all(&response).await?;
                                            }
                                            if let Some(text) = result.remote_text {
                                                let _ = self.clipboard_tx.send(RdpClipboardEvent {
                                                    session_id: self.session_id.clone(),
                                                    text,
                                                });
                                            }
                                            None
                                        } else if let Some(drive_channel_id) = rdpdr_channel_id
                                            && server_mcs_channel_id(&pdu_buf[..pdu_len])
                                                == Some(drive_channel_id)
                                        {
                                            if let Some(state) = rdpdr_state.as_mut()
                                                && let Some((_, payload)) =
                                                    server_mcs_payload(&pdu_buf[..pdu_len])
                                            {
                                                let result = state.process_channel_payload(payload);
                                                for message in result.messages {
                                                    for response in build_static_channel_message_pdus(
                                                        user_channel_id,
                                                        drive_channel_id,
                                                        &message,
                                                    ) {
                                                        tls_stream.write_all(&response).await?;
                                                    }
                                                }
                                            }
                                            None
                                        } else {
                                            process_incoming_pdu(&pdu_buf[..pdu_len])
                                        }
                                    } else {
                                        process_fastpath_output(
                                            &pdu_buf[..pdu_len],
                                            &mut fastpath_fragments,
                                            &mut debug_stats,
                                        )
                                    };
                                    debug_stats.pdus += 1;

                                    if let Some(bitmap_updates) = updates {
                                        debug_stats.bitmap_rects += bitmap_updates.len();
                                        for update in &bitmap_updates {
                                            if update.compressed {
                                                debug_stats.compressed_rects += 1;
                                            } else {
                                                debug_stats.uncompressed_rects += 1;
                                            }
                                            match update.bpp {
                                                15 => debug_stats.bpp15_rects += 1,
                                                16 => debug_stats.bpp16_rects += 1,
                                                24 => debug_stats.bpp24_rects += 1,
                                                32 => debug_stats.bpp32_rects += 1,
                                                _ => {}
                                            }
                                            if update.bmp_width != update.width
                                                || update.bmp_height != update.height
                                            {
                                                debug_stats.record_size_mismatch(update);
                                            }
                                            if let Some(decoded) = decode_bitmap_data(
                                                update,
                                                &mut decode_scratch,
                                            ) {
                                                let mut frame = shared_frame_ref.lock();
                                                if let Some(rect) = apply_bitmap_to_buffer(
                                                    &mut frame.write_buf,
                                                    width,
                                                    height,
                                                    update,
                                                    decoded,
                                                ) {
                                                    frame.mark_dirty(rect);
                                                    debug_stats.applied_rects += 1;
                                                } else {
                                                    debug_stats.skipped_rects += 1;
                                                }
                                            } else {
                                                if update.compressed {
                                                    match decode_scratch.last_rle_status {
                                                        RleDecodeStatus::Partial => {
                                                            debug_stats.rle_partial += 1
                                                        }
                                                        RleDecodeStatus::Invalid => {
                                                            debug_stats.rle_invalid += 1
                                                        }
                                                        RleDecodeStatus::Ok => {}
                                                    }
                                                }
                                                debug_stats.skipped_rects += 1;
                                            }
                                        }
                                    }

                                    pdu_buf.advance(pdu_len);
                                }

                                if pdu_buf.len() > 512 * 1024 {
                                    pdu_buf.clear();
                                }
                            }
                            Err(e) => {
                                let kind = e.kind();
                                if kind == std::io::ErrorKind::ConnectionReset
                                    || kind == std::io::ErrorKind::BrokenPipe
                                    || kind == std::io::ErrorKind::UnexpectedEof
                                {
                                    rdp_log_line(format!(
                                        "RDP: connection error {:?}, attempting reconnect",
                                        kind
                                    ));
                                    continue 'connection;
                                }
                                return Err(anyhow::anyhow!("RDP read error: {}", e));
                            }
                        }
                    }

                    _ = frame_tick.tick() => {
                        let (dirty_area, framebuffer_area) = {
                            let frame = shared_frame_ref.lock();
                            (frame.dirty_area(), frame.width * frame.height)
                        };
                        if frame_pacer.should_flush(dirty_area, framebuffer_area) {
                            let events = shared_frame_ref.lock().drain_dirty_events(
                                &self.session_id,
                                if frame_policy.bandwidth_saving { 16 } else { 8 },
                                frame_policy.bandwidth_saving,
                                frame_policy.stream_pixels,
                            );
                            for event in events {
                                debug_stats.emitted_events += 1;
                                debug_stats.emitted_bytes += event.data.len();
                                let _ = self.frame_tx.send(event);
                            }
                        }
                        debug_stats.tick(&self.session_id);
                    }

                    _ = keep_awake_tick.tick(), if keep_awake_policy.enabled => {
                        let pdu = build_shift_pulse(user_channel_id);
                        tls_stream.write_all(&pdu).await?;
                        debug_stats.keep_awake_events += 1;
                    }

                    _ = auto_unlock_tick.tick(), if !matches!(auto_unlock_state, AutoUnlockState::Disabled | AutoUnlockState::Done) => {
                        match auto_unlock_state {
                            AutoUnlockState::CheckBlank { deadline } if Instant::now() >= deadline => {
                                if shared_frame_ref.lock().is_mostly_black() {
                                    tls_stream.write_all(&build_shift_pulse(user_channel_id)).await?;
                                    debug_stats.log_connect("auto-unlock wake pulse sent");
                                    auto_unlock_state = AutoUnlockState::WakeSent {
                                        deadline: Instant::now() + auto_unlock_policy.wake_delay,
                                    };
                                } else {
                                    auto_unlock_state = AutoUnlockState::Done;
                                }
                            }
                            AutoUnlockState::WakeSent { deadline } if Instant::now() >= deadline => {
                                if let Some(password) = self.profile.password.as_deref() {
                                    if let Some(pdu) = build_text_input_pdus(user_channel_id, password, true) {
                                        tls_stream.write_all(&pdu).await?;
                                        debug_stats.log_connect("auto-unlock credentials submitted");
                                    } else {
                                        debug_stats.log_connect("auto-unlock skipped: password contains unsupported characters");
                                    }
                                }
                                auto_unlock_state = AutoUnlockState::Done;
                            }
                            _ => {}
                        }
                    }

                    cmd = async {
                        if pending_cmd.is_some() {
                            pending_cmd.take()
                        } else {
                            command_rx.recv().await
                        }
                    } => {
                        match cmd {
                            Some(RdpCommand::KeyboardInput { scancode, is_pressed }) => {
                                frame_pacer.record_input();
                                let pdu = build_keyboard_pdu(user_channel_id, scancode, is_pressed);
                                let _ = tls_stream.write_all(&pdu).await;
                            }
                            Some(RdpCommand::MouseMove { x, y }) => {
                                frame_pacer.record_input();
                                let mut latest = (x, y);
                                loop {
                                    match command_rx.try_recv() {
                                        Ok(RdpCommand::MouseMove { x, y }) => {
                                            latest = (x, y);
                                        }
                                        Ok(other) => {
                                            pending_cmd = Some(other);
                                            break;
                                        }
                                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                                    }
                                }
                                let pdu = build_mouse_move_pdu(user_channel_id, latest.0, latest.1);
                                let _ = tls_stream.write_all(&pdu).await;
                            }
                            Some(RdpCommand::MouseInput { x, y, button, is_pressed }) => {
                                frame_pacer.record_input();
                                let pdu = build_mouse_button_pdu(user_channel_id, x, y, button, is_pressed);
                                let _ = tls_stream.write_all(&pdu).await;
                            }
                            Some(RdpCommand::SetClipboardText { text }) => {
                                clipboard_state.local_text = text;
                                if let Some(channel_id) = cliprdr_channel_id {
                                    for pdu in build_cliprdr_format_list(
                                        user_channel_id,
                                        channel_id,
                                    ) {
                                        tls_stream.write_all(&pdu).await?;
                                    }
                                }
                            }
                            Some(RdpCommand::RequestFrame { response_tx }) => {
                                debug_stats.frame_requests += 1;
                                let snapshot = shared_frame_ref
                                    .lock()
                                    .snapshot_for_request()
                                    .unwrap_or_else(|| Arc::new(Vec::new()));
                                if snapshot.is_empty() {
                                    debug_stats.frame_empty += 1;
                                } else {
                                    debug_stats.frame_hits += 1;
                                }
                                let _ = response_tx.send(snapshot);
                            }
                            Some(RdpCommand::Disconnect) => {
                                let shutdown = build_shutdown_pdu(user_channel_id);
                                let _ = tls_stream.write_all(&shutdown).await;
                                return Ok(());
                            }
                            None => { continue; }
                        }
                    }
                }
            }
        } // end 'connection loop
    }
}

// ─── Stream Abstraction ──────────────────────────────────────────────────────

enum RdpStream {
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
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
    // Negotiation Request: type=1, flags=0, length=8, protocols=TLS(0x01).
    //
    // CredSSP/NLA (0x02) is intentionally not advertised here. This client does
    // not implement the CredSSP handshake yet; advertising it allows Windows and
    // some xrdp configurations to select NLA, after which they reset the
    // connection when the client continues with the standard TLS RDP sequence.
    let neg_req: [u8; 8] = [0x01, 0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00];
    // LI counts: CR(1) + DST-REF(2) + SRC-REF(2) + CLASS(1) + cookie + neg_req
    let li = 6 + cookie_bytes.len() + neg_req.len();
    // TPKT length = TPKT header(4) + LI byte(1) + LI value
    let tpkt_len = 4 + 1 + li;

    let mut pdu = Vec::with_capacity(tpkt_len);
    pdu.push(0x03);
    pdu.push(0x00);
    pdu.extend_from_slice(&(tpkt_len as u16).to_be_bytes());
    pdu.push(li as u8);
    pdu.push(0xE0); // CR
    pdu.extend_from_slice(&[0x00, 0x00]); // DST-REF
    pdu.extend_from_slice(&[0x00, 0x00]); // SRC-REF
    pdu.push(0x00); // Class 0
    pdu.extend_from_slice(cookie_bytes);
    pdu.extend_from_slice(&neg_req);
    pdu
}

/// Build X.224 CR without negotiation request (for servers that don't support TLS).
fn build_x224_connection_request_plain(profile: &RdpProfile) -> Vec<u8> {
    let cookie = format!("Cookie: mstshash={}\r\n", profile.username);
    let cookie_bytes = cookie.as_bytes();
    let li = 6 + cookie_bytes.len();
    let tpkt_len = 4 + 1 + li;

    let mut pdu = Vec::with_capacity(tpkt_len);
    pdu.push(0x03);
    pdu.push(0x00);
    pdu.extend_from_slice(&(tpkt_len as u16).to_be_bytes());
    pdu.push(li as u8);
    pdu.push(0xE0); // CR
    pdu.extend_from_slice(&[0x00, 0x00]); // DST-REF
    pdu.extend_from_slice(&[0x00, 0x00]); // SRC-REF
    pdu.push(0x00); // Class 0
    pdu.extend_from_slice(cookie_bytes);
    pdu
}

fn parse_x224_confirm(data: &[u8]) -> anyhow::Result<u8> {
    // Minimum: TPKT header (4) + at least 1 byte X.224
    if data.len() < 7 {
        return Err(anyhow::anyhow!(
            "X.224 confirm too short ({} bytes)",
            data.len()
        ));
    }
    if data[0] != 0x03 {
        return Err(anyhow::anyhow!("Invalid TPKT version: 0x{:02X}", data[0]));
    }

    let tpkt_len = u16::from_be_bytes([data[2], data[3]]) as usize;

    // Check X.224 type byte (offset 5)
    let x224_type = data[5];
    if x224_type == 0xD0 {
        // Standard Connection Confirm (CC)
        // Look for negotiation response (type 0x02) at end of PDU
        if tpkt_len >= 15 && tpkt_len <= data.len() {
            let neg_start = tpkt_len - 8;
            if neg_start < data.len() && data[neg_start] == 0x02 {
                // TYPE_RDP_NEG_RSP
                let selected_protocol = data[neg_start + 4];
                // NLA/CredSSP (0x02) or RDSTLS (0x08): we don't implement
                // CredSSP. Return a clear, actionable error.
                if selected_protocol == 0x02 || selected_protocol == 0x08 {
                    return Err(anyhow::anyhow!(
                        "RDP server requires NLA/CredSSP authentication, which is not \
                         supported. Disable NLA on the server, or in xrdp.ini set \
                         security_layer=tls under [Globals]."
                    ));
                }
                return Ok(selected_protocol);
            }
        }
        // CC without negotiation response = plain RDP (protocol 0)
        return Ok(0);
    }

    // Some servers (like xrdp without proper config) send 0xF0 (Data TPDU)
    // or other unexpected responses — treat as negotiation failure so the
    // caller can retry with a plain (no-negotiation) X.224 CR.
    Err(anyhow::anyhow!(
        "X.224 negotiation failed (got 0x{:02X}). Retrying without TLS negotiation.",
        x224_type
    ))
}

fn build_mcs_connect_initial(profile: &RdpProfile, selected_protocol: u8) -> Vec<u8> {
    let width = profile.width;
    let height = profile.height;

    // Client Core Data (TS_UD_CS_CORE) - 234 bytes matching modern RDP clients
    let mut core = Vec::new();
    core.extend_from_slice(&0xC001u16.to_le_bytes()); // CS_CORE
    core.extend_from_slice(&234u16.to_le_bytes()); // length = 234
    core.extend_from_slice(&0x00080004u32.to_le_bytes()); // version RDP 5.0+
    core.extend_from_slice(&width.to_le_bytes());
    core.extend_from_slice(&height.to_le_bytes());
    core.extend_from_slice(&0xCA01u16.to_le_bytes()); // colorDepth
    core.extend_from_slice(&0xAA03u16.to_le_bytes()); // SASSequence
    core.extend_from_slice(&0x00000409u32.to_le_bytes()); // keyboard layout US
    core.extend_from_slice(&2600u32.to_le_bytes()); // clientBuild
    // clientName (32 bytes UTF-16LE)
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
    core.extend_from_slice(&(selected_protocol as u32).to_le_bytes()); // serverSelectedProtocol
    // Extended fields (18 bytes) - required by modern xrdp
    core.extend_from_slice(&1000u32.to_le_bytes()); // desktopPhysicalWidth
    core.extend_from_slice(&1000u32.to_le_bytes()); // desktopPhysicalHeight
    core.extend_from_slice(&0u16.to_le_bytes()); // desktopOrientation
    core.extend_from_slice(&100u32.to_le_bytes()); // desktopScaleFactor
    core.extend_from_slice(&100u32.to_le_bytes()); // deviceScaleFactor
    debug_assert_eq!(core.len(), 234);

    // Client Cluster Data (TS_UD_CS_CLUSTER) - 12 bytes
    let mut cluster = Vec::new();
    cluster.extend_from_slice(&0xC004u16.to_le_bytes()); // CS_CLUSTER
    cluster.extend_from_slice(&12u16.to_le_bytes()); // length
    cluster.extend_from_slice(&0x0000000Du32.to_le_bytes()); // flags: REDIRECTION_SUPPORTED | VERSION_3
    cluster.extend_from_slice(&0u32.to_le_bytes()); // redirectedSessionID

    // Client Security Data (TS_UD_CS_SEC) - 12 bytes
    let mut sec = Vec::new();
    sec.extend_from_slice(&0xC002u16.to_le_bytes());
    sec.extend_from_slice(&12u16.to_le_bytes());
    sec.extend_from_slice(&0x0000001Bu32.to_le_bytes()); // encryptionMethods
    sec.extend_from_slice(&0u32.to_le_bytes()); // extEncryptionMethods

    // Client Network Data (TS_UD_CS_NET) with clipboard and optional drive redirection.
    let drive_enabled = DriveRedirectionPolicy::from_profile(profile).enabled();
    let channel_count = if drive_enabled { 2u32 } else { 1u32 };
    let mut net = Vec::new();
    net.extend_from_slice(&0xC003u16.to_le_bytes());
    net.extend_from_slice(&(8u16 + channel_count as u16 * 12).to_le_bytes());
    net.extend_from_slice(&channel_count.to_le_bytes());
    net.extend_from_slice(CLIPRDR_CHANNEL_NAME);
    // INITIALIZED | ENCRYPT_RDP | COMPRESS_RDP | SHOW_PROTOCOL
    net.extend_from_slice(&0xC0A0_0000u32.to_le_bytes());
    if drive_enabled {
        net.extend_from_slice(RDPDR_CHANNEL_NAME);
        // INITIALIZED | ENCRYPT_RDP | COMPRESS_RDP
        net.extend_from_slice(&0xC080_0000u32.to_le_bytes());
    }

    let user_data = [&core[..], &cluster[..], &sec[..], &net[..]].concat();
    let gcc = build_gcc_wrapper(&user_data);
    build_mcs_ci_pdu(&gcc)
}

fn build_gcc_wrapper(user_data: &[u8]) -> Vec<u8> {
    let mut gcc = Vec::new();
    gcc.extend_from_slice(&[0x00, 0x05, 0x00, 0x14, 0x7C, 0x00, 0x01]);
    // ConnectData::connectPDU PER length
    // Overhead: 8 (conference fields) + 4 (Duca) + 2 (ud PER len) = 14
    let pdu_len = user_data.len() + 14;
    // PER 16-bit length encoding (high bit set for values > 127)
    gcc.push(0x80 | ((pdu_len >> 8) & 0x7F) as u8);
    gcc.push((pdu_len & 0xFF) as u8);
    // ConferenceCreateRequest fields
    gcc.extend_from_slice(&[0x00, 0x08, 0x00, 0x10, 0x00, 0x01, 0xC0, 0x00]);
    gcc.push(0x44);
    gcc.push(0x75);
    gcc.push(0x63);
    gcc.push(0x61); // "Duca"
    // userData PER length (16-bit)
    let ud_len = user_data.len();
    gcc.push(0x80 | ((ud_len >> 8) & 0x7F) as u8);
    gcc.push((ud_len & 0xFF) as u8);
    gcc.extend_from_slice(user_data);
    gcc
}

fn build_mcs_ci_pdu(gcc_data: &[u8]) -> Vec<u8> {
    let mut mcs = vec![
        0x04, 0x01, 0x01, // callingDomainSelector
        0x04, 0x01, 0x01, // calledDomainSelector
        0x01, 0x01, 0xFF, // upwardFlag
    ];
    // Parameters
    mcs.extend_from_slice(&build_domain_params([34, 2, 0, 1, 0, 1, 0xFFFF, 2]));
    mcs.extend_from_slice(&build_domain_params([1, 1, 1, 1, 0, 1, 0x420, 2]));
    mcs.extend_from_slice(&build_domain_params([
        0xFFFF, 0xFC17, 0xFFFF, 1, 0, 1, 0xFFFF, 2,
    ]));
    // userData
    mcs.push(0x04);
    ber_write_length(&mut mcs, gcc_data.len());
    mcs.extend_from_slice(gcc_data);

    let content_len = mcs.len();
    let mut final_pdu = Vec::new();
    final_pdu.push(0x7F);
    final_pdu.push(0x65);
    ber_write_length(&mut final_pdu, content_len);
    final_pdu.extend_from_slice(&mcs);

    wrap_tpkt_x224_data(&final_pdu)
}

fn build_domain_params(values: [u32; 8]) -> Vec<u8> {
    let mut content = Vec::new();
    for v in values {
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
        buf.push(1);
        buf.push(value as u8);
    } else if value <= 0x7FFF {
        buf.push(2);
        buf.push((value >> 8) as u8);
        buf.push(value as u8);
    } else if value <= 0x7FFFFF {
        buf.push(3);
        buf.push((value >> 16) as u8);
        buf.push((value >> 8) as u8);
        buf.push(value as u8);
    } else {
        buf.push(4);
        buf.push((value >> 24) as u8);
        buf.push((value >> 16) as u8);
        buf.push((value >> 8) as u8);
        buf.push(value as u8);
    }
}

fn ber_write_length(buf: &mut Vec<u8>, len: usize) {
    if len < 128 {
        buf.push(len as u8);
    } else if len < 256 {
        buf.push(0x81);
        buf.push(len as u8);
    } else {
        buf.push(0x82);
        buf.push((len >> 8) as u8);
        buf.push(len as u8);
    }
}

fn wrap_tpkt_x224_data(data: &[u8]) -> Vec<u8> {
    let total = 4 + 3 + data.len();
    let mut pdu = Vec::with_capacity(total);
    pdu.push(0x03);
    pdu.push(0x00);
    pdu.extend_from_slice(&(total as u16).to_be_bytes());
    pdu.push(0x02);
    pdu.push(0xF0);
    pdu.push(0x80);
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

fn build_client_info_pdu(user_channel_id: u16, profile: &RdpProfile, encryption_level: u32) -> Vec<u8> {
    let mut info = Vec::new();
    // Security header: only include SEC_INFO_PKT (0x40) when encryption is active.
    // With security_layer=tls and encryptionLevel=0, the security header must be
    // omitted — xrdp will disconnect with MCS Disconnect Provider Ultimatum if
    // it receives an unexpected security header in no-encryption mode.
    if encryption_level > 0 {
        info.extend_from_slice(&0x00000040u32.to_le_bytes()); // SEC_INFO_PKT
    }
    // TS_INFO_PACKET
    info.extend_from_slice(&0u32.to_le_bytes()); // CodePage
    info.extend_from_slice(&client_info_flags(profile).to_le_bytes());

    let domain = profile.domain.as_deref().unwrap_or("");
    let domain_utf16: Vec<u8> = domain
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();
    let username_utf16: Vec<u8> = profile
        .username
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();
    let password = profile.password.as_deref().unwrap_or("");
    let password_utf16: Vec<u8> = password
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();

    let alt_shell = profile
        .extra
        .get("alternate shell")
        .or_else(|| profile.extra.get("alternate_shell"))
        .map(|s| s.as_str())
        .unwrap_or("");
    let alt_shell_utf16: Vec<u8> = alt_shell
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();

    info.extend_from_slice(&(domain_utf16.len() as u16).to_le_bytes());
    info.extend_from_slice(&(username_utf16.len() as u16).to_le_bytes());
    info.extend_from_slice(&(password_utf16.len() as u16).to_le_bytes());
    info.extend_from_slice(&(alt_shell_utf16.len() as u16).to_le_bytes()); // AlternateShell length
    info.extend_from_slice(&0u16.to_le_bytes()); // WorkingDir length

    info.extend_from_slice(&domain_utf16);
    info.extend_from_slice(&[0, 0]);
    info.extend_from_slice(&username_utf16);
    info.extend_from_slice(&[0, 0]);
    info.extend_from_slice(&password_utf16);
    info.extend_from_slice(&[0, 0]);
    info.extend_from_slice(&alt_shell_utf16);
    info.extend_from_slice(&[0, 0]); // AlternateShell null
    info.extend_from_slice(&[0, 0]); // WorkingDir null

    // TS_EXTENDED_INFO_PACKET (required by xrdp 0.10+)
    info.extend_from_slice(&2u16.to_le_bytes()); // clientAddressFamily: AF_INET
    // cbClientAddress (including null terminator, in bytes)
    let client_addr = "localhost";
    let addr_utf16: Vec<u8> = client_addr
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();
    info.extend_from_slice(&((addr_utf16.len() + 2) as u16).to_le_bytes()); // +2 for null
    info.extend_from_slice(&addr_utf16);
    info.extend_from_slice(&[0, 0]); // null terminator
    // cbClientDir
    let client_dir = "";
    let dir_utf16: Vec<u8> = client_dir
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();
    info.extend_from_slice(&((dir_utf16.len() + 2) as u16).to_le_bytes());
    info.extend_from_slice(&dir_utf16);
    info.extend_from_slice(&[0, 0]);
    // clientTimeZone (172 bytes of zeros for default)
    info.extend_from_slice(&[0u8; 172]);
    // clientSessionId
    info.extend_from_slice(&0u32.to_le_bytes());
    // performanceFlags
    info.extend_from_slice(&0u32.to_le_bytes());

    wrap_mcs_send_data(user_channel_id, 1003, &info)
}

fn client_info_flags(profile: &RdpProfile) -> u32 {
    const INFO_MOUSE: u32 = 0x0000_0001;
    const INFO_DISABLECTRLALTDEL: u32 = 0x0000_0002;
    const INFO_AUTOLOGON: u32 = 0x0000_0008;
    const INFO_UNICODE: u32 = 0x0000_0010;
    const INFO_MAXIMIZESHELL: u32 = 0x0000_0020;
    const INFO_LOGONNOTIFY: u32 = 0x0000_0040;
    const INFO_ENABLEWINDOWSKEY: u32 = 0x0000_0100;
    const INFO_LOGONERRORS: u32 = 0x0001_0000;

    let mut flags = INFO_MOUSE
        | INFO_DISABLECTRLALTDEL
        | INFO_UNICODE
        | INFO_LOGONNOTIFY
        | INFO_ENABLEWINDOWSKEY
        | INFO_LOGONERRORS;
    // INFO_MAXIMIZESHELL only when an alternate shell is set (PSM/RemoteApp scenarios)
    if profile.extra.contains_key("alternate shell")
        || profile.extra.contains_key("alternate_shell")
    {
        flags |= INFO_MAXIMIZESHELL;
    }
    if !profile.username.is_empty()
        && profile
            .password
            .as_deref()
            .is_some_and(|password| !password.is_empty())
    {
        flags |= INFO_AUTOLOGON;
    }
    flags
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

/// Parse the encryptionLevel from the SC_SECURITY (0x0C02) block inside the
/// MCS Connect Response GCC user data.
///
/// Returns 0 if no SC_SECURITY block is found or encryption is disabled.
/// A non-zero value means RDP security layer is active and the Client Info
/// PDU must carry the SEC_INFO_PKT (0x40) security header.
fn parse_server_encryption_level(data: &[u8]) -> u32 {
    // SC_SECURITY block: type=0x0C02 (LE) | length(2) | encryptionMethod(4) | encryptionLevel(4)
    for offset in 0..data.len().saturating_sub(12) {
        if u16::from_le_bytes([data[offset], data[offset + 1]]) != 0x0C02 {
            continue;
        }
        let block_len = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) as usize;
        if block_len < 12 || offset + block_len > data.len() {
            continue;
        }
        // encryptionLevel is at offset+8 (after type + length + encryptionMethod)
        return u32::from_le_bytes([
            data[offset + 8],
            data[offset + 9],
            data[offset + 10],
            data[offset + 11],
        ]);
    }
    // No SC_SECURITY found — default to 0 (no encryption, no security header needed)
    0
}

fn parse_server_static_channel_id(data: &[u8], channel_index: usize) -> Option<u16> {
    for offset in 0..data.len().saturating_sub(8) {
        if u16::from_le_bytes([data[offset], data[offset + 1]]) != 0x0C03 {
            continue;
        }
        let block_len = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) as usize;
        if block_len < 8 || offset + block_len > data.len() {
            continue;
        }
        let channel_count = u16::from_le_bytes([data[offset + 6], data[offset + 7]]) as usize;
        if channel_index >= channel_count || 8 + channel_count * 2 > block_len {
            continue;
        }
        let channel_offset = offset + 8 + channel_index * 2;
        return Some(u16::from_le_bytes([
            data[channel_offset],
            data[channel_offset + 1],
        ]));
    }
    None
}

fn server_mcs_payload(data: &[u8]) -> Option<(u16, &[u8])> {
    if data.len() < 14 || data[0] != 0x03 || data.get(5).copied() != Some(0xF0) {
        return None;
    }
    let packet_len = u16::from_be_bytes([data[2], data[3]]) as usize;
    if packet_len > data.len() || data.get(7).copied() != Some(0x68) {
        return None;
    }

    let channel_id = u16::from_be_bytes([data[10], data[11]]);
    let mut offset = 13;
    let first_len = *data.get(offset)?;
    let payload_len = if first_len & 0x80 == 0 {
        offset += 1;
        first_len as usize
    } else {
        let second_len = *data.get(offset + 1)?;
        offset += 2;
        (((first_len & 0x7F) as usize) << 8) | second_len as usize
    };
    if offset + payload_len > packet_len {
        return None;
    }
    Some((channel_id, &data[offset..offset + payload_len]))
}

fn server_mcs_channel_id(data: &[u8]) -> Option<u16> {
    server_mcs_payload(data).map(|(channel_id, _)| channel_id)
}

#[derive(Default)]
struct ClipboardProcessResult {
    responses: Vec<Vec<u8>>,
    remote_text: Option<String>,
}

fn process_cliprdr_packet(
    packet: &[u8],
    user_channel_id: u16,
    cliprdr_channel_id: u16,
    state: &mut ClipboardChannelState,
) -> ClipboardProcessResult {
    let mut result = ClipboardProcessResult::default();
    let Some((_, payload)) = server_mcs_payload(packet) else {
        return result;
    };
    if payload.len() < 8 {
        return result;
    }

    let total_length =
        u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let channel_flags = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    if channel_flags & CHANNEL_FLAG_FIRST != 0 {
        state.fragment_data.clear();
        state.fragment_total_length = total_length;
    }
    state.fragment_data.extend_from_slice(&payload[8..]);
    if channel_flags & CHANNEL_FLAG_LAST == 0 {
        return result;
    }

    let expected = state.fragment_total_length.min(state.fragment_data.len());
    let message = state.fragment_data[..expected].to_vec();
    state.fragment_data.clear();
    state.fragment_total_length = 0;
    if message.len() < 8 {
        return result;
    }

    let message_type = u16::from_le_bytes([message[0], message[1]]);
    let message_flags = u16::from_le_bytes([message[2], message[3]]);
    let data_length = u32::from_le_bytes([message[4], message[5], message[6], message[7]]) as usize;
    if 8 + data_length > message.len() {
        return result;
    }
    let data = &message[8..8 + data_length];
    if state.debug {
        rdp_log_line(format!(
            "RDP DEBUG clipboard: type={} flags=0x{message_flags:04x} data={}B",
            message_type,
            data.len()
        ));
    }

    match message_type {
        CB_MONITOR_READY => {
            result.responses.extend(build_cliprdr_capabilities(
                user_channel_id,
                cliprdr_channel_id,
            ));
            result.responses.extend(build_cliprdr_format_list(
                user_channel_id,
                cliprdr_channel_id,
            ));
        }
        CB_FORMAT_LIST => {
            result.responses.extend(build_cliprdr_message_pdus(
                user_channel_id,
                cliprdr_channel_id,
                CB_FORMAT_LIST_RESPONSE,
                CB_RESPONSE_OK,
                &[],
            ));
            if let Some(format_id) = preferred_clipboard_format(data) {
                state.pending_remote_format = Some(format_id);
                result.responses.extend(build_cliprdr_message_pdus(
                    user_channel_id,
                    cliprdr_channel_id,
                    CB_FORMAT_DATA_REQUEST,
                    0,
                    &format_id.to_le_bytes(),
                ));
            }
        }
        CB_FORMAT_DATA_REQUEST => {
            if data.len() >= 4 {
                let format_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                let response_data = encode_local_clipboard(&state.local_text, format_id);
                let response_flag = if response_data.is_some() {
                    CB_RESPONSE_OK
                } else {
                    CB_RESPONSE_FAIL
                };
                result.responses.extend(build_cliprdr_message_pdus(
                    user_channel_id,
                    cliprdr_channel_id,
                    CB_FORMAT_DATA_RESPONSE,
                    response_flag,
                    response_data.as_deref().unwrap_or_default(),
                ));
            }
        }
        CB_FORMAT_DATA_RESPONSE if message_flags & CB_RESPONSE_OK != 0 => {
            if let Some(format_id) = state.pending_remote_format.take() {
                result.remote_text = decode_remote_clipboard(data, format_id);
            }
        }
        CB_CLIP_CAPS | CB_FORMAT_LIST_RESPONSE | CB_FORMAT_DATA_RESPONSE => {}
        _ => {}
    }

    result
}

fn preferred_clipboard_format(data: &[u8]) -> Option<u32> {
    let mut offset = 0usize;
    let mut fallback = None;
    while offset + 4 <= data.len() {
        let format_id = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        if format_id == CF_UNICODETEXT {
            return Some(format_id);
        }
        if format_id == CF_TEXT {
            fallback = Some(format_id);
        }
        offset += 4;

        // Long format names are null-terminated UTF-16 strings. Empty names,
        // which are standard for predefined clipboard formats, occupy 2 bytes.
        while offset + 1 < data.len() {
            let end = data[offset] == 0 && data[offset + 1] == 0;
            offset += 2;
            if end {
                break;
            }
        }
    }
    fallback
}

fn encode_local_clipboard(text: &str, format_id: u32) -> Option<Vec<u8>> {
    match format_id {
        CF_UNICODETEXT => {
            let mut encoded = Vec::with_capacity((text.len() + 1) * 2);
            for code_unit in text.encode_utf16().chain(std::iter::once(0)) {
                encoded.extend_from_slice(&code_unit.to_le_bytes());
            }
            Some(encoded)
        }
        CF_TEXT => {
            let mut encoded = text.as_bytes().to_vec();
            encoded.push(0);
            Some(encoded)
        }
        _ => None,
    }
}

fn decode_remote_clipboard(data: &[u8], format_id: u32) -> Option<String> {
    match format_id {
        CF_UNICODETEXT => {
            let units = data
                .chunks_exact(2)
                .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
                .take_while(|unit| *unit != 0)
                .collect::<Vec<_>>();
            String::from_utf16(&units).ok()
        }
        CF_TEXT => Some(
            String::from_utf8_lossy(data.split(|byte| *byte == 0).next().unwrap_or_default())
                .into_owned(),
        ),
        _ => None,
    }
}

fn build_cliprdr_capabilities(user_id: u16, channel_id: u16) -> Vec<Vec<u8>> {
    let mut data = Vec::with_capacity(16);
    data.extend_from_slice(&1u16.to_le_bytes()); // cCapabilitiesSets
    data.extend_from_slice(&0u16.to_le_bytes());
    data.extend_from_slice(&1u16.to_le_bytes()); // CB_CAPSTYPE_GENERAL
    data.extend_from_slice(&12u16.to_le_bytes());
    data.extend_from_slice(&2u32.to_le_bytes()); // CB_CAPS_VERSION_2
    data.extend_from_slice(&CB_USE_LONG_FORMAT_NAMES.to_le_bytes());
    build_cliprdr_message_pdus(user_id, channel_id, CB_CLIP_CAPS, 0, &data)
}

fn build_cliprdr_format_list(user_id: u16, channel_id: u16) -> Vec<Vec<u8>> {
    let mut data = Vec::with_capacity(6);
    data.extend_from_slice(&CF_UNICODETEXT.to_le_bytes());
    data.extend_from_slice(&0u16.to_le_bytes()); // empty Unicode format name
    build_cliprdr_message_pdus(user_id, channel_id, CB_FORMAT_LIST, 0, &data)
}

fn build_cliprdr_message_pdus(
    user_id: u16,
    channel_id: u16,
    message_type: u16,
    message_flags: u16,
    data: &[u8],
) -> Vec<Vec<u8>> {
    let mut message = Vec::with_capacity(8 + data.len());
    message.extend_from_slice(&message_type.to_le_bytes());
    message.extend_from_slice(&message_flags.to_le_bytes());
    message.extend_from_slice(&(data.len() as u32).to_le_bytes());
    message.extend_from_slice(data);

    build_static_channel_message_pdus(user_id, channel_id, &message)
}

fn build_static_channel_message_pdus(
    user_id: u16,
    channel_id: u16,
    message: &[u8],
) -> Vec<Vec<u8>> {
    let total_length = message.len() as u32;
    let chunks = message.chunks(1600).collect::<Vec<_>>();
    let chunk_count = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| {
            let mut flags = 0u32;
            if index == 0 {
                flags |= CHANNEL_FLAG_FIRST;
            }
            if index + 1 == chunk_count {
                flags |= CHANNEL_FLAG_LAST;
            }
            let mut channel_data = Vec::with_capacity(8 + chunk.len());
            channel_data.extend_from_slice(&total_length.to_le_bytes());
            channel_data.extend_from_slice(&flags.to_le_bytes());
            channel_data.extend_from_slice(chunk);
            wrap_mcs_send_data(user_id, channel_id, &channel_data)
        })
        .collect()
}

/// Determine the total length of the next PDU in the buffer.
/// Returns None if not enough data to determine length.
/// Supports TPKT (0x03 header) and FastPath (other first bytes).
fn get_pdu_length(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }

    if buf[0] == 0x03 {
        // TPKT: 4-byte header with 2-byte BE length at offset 2
        if buf.len() < 4 {
            return None;
        }
        let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        if len < 4 {
            // Invalid TPKT — skip 1 byte to recover
            return Some(1);
        }
        Some(len)
    } else {
        // FastPath: length in byte 1 (and optionally byte 2)
        if buf.len() < 2 {
            return None;
        }
        if (buf[1] & 0x80) == 0 {
            // 1-byte length
            let len = buf[1] as usize;
            if len < 2 {
                return Some(2);
            }
            Some(len)
        } else {
            // 2-byte length
            if buf.len() < 3 {
                return None;
            }
            let len = (((buf[1] & 0x7F) as usize) << 8) | (buf[2] as usize);
            if len < 3 {
                return Some(3);
            }
            Some(len)
        }
    }
}

// ─── Licensing Exchange ───────────────────────────────────────────────────────
//
// MS-RDPBCGR §5.4.4 specifies that after the Client Info PDU the server MUST
// send a licensing PDU before the Demand Active PDU.  xrdp in "no-license"
// mode sends a Server License Error PDU (STATUS_VALID_CLIENT / 0xFF03) which
// acts as "no license required".  Full Windows servers send a longer exchange.
//
// Security header byte at offset 12 of the inner payload identifies the PDU:
//   0x0080 = SEC_LICENSE_PKT
//
// bMsgType inside the license packet:
//   0x01 = LICENSE_REQUEST           (server → client, needs reply)
//   0x12 = PLATFORM_CHALLENGE        (server → client, needs reply)
//   0x03 = NEW_LICENSE               (server → client, terminal)
//   0x04 = UPGRADE_LICENSE           (server → client, terminal)
//   0xFF = ERROR_ALERT               (server → client, may be terminal)
//
// We respond to LICENSE_REQUEST with a Client New License Request and to
// PLATFORM_CHALLENGE with a Client Platform Challenge Response so that both
// xrdp and Windows accept us without requiring NLA.

const SEC_LICENSE_PKT: u16 = 0x0080;
const LICENSE_REQUEST: u8 = 0x01;
const PLATFORM_CHALLENGE: u8 = 0x12;
const NEW_LICENSE: u8 = 0x03;
const UPGRADE_LICENSE: u8 = 0x04;
const LICENSE_ERROR_ALERT: u8 = 0xFF;

/// Extract the security header flags and licensing payload from a TPKT/MCS
/// packet. Returns `(sec_flags, license_payload)` if the structure looks like
/// a licensing PDU, otherwise `None`.
fn extract_license_payload(data: &[u8]) -> Option<(u16, &[u8])> {
    // Minimum: TPKT(4) + X224(3) + MCS SendDataIndication(7) + sec_hdr(4)
    if data.len() < 18 || data[0] != 0x03 {
        return None;
    }
    let tpkt_len = u16::from_be_bytes([data[2], data[3]]) as usize;
    if tpkt_len > data.len() || data.get(5).copied() != Some(0xF0) {
        return None;
    }
    // MCS SendDataIndication tag = 0x68
    if data.get(7).copied() != Some(0x68) {
        return None;
    }
    // Skip: MCS tag(1) + initiator(2) + channelId(2) + dataPriority(1) = 6 bytes past offset 7
    let mut offset = 13;
    // MCS length field (1 or 2 bytes)
    let len_byte = *data.get(offset)?;
    offset += if len_byte & 0x80 != 0 { 2 } else { 1 };

    // Security header: 4 bytes (flags u16 LE + flagsHi u16 LE)
    if offset + 4 > data.len() {
        return None;
    }
    let sec_flags = u16::from_le_bytes([data[offset], data[offset + 1]]);
    offset += 4;

    if sec_flags & SEC_LICENSE_PKT == 0 {
        return None;
    }

    Some((sec_flags, &data[offset..tpkt_len.min(data.len())]))
}

/// Returns the bMsgType from a license PDU payload (first byte after the
/// security header), or `None` if the payload is too short.
fn license_msg_type(payload: &[u8]) -> Option<u8> {
    payload.first().copied()
}

/// Build a minimal Client New License Request (LICENSE_REQUEST response).
/// We send a null/empty request which is sufficient for xrdp and causes full
/// Windows to proceed to the PLATFORM_CHALLENGE step.
fn build_client_new_license_request(user_channel_id: u16) -> Vec<u8> {
    // TS_LICENSE_PDU: bMsgType(1) + flags(1) + wMsgSize(2) + ClientRandom(32)
    // + PreMasterSecret length(4=0) + EncryptedPreMasterSecret length(4=0)
    // + ClientUserName cbbString(4) + ClientMachineName cbbString(4)
    let mut lic = Vec::new();
    lic.push(0x13); // bMsgType = CLIENT_NEW_LICENSE_REQUEST
    lic.push(0x00); // flags
    // wMsgSize: we fill this in after
    let size_offset = lic.len();
    lic.extend_from_slice(&0u16.to_le_bytes()); // placeholder
    // ClientRandom (32 bytes of zeros — acceptable for null license exchange)
    lic.extend_from_slice(&[0u8; 32]);
    // ConnectFlags (4 bytes)
    lic.extend_from_slice(&0u32.to_le_bytes());
    // EncryptedPreMasterSecret: length (4) + data (0)
    lic.extend_from_slice(&0u32.to_le_bytes());
    // LicensingBlobType (2) + cbEncryptedBlob (2) + blob (0)
    lic.extend_from_slice(&0u16.to_le_bytes()); // LicensingBlobType
    lic.extend_from_slice(&0u16.to_le_bytes()); // cbEncryptedBlob
    // ClientUserName: cbString(2) + string
    let username = b"portix\0";
    lic.extend_from_slice(&(username.len() as u16).to_le_bytes());
    lic.extend_from_slice(username);
    // ClientMachineName: cbString(2) + string
    let machine = b"PORTIX\0";
    lic.extend_from_slice(&(machine.len() as u16).to_le_bytes());
    lic.extend_from_slice(machine);

    // Patch wMsgSize
    let msg_size = lic.len() as u16;
    lic[size_offset..size_offset + 2].copy_from_slice(&msg_size.to_le_bytes());

    // Wrap in security header (SEC_LICENSE_PKT = 0x0080) + MCS + TPKT
    build_license_pdu(user_channel_id, &lic)
}

/// Build a Client Platform Challenge Response.  The actual cryptography is
/// skipped (zeros) which is fine for xrdp and causes Windows to respond with
/// a NEW_LICENSE or ERROR_ALERT that we then treat as "licensing done".
fn build_client_platform_challenge_response(user_channel_id: u16) -> Vec<u8> {
    let mut lic = Vec::new();
    lic.push(0x15); // bMsgType = CLIENT_PLATFORM_CHALLENGE_RESPONSE
    lic.push(0x00); // flags
    let size_offset = lic.len();
    lic.extend_from_slice(&0u16.to_le_bytes()); // wMsgSize placeholder
    // EncryptedPlatformChallengeResponse blob (type 0, len 0)
    lic.extend_from_slice(&0u16.to_le_bytes()); // BlobType
    lic.extend_from_slice(&0u16.to_le_bytes()); // cbEncryptedBlob
    // EncryptedHWID blob (type 0, len 0)
    lic.extend_from_slice(&0u16.to_le_bytes());
    lic.extend_from_slice(&0u16.to_le_bytes());
    // MACData (16 zeros)
    lic.extend_from_slice(&[0u8; 16]);

    let msg_size = lic.len() as u16;
    lic[size_offset..size_offset + 2].copy_from_slice(&msg_size.to_le_bytes());

    build_license_pdu(user_channel_id, &lic)
}

/// Wrap a raw license PDU body in SEC_LICENSE_PKT + MCS SendDataRequest + TPKT.
fn build_license_pdu(user_channel_id: u16, body: &[u8]) -> Vec<u8> {
    // Security header: SEC_LICENSE_PKT(0x0080) + flagsHi(0x0000)
    let mut payload = Vec::with_capacity(4 + body.len());
    payload.extend_from_slice(&0x0080u16.to_le_bytes()); // secFlags
    payload.extend_from_slice(&0x0000u16.to_le_bytes()); // secFlagsHi
    payload.extend_from_slice(body);
    wrap_mcs_send_data(user_channel_id, 1003, &payload)
}

/// Check whether a licensing ERROR_ALERT represents a terminal "no license
/// required" / "valid client" status (i.e. we can proceed to Demand Active).
fn is_license_error_terminal(payload: &[u8]) -> bool {
    // Payload layout after bMsgType:
    //   flags(1) + wMsgSize(2) + dwErrorCode(4) + dwStateTransition(4) + blob
    // dwStateTransition == 0x00000002 means ST_NO_TRANSITION (done)
    // dwStateTransition == 0x00000004 means ST_TOTAL_ABORT (error, but we
    //   still treat it as "licensing done" because xrdp uses TOTAL_ABORT with
    //   STATUS_VALID_CLIENT to signal "no license needed").
    if payload.len() < 12 {
        return true; // too short to parse — assume terminal
    }
    let state_transition = u32::from_le_bytes([
        payload[8],
        payload[9],
        payload[10],
        payload[11],
    ]);
    // 0x01 = ST_PUSH_LICENSE  (not terminal — more packets coming)
    state_transition != 0x00000001
}

/// Drive the licensing exchange and return `true` once the Demand Active PDU
/// is received (or `false` on timeout / server close).
///
/// The function reads packets in a loop:
///   - Licensing packets are handled / replied-to and the loop continues.
///   - Any packet that contains a Demand Active PDU exits with `true`.
///   - If 60 consecutive packets pass without a Demand Active, we give up.
async fn wait_for_demand_active(
    stream: &mut RdpStream,
    buf: &mut Vec<u8>,
    user_channel_id: u16,
    debug: &RdpDebugStats,
) -> anyhow::Result<bool> {
    // Max read iterations: generous enough for a full Windows licensing round-
    // trip (typically 4–6 packets) plus some slack for slow links.
    const MAX_ITERS: usize = 60;
    const READ_TIMEOUT: Duration = Duration::from_secs(15);

    for i in 0..MAX_ITERS {
        let n = tokio::time::timeout(READ_TIMEOUT, stream.read(buf))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Timed out waiting for server response after Client Info PDU \
                     (iteration {i}). Ensure NLA is disabled on the RDP server."
                )
            })??;

        if n == 0 {
            return Ok(false);
        }

        let packet = &buf[..n];

        // ── Fast path: Demand Active arrives ────────────────────────────────
        if contains_demand_active(packet) {
            debug.log_connect(format!("licensing done: demand active received at iter {i}"));
            return Ok(true);
        }

        // ── Licensing PDU? ───────────────────────────────────────────────────
        if let Some((_sec_flags, lic_payload)) = extract_license_payload(packet) {
            match license_msg_type(lic_payload) {
                Some(LICENSE_REQUEST) => {
                    debug.log_connect("licensing: <- LICENSE_REQUEST, sending new license request");
                    let resp = build_client_new_license_request(user_channel_id);
                    stream.write_all(&resp).await?;
                }
                Some(PLATFORM_CHALLENGE) => {
                    debug.log_connect("licensing: <- PLATFORM_CHALLENGE, sending challenge response");
                    let resp = build_client_platform_challenge_response(user_channel_id);
                    stream.write_all(&resp).await?;
                }
                Some(NEW_LICENSE) | Some(UPGRADE_LICENSE) => {
                    // Server granted a license — next packet should be Demand Active
                    debug.log_connect("licensing: <- NEW/UPGRADE_LICENSE, licensing complete");
                }
                Some(LICENSE_ERROR_ALERT) => {
                    // xrdp sends STATUS_VALID_CLIENT (0xFF03) here — means "no license needed"
                    if is_license_error_terminal(lic_payload) {
                        debug.log_connect("licensing: <- ERROR_ALERT (terminal), licensing complete");
                    } else {
                        debug.log_connect("licensing: <- ERROR_ALERT (non-terminal), continuing");
                    }
                }
                Some(other) => {
                    debug.log_connect(format!("licensing: <- unknown msg_type=0x{other:02X}, continuing"));
                }
                None => {
                    debug.log_connect("licensing: empty payload, continuing");
                }
            }
            // Loop back to read next packet regardless of license type
            continue;
        }

        // ── Not a licensing PDU and not a Demand Active: could be an early
        //    Deactivate-All or similar — just skip and keep waiting.
        debug.log_connect(format!(
            "licensing wait: non-license non-demand packet ({n} bytes) at iter {i}, skipping"
        ));
    }

    Ok(false)
}

fn contains_demand_active(data: &[u8]) -> bool {
    let mut offset = 0;
    while offset < data.len() {
        let Some(pdu_len) = get_pdu_length(&data[offset..]) else {
            break;
        };
        if pdu_len == 0 || offset + pdu_len > data.len() {
            break;
        }
        if is_demand_active_pdu(&data[offset..offset + pdu_len]) {
            return true;
        }
        offset += pdu_len;
    }
    false
}

fn is_demand_active_pdu(data: &[u8]) -> bool {
    share_control_pdu_type(data) == Some(0x01)
}

/// Read the Share Control PDU type from a complete server TPKT packet.
///
/// Bitmap compression bytes are arbitrary and may contain `11 00`, so searching
/// the whole packet for the Demand Active marker can drop valid screen updates.
fn share_control_pdu_type(data: &[u8]) -> Option<u16> {
    if data.len() < 18 || data[0] != 0x03 {
        return None;
    }

    let packet_len = u16::from_be_bytes([data[2], data[3]]) as usize;
    if packet_len > data.len() || data.get(5).copied() != Some(0xF0) {
        return None;
    }

    let mut offset = 7;
    if data.get(offset).copied() != Some(0x68) {
        return None;
    }
    offset += 1;

    // MCS SendDataIndication: initiator, channelId, dataPriority/segmentation.
    offset = offset.checked_add(5)?;
    let length_byte = *data.get(offset)?;
    offset += if length_byte & 0x80 != 0 { 2 } else { 1 };

    if offset + 6 > packet_len {
        return None;
    }
    let share_control_type = u16::from_le_bytes([data[offset + 2], data[offset + 3]]);
    Some(share_control_type & 0x000F)
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
    // Keep bitmap compression disabled for now. The custom RLE decoder is not
    // complete enough for all xrdp/Fedora update patterns and can produce black
    // horizontal artifacts. On a local/LAN connection, uncompressed bitmap
    // updates are a better correctness tradeoff until the decoder is replaced
    // with a full MS-RDPBCGR compatible implementation.
    bitmap_cap.extend_from_slice(&0u16.to_le_bytes()); // bitmapCompressionFlag
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
    let all_caps = [
        &general_cap[..],
        &bitmap_cap[..],
        &order_cap[..],
        &input_cap[..],
    ]
    .concat();

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
    ctrl.push(0);
    ctrl.push(1);
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
    req.push(0);
    req.push(1);
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
    font.push(0);
    font.push(1);
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
    pdu.push(0);
    pdu.push(1);
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

fn build_shift_pulse(user_channel_id: u16) -> Vec<u8> {
    let mut pdus = build_keyboard_pdu(user_channel_id, 0x2A, true);
    pdus.extend(build_keyboard_pdu(user_channel_id, 0x2A, false));
    pdus
}

fn build_text_input_pdus(user_channel_id: u16, text: &str, press_enter: bool) -> Option<Vec<u8>> {
    let mut pdus = Vec::new();
    for character in text.chars() {
        let (scancode, shift) = ascii_scancode(character)?;
        if shift {
            pdus.extend(build_keyboard_pdu(user_channel_id, 0x2A, true));
        }
        pdus.extend(build_keyboard_pdu(user_channel_id, scancode, true));
        pdus.extend(build_keyboard_pdu(user_channel_id, scancode, false));
        if shift {
            pdus.extend(build_keyboard_pdu(user_channel_id, 0x2A, false));
        }
    }
    if press_enter {
        pdus.extend(build_keyboard_pdu(user_channel_id, 0x1C, true));
        pdus.extend(build_keyboard_pdu(user_channel_id, 0x1C, false));
    }
    Some(pdus)
}

fn ascii_scancode(character: char) -> Option<(u16, bool)> {
    if character.is_ascii_uppercase() {
        return ascii_scancode(character.to_ascii_lowercase()).map(|(code, _)| (code, true));
    }

    let mapping = match character {
        '!' => (0x02, true),
        '@' => (0x03, true),
        '#' => (0x04, true),
        '$' => (0x05, true),
        '%' => (0x06, true),
        '^' => (0x07, true),
        '&' => (0x08, true),
        '*' => (0x09, true),
        '(' => (0x0A, true),
        ')' => (0x0B, true),
        '_' => (0x0C, true),
        '+' => (0x0D, true),
        '{' => (0x1A, true),
        '}' => (0x1B, true),
        '|' => (0x2B, true),
        ':' => (0x27, true),
        '"' => (0x28, true),
        '~' => (0x29, true),
        '<' => (0x33, true),
        '>' => (0x34, true),
        '?' => (0x35, true),
        '1' => (0x02, false),
        '2' => (0x03, false),
        '3' => (0x04, false),
        '4' => (0x05, false),
        '5' => (0x06, false),
        '6' => (0x07, false),
        '7' => (0x08, false),
        '8' => (0x09, false),
        '9' => (0x0A, false),
        '0' => (0x0B, false),
        '-' => (0x0C, false),
        '=' => (0x0D, false),
        'q' => (0x10, false),
        'w' => (0x11, false),
        'e' => (0x12, false),
        'r' => (0x13, false),
        't' => (0x14, false),
        'y' => (0x15, false),
        'u' => (0x16, false),
        'i' => (0x17, false),
        'o' => (0x18, false),
        'p' => (0x19, false),
        '[' => (0x1A, false),
        ']' => (0x1B, false),
        'a' => (0x1E, false),
        's' => (0x1F, false),
        'd' => (0x20, false),
        'f' => (0x21, false),
        'g' => (0x22, false),
        'h' => (0x23, false),
        'j' => (0x24, false),
        'k' => (0x25, false),
        'l' => (0x26, false),
        ';' => (0x27, false),
        '\'' => (0x28, false),
        '`' => (0x29, false),
        '\\' => (0x2B, false),
        'z' => (0x2C, false),
        'x' => (0x2D, false),
        'c' => (0x2E, false),
        'v' => (0x2F, false),
        'b' => (0x30, false),
        'n' => (0x31, false),
        'm' => (0x32, false),
        ',' => (0x33, false),
        '.' => (0x34, false),
        '/' => (0x35, false),
        ' ' => (0x39, false),
        _ => return None,
    };
    Some(mapping)
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
        (MouseButton::Left, true) => 0x8000 | 0x1000, // PTRFLAGS_DOWN | PTRFLAGS_BUTTON1
        (MouseButton::Left, false) => 0x1000,
        (MouseButton::Right, true) => 0x8000 | 0x2000, // PTRFLAGS_DOWN | PTRFLAGS_BUTTON2
        (MouseButton::Right, false) => 0x2000,
        (MouseButton::Middle, true) => 0x8000 | 0x4000, // PTRFLAGS_DOWN | PTRFLAGS_BUTTON3
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
    pdu.push(0);
    pdu.push(1);
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
    pdu.push(0);
    pdu.push(1);
    pdu.extend_from_slice(&0u16.to_le_bytes());
    pdu.push(36); // PDUTYPE2_SHUTDOWN_REQUEST
    pdu.push(0);
    pdu.extend_from_slice(&0u16.to_le_bytes());

    wrap_mcs_send_data(user_channel_id, 1003, &pdu)
}

// ─── Bitmap Processing ───────────────────────────────────────────────────────

struct BitmapUpdate<'a> {
    x: u16,
    y: u16,
    width: u16,
    height: u16,
    bpp: u16,
    /// The actual bitmap width in the compressed stream (may differ from dest width due to padding)
    bmp_width: u16,
    /// The actual bitmap height in the compressed stream
    bmp_height: u16,
    compressed: bool,
    data: Cow<'a, [u8]>,
}

impl<'a> BitmapUpdate<'a> {
    fn into_owned(self) -> BitmapUpdate<'static> {
        BitmapUpdate {
            x: self.x,
            y: self.y,
            width: self.width,
            height: self.height,
            bpp: self.bpp,
            bmp_width: self.bmp_width,
            bmp_height: self.bmp_height,
            compressed: self.compressed,
            data: Cow::Owned(self.data.into_owned()),
        }
    }
}

#[derive(Default)]
struct DecodeScratch {
    rle_work: Vec<u8>,
    rle_flipped: Vec<u8>,
    last_rle_status: RleDecodeStatus,
}

struct DecodedBitmap<'a> {
    data: &'a [u8],
    top_down: bool,
    stride: usize,
}

#[derive(Default)]
struct FastPathFragmentState {
    code: u8,
    data: Vec<u8>,
    active: bool,
}

const CLIPRDR_CHANNEL_NAME: &[u8; 8] = b"cliprdr\0";
const RDPDR_CHANNEL_NAME: &[u8; 8] = b"rdpdr\0\0\0";
const CHANNEL_FLAG_FIRST: u32 = 0x0000_0001;
const CHANNEL_FLAG_LAST: u32 = 0x0000_0002;
const CB_MONITOR_READY: u16 = 0x0001;
const CB_FORMAT_LIST: u16 = 0x0002;
const CB_FORMAT_LIST_RESPONSE: u16 = 0x0003;
const CB_FORMAT_DATA_REQUEST: u16 = 0x0004;
const CB_FORMAT_DATA_RESPONSE: u16 = 0x0005;
const CB_CLIP_CAPS: u16 = 0x0007;
const CB_RESPONSE_OK: u16 = 0x0001;
const CB_RESPONSE_FAIL: u16 = 0x0002;
const CB_USE_LONG_FORMAT_NAMES: u32 = 0x0000_0002;
const CF_TEXT: u32 = 1;
const CF_UNICODETEXT: u32 = 13;

#[derive(Default)]
struct ClipboardChannelState {
    local_text: String,
    fragment_data: Vec<u8>,
    fragment_total_length: usize,
    pending_remote_format: Option<u32>,
    debug: bool,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum RleDecodeStatus {
    #[default]
    Ok,
    Partial,
    Invalid,
}

/// Process an incoming RDP PDU by stripping protocol layers:
/// TPKT (4) → X.224 DT (3) → MCS (variable) → Share Control → payload
fn process_incoming_pdu(data: &[u8]) -> Option<Vec<BitmapUpdate<'_>>> {
    if data.len() < 15 {
        return None;
    }

    // Skip TPKT header (4 bytes): version(1) + reserved(1) + length(2)
    if data[0] != 0x03 {
        return None;
    }
    let mut offset = 4;

    // Skip X.224 Data TPDU (3 bytes): LI(1) + 0xF0(1) + EOT(1)
    if offset + 3 > data.len() || data[offset + 1] != 0xF0 {
        return None;
    }
    offset += 3;

    // MCS SendDataIndication: first byte should be 0x68 (tag for SendDataIndication)
    if offset >= data.len() {
        return None;
    }
    let mcs_tag = data[offset];
    if mcs_tag != 0x68 {
        // Not a SendDataIndication — might be other MCS PDU, skip
        return None;
    }
    offset += 1;

    // Skip initiator (2 bytes) + channel_id (2 bytes)
    offset += 4;
    if offset >= data.len() {
        return None;
    }

    // Skip DataPriority + Segmentation (1 byte)
    offset += 1;

    // PER encoded userData length
    if offset >= data.len() {
        return None;
    }
    if (data[offset] & 0x80) != 0 {
        // 2-byte length
        offset += 2;
    } else {
        offset += 1;
    }

    // Now we're at the Share Control Header
    if offset + 6 > data.len() {
        return None;
    }

    let _total_length = u16::from_le_bytes([data[offset], data[offset + 1]]);
    let pdu_type = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) & 0x000F;
    // let _pdu_source = u16::from_le_bytes([data[offset + 4], data[offset + 5]]);
    offset += 6;

    // PDU type 0x07 = PDUTYPE_DATAPDU (contains share data header)
    if pdu_type == 0x07 {
        // Share Data Header: shareId(4) + pad(1) + streamId(1) + uncompressedLength(2) +
        // pduType2(1) + compressedType(1) + compressedLength(2) = 12 bytes
        if offset + 12 > data.len() {
            return None;
        }
        let pdu_type2 = data[offset + 8];
        offset += 12;

        // pduType2 = 0x02 means PDUTYPE2_UPDATE
        if pdu_type2 == 0x02 {
            return parse_update_pdu(&data[offset..]);
        }
    }

    None
}

/// Parse an Update PDU payload to extract bitmap updates.
/// Format: updateType(2) + type-specific data
fn parse_update_pdu(data: &[u8]) -> Option<Vec<BitmapUpdate<'_>>> {
    if data.len() < 4 {
        return None;
    }

    let update_type = u16::from_le_bytes([data[0], data[1]]);

    // 0x0001 = UPDATETYPE_BITMAP
    if update_type != 0x0001 {
        return None;
    }

    // TS_UPDATE_BITMAP_DATA: numberRectangles(2) + rectangles...
    let num_rects = u16::from_le_bytes([data[2], data[3]]) as usize;
    if num_rects == 0 || num_rects > 256 {
        return None;
    }

    let mut updates = Vec::new();
    let mut offset = 4;

    for i in 0..num_rects {
        if offset + 18 > data.len() {
            rdp_log_line(format!(
                "RDP bitmap: rect {} truncated at offset {}/{}",
                i,
                offset,
                data.len()
            ));
            break;
        }

        let dest_left = u16::from_le_bytes([data[offset], data[offset + 1]]);
        let dest_top = u16::from_le_bytes([data[offset + 2], data[offset + 3]]);
        let dest_right = u16::from_le_bytes([data[offset + 4], data[offset + 5]]);
        let dest_bottom = u16::from_le_bytes([data[offset + 6], data[offset + 7]]);
        let bmp_width = u16::from_le_bytes([data[offset + 8], data[offset + 9]]);
        let bmp_height = u16::from_le_bytes([data[offset + 10], data[offset + 11]]);
        let bpp = u16::from_le_bytes([data[offset + 12], data[offset + 13]]);
        let flags = u16::from_le_bytes([data[offset + 14], data[offset + 15]]);
        let bmp_length = u16::from_le_bytes([data[offset + 16], data[offset + 17]]) as usize;

        offset += 18;

        if bmp_width == 0 || bmp_height == 0 || bpp == 0 || bmp_length == 0 {
            break;
        }
        if bmp_width > 4096 || bmp_height > 4096 || bpp > 32 {
            rdp_log_line(format!(
                "RDP bitmap: out of range w={} h={} bpp={}",
                bmp_width, bmp_height, bpp
            ));
            break;
        }
        if offset + bmp_length > data.len() {
            rdp_log_line(format!(
                "RDP bitmap: data truncated need={} have={}",
                bmp_length,
                data.len() - offset
            ));
            break;
        }

        let compressed = (flags & 0x0001) != 0;
        let bmp_data = extract_compressed_data(data, offset, bmp_length, compressed, flags);
        offset += bmp_length;

        let w = dest_right.saturating_sub(dest_left) + 1;
        let h = dest_bottom.saturating_sub(dest_top) + 1;

        updates.push(BitmapUpdate {
            x: dest_left,
            y: dest_top,
            width: w,
            height: h,
            bpp,
            bmp_width,
            bmp_height,
            compressed,
            data: bmp_data,
        });
    }

    if updates.is_empty() {
        None
    } else {
        Some(updates)
    }
}

// Keep the old function as fallback (unused for now)
#[allow(dead_code)]
fn extract_bitmap_updates(data: &[u8]) -> Option<Vec<BitmapUpdate<'_>>> {
    process_incoming_pdu(data)
}

/// Process FastPath output PDU (used by xrdp for high-performance bitmap delivery).
/// FastPath header: actionFlags(1) + length(1-2) + updateData
fn process_fastpath_output(
    data: &[u8],
    fragments: &mut FastPathFragmentState,
    debug_stats: &mut RdpDebugStats,
) -> Option<Vec<BitmapUpdate<'static>>> {
    if data.len() < 4 {
        return None;
    }

    let action = (data[0] >> 2) & 0x03;
    if action != 0 {
        // Not a FastPath output update
        return None;
    }

    // Parse length
    let (payload_start, _total_len) = if (data[1] & 0x80) != 0 {
        // 2-byte length
        if data.len() < 3 {
            return None;
        }
        let len = (((data[1] & 0x7F) as usize) << 8) | (data[2] as usize);
        (3, len)
    } else {
        (2, data[1] as usize)
    };

    // Parse FastPath update PDUs
    let mut offset = payload_start;
    let mut all_updates = Vec::new();
    while offset < data.len() {
        // updateHeader: updateCode(4bits) | fragmentation(2bits) | compression(2bits)
        let update_header = data[offset];
        let update_code = update_header & 0x0F;
        let fragmentation = (update_header >> 4) & 0x03;
        let compression = (update_header >> 6) & 0x03;
        offset += 1;

        // If compression flag set, skip compressionFlags byte
        if compression != 0 {
            if offset >= data.len() {
                break;
            }
            offset += 1; // compressionFlags
        }

        // size (2 bytes LE)
        if offset + 2 > data.len() {
            break;
        }
        let update_size = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        if offset + update_size > data.len() {
            break;
        }

        let chunk = &data[offset..offset + update_size];
        offset += update_size;

        let (code, update_data): (u8, &[u8]) = match fragmentation {
            0 => {
                // FASTPATH_FRAGMENT_SINGLE — complete update
                fragments.active = false;
                (update_code, chunk)
            }
            2 => {
                // FASTPATH_FRAGMENT_FIRST — start accumulating
                fragments.data.clear();
                fragments.data.extend_from_slice(chunk);
                fragments.code = update_code;
                fragments.active = true;
                continue;
            }
            3 => {
                // FASTPATH_FRAGMENT_NEXT — continue accumulating
                if fragments.active && fragments.code == update_code {
                    fragments.data.extend_from_slice(chunk);
                }
                continue;
            }
            1 => {
                // FASTPATH_FRAGMENT_LAST — complete the fragment
                if !fragments.active || fragments.code != update_code {
                    fragments.active = false;
                    fragments.data.clear();
                    continue;
                }
                fragments.data.extend_from_slice(chunk);
                fragments.active = false;
                (fragments.code, fragments.data.as_slice())
            }
            _ => continue,
        };

        // updateCode 0x01 = FASTPATH_UPDATETYPE_BITMAP
        if code == 0x01
            && let Some(updates) = parse_update_pdu_bitmap_only(update_data)
        {
            all_updates.extend(updates.into_iter().map(BitmapUpdate::into_owned));
        }
        if code == 0x00 {
            debug_stats.fastpath_orders += 1;
        }
        // updateCode 0x04 = FASTPATH_UPDATETYPE_SYNCHRONIZE
    }

    if all_updates.is_empty() {
        None
    } else {
        Some(all_updates)
    }
}

/// Parse bitmap data from a FastPath bitmap update (same format as slowpath but without updateType field).
fn parse_update_pdu_bitmap_only(data: &[u8]) -> Option<Vec<BitmapUpdate<'_>>> {
    parse_bitmap_rectangles(data)
}

/// Parse bitmap rectangle data directly
fn parse_bitmap_rectangles(data: &[u8]) -> Option<Vec<BitmapUpdate<'_>>> {
    // Try to interpret as numRects + rectangles directly
    if data.len() < 2 {
        return None;
    }
    let num_rects = u16::from_le_bytes([data[0], data[1]]) as usize;
    if num_rects == 0 || num_rects > 256 {
        return None;
    }
    parse_bitmap_rectangles_at(&data[2..], num_rects)
}

fn parse_bitmap_rectangles_at(data: &[u8], num_rects: usize) -> Option<Vec<BitmapUpdate<'_>>> {
    let mut updates = Vec::new();
    let mut offset = 0;

    for _ in 0..num_rects {
        if offset + 18 > data.len() {
            break;
        }

        let dest_left = u16::from_le_bytes([data[offset], data[offset + 1]]);
        let dest_top = u16::from_le_bytes([data[offset + 2], data[offset + 3]]);
        let dest_right = u16::from_le_bytes([data[offset + 4], data[offset + 5]]);
        let dest_bottom = u16::from_le_bytes([data[offset + 6], data[offset + 7]]);
        let bmp_width = u16::from_le_bytes([data[offset + 8], data[offset + 9]]);
        let bmp_height = u16::from_le_bytes([data[offset + 10], data[offset + 11]]);
        let bpp = u16::from_le_bytes([data[offset + 12], data[offset + 13]]);
        let flags = u16::from_le_bytes([data[offset + 14], data[offset + 15]]);
        let bmp_length = u16::from_le_bytes([data[offset + 16], data[offset + 17]]) as usize;

        offset += 18;

        if bmp_width == 0 || bmp_height == 0 || bpp == 0 || bmp_length == 0 {
            break;
        }
        if bmp_width > 4096 || bmp_height > 4096 || bpp > 32 {
            break;
        }
        if offset + bmp_length > data.len() {
            break;
        }

        let compressed = (flags & 0x0001) != 0;
        let bmp_data = extract_compressed_data(data, offset, bmp_length, compressed, flags);
        offset += bmp_length;

        let w = dest_right.saturating_sub(dest_left) + 1;
        let h = dest_bottom.saturating_sub(dest_top) + 1;

        updates.push(BitmapUpdate {
            x: dest_left,
            y: dest_top,
            width: w,
            height: h,
            bpp,
            bmp_width,
            bmp_height,
            compressed,
            data: bmp_data,
        });
    }

    if updates.is_empty() {
        None
    } else {
        Some(updates)
    }
}

/// Extract RLE compressed data, handling optional compression header.
/// When flags don't have NO_BITMAP_COMPRESSION_HDR (0x0400) set,
/// the data has an 8-byte compression header that must be skipped.
fn extract_compressed_data(
    data: &[u8],
    offset: usize,
    bmp_length: usize,
    compressed: bool,
    flags: u16,
) -> Cow<'_, [u8]> {
    if !compressed {
        return Cow::Borrowed(&data[offset..offset + bmp_length]);
    }

    let no_compression_hdr = (flags & 0x0400) != 0;

    if no_compression_hdr || bmp_length <= 8 {
        // Flag says no header, or data too small for header
        return Cow::Borrowed(&data[offset..offset + bmp_length]);
    }

    // Skip 8-byte TS_CD_HEADER: cbCompFirstRowSize(2) + cbCompMainBodySize(2) +
    // cbScanWidth(2) + cbUncompressedSize(2)
    Cow::Borrowed(&data[offset + 8..offset + bmp_length])
}

fn decode_bitmap_data<'a>(
    update: &'a BitmapUpdate<'a>,
    scratch: &'a mut DecodeScratch,
) -> Option<DecodedBitmap<'a>> {
    scratch.last_rle_status = RleDecodeStatus::Ok;
    let bpp = update.bpp as usize;
    let bytes_per_pixel = match bpp {
        15 | 16 => 2,
        24 => 3,
        32 => 4,
        _ => return None,
    };

    if !update.compressed {
        let stride = uncompressed_bitmap_stride(update.bmp_width as usize, bytes_per_pixel);
        let min_len = stride * update.bmp_height as usize;
        if update.data.len() < min_len {
            return None;
        }
        return Some(DecodedBitmap {
            data: update.data.as_ref(),
            top_down: false,
            stride,
        });
    }

    let stride = update.bmp_width as usize * bytes_per_pixel;
    match rle_decompress_into(
        update.data.as_ref(),
        update.bmp_width as usize,
        update.bmp_height as usize,
        bytes_per_pixel,
        &mut scratch.rle_work,
        &mut scratch.rle_flipped,
    ) {
        RleDecodeStatus::Ok => Some(DecodedBitmap {
            data: scratch.rle_flipped.as_slice(),
            top_down: true,
            stride,
        }),
        status => {
            scratch.last_rle_status = status;
            None
        }
    }
}

#[inline]
fn uncompressed_bitmap_stride(width: usize, bytes_per_pixel: usize) -> usize {
    (width * bytes_per_pixel).div_ceil(4) * 4
}

fn apply_bitmap_to_buffer(
    frame_buffer: &mut [u8],
    frame_width: u16,
    frame_height: u16,
    update: &BitmapUpdate<'_>,
    decoded: DecodedBitmap<'_>,
) -> Option<DirtyRect> {
    let bpp = update.bpp as usize;
    let bytes_per_pixel = match bpp {
        15 | 16 => 2,
        24 => 3,
        32 => 4,
        _ => return None,
    };
    let stride = frame_width as usize * 4;
    let frame_w = frame_width as usize;
    let frame_h = frame_height as usize;
    let dst_x = update.x as usize;
    let dst_y0 = update.y as usize;

    if dst_x >= frame_w || dst_y0 >= frame_h || frame_buffer.len() < stride * frame_h {
        return None;
    }

    let bmp_w = update.bmp_width as usize;
    let bmp_h = update.bmp_height as usize;
    let draw_w = (update.width as usize).min(frame_w.saturating_sub(dst_x));
    let draw_h = (update.height as usize).min(frame_h.saturating_sub(dst_y0));

    if draw_w == 0 || draw_h == 0 || bmp_w == 0 || bmp_h == 0 {
        return None;
    }

    let src_row_bytes = decoded.stride;

    if decoded.data.len() < src_row_bytes * bmp_h {
        return None;
    }

    for row in 0..draw_h {
        let dest_y = dst_y0 + row;
        let src_row_in_bitmap = map_bitmap_coordinate(row, draw_h, bmp_h);

        let src_row = if !decoded.top_down && bmp_h > 1 {
            (bmp_h - 1) - src_row_in_bitmap
        } else {
            src_row_in_bitmap
        };
        let src_offset = src_row * src_row_bytes;
        let dest_offset = dest_y * stride + dst_x * 4;
        let row_end = dest_y * stride + frame_w * 4;

        for col in 0..draw_w {
            let src_col = map_bitmap_coordinate(col, draw_w, bmp_w);
            let src_px = src_offset + src_col * bytes_per_pixel;
            let dst_px = dest_offset + col * 4;

            if src_px + bytes_per_pixel > decoded.data.len()
                || dst_px + 4 > frame_buffer.len()
                || dst_px + 4 > row_end
            {
                break;
            }

            match bpp {
                32 => {
                    frame_buffer[dst_px] = decoded.data[src_px + 2]; // R
                    frame_buffer[dst_px + 1] = decoded.data[src_px + 1]; // G
                    frame_buffer[dst_px + 2] = decoded.data[src_px]; // B
                    frame_buffer[dst_px + 3] = 255; // A
                }
                24 => {
                    // xrdp 24bpp format: byte0=B, byte1=G, byte2=R
                    // Flutter RGBA8888: byte0=R, byte1=G, byte2=B, byte3=A
                    frame_buffer[dst_px] = decoded.data[src_px + 2]; // R
                    frame_buffer[dst_px + 1] = decoded.data[src_px + 1]; // G
                    frame_buffer[dst_px + 2] = decoded.data[src_px]; // B
                    frame_buffer[dst_px + 3] = 255; // A
                }
                16 => {
                    let pixel =
                        u16::from_le_bytes([decoded.data[src_px], decoded.data[src_px + 1]]);
                    frame_buffer[dst_px] = ((pixel >> 11) as u8) << 3; // R
                    frame_buffer[dst_px + 1] = (((pixel >> 5) & 0x3F) as u8) << 2; // G
                    frame_buffer[dst_px + 2] = ((pixel & 0x1F) as u8) << 3; // B
                    frame_buffer[dst_px + 3] = 255;
                }
                15 => {
                    let pixel =
                        u16::from_le_bytes([decoded.data[src_px], decoded.data[src_px + 1]]);
                    frame_buffer[dst_px] = (((pixel >> 10) & 0x1F) as u8) << 3;
                    frame_buffer[dst_px + 1] = (((pixel >> 5) & 0x1F) as u8) << 3;
                    frame_buffer[dst_px + 2] = ((pixel & 0x1F) as u8) << 3;
                    frame_buffer[dst_px + 3] = 255;
                }
                _ => {}
            }
        }
    }

    Some(DirtyRect {
        x: update.x,
        y: update.y,
        width: draw_w as u16,
        height: draw_h as u16,
    })
}

#[inline]
fn map_bitmap_coordinate(dst_index: usize, dst_len: usize, bmp_len: usize) -> usize {
    if bmp_len >= dst_len {
        dst_index.min(bmp_len.saturating_sub(1))
    } else {
        (dst_index * bmp_len / dst_len).min(bmp_len.saturating_sub(1))
    }
}

/// RLE decompression for RDP bitmap data.
/// Faithful port of FreeRDP interleaved RLE (MS-RDPBCGR 3.1.9).
/// Output is bottom-up scanline order (as per RDP spec).
#[allow(clippy::needless_borrow)]
fn rle_decompress_into(
    src: &[u8],
    width: usize,
    height: usize,
    bytes_per_pixel: usize,
    work: &mut Vec<u8>,
    flipped: &mut Vec<u8>,
) -> RleDecodeStatus {
    let row_delta = width * bytes_per_pixel;
    let output_size = row_delta * height;
    work.clear();
    work.resize(output_size, 0);
    let mut dst = work.as_mut_slice();

    let mut si = 0usize;
    let mut di = 0usize;
    let mut fg_pel = match bytes_per_pixel {
        1 => 0xFFu32,
        2 => 0xFFFFu32,
        3 => 0xFFFFFFu32,
        _ => 0xFFFFFFFFu32,
    };
    let mut f_insert_fg_pel = false;
    let mut f_first_line = true;

    let src_end = src.len();

    while si < src_end {
        // Track first line based on output position
        if f_first_line && di >= row_delta {
            f_first_line = false;
            f_insert_fg_pel = false;
        }

        if di >= output_size {
            break;
        }

        let code = extract_code_id(src[si]);

        // Handle Background Run Orders
        if code == REGULAR_BG_RUN || code == MEGA_MEGA_BG_RUN {
            let (run_length, advance) = extract_run_length(code, src, si, src_end);
            if advance == 0 {
                return RleDecodeStatus::Invalid;
            }
            si += advance;
            let mut run_length = run_length;

            if f_first_line {
                if f_insert_fg_pel {
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    write_pixel_value(&mut dst, di, fg_pel, bytes_per_pixel);
                    di += bytes_per_pixel;
                    run_length = run_length.saturating_sub(1);
                }
                for _ in 0..run_length {
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    write_pixel_value(&mut dst, di, 0, bytes_per_pixel);
                    di += bytes_per_pixel;
                }
            } else {
                if f_insert_fg_pel {
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    let prev = read_pixel_value(&dst, di - row_delta, bytes_per_pixel);
                    write_pixel_value(&mut dst, di, prev ^ fg_pel, bytes_per_pixel);
                    di += bytes_per_pixel;
                    run_length = run_length.saturating_sub(1);
                }
                for _ in 0..run_length {
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    let prev = read_pixel_value(&dst, di - row_delta, bytes_per_pixel);
                    write_pixel_value(&mut dst, di, prev, bytes_per_pixel);
                    di += bytes_per_pixel;
                }
            }
            f_insert_fg_pel = true;
            continue;
        }

        // For any other order, clear the insert fg flag
        f_insert_fg_pel = false;

        match code {
            // Foreground Run Orders
            REGULAR_FG_RUN | MEGA_MEGA_FG_RUN | LITE_SET_FG_FG_RUN | MEGA_MEGA_SET_FG_RUN => {
                let (run_length, advance) = extract_run_length(code, src, si, src_end);
                if advance == 0 {
                    return RleDecodeStatus::Invalid;
                }
                si += advance;

                if code == LITE_SET_FG_FG_RUN || code == MEGA_MEGA_SET_FG_RUN {
                    if si + bytes_per_pixel > src_end {
                        return RleDecodeStatus::Invalid;
                    }
                    fg_pel = src_read_pixel(src, &mut si, bytes_per_pixel);
                }

                if f_first_line {
                    for _ in 0..run_length {
                        if di + bytes_per_pixel > output_size {
                            break;
                        }
                        write_pixel_value(&mut dst, di, fg_pel, bytes_per_pixel);
                        di += bytes_per_pixel;
                    }
                } else {
                    for _ in 0..run_length {
                        if di + bytes_per_pixel > output_size {
                            break;
                        }
                        let prev = read_pixel_value(&dst, di - row_delta, bytes_per_pixel);
                        write_pixel_value(&mut dst, di, prev ^ fg_pel, bytes_per_pixel);
                        di += bytes_per_pixel;
                    }
                }
            }

            // Dithered Run Orders
            LITE_DITHERED_RUN | MEGA_MEGA_DITHERED_RUN => {
                let (run_length, advance) = extract_run_length(code, src, si, src_end);
                if advance == 0 {
                    return RleDecodeStatus::Invalid;
                }
                si += advance;

                if si + bytes_per_pixel > src_end {
                    return RleDecodeStatus::Invalid;
                }
                let pixel_a = src_read_pixel(src, &mut si, bytes_per_pixel);
                if si + bytes_per_pixel > src_end {
                    return RleDecodeStatus::Invalid;
                }
                let pixel_b = src_read_pixel(src, &mut si, bytes_per_pixel);

                for _ in 0..run_length {
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    write_pixel_value(&mut dst, di, pixel_a, bytes_per_pixel);
                    di += bytes_per_pixel;
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    write_pixel_value(&mut dst, di, pixel_b, bytes_per_pixel);
                    di += bytes_per_pixel;
                }
            }

            // Color Run Orders
            REGULAR_COLOR_RUN | MEGA_MEGA_COLOR_RUN => {
                let (run_length, advance) = extract_run_length(code, src, si, src_end);
                if advance == 0 {
                    return RleDecodeStatus::Invalid;
                }
                si += advance;

                if si + bytes_per_pixel > src_end {
                    return RleDecodeStatus::Invalid;
                }
                let pixel_a = src_read_pixel(src, &mut si, bytes_per_pixel);

                for _ in 0..run_length {
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    write_pixel_value(&mut dst, di, pixel_a, bytes_per_pixel);
                    di += bytes_per_pixel;
                }
            }

            // Foreground/Background Image Orders
            REGULAR_FGBG_IMAGE
            | MEGA_MEGA_FGBG_IMAGE
            | LITE_SET_FG_FGBG_IMAGE
            | MEGA_MEGA_SET_FGBG_IMAGE => {
                let (run_length, advance) = extract_run_length(code, src, si, src_end);
                if advance == 0 {
                    return RleDecodeStatus::Invalid;
                }
                si += advance;

                if code == LITE_SET_FG_FGBG_IMAGE || code == MEGA_MEGA_SET_FGBG_IMAGE {
                    if si + bytes_per_pixel > src_end {
                        return RleDecodeStatus::Invalid;
                    }
                    fg_pel = src_read_pixel(src, &mut si, bytes_per_pixel);
                }

                let mut remaining = run_length;
                while remaining > 8 {
                    if si >= src_end {
                        return RleDecodeStatus::Invalid;
                    }
                    let bitmask = src[si];
                    si += 1;
                    if f_first_line {
                        write_first_line_fgbg_image(
                            &mut dst,
                            &mut di,
                            output_size,
                            bitmask,
                            fg_pel,
                            8,
                            bytes_per_pixel,
                        );
                    } else {
                        write_fgbg_image(
                            &mut dst,
                            &mut di,
                            output_size,
                            row_delta,
                            bitmask,
                            fg_pel,
                            8,
                            bytes_per_pixel,
                        );
                    }
                    remaining -= 8;
                }
                if remaining > 0 {
                    if si >= src_end {
                        return RleDecodeStatus::Invalid;
                    }
                    let bitmask = src[si];
                    si += 1;
                    if f_first_line {
                        write_first_line_fgbg_image(
                            &mut dst,
                            &mut di,
                            output_size,
                            bitmask,
                            fg_pel,
                            remaining,
                            bytes_per_pixel,
                        );
                    } else {
                        write_fgbg_image(
                            &mut dst,
                            &mut di,
                            output_size,
                            row_delta,
                            bitmask,
                            fg_pel,
                            remaining,
                            bytes_per_pixel,
                        );
                    }
                }
            }

            // Color Image Orders
            REGULAR_COLOR_IMAGE | MEGA_MEGA_COLOR_IMAGE => {
                let (run_length, advance) = extract_run_length(code, src, si, src_end);
                if advance == 0 {
                    return RleDecodeStatus::Invalid;
                }
                si += advance;

                for _ in 0..run_length {
                    if si + bytes_per_pixel > src_end {
                        break;
                    }
                    if di + bytes_per_pixel > output_size {
                        break;
                    }
                    let pix = src_read_pixel(src, &mut si, bytes_per_pixel);
                    write_pixel_value(&mut dst, di, pix, bytes_per_pixel);
                    di += bytes_per_pixel;
                }
            }

            // Special FGBG 1 (mask = 0x03)
            SPECIAL_FGBG_1 => {
                si += 1; // consume the order byte
                if f_first_line {
                    write_first_line_fgbg_image(
                        &mut dst,
                        &mut di,
                        output_size,
                        0x03,
                        fg_pel,
                        8,
                        bytes_per_pixel,
                    );
                } else {
                    write_fgbg_image(
                        &mut dst,
                        &mut di,
                        output_size,
                        row_delta,
                        0x03,
                        fg_pel,
                        8,
                        bytes_per_pixel,
                    );
                }
            }

            // Special FGBG 2 (mask = 0x05)
            SPECIAL_FGBG_2 => {
                si += 1;
                if f_first_line {
                    write_first_line_fgbg_image(
                        &mut dst,
                        &mut di,
                        output_size,
                        0x05,
                        fg_pel,
                        8,
                        bytes_per_pixel,
                    );
                } else {
                    write_fgbg_image(
                        &mut dst,
                        &mut di,
                        output_size,
                        row_delta,
                        0x05,
                        fg_pel,
                        8,
                        bytes_per_pixel,
                    );
                }
            }

            // Special White
            SPECIAL_WHITE => {
                si += 1;
                if di + bytes_per_pixel > output_size {
                    break;
                }
                let white = match bytes_per_pixel {
                    1 => 0xFF,
                    2 => 0xFFFF,
                    3 => 0xFFFFFF,
                    _ => 0xFFFFFFFF,
                };
                write_pixel_value(&mut dst, di, white, bytes_per_pixel);
                di += bytes_per_pixel;
            }

            // Special Black
            SPECIAL_BLACK => {
                si += 1;
                if di + bytes_per_pixel > output_size {
                    break;
                }
                write_pixel_value(&mut dst, di, 0, bytes_per_pixel);
                di += bytes_per_pixel;
            }

            _ => {
                return RleDecodeStatus::Invalid;
            }
        }
    }

    if di == 0 {
        return RleDecodeStatus::Invalid;
    }
    if di != output_size {
        return RleDecodeStatus::Partial;
    }

    // RLE decompresses bottom-up (first output byte = bottom-left pixel per RDP spec).
    // Flip vertically so row 0 = top visual row for correct rendering.
    flipped.clear();
    flipped.resize(output_size, 0);
    for row in 0..height {
        let src_start = (height - 1 - row) * row_delta;
        let dst_start = row * row_delta;
        if src_start + row_delta <= dst.len() && dst_start + row_delta <= output_size {
            flipped[dst_start..dst_start + row_delta]
                .copy_from_slice(&dst[src_start..src_start + row_delta]);
        }
    }
    RleDecodeStatus::Ok
}

// --- RLE order code constants (matching FreeRDP/MS-RDPBCGR) ---
const REGULAR_BG_RUN: u32 = 0x00;
const MEGA_MEGA_BG_RUN: u32 = 0xF0;
const REGULAR_FG_RUN: u32 = 0x01;
const MEGA_MEGA_FG_RUN: u32 = 0xF1;
const LITE_SET_FG_FG_RUN: u32 = 0x0C;
const MEGA_MEGA_SET_FG_RUN: u32 = 0xF6;
const LITE_DITHERED_RUN: u32 = 0x0E;
const MEGA_MEGA_DITHERED_RUN: u32 = 0xF8;
const REGULAR_COLOR_RUN: u32 = 0x03;
const MEGA_MEGA_COLOR_RUN: u32 = 0xF3;
const REGULAR_FGBG_IMAGE: u32 = 0x02;
const MEGA_MEGA_FGBG_IMAGE: u32 = 0xF2;
const LITE_SET_FG_FGBG_IMAGE: u32 = 0x0D;
const MEGA_MEGA_SET_FGBG_IMAGE: u32 = 0xF7;
const REGULAR_COLOR_IMAGE: u32 = 0x04;
const MEGA_MEGA_COLOR_IMAGE: u32 = 0xF4;
const SPECIAL_FGBG_1: u32 = 0xF9;
const SPECIAL_FGBG_2: u32 = 0xFA;
const SPECIAL_WHITE: u32 = 0xFD;
const SPECIAL_BLACK: u32 = 0xFE;

/// Extract the code ID from the order header byte (FreeRDP ExtractCodeId).
#[inline]
fn extract_code_id(order_hdr: u8) -> u32 {
    if (order_hdr & 0xC0) != 0xC0 {
        // REGULAR orders: top 3 bits
        (order_hdr >> 5) as u32
    } else if (order_hdr & 0xF0) == 0xF0 {
        // MEGA and SPECIAL orders
        order_hdr as u32
    } else {
        // LITE orders: top 4 bits
        (order_hdr >> 4) as u32
    }
}

/// Extract run length based on code type (FreeRDP ExtractRunLength).
/// Returns (run_length, bytes_advanced_including_header). advance=0 means error.
#[inline]
fn extract_run_length(code: u32, src: &[u8], si: usize, src_end: usize) -> (usize, usize) {
    if si >= src_end {
        return (0, 0);
    }
    let order_hdr = src[si];

    match code {
        REGULAR_BG_RUN | REGULAR_FG_RUN | REGULAR_COLOR_RUN | REGULAR_COLOR_IMAGE => {
            // Regular: low 5 bits; if 0, next byte + 32
            let mut run = (order_hdr & 0x1F) as usize;
            if run == 0 {
                if si + 1 >= src_end {
                    return (0, 0);
                }
                run = src[si + 1] as usize + 32;
                (run, 2)
            } else {
                (run, 1)
            }
        }
        REGULAR_FGBG_IMAGE => {
            // Regular FGBG: low 5 bits; if 0, next byte + 1; else multiply by 8
            let mut run = (order_hdr & 0x1F) as usize;
            if run == 0 {
                if si + 1 >= src_end {
                    return (0, 0);
                }
                run = src[si + 1] as usize + 1;
                (run, 2)
            } else {
                run *= 8;
                (run, 1)
            }
        }
        LITE_SET_FG_FG_RUN | LITE_DITHERED_RUN => {
            // Lite: low 4 bits; if 0, next byte + 16
            let mut run = (order_hdr & 0x0F) as usize;
            if run == 0 {
                if si + 1 >= src_end {
                    return (0, 0);
                }
                run = src[si + 1] as usize + 16;
                (run, 2)
            } else {
                (run, 1)
            }
        }
        LITE_SET_FG_FGBG_IMAGE => {
            // Lite FGBG: low 4 bits; if 0, next byte + 1; else multiply by 8
            let mut run = (order_hdr & 0x0F) as usize;
            if run == 0 {
                if si + 1 >= src_end {
                    return (0, 0);
                }
                run = src[si + 1] as usize + 1;
                (run, 2)
            } else {
                run *= 8;
                (run, 1)
            }
        }
        MEGA_MEGA_BG_RUN
        | MEGA_MEGA_FG_RUN
        | MEGA_MEGA_SET_FG_RUN
        | MEGA_MEGA_DITHERED_RUN
        | MEGA_MEGA_COLOR_RUN
        | MEGA_MEGA_FGBG_IMAGE
        | MEGA_MEGA_SET_FGBG_IMAGE
        | MEGA_MEGA_COLOR_IMAGE => {
            // Mega-mega: next 2 bytes LE16
            if si + 2 >= src_end {
                return (0, 0);
            }
            let run = u16::from_le_bytes([src[si + 1], src[si + 2]]) as usize;
            (run, 3)
        }
        _ => (0, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::events::ConnectionStatusEvent;
    use crate::domain::rdp_profile::RdpProfile;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::{broadcast, mpsc, oneshot};

    fn test_profile(password: Option<&str>) -> RdpProfile {
        RdpProfile {
            id: "test-profile".to_owned(),
            name: "Test profile".to_owned(),
            host: "testhost".to_owned(),
            port: 3389,
            username: "testuser".to_owned(),
            password: password.map(str::to_owned),
            domain: None,
            width: 1280,
            height: 720,
            screen_mode: 1,
            extra: HashMap::new(),
        }
    }

    fn server_share_control_pdu(pdu_type: u16, payload: &[u8]) -> Vec<u8> {
        let share_len = 6 + payload.len();
        let mut share = Vec::with_capacity(share_len);
        share.extend_from_slice(&(share_len as u16).to_le_bytes());
        share.extend_from_slice(&pdu_type.to_le_bytes());
        share.extend_from_slice(&1004u16.to_le_bytes());
        share.extend_from_slice(payload);

        let mut mcs = Vec::new();
        mcs.push(0x68); // SendDataIndication
        mcs.extend_from_slice(&3u16.to_be_bytes());
        mcs.extend_from_slice(&1003u16.to_be_bytes());
        mcs.push(0x70);
        mcs.push(share.len() as u8);
        mcs.extend_from_slice(&share);

        let packet_len = 4 + 3 + mcs.len();
        let mut packet = vec![0x03, 0x00];
        packet.extend_from_slice(&(packet_len as u16).to_be_bytes());
        packet.extend_from_slice(&[0x02, 0xF0, 0x80]);
        packet.extend_from_slice(&mcs);
        packet
    }

    #[test]
    fn demand_active_detection_reads_the_share_control_header() {
        let demand_active = server_share_control_pdu(0x0011, &[]);
        assert!(is_demand_active_pdu(&demand_active));
        assert!(contains_demand_active(&demand_active));
    }

    #[test]
    fn bitmap_payload_marker_is_not_mistaken_for_demand_active() {
        let bitmap = server_share_control_pdu(0x0017, &[0xAA, 0x11, 0x00, 0xBB]);
        assert!(!is_demand_active_pdu(&bitmap));
        assert!(!contains_demand_active(&bitmap));
    }

    // ─── Licensing PDU helpers ────────────────────────────────────────────────

    /// Build a minimal server licensing PDU (TPKT + MCS + SEC_LICENSE_PKT).
    fn server_license_pdu(msg_type: u8, extra_payload: &[u8]) -> Vec<u8> {
        // TS_LICENSE_PDU body: bMsgType(1) + flags(1) + wMsgSize(2) + data
        let body_size = 4 + extra_payload.len();
        let mut body = Vec::with_capacity(body_size);
        body.push(msg_type);
        body.push(0x00); // flags
        body.extend_from_slice(&(body_size as u16).to_le_bytes()); // wMsgSize
        body.extend_from_slice(extra_payload);

        // Security header: SEC_LICENSE_PKT(0x0080) + flagsHi(0x0000)
        let mut payload = Vec::new();
        payload.extend_from_slice(&0x0080u16.to_le_bytes());
        payload.extend_from_slice(&0x0000u16.to_le_bytes());
        payload.extend_from_slice(&body);

        // MCS SendDataIndication tag = 0x68
        let mut mcs = Vec::new();
        mcs.push(0x68); // SendDataIndication
        mcs.extend_from_slice(&3u16.to_be_bytes()); // initiator
        mcs.extend_from_slice(&1003u16.to_be_bytes()); // channelId
        mcs.push(0x70); // dataPriority|segmentation
        mcs.push(payload.len() as u8); // length (single byte, fits in test)
        mcs.extend_from_slice(&payload);

        let packet_len = 4 + 3 + mcs.len();
        let mut packet = vec![0x03, 0x00];
        packet.extend_from_slice(&(packet_len as u16).to_be_bytes());
        packet.extend_from_slice(&[0x02, 0xF0, 0x80]);
        packet.extend_from_slice(&mcs);
        packet
    }

    #[test]
    fn license_pdu_is_detected_as_sec_license_pkt() {
        let pkt = server_license_pdu(LICENSE_ERROR_ALERT, &[0u8; 12]);
        let result = extract_license_payload(&pkt);
        assert!(result.is_some(), "should detect SEC_LICENSE_PKT");
        let (sec_flags, body) = result.unwrap();
        assert_eq!(sec_flags & SEC_LICENSE_PKT, SEC_LICENSE_PKT);
        assert_eq!(license_msg_type(body), Some(LICENSE_ERROR_ALERT));
    }

    #[test]
    fn license_error_alert_with_st_total_abort_is_terminal() {
        // STATE_TRANSITION = 0x00000004 (ST_TOTAL_ABORT), STATUS_VALID_CLIENT
        // xrdp sends this to indicate "no license required"
        let mut extra = vec![0u8; 12];
        // dwErrorCode at offset 4, dwStateTransition at offset 8
        extra[8..12].copy_from_slice(&0x00000004u32.to_le_bytes());
        let pkt = server_license_pdu(LICENSE_ERROR_ALERT, &extra);
        let (_, body) = extract_license_payload(&pkt).unwrap();
        // body = bMsgType(1)+flags(1)+wMsgSize(2)+extra
        assert!(is_license_error_terminal(&body[4..]));
    }

    #[test]
    fn license_error_alert_with_st_push_license_is_not_terminal() {
        let mut extra = vec![0u8; 12];
        extra[8..12].copy_from_slice(&0x00000001u32.to_le_bytes()); // ST_PUSH_LICENSE
        let pkt = server_license_pdu(LICENSE_ERROR_ALERT, &extra);
        let (_, body) = extract_license_payload(&pkt).unwrap();
        assert!(!is_license_error_terminal(&body[4..]));
    }

    #[test]
    fn demand_active_is_not_misidentified_as_license_pdu() {
        let demand = server_share_control_pdu(0x0011, &[]);
        assert!(extract_license_payload(&demand).is_none());
    }

    /// wait_for_demand_active should succeed when the server sends a licensing
    /// ERROR_ALERT (xrdp no-license mode) followed by the Demand Active PDU.
    #[tokio::test]
    async fn wait_for_demand_active_handles_xrdp_license_then_demand() {
        // Build: license error + demand active
        let license_pkt = server_license_pdu(LICENSE_ERROR_ALERT, &{
            let mut e = vec![0u8; 12];
            e[8..12].copy_from_slice(&0x00000004u32.to_le_bytes()); // ST_TOTAL_ABORT
            e
        });
        let demand_active = server_share_control_pdu(0x0011, &[]);
        let combined = [license_pkt.as_slice(), demand_active.as_slice()].concat();

        // Use a tokio loopback pair so both ends are async from the start
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let combined_clone = combined.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut sock, &combined_clone)
                .await
                .unwrap();
            // socket drops here → client sees EOF
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut rdp_stream = RdpStream::Plain(client);
        let mut buf = vec![0u8; 65536];
        let debug = RdpDebugStats::default();

        let result = wait_for_demand_active(&mut rdp_stream, &mut buf, 1007, &debug).await;
        assert!(result.unwrap(), "should find demand active after license PDU");
    }

    #[test]
    fn client_network_data_requests_cliprdr_channel() {
        let packet = build_mcs_connect_initial(&test_profile(Some("secret")), 1);
        assert!(
            packet
                .windows(CLIPRDR_CHANNEL_NAME.len())
                .any(|window| window == CLIPRDR_CHANNEL_NAME)
        );
    }

    #[test]
    fn client_network_data_requests_rdpdr_only_with_a_mapped_folder() {
        let temp = tempfile::tempdir().unwrap();
        let mut profile = test_profile(Some("secret"));
        let without_drive = build_mcs_connect_initial(&profile, 1);
        assert!(
            !without_drive
                .windows(RDPDR_CHANNEL_NAME.len())
                .any(|window| window == RDPDR_CHANNEL_NAME)
        );

        profile.extra.insert(
            "portix_drive_path".to_owned(),
            temp.path().to_string_lossy().into_owned(),
        );
        let with_drive = build_mcs_connect_initial(&profile, 1);
        assert!(
            with_drive
                .windows(RDPDR_CHANNEL_NAME.len())
                .any(|window| window == RDPDR_CHANNEL_NAME)
        );
    }

    #[test]
    fn parses_server_static_virtual_channel_id() {
        let mut response = vec![0xAA, 0xBB];
        response.extend_from_slice(&0x0C03u16.to_le_bytes());
        response.extend_from_slice(&10u16.to_le_bytes());
        response.extend_from_slice(&1003u16.to_le_bytes());
        response.extend_from_slice(&1u16.to_le_bytes());
        response.extend_from_slice(&1005u16.to_le_bytes());
        assert_eq!(parse_server_static_channel_id(&response, 0), Some(1005));
    }

    #[test]
    fn clipboard_unicode_roundtrip_preserves_non_ascii_text() {
        let source = "Portix clipboard: halo dunia";
        let encoded = encode_local_clipboard(source, CF_UNICODETEXT).unwrap();
        assert_eq!(
            decode_remote_clipboard(&encoded, CF_UNICODETEXT).as_deref(),
            Some(source)
        );
    }

    #[test]
    fn clipboard_prefers_unicode_format() {
        let mut formats = Vec::new();
        formats.extend_from_slice(&CF_TEXT.to_le_bytes());
        formats.extend_from_slice(&0u16.to_le_bytes());
        formats.extend_from_slice(&CF_UNICODETEXT.to_le_bytes());
        formats.extend_from_slice(&0u16.to_le_bytes());
        assert_eq!(preferred_clipboard_format(&formats), Some(CF_UNICODETEXT));
    }

    #[test]
    fn client_info_requests_autologon_only_with_a_password() {
        const INFO_AUTOLOGON: u32 = 0x0000_0008;
        const INFO_LOGONERRORS: u32 = 0x0001_0000;

        let with_password = client_info_flags(&test_profile(Some("secret")));
        assert_ne!(with_password & INFO_AUTOLOGON, 0);
        assert_ne!(with_password & INFO_LOGONERRORS, 0);

        let without_password = client_info_flags(&test_profile(None));
        assert_eq!(without_password & INFO_AUTOLOGON, 0);
    }

    #[test]
    fn keep_awake_is_enabled_by_default_and_can_be_disabled() {
        let default_policy = KeepAwakePolicy::from_profile(&test_profile(Some("secret")));
        assert!(default_policy.enabled);
        assert_eq!(default_policy.interval, Duration::from_secs(30));

        let mut disabled = test_profile(Some("secret"));
        disabled
            .extra
            .insert("portix_keep_awake".to_owned(), "false".to_owned());
        let disabled_policy = KeepAwakePolicy::from_profile(&disabled);
        assert!(!disabled_policy.enabled);
    }

    #[test]
    fn auto_unlock_is_opt_in_and_supports_the_local_password_charset() {
        let default_policy = AutoUnlockPolicy::from_profile(&test_profile(Some("secret")));
        assert!(!default_policy.enabled);

        let mut enabled = test_profile(Some("test"));
        enabled
            .extra
            .insert("portix_auto_unlock".to_owned(), "1".to_owned());
        assert!(AutoUnlockPolicy::from_profile(&enabled).enabled);
        assert!(build_text_input_pdus(1004, "test", true).is_some());
        assert!(build_text_input_pdus(1004, "password-123", true).is_some());
        assert!(build_text_input_pdus(1004, "sandi🔒", true).is_none());
    }

    fn fastpath_packet(fragmentation: u8, chunk: &[u8]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(0x01 | (fragmentation << 4));
        payload.extend_from_slice(&(chunk.len() as u16).to_le_bytes());
        payload.extend_from_slice(chunk);

        let total_len = payload.len() + 2;
        let mut packet = Vec::new();
        packet.push(0x00);
        packet.push(total_len as u8);
        packet.extend_from_slice(&payload);
        packet
    }

    fn bitmap_update_payload() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&24u16.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&8u16.to_le_bytes());
        payload.extend_from_slice(&[0, 0, 255, 0, 255, 0, 0, 0]);
        payload
    }

    #[test]
    fn fastpath_bitmap_fragments_are_assembled_across_packets() {
        let payload = bitmap_update_payload();
        let split_at = payload.len() / 2;
        let first = fastpath_packet(2, &payload[..split_at]);
        let last = fastpath_packet(1, &payload[split_at..]);
        let mut fragments = FastPathFragmentState::default();
        let mut stats = RdpDebugStats::default();

        assert!(process_fastpath_output(&first, &mut fragments, &mut stats).is_none());
        let updates =
            process_fastpath_output(&last, &mut fragments, &mut stats).expect("assembled bitmap");

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].x, 0);
        assert_eq!(updates[0].y, 0);
        assert_eq!(updates[0].width, 2);
        assert_eq!(updates[0].height, 1);
        assert!(!updates[0].compressed);
        assert_eq!(updates[0].data.len(), 8);
    }

    #[test]
    fn apply_bitmap_uses_decoded_stride_independent_of_orientation() {
        let update = BitmapUpdate {
            x: 0,
            y: 0,
            width: 2,
            height: 2,
            bpp: 24,
            bmp_width: 2,
            bmp_height: 2,
            compressed: true,
            data: Cow::Borrowed(&[]),
        };
        let pixels = [0, 0, 255, 0, 255, 0, 255, 0, 0, 255, 255, 255];
        let decoded = DecodedBitmap {
            data: &pixels,
            top_down: false,
            stride: 6,
        };
        let mut frame = vec![0u8; 2 * 2 * 4];

        let dirty =
            apply_bitmap_to_buffer(&mut frame, 2, 2, &update, decoded).expect("bitmap applied");

        assert_eq!(dirty.width, 2);
        assert_eq!(dirty.height, 2);
        assert_eq!(&frame[0..4], &[0, 0, 255, 255]);
        assert_eq!(&frame[4..8], &[255, 255, 255, 255]);
        assert_eq!(&frame[8..12], &[255, 0, 0, 255]);
        assert_eq!(&frame[12..16], &[0, 255, 0, 255]);
    }

    #[test]
    fn apply_bitmap_scales_when_source_and_destination_sizes_differ() {
        let update = BitmapUpdate {
            x: 0,
            y: 0,
            width: 4,
            height: 2,
            bpp: 24,
            bmp_width: 2,
            bmp_height: 1,
            compressed: true,
            data: Cow::Borrowed(&[]),
        };
        let pixels = [0, 0, 255, 0, 255, 0];
        let decoded = DecodedBitmap {
            data: &pixels,
            top_down: true,
            stride: 6,
        };
        let mut frame = vec![0u8; 4 * 2 * 4];

        let dirty =
            apply_bitmap_to_buffer(&mut frame, 4, 2, &update, decoded).expect("bitmap applied");

        assert_eq!(dirty.width, 4);
        assert_eq!(dirty.height, 2);
        assert_eq!(&frame[0..4], &[255, 0, 0, 255]);
        assert_eq!(&frame[4..8], &[255, 0, 0, 255]);
        assert_eq!(&frame[8..12], &[0, 255, 0, 255]);
        assert_eq!(&frame[12..16], &[0, 255, 0, 255]);
        assert_eq!(&frame[16..20], &[255, 0, 0, 255]);
        assert_eq!(&frame[24..28], &[0, 255, 0, 255]);
    }

    #[test]
    fn apply_bitmap_crops_alignment_padding_instead_of_scaling() {
        let update = BitmapUpdate {
            x: 0,
            y: 0,
            width: 2,
            height: 1,
            bpp: 24,
            bmp_width: 4,
            bmp_height: 1,
            compressed: true,
            data: Cow::Borrowed(&[]),
        };
        let pixels = [
            0, 0, 255, // red
            0, 255, 0, // green
            255, 0, 0, // blue padding/content outside destination
            255, 255, 255, // white padding/content outside destination
        ];
        let decoded = DecodedBitmap {
            data: &pixels,
            top_down: true,
            stride: 12,
        };
        let mut frame = vec![0u8; 2 * 4];

        let dirty =
            apply_bitmap_to_buffer(&mut frame, 2, 1, &update, decoded).expect("bitmap applied");

        assert_eq!(dirty.width, 2);
        assert_eq!(dirty.height, 1);
        assert_eq!(&frame[0..4], &[255, 0, 0, 255]);
        assert_eq!(&frame[4..8], &[0, 255, 0, 255]);
    }

    #[test]
    fn framebuffer_signal_mode_emits_empty_event_without_losing_snapshot() {
        let mut framebuffer = Framebuffer::new(2, 2);
        framebuffer.write_buf.fill(7);
        framebuffer.mark_dirty(DirtyRect {
            x: 0,
            y: 0,
            width: 2,
            height: 2,
        });

        let events = framebuffer.drain_dirty_events("session", 1, false, false);
        assert_eq!(events.len(), 1);
        assert!(events[0].data.is_empty());

        let snapshot = framebuffer
            .snapshot_for_request()
            .expect("snapshot remains available");
        assert_eq!(snapshot.len(), 16);
        assert!(snapshot.iter().all(|value| *value == 7));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires reachable xrdp server; run with cargo test rdp_live_frame_diagnostic -- --ignored --nocapture"]
    async fn rdp_live_frame_diagnostic() {
        let host =
            std::env::var("PORTIX_RDP_TEST_HOST").unwrap_or_else(|_| "testhost-ip".to_owned());
        let username =
            std::env::var("PORTIX_RDP_TEST_USER").unwrap_or_else(|_| "testuser".to_owned());
        let password =
            std::env::var("PORTIX_RDP_TEST_PASSWORD").unwrap_or_else(|_| "testpassword".to_owned());
        let width = std::env::var("PORTIX_RDP_TEST_WIDTH")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(1440);
        let height = std::env::var("PORTIX_RDP_TEST_HEIGHT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(772);

        let mut extra = HashMap::new();
        extra.insert("portix_debug".to_owned(), "1".to_owned());
        extra.insert("portix_stream_pixels".to_owned(), "0".to_owned());
        extra.insert(
            "portix_keep_awake_interval_seconds".to_owned(),
            "2".to_owned(),
        );
        extra.insert("portix_auto_unlock".to_owned(), "1".to_owned());
        if let Ok(path) = std::env::var("PORTIX_RDP_TEST_DRIVE_PATH") {
            extra.insert("portix_drive_path".to_owned(), path);
            extra.insert(
                "portix_drive_name".to_owned(),
                std::env::var("PORTIX_RDP_TEST_DRIVE_NAME").unwrap_or_else(|_| "PORTIX".to_owned()),
            );
        }

        let profile = RdpProfile {
            id: "rdp-live-test".to_owned(),
            name: "RDP live diagnostic".to_owned(),
            host,
            port: 3389,
            username,
            password: Some(password),
            domain: None,
            width,
            height,
            screen_mode: 1,
            extra,
        };

        let (frame_tx, _frame_rx) = broadcast::channel::<RdpFrameEvent>(64);
        let (clipboard_tx, mut clipboard_rx) = broadcast::channel::<RdpClipboardEvent>(16);
        let (status_tx, mut status_rx) = broadcast::channel::<ConnectionStatusEvent>(16);
        let (command_tx, command_rx) = mpsc::channel::<RdpCommand>(32);
        let runtime = RdpRuntime::new(
            profile,
            "rdp-live-test-session".to_owned(),
            frame_tx,
            clipboard_tx,
            status_tx,
        );
        let handle = tokio::spawn(async move { runtime.run(command_rx).await });

        let mut initial_snapshot: Option<Arc<Vec<u8>>> = None;
        let mut snapshot = Vec::new();
        let mut last_status = None;
        let mut responsive_frame_requests = 0usize;
        let clipboard_probe = std::env::var("PORTIX_RDP_TEST_CLIPBOARD").ok();
        let mut remote_clipboard_text = None;
        // XRDP can spend about ten seconds reconnecting an existing desktop
        // while it waits for chansrv, so keep sampling past that transition.
        for sample in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if clipboard_probe.is_some() && sample == 24 {
                let _ = command_tx
                    .send(RdpCommand::SetClipboardText {
                        text: clipboard_probe.clone().unwrap_or_default(),
                    })
                    .await;
                for (scancode, is_pressed) in [
                    (0x1D, true),
                    (0x38, true),
                    (0x14, true),
                    (0x14, false),
                    (0x38, false),
                    (0x1D, false),
                ] {
                    let _ = command_tx
                        .send(RdpCommand::KeyboardInput {
                            scancode,
                            is_pressed,
                        })
                        .await;
                }
            }
            if clipboard_probe.is_some() && sample == 29 {
                for (scancode, is_pressed) in [
                    (0x1D, true),
                    (0x2A, true),
                    (0x2F, true),
                    (0x2F, false),
                    (0x2A, false),
                    (0x1D, false),
                ] {
                    let _ = command_tx
                        .send(RdpCommand::KeyboardInput {
                            scancode,
                            is_pressed,
                        })
                        .await;
                }
            }
            if clipboard_probe.is_some() && sample == 32 {
                for command in [
                    RdpCommand::MouseMove { x: 76, y: 96 },
                    RdpCommand::MouseInput {
                        x: 76,
                        y: 96,
                        button: MouseButton::Left,
                        is_pressed: true,
                    },
                    RdpCommand::MouseMove { x: 270, y: 96 },
                    RdpCommand::MouseInput {
                        x: 270,
                        y: 96,
                        button: MouseButton::Left,
                        is_pressed: false,
                    },
                ] {
                    let _ = command_tx.send(command).await;
                }
            }
            if clipboard_probe.is_some() && sample == 33 {
                for (scancode, is_pressed) in [
                    (0x1D, true),
                    (0x2A, true),
                    (0x2E, true),
                    (0x2E, false),
                    (0x2A, false),
                    (0x1D, false),
                ] {
                    let _ = command_tx
                        .send(RdpCommand::KeyboardInput {
                            scancode,
                            is_pressed,
                        })
                        .await;
                }
            }
            while let Ok(event) = clipboard_rx.try_recv() {
                remote_clipboard_text = Some(event.text);
            }
            while let Ok(status) = status_rx.try_recv() {
                last_status = Some(format!("{:?}: {:?}", status.status, status.message));
            }
            let (response_tx, response_rx) = oneshot::channel();
            if command_tx
                .send(RdpCommand::RequestFrame { response_tx })
                .await
                .is_err()
            {
                break;
            }
            if let Ok(frame) = tokio::time::timeout(Duration::from_secs(2), response_rx).await {
                let Ok(frame) = frame else {
                    break;
                };
                responsive_frame_requests += 1;
                if !frame.is_empty() {
                    initial_snapshot.get_or_insert_with(|| frame.clone());
                    snapshot = (*frame).clone();
                }
            }
        }

        assert!(
            !handle.is_finished(),
            "RDP runtime stopped before the idle/keep-awake observation completed"
        );
        let _ = command_tx.send(RdpCommand::Disconnect).await;
        let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;

        assert!(
            !snapshot.is_empty(),
            "RDP live diagnostic did not receive a framebuffer; last_status={last_status:?}. \
             Check PORTIX_RDP_TEST_HOST and port 3389 reachability."
        );
        assert_eq!(snapshot.len(), width as usize * height as usize * 4);
        assert!(
            responsive_frame_requests >= 30,
            "RDP command loop stopped responding during the idle period: responses={responsive_frame_requests}/40"
        );
        write_ppm(
            "/tmp/portix-rdp-live-frame.ppm",
            &snapshot,
            width as usize,
            height as usize,
        )
        .expect("write diagnostic ppm");
        let initial_snapshot =
            initial_snapshot.expect("RDP did not provide an initial login frame");
        let changed_pixels = initial_snapshot
            .chunks_exact(4)
            .zip(snapshot.chunks_exact(4))
            .filter(|(before, after)| before[..3] != after[..3])
            .count();
        let total_pixels = width as usize * height as usize;
        assert!(
            changed_pixels > total_pixels / 20,
            "RDP framebuffer did not progress beyond the initial login view: changed_pixels={changed_pixels}/{total_pixels}"
        );
        if let Some(probe) = clipboard_probe {
            let copied = remote_clipboard_text.unwrap_or_default();
            assert!(
                copied.contains(&probe),
                "remote clipboard did not return the selected probe: {copied:?}"
            );
        }
        let report = analyze_black_rows(&snapshot, width as usize, height as usize);
        eprintln!(
            "RDP LIVE FRAME REPORT changed_pixels={}/{} blackish_rows={} longest_run={} row_samples={:?}",
            changed_pixels,
            total_pixels,
            report.blackish_rows,
            report.longest_run,
            report.row_samples
        );
    }

    #[derive(Debug)]
    struct BlackRowReport {
        blackish_rows: usize,
        longest_run: usize,
        row_samples: Vec<usize>,
    }

    fn analyze_black_rows(frame: &[u8], width: usize, height: usize) -> BlackRowReport {
        let mut blackish_rows = 0usize;
        let mut longest_run = 0usize;
        let mut current_run = 0usize;
        let mut row_samples = Vec::new();
        for row in 0..height {
            let start = row * width * 4;
            let end = start + width * 4;
            let blackish = frame[start..end]
                .chunks_exact(4)
                .filter(|px| px[0] < 8 && px[1] < 8 && px[2] < 8)
                .count();
            if blackish * 100 / width >= 80 {
                blackish_rows += 1;
                current_run += 1;
                longest_run = longest_run.max(current_run);
                if row_samples.len() < 12 {
                    row_samples.push(row);
                }
            } else {
                current_run = 0;
            }
        }
        BlackRowReport {
            blackish_rows,
            longest_run,
            row_samples,
        }
    }

    fn write_ppm(path: &str, frame: &[u8], width: usize, height: usize) -> std::io::Result<()> {
        let mut out = Vec::with_capacity(32 + width * height * 3);
        out.extend_from_slice(format!("P6\n{} {}\n255\n", width, height).as_bytes());
        for px in frame.chunks_exact(4) {
            out.extend_from_slice(&px[0..3]);
        }
        std::fs::write(path, out)
    }
}

/// Read a pixel value from source stream and advance index.
#[inline]
fn src_read_pixel(src: &[u8], si: &mut usize, bpp: usize) -> u32 {
    let val = match bpp {
        1 => src[*si] as u32,
        2 => (src[*si] as u32) | ((src[*si + 1] as u32) << 8),
        3 => (src[*si] as u32) | ((src[*si + 1] as u32) << 8) | ((src[*si + 2] as u32) << 16),
        _ => {
            (src[*si] as u32)
                | ((src[*si + 1] as u32) << 8)
                | ((src[*si + 2] as u32) << 16)
                | ((src[*si + 3] as u32) << 24)
        }
    };
    *si += bpp;
    val
}

/// Read a pixel value from destination buffer at offset.
#[inline]
fn read_pixel_value(dst: &[u8], offset: usize, bpp: usize) -> u32 {
    match bpp {
        1 => dst[offset] as u32,
        2 => (dst[offset] as u32) | ((dst[offset + 1] as u32) << 8),
        3 => {
            (dst[offset] as u32)
                | ((dst[offset + 1] as u32) << 8)
                | ((dst[offset + 2] as u32) << 16)
        }
        _ => {
            (dst[offset] as u32)
                | ((dst[offset + 1] as u32) << 8)
                | ((dst[offset + 2] as u32) << 16)
                | ((dst[offset + 3] as u32) << 24)
        }
    }
}

/// Write a pixel value to destination buffer at offset.
#[inline]
fn write_pixel_value(dst: &mut [u8], offset: usize, pix: u32, bpp: usize) {
    match bpp {
        1 => {
            dst[offset] = pix as u8;
        }
        2 => {
            dst[offset] = (pix & 0xFF) as u8;
            dst[offset + 1] = ((pix >> 8) & 0xFF) as u8;
        }
        3 => {
            dst[offset] = (pix & 0xFF) as u8;
            dst[offset + 1] = ((pix >> 8) & 0xFF) as u8;
            dst[offset + 2] = ((pix >> 16) & 0xFF) as u8;
        }
        _ => {
            dst[offset] = (pix & 0xFF) as u8;
            dst[offset + 1] = ((pix >> 8) & 0xFF) as u8;
            dst[offset + 2] = ((pix >> 16) & 0xFF) as u8;
            dst[offset + 3] = ((pix >> 24) & 0xFF) as u8;
        }
    }
}

/// Write foreground/background image for first scanline.
/// On first line: fg bit → fgPel, bg bit → BLACK (0).
#[inline]
fn write_first_line_fgbg_image(
    dst: &mut [u8],
    di: &mut usize,
    output_size: usize,
    bitmask: u8,
    fg_pel: u32,
    count: usize,
    bpp: usize,
) {
    let mut mask: u8 = 0x01;
    for _ in 0..count {
        if *di + bpp > output_size {
            break;
        }
        let data = if (bitmask & mask) != 0 { fg_pel } else { 0 };
        write_pixel_value(dst, *di, data, bpp);
        *di += bpp;
        mask = mask.wrapping_shl(1);
    }
}

/// Write foreground/background image for non-first scanlines.
/// bg bit → copy pixel above; fg bit → XOR pixel above with fgPel.
#[inline]
#[allow(clippy::too_many_arguments)]
fn write_fgbg_image(
    dst: &mut [u8],
    di: &mut usize,
    output_size: usize,
    row_delta: usize,
    bitmask: u8,
    fg_pel: u32,
    count: usize,
    bpp: usize,
) {
    let mut mask: u8 = 0x01;
    for _ in 0..count {
        if *di + bpp > output_size {
            break;
        }
        let xor_pixel = read_pixel_value(dst, *di - row_delta, bpp);
        let data = if (bitmask & mask) != 0 {
            xor_pixel ^ fg_pel
        } else {
            xor_pixel
        };
        write_pixel_value(dst, *di, data, bpp);
        *di += bpp;
        mask = mask.wrapping_shl(1);
    }
}
