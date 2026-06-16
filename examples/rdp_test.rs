/// Standalone RDP connection tester.
/// Run with:
///   cargo run --example rdp_test -- 192.168.18.172 asepimam 'Drfvetcb!$%^'
///
/// Set PORTIX_RDP_DEBUG=1 for verbose packet-level output.

use std::collections::HashMap;
use std::env;

use portix_serv::application::rdp_session_manager::RdpSessionManager;
use portix_serv::domain::rdp_profile::RdpProfile;
use portix_serv::domain::session::ConnectionStatus;

#[tokio::main]
async fn main() {
    // ── Enable debug logging ─────────────────────────────────────────────────
    unsafe { env::set_var("PORTIX_RDP_DEBUG", "1") };

    let args: Vec<String> = env::args().collect();
    let host = args.get(1).cloned().unwrap_or_else(|| "192.168.18.172".into());
    let username = args.get(2).cloned().unwrap_or_else(|| "asepimam".into());
    let password = args.get(3).cloned().unwrap_or_else(|| "Drfvetcb!$%^".into());

    println!("=== Portix RDP Connection Test ===");
    println!("Target  : {}:3389", host);
    println!("Username: {}", username);
    println!();

    let profile = RdpProfile {
        id: "test-1".into(),
        name: "Test".into(),
        host: host.clone(),
        port: 3389,
        username: username.clone(),
        password: Some(password),
        domain: None,
        width: 1280,
        height: 720,
        screen_mode: 1,
        extra: HashMap::new(),
    };

    let mgr = RdpSessionManager::new();
    let mut status_rx = mgr.connection_status_stream();
    let mut error_rx = mgr.error_event_stream();

    println!("[*] Calling rdp_connect ...");
    match mgr.connect(profile).await {
        Err(e) => {
            eprintln!("[FAIL] connect() returned error immediately: {}", e);
            return;
        }
        Ok(info) => {
            println!("[OK] session created: id={} {}x{}", info.id, info.width, info.height);

            // Wait up to 20 seconds for connection result
            let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(20);
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    println!("[TIMEOUT] No connection result after 20s");
                    break;
                }

                tokio::select! {
                    Ok(ev) = status_rx.recv() => {
                        println!("[STATUS] session={} status={:?} msg={:?}",
                            ev.session_id, ev.status, ev.message);
                        match ev.status {
                            ConnectionStatus::Connected => {
                                println!("[SUCCESS] RDP connection established!");
                                // Let it run for 3 more seconds to get frame data
                                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

                                // Try requesting a frame
                                print!("[*] Requesting frame ... ");
                                match mgr.request_frame(info.id.clone()).await {
                                    Ok(data) if !data.is_empty() => {
                                        println!("OK! {} bytes ({} pixels)",
                                            data.len(), data.len() / 4);
                                    }
                                    Ok(_) => println!("empty (no frame yet)"),
                                    Err(e) => println!("ERROR: {}", e),
                                }
                                return;
                            }
                            ConnectionStatus::Error | ConnectionStatus::Disconnected => {
                                println!("[FAIL] Connection ended with status={:?}", ev.status);
                                return;
                            }
                            _ => {}
                        }
                    }
                    Ok(ev) = error_rx.recv() => {
                        eprintln!("[ERROR] session={:?} message={}",
                            ev.session_id, ev.message);
                        return;
                    }
                    _ = tokio::time::sleep(remaining) => {
                        println!("[TIMEOUT] No connection result in time");
                        break;
                    }
                }
            }
        }
    }
}
