use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use portix_serv::domain::events::ConnectionStatusEvent;
use portix_serv::domain::rdp_profile::RdpProfile;
use portix_serv::infrastructure::rdp_client::{
    RdpClipboardEvent, RdpCommand, RdpFrameEvent, RdpRuntime,
};
use tokio::sync::{broadcast, mpsc, oneshot};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: cargo run --example rdp_test -- <username> <password>");
        eprintln!("Example: cargo run --example rdp_test -- testuser mypassword");
        std::process::exit(1);
    }

    let username = &args[1];
    let password = &args[2];
    let host = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| "host.example.com".to_owned());
    let width = args
        .get(4)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(1440);
    let height = args
        .get(5)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(772);

    let mut extra = HashMap::new();
    extra.insert("portix_debug".to_owned(), "1".to_owned());
    extra.insert("portix_stream_pixels".to_owned(), "0".to_owned());
    if let Ok(path) = std::env::var("PORTIX_RDP_TEST_DRIVE_PATH") {
        extra.insert("portix_drive_path".to_owned(), path);
        extra.insert(
            "portix_drive_name".to_owned(),
            std::env::var("PORTIX_RDP_TEST_DRIVE_NAME").unwrap_or_else(|_| "PORTIX".to_owned()),
        );
    }

    let profile = RdpProfile {
        id: "test-1".to_owned(),
        name: "xrdp framebuffer diagnostic".to_owned(),
        host: host.clone(),
        port: 3389,
        username: username.clone(),
        password: Some(password.clone()),
        domain: None,
        width,
        height,
        screen_mode: 1,
        extra,
    };

    println!(
        "Connecting to RDP at {}:3389 as '{}' ({}x{}, signal-only stream)...",
        host, username, width, height
    );

    let (frame_tx, mut frame_rx) = broadcast::channel::<RdpFrameEvent>(4);
    let (clipboard_tx, _clipboard_rx) = broadcast::channel::<RdpClipboardEvent>(4);
    let (status_tx, mut status_rx) = broadcast::channel::<ConnectionStatusEvent>(16);
    let (command_tx, command_rx) = mpsc::channel::<RdpCommand>(32);

    let session_id = "test-session".to_owned();
    let runtime = RdpRuntime::new(profile, session_id, frame_tx, clipboard_tx, status_tx);

    // Spawn the RDP connection
    let handle = tokio::spawn(async move {
        match runtime.run(command_rx).await {
            Ok(()) => println!("\n✓ RDP session ended gracefully."),
            Err(e) => eprintln!("\n✗ RDP error: {}", e),
        }
    });

    let timeout = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(timeout);

    let mut frame_count = 0;
    let mut snapshot_count = 0;
    let mut last_snapshot: Option<Arc<Vec<u8>>> = None;
    let mut snapshot_tick = tokio::time::interval(Duration::from_millis(750));
    snapshot_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                println!("\nTimeout reached (30s). Disconnecting...");
                let _ = command_tx.send(RdpCommand::Disconnect).await;
                break;
            }
            _ = snapshot_tick.tick() => {
                let (response_tx, response_rx) = oneshot::channel();
                if command_tx
                    .send(RdpCommand::RequestFrame { response_tx })
                    .await
                    .is_err()
                {
                    break;
                }

                match tokio::time::timeout(Duration::from_secs(2), response_rx).await {
                    Ok(Ok(snapshot)) if !snapshot.is_empty() => {
                        snapshot_count += 1;
                        let report = analyze_black_rows(&snapshot, width as usize, height as usize);
                        println!(
                            "  Snapshot #{}: {} bytes, blackish_rows={}, longest_blackish_run={}, interior_bars={}, samples={:?}",
                            snapshot_count,
                            snapshot.len(),
                            report.blackish_rows,
                            report.longest_run,
                            report.interior_bar_runs,
                            report.row_samples
                        );
                        last_snapshot = Some(snapshot);
                    }
                    Ok(Ok(_)) => println!("  Snapshot: empty (no new framebuffer version yet)"),
                    Ok(Err(_)) => println!("  Snapshot: response channel closed"),
                    Err(_) => println!("  Snapshot: timeout"),
                }
            }
            result = status_rx.recv() => {
                if let Ok(status) = result {
                    println!("  Status: {:?} {:?}", status.status, status.message);
                }
            }
            result = frame_rx.recv() => {
                match result {
                    Ok(event) => {
                        frame_count += 1;
                        let non_zero = event.data.iter().filter(|&&b| b != 0).count();
                        println!(
                            "  Event #{}: region={}x{} at {},{}, desktop={}x{}, {} bytes, {} non-zero pixels",
                            frame_count,
                            event.width,
                            event.height,
                            event.x,
                            event.y,
                            event.desktop_width,
                            event.desktop_height,
                            event.data.len(),
                            non_zero / 4,
                        );
                        if snapshot_count >= 3 {
                            println!("\nReceived {} frame events and {} snapshots.", frame_count, snapshot_count);
                            let _ = command_tx.send(RdpCommand::Disconnect).await;
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        println!("Frame channel closed.");
                        break;
                    }
                }
            }
        }
    }

    let _ = handle.await;
    if let Some(snapshot) = last_snapshot {
        let expected = width as usize * height as usize * 4;
        if snapshot.len() != expected {
            println!(
                "Final check: snapshot size mismatch, got {} bytes expected {} bytes",
                snapshot.len(),
                expected
            );
        } else {
            let report = analyze_black_rows(&snapshot, width as usize, height as usize);
            if report.interior_bar_runs >= 3 {
                println!(
                    "Final check: suspected horizontal-bar artifact remains ({} interior bar runs, {} blackish rows, longest run {}).",
                    report.interior_bar_runs, report.blackish_rows, report.longest_run
                );
                std::process::exit(2);
            }
            println!("Final check: framebuffer did not show repeated black horizontal bars.");
        }
    } else {
        println!("Final check: no framebuffer snapshot received.");
        std::process::exit(1);
    }
}

#[derive(Debug)]
struct BlackRowReport {
    blackish_rows: usize,
    longest_run: usize,
    interior_bar_runs: usize,
    row_samples: Vec<usize>,
}

fn analyze_black_rows(frame: &[u8], width: usize, height: usize) -> BlackRowReport {
    let mut blackish_rows = 0usize;
    let mut longest_run = 0usize;
    let mut current_run = 0usize;
    let mut row_samples = Vec::new();

    if frame.len() < width.saturating_mul(height).saturating_mul(4) || width == 0 {
        return BlackRowReport {
            blackish_rows: 0,
            longest_run: 0,
            interior_bar_runs: 0,
            row_samples,
        };
    }

    let mut blackish_by_row = vec![false; height];
    for row in 0..height {
        let start = row * width * 4;
        let end = start + width * 4;
        let blackish = frame[start..end]
            .chunks_exact(4)
            .filter(|px| px[0] < 8 && px[1] < 8 && px[2] < 8)
            .count();
        if blackish * 100 / width >= 80 {
            blackish_by_row[row] = true;
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

    let mut interior_bar_runs = 0usize;
    let mut row = 1usize;
    while row + 1 < height {
        if !blackish_by_row[row] {
            row += 1;
            continue;
        }

        let start = row;
        while row + 1 < height && blackish_by_row[row] {
            row += 1;
        }
        let end = row;
        let run_len = end.saturating_sub(start);
        let has_content_above = blackish_by_row[..start].iter().any(|value| !*value);
        let has_content_below = blackish_by_row[end..].iter().any(|value| !*value);
        if has_content_above && has_content_below && run_len <= 24 {
            interior_bar_runs += 1;
        }
    }

    BlackRowReport {
        blackish_rows,
        longest_run,
        interior_bar_runs,
        row_samples,
    }
}
