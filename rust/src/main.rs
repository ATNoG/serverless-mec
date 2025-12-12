use chrono::{SecondsFormat, Utc};
use env_logger::Env;
use log::{info, warn};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::thread;
use std::time::Instant;
use uuid::Uuid;

const DEFAULT_BPF: &str =
    "(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001";

#[derive(Debug, Clone)]
struct Config {
    iface: String,
    bpf: Option<String>,
    display_filter: Option<String>,
    log_every: u64,
    ce_type: String,
    include_raw_hex: bool,
    sink_url: Option<String>,
    stdout_ndjson: bool,
    promiscuous: bool,
    send_queue_max: usize,
    ce_source: String,
}

#[derive(Serialize, Debug, Clone)]
struct CamRecord {
    timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    frame_number: Option<u64>,
    cam_layer: String,
    cam_fields: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    frame_raw_hex: Option<String>,
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn env_bool(name: &str, default: bool) -> bool {
    let val = match env::var(name) {
        Ok(v) => v,
        Err(_) => return default,
    };
    matches!(val.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
}

impl Config {
    fn from_env() -> Self {
        let iface = env::var("IFACE")
            .unwrap_or_else(|_| "eth0".to_string())
            .trim()
            .to_string();

        let bpf_raw = env::var("BPF").unwrap_or_else(|_| DEFAULT_BPF.to_string());
        let bpf = {
            let s = bpf_raw.trim().to_string();
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        };

        let display_filter = env::var("DISPLAY_FILTER")
            .unwrap_or_default()
            .trim()
            .to_string();
        let display_filter = if display_filter.is_empty() {
            None
        } else {
            Some(display_filter)
        };

        let log_every = env::var("LOG_EVERY")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);

        let ce_type = env::var("CE_TYPE").unwrap_or_else(|_| "its.cam".to_string());

        let include_raw_hex = env_bool("INCLUDE_RAW_HEX", false);
        let stdout_ndjson = env_bool("STDOUT_NDJSON", true);
        let promiscuous = env_bool("PROMISCUOUS", false);

        let send_queue_max = env::var("SEND_QUEUE_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1000);

        let sink_url = env::var("K_SINK")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let host_id = env::var("K8S_NODE_NAME")
            .or_else(|_| env::var("NODE_NAME"))
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_else(|_| "host".to_string());

        let ce_source = format!("sniffer://{}/{}", host_id.trim(), iface);

        Config {
            iface,
            bpf,
            display_filter,
            log_every,
            ce_type,
            include_raw_hex,
            sink_url,
            stdout_ndjson,
            promiscuous,
            send_queue_max,
            ce_source,
        }
    }
}

fn val_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Array(arr) => {
            if arr.is_empty() {
                None
            } else {
                let mut parts = Vec::new();
                for item in arr {
                    if let Some(s) = val_to_string(item) {
                        parts.push(s);
                    }
                }
                if parts.is_empty() {
                    None
                } else if parts.len() == 1 {
                    Some(parts.remove(0))
                } else {
                    Some(parts.join(","))
                }
            }
        }
        Value::Object(map) => {
            // Try typical tshark keys
            if let Some(inner) = map
                .get("showname_value")
                .or_else(|| map.get("show"))
                .or_else(|| map.get("value"))
            {
                val_to_string(inner)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_layer_fields(layer: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Value::Object(map) = layer {
        for (key, val) in map {
            if let Some(s) = val_to_string(val) {
                // Keep only the last part after '.'
                let simple_name = key.split('.').last().unwrap_or(key);
                out.insert(simple_name.to_string(), s);
            }
        }
    }
    out
}

fn extract_frame_number(frame_layer: &Value) -> Option<u64> {
    if let Value::Object(map) = frame_layer {
        let candidates = [
            "frame.number",
            "frame.num",
            "frame.frame_number",
            "frame.no",
        ];
        for k in candidates {
            if let Some(v) = map.get(k) {
                if let Some(s) = val_to_string(v) {
                    if let Ok(n) = s.parse::<u64>() {
                        return Some(n);
                    }
                }
            }
        }
    }
    None
}

/// Best-effort extraction of full raw frame hex, if tshark provides it.
fn extract_frame_raw_hex(layers: &Value) -> Option<String> {
    if let Value::Object(layer_map) = layers {
        if let Some(frame_raw) = layer_map.get("frame_raw") {
            if let Some(s) = val_to_string(frame_raw) {
                return Some(s);
            }
            if let Value::Object(m) = frame_raw {
                for (k, v) in m {
                    if k.contains("frame_raw") {
                        if let Some(s) = val_to_string(v) {
                            return Some(s);
                        }
                    }
                }
            }
        }
        if let Some(frame) = layer_map.get("frame") {
            if let Value::Object(m) = frame {
                for (k, v) in m {
                    if k.contains("frame_raw") {
                        if let Some(s) = val_to_string(v) {
                            return Some(s);
                        }
                    }
                }
            }
        }
    }
    None
}

fn packet_to_record(v: &Value, include_raw_hex: bool) -> Option<CamRecord> {
    let layers = v.get("layers")?;
    let its_layer = match layers.get("its") {
        Some(l) => l,
        None => return None,
    };

    let cam_fields = extract_layer_fields(its_layer);
    if cam_fields.is_empty() {
        return None;
    }

    let frame_number = layers.get("frame").and_then(|f| extract_frame_number(f));

    let frame_raw_hex = if include_raw_hex {
        extract_frame_raw_hex(layers)
    } else {
        None
    };

    Some(CamRecord {
        timestamp: now_iso(),
        frame_number,
        cam_layer: "its".to_string(),
        cam_fields,
        frame_raw_hex,
    })
}

fn post_cloudevent_structured(
    client: &reqwest::blocking::Client,
    sink_url: &str,
    event_type: &str,
    source: &str,
    data: &CamRecord,
) -> Result<(), Box<dyn Error>> {
    let stationtype = data.cam_fields.get("stationtype").cloned();
    let subject = data.frame_number.map(|n| n.to_string());

    let event_id = Uuid::new_v4().to_string();
    let event_time = now_iso();

    let mut event = serde_json::json!({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": event_id,
        "time": event_time,
        "datacontenttype": "application/json",
        "data": data,
    });

    if let Some(subj) = subject {
        event
            .as_object_mut()
            .unwrap()
            .insert("subject".to_string(), Value::String(subj));
    }

    if let Some(st) = stationtype {
        event
            .as_object_mut()
            .unwrap()
            .insert("stationtype".to_string(), Value::String(st));
    }

    let body = serde_json::to_vec(&event)?;

    let start = Instant::now();
    let resp = client
        .post(sink_url)
        .header("content-type", "application/cloudevents+json")
        .body(body)
        .send()?;
    let elapsed_ns = start.elapsed().as_nanos();
    info!(
        "sniffer POST elapsed_ns={} status={}",
        elapsed_ns,
        resp.status().as_u16()
    );

    if !resp.status().is_success() {
        return Err(format!(
            "sink returned non-success HTTP status {}",
            resp.status()
        )
        .into());
    }

    Ok(())
}

fn sender_worker(
    rx: std::sync::mpsc::Receiver<CamRecord>,
    sink_url: String,
    event_type: String,
    source: String,
) {
    let client = match reqwest::blocking::Client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            warn!("[WARN] failed to create reqwest client: {}", e);
            return;
        }
    };

    for rec in rx {
        if let Err(e) =
            post_cloudevent_structured(&client, &sink_url, &event_type, &source, &rec)
        {
            warn!("[WARN] sender failed: {}", e);
        }
    }
}

fn run_live() -> Result<(), Box<dyn Error>> {
    let cfg = Config::from_env();

    info!(
        ">> LIVE capture iface='{}' promisc={}",
        cfg.iface,
        if cfg.promiscuous { "on" } else { "off" }
    );
    info!(
        ">> BPF='{}'",
        cfg.bpf.as_deref().unwrap_or("<none / empty>")
    );
    if let Some(df) = &cfg.display_filter {
        info!(">> DISPLAY_FILTER='{}'", df);
    }
    let sink_on = cfg.sink_url.is_some();
    info!(
        ">> CloudEvents sink: {} -> {}",
        if sink_on { "on" } else { "off" },
        cfg.sink_url.as_deref().unwrap_or("-")
    );
    info!(">> SEND_QUEUE_MAX={}", cfg.send_queue_max);

    let (tx_opt, _rx_opt, sender_handle_opt): (
        Option<SyncSender<CamRecord>>,
        Option<std::sync::mpsc::Receiver<CamRecord>>,
        Option<thread::JoinHandle<()>>,
    ) = if let Some(sink_url) = cfg.sink_url.clone() {
        let (tx, rx) = sync_channel::<CamRecord>(cfg.send_queue_max);
        let event_type = cfg.ce_type.clone();
        let source = cfg.ce_source.clone();
        let handle = thread::spawn(move || sender_worker(rx, sink_url, event_type, source));
        (Some(tx), None, Some(handle))
    } else {
        (None, None, None)
    };

    let tx = tx_opt;

    // Build tshark command
    let mut cmd = Command::new("tshark");
    cmd.arg("-l"); // line-buffered
    cmd.arg("-i").arg(&cfg.iface);
    if !cfg.promiscuous {
        cmd.arg("-p"); // disable promiscuous mode
    }
    if let Some(bpf) = &cfg.bpf {
        if !bpf.trim().is_empty() {
            cmd.arg("-f").arg(bpf);
        }
    }
    if let Some(df) = &cfg.display_filter {
        if !df.trim().is_empty() {
            cmd.arg("-Y").arg(df);
        }
    }
    cmd.arg("-T").arg("ek"); // JSON per event
    cmd.arg("-n"); // no name resolution
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::inherit());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to spawn tshark: {}", e);
            std::process::exit(1);
        }
    };

    let stdout = child
        .stdout
        .take()
        .expect("failed to capture tshark stdout");
    let reader = BufReader::new(stdout);

    let mut processed: u64 = 0;

    for line_res in reader.lines() {
        let line = match line_res {
            Ok(l) => l,
            Err(e) => {
                warn!("error reading tshark stdout: {}", e);
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let json_val: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                warn!("failed to parse JSON from tshark: {} | line={}", e, trimmed);
                continue;
            }
        };

        let rec_opt = packet_to_record(&json_val, cfg.include_raw_hex);
        let rec = match rec_opt {
            Some(r) => r,
            None => continue,
        };

        if cfg.stdout_ndjson {
            let json_line = serde_json::to_string(&rec)?;
            println!("{}", json_line);
        }

        if sink_on {
            if let Some(ref tx) = tx {
                match tx.try_send(rec) {
                    Ok(_) => {}
                    Err(TrySendError::Full(_)) => {
                        warn!("[WARN] SEND_QUEUE full, dropping event");
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        warn!("sender thread disconnected; stopping sends");
                        break;
                    }
                }
            }
        }

        processed += 1;
        if cfg.log_every > 0 && processed % cfg.log_every == 0 {
            info!(
                "[{}] processed {} CAM packets (file={}, sink={})",
                now_iso(),
                processed,
                if cfg.stdout_ndjson { "on" } else { "off" },
                if sink_on { "on" } else { "off" }
            );
        }
    }

    // Wait for tshark to exit
    let status = child.wait()?;
    if !status.success() {
        // This will bubble up to main() and print "Error: ..."
        return Err(format!("tshark exited with non-zero status: {}", status).into());
    }
    info!("tshark exited with status: {}", status);

    // Drop sender to close channel
    drop(tx);

    if let Some(handle) = sender_handle_opt {
        let _ = handle.join();
    }

    Ok(())
}

fn main() {
    // Make sure *something* appears in `kubectl logs` even if logging is misconfigured.
    println!("its_live_capture starting up");

    // Default log level to info if RUST_LOG is not set.
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    if let Err(e) = run_live() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
