use chrono::{SecondsFormat, TimeZone, Utc};
use cloudevents::{EventBuilder, EventBuilderV10};
use crossbeam_channel::{bounded, Receiver, TrySendError};
use env_logger::Env;
use log::{info, warn};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::env;
use std::io::{BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_BPF: &str =
    "(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001";

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

#[derive(Debug, Clone)]
struct Config {
    iface: String,
    bpf: Option<String>,
    log_every: u64,
    ce_type: String,
    stdout_ndjson: bool,
    promiscuous: bool,
    send_queue_max: usize,
    sink_url: Option<String>,
    ce_source: String,
    snaplen: u32,
    http_timeout_secs: u64,
}

impl Config {
    fn from_env() -> Self {
        let iface = env::var("IFACE")
            .unwrap_or_else(|_| "eth0".to_string())
            .trim()
            .to_string();

        // Match your previous behavior:
        // - If BPF is missing => use DEFAULT_BPF
        // - If BPF is present but empty => disable filter
        let bpf = match env::var("BPF") {
            Ok(v) => {
                let s = v.trim().to_string();
                if s.is_empty() { None } else { Some(s) }
            }
            Err(_) => {
                let s = DEFAULT_BPF.trim().to_string();
                if s.is_empty() { None } else { Some(s) }
            }
        };

        let log_every = env::var("LOG_EVERY")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);

        let ce_type = env::var("CE_TYPE").unwrap_or_else(|_| "its.cam".to_string());

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

        let snaplen = env::var("SNAPLEN")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(262_144);

        let http_timeout_secs = env::var("HTTP_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5);

        Self {
            iface,
            bpf,
            log_every,
            ce_type,
            stdout_ndjson,
            promiscuous,
            send_queue_max,
            sink_url,
            ce_source,
            snaplen,
            http_timeout_secs,
        }
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

fn fmt_mac(s: &[u8]) -> String {
    if s.len() != 6 {
        return "".to_string();
    }
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        s[0], s[1], s[2], s[3], s[4], s[5]
    )
}

fn dlt_name(dlt: u32) -> &'static str {
    match dlt {
        127 => "IEEE802_11_RADIO",
        1 => "EN10MB",
        113 => "LINUX_SLL",
        276 => "LINUX_SLL2",
        _ => "UNKNOWN",
    }
}

// Very small “meta” extraction for DLT=127 (radiotap + 802.11)
// Enough to get fields like your example: radiotap_len, wlan_type/subtype, to_ds/from_ds, qos, addr1/2/3.
fn parse_meta(dlt: u32, pkt: &[u8]) -> Option<Value> {
    if dlt != 127 {
        return Some(json!({ "linktype": dlt_name(dlt) }));
    }

    if pkt.len() < 8 {
        return None;
    }
    // Radiotap header: version(1), pad(1), len(2 LE), present(4...)
    let rt_len = u16::from_le_bytes([pkt[2], pkt[3]]) as usize;
    if pkt.len() < rt_len + 4 {
        return None;
    }
    // 802.11 frame control at start of 802.11 header
    if pkt.len() < rt_len + 24 {
        return Some(json!({
            "linktype": "IEEE802_11_RADIO",
            "radiotap_len": rt_len
        }));
    }

    let fc = u16::from_le_bytes([pkt[rt_len], pkt[rt_len + 1]]);
    let wlan_type = ((fc >> 2) & 0x3) as u8;
    let wlan_subtype = ((fc >> 4) & 0xF) as u8;
    let flags = (fc >> 8) as u8;
    let to_ds = (flags & 0x01) != 0;
    let from_ds = (flags & 0x02) != 0;
    let qos = wlan_type == 2 && (wlan_subtype & 0x8) != 0;

    // For mgmt/data frames addr1/2/3 are at fixed positions
    let a1 = fmt_mac(&pkt[rt_len + 4..rt_len + 10]);
    let a2 = fmt_mac(&pkt[rt_len + 10..rt_len + 16]);
    let a3 = fmt_mac(&pkt[rt_len + 16..rt_len + 22]);

    Some(json!({
        "linktype": "IEEE802_11_RADIO",
        "radiotap_len": rt_len,
        "wlan_type": wlan_type,
        "wlan_subtype": wlan_subtype,
        "to_ds": if to_ds {1} else {0},
        "from_ds": if from_ds {1} else {0},
        "qos": qos,
        "addr1": a1,
        "addr2": a2,
        "addr3": a3
    }))
}

#[derive(Clone)]
struct PostJob {
    body: Vec<u8>, // structured CloudEvent JSON
}

fn spawn_dumpcap(cfg: &Config) -> std::io::Result<Child> {
    // We force libpcap format so we can parse the classic global header easily.
    // (dumpcap defaults to pcapng unless -P) and your earlier log showed global-header EOF when it wasn’t pcap.
    let mut cmd = Command::new("dumpcap");
    cmd.arg("-i").arg(&cfg.iface);
    cmd.arg("-P"); // libpcap format (NOT pcapng)
    cmd.arg("-q"); // quiet
    cmd.arg("-s").arg(cfg.snaplen.to_string());
    if !cfg.promiscuous {
        cmd.arg("-p"); // no promiscuous
    }
    if let Some(bpf) = &cfg.bpf {
        cmd.arg("-f").arg(bpf);
    }
    cmd.arg("-w").arg("-"); // write capture to stdout

    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::inherit());

    cmd.spawn()
}

#[derive(Copy, Clone)]
enum Endian {
    Little,
    Big,
}

fn read_u32(endian: Endian, b: [u8; 4]) -> u32 {
    match endian {
        Endian::Little => u32::from_le_bytes(b),
        Endian::Big => u32::from_be_bytes(b),
    }
}

fn read_u16(endian: Endian, b: [u8; 2]) -> u16 {
    match endian {
        Endian::Little => u16::from_le_bytes(b),
        Endian::Big => u16::from_be_bytes(b),
    }
}

struct PcapGlobal {
    endian: Endian,
    nano: bool,
    snaplen: u32,
    dlt: u32,
}

fn parse_pcap_global(r: &mut dyn Read) -> std::io::Result<PcapGlobal> {
    let mut gh = [0u8; 24];
    r.read_exact(&mut gh)?;

    let magic = u32::from_le_bytes([gh[0], gh[1], gh[2], gh[3]]);
    let (endian, nano) = match magic {
        0xa1b2c3d4 => (Endian::Little, false),
        0xd4c3b2a1 => (Endian::Big, false),
        0xa1b23c4d => (Endian::Little, true), // nanosecond resolution
        0x4d3cb2a1 => (Endian::Big, true),
        _ => (Endian::Little, false), // best-effort; you can hard-fail if you prefer
    };

    let _ver_major = read_u16(endian, [gh[4], gh[5]]);
    let _ver_minor = read_u16(endian, [gh[6], gh[7]]);

    let snaplen = read_u32(endian, [gh[16], gh[17], gh[18], gh[19]]);
    let dlt = read_u32(endian, [gh[20], gh[21], gh[22], gh[23]]);

    Ok(PcapGlobal {
        endian,
        nano,
        snaplen,
        dlt,
    })
}

fn build_record_json(cfg: &Config, frame_no: u64, dlt: u32, ts_sec: u32, ts_sub: u32, pkt: &[u8]) -> Value {
    let nanos = if ts_sub > 999_999_999 { 0 } else { ts_sub * if cfg_time_is_nano() { 1 } else { 1000 } };
    // NOTE: we decide later whether it's nano or usec based on pcap header; here we just format both fields below.
    let dt = Utc.timestamp_opt(ts_sec as i64, nanos).single().unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());
    let ts_iso = dt.to_rfc3339_opts(SecondsFormat::Micros, true);

    let payload_hex = bytes_to_hex(pkt);
    let meta = parse_meta(dlt, pkt).unwrap_or_else(|| json!({ "linktype": dlt_name(dlt) }));

    json!({
        "timestamp": ts_iso,
        "frame_number": frame_no.to_string(),
        "cam_layer": "raw",
        "cam_fields": {
            "frame_raw_hex": payload_hex
        },
        "meta": meta,
        "pcap_ts_sec": ts_sec,
        // Keep both names people use:
        "pcap_ts_usec": ts_sub,
        "pcap_ts_sub": ts_sub
    })
}

// we don’t know nano/usec at compile time; this is only used for formatting above,
// but we still include raw fields in JSON. We’ll set `time` on the event using the correct resolution later.
fn cfg_time_is_nano() -> bool { false }

fn sender_worker(stop: Arc<AtomicBool>, sink: String, http_timeout_secs: u64, rx: Receiver<PostJob>) {
    let client = match Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(http_timeout_secs))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("[WARN] failed to create HTTP client: {}", e);
            return;
        }
    };

    while !stop.load(Ordering::Relaxed) {
        let job = match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(j) => j,
            Err(_) => continue,
        };

        let start = Instant::now();
        let resp = client
            .post(&sink)
            .header("content-type", "application/cloudevents+json")
            .body(job.body)
            .send();

        match resp {
            Ok(r) => {
                let elapsed_ns = start.elapsed().as_nanos();
                info!("sniffer POST elapsed_ns={} status={}", elapsed_ns, r.status().as_u16());
            }
            Err(e) => {
                warn!("[WARN] sender failed: {}", e);
            }
        }
    }
}

fn run_live() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::from_env();

    println!("its_live_capture starting up");
    info!(
        ">> LIVE capture iface='{}' promisc={}",
        cfg.iface,
        if cfg.promiscuous { "on" } else { "off" }
    );
    info!(
        ">> BPF='{}'",
        cfg.bpf.as_deref().unwrap_or("<none / empty>")
    );
    info!(
        ">> CloudEvents sink: {} -> {}",
        if cfg.sink_url.is_some() { "on" } else { "off" },
        cfg.sink_url.as_deref().unwrap_or("-")
    );
    info!(">> SEND_QUEUE_MAX={}", cfg.send_queue_max);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let _ = ctrlc::set_handler(move || {
        stop2.store(true, Ordering::SeqCst);
    });

    let sink_on = cfg.sink_url.is_some();

    let (tx, rx) = bounded::<PostJob>(cfg.send_queue_max);

    // sender threads (only if sink configured)
    if let Some(sink) = cfg.sink_url.clone() {
        for _ in 0..2 {
            let stop_w = stop.clone();
            let rx_w = rx.clone();
            let sink_w = sink.clone();
            let http_timeout_secs = cfg.http_timeout_secs;
            thread::spawn(move || sender_worker(stop_w, sink_w, http_timeout_secs, rx_w));
        }
    }

    let mut processed: u64 = 0;
    let mut dropped: u64 = 0;
    let mut frame_no: u64 = 0;

    while !stop.load(Ordering::Relaxed) {
        let mut child = match spawn_dumpcap(&cfg) {
            Ok(c) => c,
            Err(e) => {
                warn!("[WARN] failed to spawn dumpcap: {}. retrying...", e);
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        let stdout = match child.stdout.take() {
            Some(s) => s,
            None => {
                warn!("[WARN] dumpcap stdout not available. retrying...");
                let _ = child.kill();
                continue;
            }
        };

        let mut reader = BufReader::new(stdout);

        let pcap = match parse_pcap_global(&mut reader) {
            Ok(p) => p,
            Err(e) => {
                warn!("[WARN] failed to parse pcap global header: {}. restarting dumpcap...", e);
                let _ = child.kill();
                let _ = child.wait();
                continue;
            }
        };

        info!(
            ">> dumpcap pcap header: dlt={}({}) snaplen={} ts_res={}",
            pcap.dlt,
            dlt_name(pcap.dlt),
            pcap.snaplen,
            if pcap.nano { "ns" } else { "us" }
        );

        // packet loop
        loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }

            // per-packet header (classic pcap)
            let mut ph = [0u8; 16];
            if let Err(_) = reader.read_exact(&mut ph) {
                break;
            }

            let ts_sec = read_u32(pcap.endian, [ph[0], ph[1], ph[2], ph[3]]);
            let ts_sub = read_u32(pcap.endian, [ph[4], ph[5], ph[6], ph[7]]);
            let incl_len = read_u32(pcap.endian, [ph[8], ph[9], ph[10], ph[11]]);
            let orig_len = read_u32(pcap.endian, [ph[12], ph[13], ph[14], ph[15]]);

            let mut pkt = vec![0u8; incl_len as usize];
            if let Err(_) = reader.read_exact(&mut pkt) {
                break;
            }

            frame_no += 1;

            // Create time attribute from pcap timestamp (us or ns)
            let nanos = if pcap.nano {
                ts_sub
            } else {
                ts_sub.saturating_mul(1000)
            };
            let ev_time = Utc
                .timestamp_opt(ts_sec as i64, nanos)
                .single()
                .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());

            // Build JSON record (what you wanted to see in Data)
            let rec = {
                let mut rec = build_record_json(&cfg, frame_no, pcap.dlt, ts_sec, ts_sub, &pkt);

                // Add lengths too (useful & cheap)
                if let Value::Object(obj) = &mut rec {
                    obj.insert("cap_len".to_string(), json!(incl_len));
                    obj.insert("orig_len".to_string(), json!(orig_len));
                }
                rec
            };

            if cfg.stdout_ndjson {
                if let Ok(line) = serde_json::to_string(&rec) {
                    println!("{}", line);
                }
            }

            if sink_on {
                // Build CloudEvent using official SDK builder
                let event = EventBuilderV10::new()
                    .id(Uuid::new_v4().to_string())
                    .ty(cfg.ce_type.clone())
                    .source(cfg.ce_source.clone())
                    .subject(frame_no.to_string())
                    .time(ev_time)
                    .data("application/json", rec)
                    .extension("dlt", pcap.dlt.to_string())
                    .extension("iface", cfg.iface.clone())
                    .build()?;

                // Structured mode JSON body
                let body = serde_json::to_vec(&event)?;

                match tx.try_send(PostJob { body }) {
                    Ok(_) => {}
                    Err(TrySendError::Full(_)) => {
                        dropped += 1;
                        warn!("[WARN] SEND_QUEUE full, dropping event");
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        warn!("[WARN] sender thread(s) disconnected");
                        break;
                    }
                }
            }

            processed += 1;
            if cfg.log_every > 0 && processed % cfg.log_every == 0 {
                info!(
                    "[{}] processed={} dropped={} sink={}",
                    now_iso(),
                    processed,
                    dropped,
                    if sink_on { "on" } else { "off" }
                );
            }
        }

        // cleanup child
        let _ = child.kill();
        let _ = child.wait();

        // slight backoff if we’re restarting often
        if !stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(200));
        }
    }

    Ok(())
}

fn main() {
    // make sure *something* shows even if logger is misconfigured
    println!("its_live_capture starting up");

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    if let Err(e) = run_live() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
