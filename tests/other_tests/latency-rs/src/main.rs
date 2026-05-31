use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};

#[derive(Parser, Debug)]
#[command(name = "latency-test", about = "MQTT publish-subscribe latency measurement, live throughput monitoring, and high-performance publishing")]
struct Args {
    /// Broker host (used for both publisher and subscriber unless overridden)
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Broker port (used for both publisher and subscriber unless overridden)
    #[arg(long, default_value_t = 1883)]
    port: u16,

    /// Publisher broker host (overrides --host for the publisher)
    #[arg(long)]
    pub_host: Option<String>,

    /// Publisher broker port (overrides --port for the publisher)
    #[arg(long)]
    pub_port: Option<u16>,

    /// Subscriber broker host (overrides --host for the subscriber)
    #[arg(long)]
    sub_host: Option<String>,

    /// Subscriber broker port (overrides --port for the subscriber)
    #[arg(long)]
    sub_port: Option<u16>,

    /// MQTT username (pass empty string "" to connect anonymously without credentials)
    #[arg(long, default_value = "Test")]
    username: String,

    /// MQTT password
    #[arg(long, default_value = "Test")]
    password: String,

    /// QoS level (0, 1, or 2)
    #[arg(long, default_value_t = 1)]
    qos: u8,

    /// Publish interval in milliseconds
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,

    /// Test duration in seconds (only for latency benchmark)
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Use persistent session (clean_session=false) for the subscriber
    #[arg(long)]
    persistent: bool,

    /// Live throughput subscription mode: Topic to subscribe to (e.g. "#"). Disables publisher.
    #[arg(long)]
    topic: Option<String>,

    /// Live publishing mode: Topic to publish to (e.g. "test/topic"). Disables subscriber.
    #[arg(long)]
    publish: Option<String>,

    /// Payload to publish in --publish mode (placeholders: {seq} for sequence, {ts} for timestamp)
    #[arg(long)]
    payload: Option<String>,

    /// Number of messages to publish (defaults to 1; set to 0 to run indefinitely)
    #[arg(long, default_value_t = 1)]
    count: u64,
}

fn qos_from_u8(q: u8) -> QoS {
    match q {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => panic!("Invalid QoS: {q}. Must be 0, 1, or 2."),
    }
}

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

fn parse_field(payload: &str, key: &str) -> Option<f64> {
    // Parse a numeric field from simple JSON like {"ts":123.4,"seq":5}
    let search = format!("\"{}\":", key);
    let start = payload.find(&search)? + search.len();
    let rest = &payload[start..];
    let end = rest.find(|c: char| c == ',' || c == '}').unwrap_or(rest.len());
    rest[..end].trim().parse::<f64>().ok()
}

fn main() {
    let args = Args::parse();
    let qos = qos_from_u8(args.qos);

    let sub_host = args.sub_host.clone().unwrap_or_else(|| args.host.clone());
    let sub_port = args.sub_port.unwrap_or(args.port);
    let pub_host = args.pub_host.clone().unwrap_or_else(|| args.host.clone());
    let pub_port = args.pub_port.unwrap_or(args.port);
    
    let uid = &uuid::Uuid::new_v4().to_string()[..8];

    // If --publish is specified, run in dedicated Publisher mode
    if let Some(pub_topic) = args.publish {
        let interval = Duration::from_millis(args.interval_ms);

        let pub_id = format!("live_pub_{uid}");
        let mut pub_opts = MqttOptions::new(&pub_id, &pub_host, pub_port);
        pub_opts.set_keep_alive(Duration::from_secs(60));
        pub_opts.set_max_packet_size(64 * 1024 * 1024, 64 * 1024 * 1024);
        if !args.username.is_empty() {
            pub_opts.set_credentials(&args.username, &args.password);
        }
        pub_opts.set_clean_session(true);

        let (pub_client, mut pub_connection) = Client::new(pub_opts, 256);

        // Wait for publisher ConnAck in background
        let pub_ready = Arc::new(std::sync::Barrier::new(2));
        let pub_ready_clone = Arc::clone(&pub_ready);
        thread::spawn(move || {
            for notification in pub_connection.iter() {
                match notification {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        pub_ready_clone.wait();
                    }
                    Err(e) => {
                        eprintln!("\n[PUB] Connection error: {:?}", e);
                        std::process::exit(1);
                    }
                    Ok(Event::Incoming(Packet::Disconnect)) => {
                        eprintln!("\n[PUB] Disconnected before ready.");
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
        });

        pub_ready.wait();
        println!("[PUB] Connected successfully to {pub_host}:{pub_port}");

        let payload_base = args.payload.unwrap_or_else(|| "hello from monstermq rust client".to_string());
        let total_to_pub = args.count;
        let mut seq: u64 = 0;

        println!("Publishing to topic '{}' with QoS {}...", pub_topic, args.qos);
        
        loop {
            if total_to_pub > 0 && seq >= total_to_pub {
                break;
            }
            
            // If payload contains "{seq}" or "{ts}", format it dynamically
            let payload = payload_base
                .replace("{seq}", &seq.to_string())
                .replace("{ts}", &now_ms().to_string());

            pub_client.publish(&pub_topic, qos, false, payload.as_bytes()).unwrap();
            seq += 1;
            
            if total_to_pub > 0 && seq >= total_to_pub {
                break;
            }
            
            thread::sleep(interval);
        }

        println!("Successfully published {} messages.", seq);
        // Wait a short duration to ensure packet hits TCP buffer before disconnect
        thread::sleep(Duration::from_millis(200));
        let _ = pub_client.disconnect();
        return;
    }

    // If --topic is specified, run in dedicated Throughput Monitor mode
    if let Some(custom_topic) = args.topic {
        let total_count = Arc::new(AtomicUsize::new(0));
        let interval_count = Arc::new(AtomicUsize::new(0));
        
        let total_count_clone = Arc::clone(&total_count);
        let interval_count_clone = Arc::clone(&interval_count);

        let start = Instant::now();
        let topic_str = custom_topic.clone();
        let sub_host_print = sub_host.clone();
        let qos_val = args.qos;
        
        // Spawn reporting thread
        thread::spawn(move || {
            let mut last_report = Instant::now();
            println!("\n============================================================");
            println!(" MonsterMQ Live Throughput Monitor (Rust)");
            println!("============================================================");
            println!("  Broker: {}:{}", sub_host_print, sub_port);
            println!("  Topic:  '{}'", topic_str);
            println!("  QoS:    {}", qos_val);
            println!("------------------------------------------------------------");
            println!("  Format: [Elapsed] | Live Rate (msgs/s) | Running Average | Total Received");
            println!("------------------------------------------------------------");
            
            loop {
                thread::sleep(Duration::from_secs(1));
                let now = Instant::now();
                let elapsed = start.elapsed().as_secs_f64();
                let interval_elapsed = now.duration_since(last_report).as_secs_f64();
                
                let total = total_count_clone.load(Ordering::Relaxed);
                let interval = interval_count_clone.swap(0, Ordering::Relaxed);
                
                let current_rate = if interval_elapsed > 0.0 { interval as f64 / interval_elapsed } else { 0.0 };
                let overall_rate = if elapsed > 0.0 { total as f64 / elapsed } else { 0.0 };
                
                let mins = elapsed as u64 / 60;
                let secs = elapsed as u64 % 60;
                let hours = mins / 60;
                let mins = mins % 60;
                
                print!(
                    "\r  [{:02}:{:02}:{:02}] | Live: {:8.2} msg/s | Avg: {:8.2} msg/s | Total: {}",
                    hours, mins, secs, current_rate, overall_rate, total
                );
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
                last_report = now;
            }
        });

        // Run client connection
        let sub_id = format!("live_sub_{uid}");
        let mut sub_opts = MqttOptions::new(&sub_id, &sub_host, sub_port);
        sub_opts.set_keep_alive(Duration::from_secs(60));
        // Allow packets up to 64MB to handle very large payloads without deserialization errors
        sub_opts.set_max_packet_size(64 * 1024 * 1024, 64 * 1024 * 1024);
        if !args.username.is_empty() {
            sub_opts.set_credentials(&args.username, &args.password);
        }
        sub_opts.set_clean_session(!args.persistent);

        let (sub_client, mut sub_connection) = Client::new(sub_opts, 256);
        sub_client.subscribe(&custom_topic, qos).unwrap();

        for notification in sub_connection.iter() {
            match notification {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    // Connected successfully
                }
                Ok(Event::Incoming(Packet::Publish(_))) => {
                    total_count.fetch_add(1, Ordering::Relaxed);
                    interval_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    eprintln!("\n[SUB] Disconnected from broker.");
                    break;
                }
                Err(e) => {
                    eprintln!("\n[SUB] Connection error: {:?}", e);
                    break;
                }
                _ => {}
            }
        }
        return;
    }

    let interval = Duration::from_millis(args.interval_ms);
    let duration = Duration::from_secs(args.duration);

    let topic = format!("test/latency/{uid}");

    // Shared collections
    let latencies: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let received_seqs: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    println!("============================================================");
    println!(
        "Latency test: QoS={}  interval={}ms  duration={}s  persistent={}",
        args.qos, args.interval_ms, args.duration, args.persistent
    );
    println!("  pub: {}:{}  sub: {}:{}", pub_host, pub_port, sub_host, sub_port);
    println!("============================================================");

    // -- Subscriber -----------------------------------------------------------
    let sub_id = format!("lat_sub_{uid}");
    let mut sub_opts = MqttOptions::new(&sub_id, &sub_host, sub_port);
    sub_opts.set_keep_alive(Duration::from_secs(60));
    sub_opts.set_max_packet_size(64 * 1024 * 1024, 64 * 1024 * 1024);
    if !args.username.is_empty() {
        sub_opts.set_credentials(&args.username, &args.password);
    }
    sub_opts.set_clean_session(!args.persistent);

    let (sub_client, mut sub_connection) = Client::new(sub_opts, 256);
    sub_client.subscribe(&topic, qos).unwrap();

    // Wait for ConnAck and SubAck before starting
    let sub_ready = Arc::new(std::sync::Barrier::new(2));
    let sub_ready_clone = Arc::clone(&sub_ready);

    let latencies_clone = Arc::clone(&latencies);
    let seqs_clone = Arc::clone(&received_seqs);
    let sub_handle = thread::spawn(move || {
        let mut got_connack = false;
        let mut got_suback = false;
        for notification in sub_connection.iter() {
            match notification {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    got_connack = true;
                    if got_suback {
                        sub_ready_clone.wait();
                    }
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    got_suback = true;
                    if got_connack {
                        sub_ready_clone.wait();
                    }
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let recv_ts = now_ms();
                    if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                        if let Some(send_ts) = parse_field(payload, "ts") {
                            let latency_ms = recv_ts - send_ts;
                            latencies_clone.lock().unwrap().push(latency_ms);
                        }
                        if let Some(seq) = parse_field(payload, "seq") {
                            seqs_clone.lock().unwrap().push(seq as u64);
                        }
                    }
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    eprintln!("[SUB] Disconnected before subscription ready.");
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("[SUB] Connection error: {:?}", e);
                    std::process::exit(1);
                }
                _ => {}
            }
        }
    });

    // Wait until subscriber has received ConnAck + SubAck
    sub_ready.wait();
    println!("[SUB] Connected and subscribed");

    // -- Publisher -------------------------------------------------------------
    let pub_id = format!("lat_pub_{uid}");
    let mut pub_opts = MqttOptions::new(&pub_id, &pub_host, pub_port);
    pub_opts.set_keep_alive(Duration::from_secs(60));
    pub_opts.set_max_packet_size(64 * 1024 * 1024, 64 * 1024 * 1024);
    if !args.username.is_empty() {
        pub_opts.set_credentials(&args.username, &args.password);
    }
    pub_opts.set_clean_session(true);

    let (pub_client, mut pub_connection) = Client::new(pub_opts, 256);

    // Wait for publisher ConnAck
    let pub_ready = Arc::new(std::sync::Barrier::new(2));
    let pub_ready_clone = Arc::clone(&pub_ready);

    let pub_loop = thread::spawn(move || {
        for notification in pub_connection.iter() {
            match notification {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    pub_ready_clone.wait();
                }
                Err(e) => {
                    eprintln!("[PUB] Connection error: {:?}", e);
                    std::process::exit(1);
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    eprintln!("[PUB] Disconnected before publisher ready.");
                    std::process::exit(1);
                }
                _ => {}
            }
        }
    });

    pub_ready.wait();
    println!("[PUB] Connected");

    // -- Publish for the configured duration ----------------------------------
    let start = Instant::now();
    let mut seq: u64 = 0;
    while start.elapsed() < duration {
        let payload = format!("{{\"ts\":{},\"seq\":{}}}", now_ms(), seq);
        pub_client
            .publish(&topic, qos, false, payload.as_bytes())
            .unwrap();
        seq += 1;
        thread::sleep(interval);
    }
    let count = seq;

    // Wait for remaining messages to arrive
    thread::sleep(Duration::from_secs(1));

    // -- Cleanup --------------------------------------------------------------
    let _ = pub_client.disconnect();
    let _ = pub_loop.join();
    let _ = sub_client.disconnect();
    let _ = sub_handle.join();

    // -- Results --------------------------------------------------------------
    let mut lats = latencies.lock().unwrap().clone();
    let n = lats.len();

    println!("\nPublished {count} messages, received {n}");

    if n == 0 {
        eprintln!("ERROR: No messages received.");
        std::process::exit(1);
    }

    lats.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let sum: f64 = lats.iter().sum();
    let avg = sum / n as f64;
    let med = if n % 2 == 0 {
        (lats[n / 2 - 1] + lats[n / 2]) / 2.0
    } else {
        lats[n / 2]
    };
    let variance: f64 = lats.iter().map(|v| (v - avg).powi(2)).sum::<f64>() / n as f64;
    let stdev = variance.sqrt();
    let p95 = lats[(n as f64 * 0.95) as usize].min(lats[n - 1]);
    let p99 = lats[(n as f64 * 0.99) as usize].min(lats[n - 1]);

    println!("\n[LATENCY] {n} samples");
    println!("  Min:    {:.2} ms", lats[0]);
    println!("  Max:    {:.2} ms", lats[n - 1]);
    println!("  Avg:    {:.2} ms", avg);
    println!("  Median: {:.2} ms", med);
    println!("  StdDev: {:.2} ms", stdev);
    println!("  P95:    {:.2} ms", p95);
    println!("  P99:    {:.2} ms", p99);

    let loss = count.saturating_sub(n as u64);
    println!(
        "  Loss:   {loss}/{count} ({:.1}%)",
        if count > 0 {
            loss as f64 / count as f64 * 100.0
        } else {
            0.0
        }
    );

    // -- Sequence validation --------------------------------------------------
    let seqs = received_seqs.lock().unwrap();
    if !seqs.is_empty() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut duplicates: u64 = 0;
        let mut out_of_order: u64 = 0;
        let mut prev = seqs[0];
        seen.insert(prev);

        for &s in &seqs[1..] {
            if !seen.insert(s) {
                duplicates += 1;
            } else {
                if s < prev {
                    out_of_order += 1;
                }
                prev = s;
            }
        }

        let expected: HashSet<u64> = (0..count).collect();
        let missing: Vec<u64> = {
            let mut m: Vec<u64> = expected.difference(&seen).copied().collect();
            m.sort();
            m
        };

        println!("\n[SEQUENCE] {} messages checked", seqs.len());
        println!("  Missing:      {}", missing.len());
        println!("  Duplicates:   {duplicates}");
        println!("  Out-of-order: {out_of_order}");
        if !missing.is_empty() {
            let sample: Vec<_> = missing.iter().take(20).collect();
            print!("  First missing: {:?}", sample);
            if missing.len() > 20 {
                println!("...");
            } else {
                println!();
            }
        }
    }
}
