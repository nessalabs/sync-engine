//! Two small host applications sharing the same replication and SQLite ports.
//! Payload interpretation, read models, and browser UI are deliberately local
//! to this example rather than part of the reusable sync core.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nessa_sync::replication::application::{begin_pass, finish_pass};
use nessa_sync::replication::domain::{Id, Limits, Scope};
use nessa_sync::replication::infrastructure::{
    LoopbackClient, LoopbackConfig, LoopbackServer, SqliteReplicaStore,
};

type AnyError = Box<dyn std::error::Error>;

#[derive(Clone, Copy)]
enum App {
    Transcript,
    Tasks,
}

impl App {
    fn parse(value: &str) -> Result<Self, AnyError> {
        match value {
            "transcript" => Ok(Self::Transcript),
            "tasks" => Ok(Self::Tasks),
            _ => Err("app must be transcript or tasks".into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Transcript => "Transcript",
            Self::Tasks => "Task board",
        }
    }

    fn origin(self) -> Id {
        id(match self {
            Self::Transcript => "transcript-example",
            Self::Tasks => "tasks-example",
        })
    }

    fn stream(self) -> Id {
        id(match self {
            Self::Transcript => "messages",
            Self::Tasks => "task-events",
        })
    }

    fn schema(self) -> Id {
        id(match self {
            Self::Transcript => "message-v1",
            Self::Tasks => "task-v1",
        })
    }

    fn scope(self, receiver: &str) -> Result<Scope, AnyError> {
        Ok(Scope::new(
            external_id(receiver)?,
            self.origin(),
            self.stream(),
            id("first"),
            self.schema(),
            id("local-epoch-1"),
        ))
    }

    fn config(
        self,
        source_path: PathBuf,
        read: &str,
        write: &str,
    ) -> Result<LoopbackConfig, AnyError> {
        Ok(LoopbackConfig {
            source_path,
            origin: self.origin(),
            stream: self.stream(),
            incarnation: id("first"),
            schema: self.schema(),
            access_epoch: id("local-epoch-1"),
            read_token: external_id(read)?,
            write_token: external_id(write)?,
            allowed_receivers: vec![id("phone"), id("laptop")],
        })
    }
}

fn id(value: &str) -> Id {
    Id::new(value).expect("valid example identity")
}

fn external_id(value: &str) -> Result<Id, AnyError> {
    Id::new(value).map_err(|error| other(error).into())
}

fn other<E: std::fmt::Debug>(value: E) -> io::Error {
    io::Error::other(format!("{value:?}"))
}

fn address(value: &str) -> Result<SocketAddrV4, AnyError> {
    match value.parse::<SocketAddr>()? {
        SocketAddr::V4(address) if *address.ip() == Ipv4Addr::LOCALHOST => Ok(address),
        _ => Err("IPv4 loopback address required".into()),
    }
}

fn client(endpoint: &str, token: &str) -> Result<LoopbackClient, AnyError> {
    Ok(LoopbackClient::new(
        address(endpoint)?,
        external_id(token)?,
    )?)
}

fn put_text(out: &mut Vec<u8>, value: &str) -> Result<(), AnyError> {
    let len = u16::try_from(value.len())?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn payload(app: App, operation: &[String]) -> Result<Vec<u8>, AnyError> {
    let Some(kind) = operation.first().map(String::as_str) else {
        return Err("missing mutation".into());
    };
    match (app, kind, operation.len()) {
        (App::Transcript, "message", 2) => {
            let mut bytes = vec![1];
            put_text(&mut bytes, &operation[1])?;
            Ok(bytes)
        }
        (App::Tasks, "create", 3) => {
            let mut bytes = vec![17];
            put_text(&mut bytes, &operation[1])?;
            put_text(&mut bytes, &operation[2])?;
            Ok(bytes)
        }
        (App::Tasks, "title", 3) => {
            let mut bytes = vec![18];
            put_text(&mut bytes, &operation[1])?;
            put_text(&mut bytes, &operation[2])?;
            Ok(bytes)
        }
        (App::Tasks, "complete", 3) if operation[2] == "true" || operation[2] == "false" => {
            let mut bytes = vec![19];
            put_text(&mut bytes, &operation[1])?;
            bytes.push(u8::from(operation[2] == "true"));
            Ok(bytes)
        }
        (App::Tasks, "delete", 2) => {
            let mut bytes = vec![20];
            put_text(&mut bytes, &operation[1])?;
            Ok(bytes)
        }
        _ => Err("invalid app mutation".into()),
    }
}

struct PayloadReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn byte(&mut self) -> Result<u8, AnyError> {
        let result = *self.bytes.get(self.cursor).ok_or("short payload")?;
        self.cursor += 1;
        Ok(result)
    }

    fn text(&mut self) -> Result<String, AnyError> {
        let len: [u8; 2] = self
            .bytes
            .get(self.cursor..self.cursor + 2)
            .ok_or("short length")?
            .try_into()?;
        self.cursor += 2;
        let len = u16::from_be_bytes(len) as usize;
        let text = std::str::from_utf8(
            self.bytes
                .get(self.cursor..self.cursor + len)
                .ok_or("short text")?,
        )?;
        self.cursor += len;
        Ok(text.to_owned())
    }

    fn finish(self) -> Result<(), AnyError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing payload bytes".into())
        }
    }
}

#[derive(Default)]
struct Status {
    loaded: bool,
    online: bool,
    checked_ms: u64,
    applied_ms: u64,
    checkpoint: u64,
    payload_bytes: u64,
    protocol_bytes: u64,
    head_checks: u64,
}

impl Status {
    fn read(path: &Path) -> Self {
        let Ok(contents) = fs::read_to_string(path) else {
            return Self::default();
        };
        let fields: Vec<_> = contents.trim().split('\t').collect();
        let parse = || -> Option<Self> {
            if fields.len() != 8 {
                return None;
            }
            Some(Self {
                loaded: fields[0] == "1",
                online: fields[1] == "1",
                checked_ms: fields[2].parse().ok()?,
                applied_ms: fields[3].parse().ok()?,
                checkpoint: fields[4].parse().ok()?,
                payload_bytes: fields[5].parse().ok()?,
                protocol_bytes: fields[6].parse().ok()?,
                head_checks: fields[7].parse().ok()?,
            })
        };
        parse().unwrap_or_default()
    }

    fn write(&self, path: &Path) -> io::Result<()> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(other)?
            .as_nanos();
        let temporary = path.with_extension(format!("{}.{}.tmp", std::process::id(), nanos));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            u8::from(self.loaded),
            u8::from(self.online),
            self.checked_ms,
            self.applied_ms,
            self.checkpoint,
            self.payload_bytes,
            self.protocol_bytes,
            self.head_checks
        )?;
        file.sync_all()?;
        fs::rename(temporary, path)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn sync(
    app: App,
    address: &str,
    replica: &Path,
    status_path: &Path,
    receiver: &str,
    token: &str,
) -> Result<(), AnyError> {
    let target = app.scope(receiver)?;
    let mut status = Status::read(status_path);
    let mut authorizer = client(address, token)?;
    let mut source = client(address, token)?;
    let mut store = SqliteReplicaStore::open(replica)?;
    let result = (|| -> Result<(u64, u64), AnyError> {
        let _subscription = source.subscribe(&target).map_err(other)?;
        let limits = Limits::new(32, 128 * 1024, 128 * 1024).map_err(other)?;
        let mut pass =
            begin_pass(&target, &mut authorizer, &mut source, &mut store).map_err(other)?;
        let initial = pass.position();
        loop {
            finish_pass(&mut pass, limits, &mut authorizer, &mut source, &mut store)
                .map_err(other)?;
            let applied = pass.position();
            pass = begin_pass(&target, &mut authorizer, &mut source, &mut store).map_err(other)?;
            if pass.is_complete() {
                return Ok((applied, initial));
            }
        }
    })();
    let auth = authorizer.counters();
    let wire = source.counters();
    status.online = result.is_ok();
    status.checked_ms = now_ms();
    status.payload_bytes = wire.payload_bytes;
    status.protocol_bytes = auth.protocol_bytes + wire.protocol_bytes;
    status.head_checks = wire.head_checks;
    if let Ok((checkpoint, initial)) = result {
        if checkpoint > initial {
            status.applied_ms = status.checked_ms;
        }
        status.loaded = true;
        status.checkpoint = checkpoint;
        status.write(status_path)?;
        println!(
            "{{\"checkpoint\":{checkpoint},\"payload_bytes\":{},\"protocol_bytes\":{},\"duplicate_bytes\":{},\"head_checks\":{},\"applied_lag\":0}}",
            status.payload_bytes, status.protocol_bytes, wire.duplicate_bytes, status.head_checks
        );
        Ok(())
    } else {
        status.write(status_path)?;
        Err(other(result.unwrap_err()).into())
    }
}

#[derive(Clone)]
struct Task {
    title: String,
    complete: bool,
}

fn project(app: App, replica: &Path, target: &Scope) -> Result<(u64, Vec<String>), AnyError> {
    if !replica.exists() {
        return Ok((0, Vec::new()));
    }
    let mut store = SqliteReplicaStore::open(replica)?;
    let checkpoint = store
        .checkpoint(target)
        .map_err(other)?
        .map_or(0, |c| c.position());
    let mut after = 0;
    let mut messages = Vec::new();
    let mut tasks = BTreeMap::<String, Task>::new();
    while after < checkpoint {
        let records = store
            .read_after(target, after, 64, 128 * 1024)
            .map_err(other)?;
        if records.is_empty() {
            return Err("checkpoint ahead of cached records".into());
        }
        for record in records {
            after = record.position;
            let mut reader = PayloadReader::new(&record.payload);
            match (app, reader.byte()?) {
                (App::Transcript, 1) => {
                    messages.push(reader.text()?);
                }
                (App::Tasks, 17) => {
                    let key = reader.text()?;
                    let title = reader.text()?;
                    if tasks
                        .insert(
                            key,
                            Task {
                                title,
                                complete: false,
                            },
                        )
                        .is_some()
                    {
                        return Err("duplicate task creation".into());
                    }
                }
                (App::Tasks, 18) => {
                    let key = reader.text()?;
                    let title = reader.text()?;
                    tasks.get_mut(&key).ok_or("title for missing task")?.title = title;
                }
                (App::Tasks, 19) => {
                    let key = reader.text()?;
                    let complete = reader.byte()?;
                    if complete > 1 {
                        return Err("invalid completion value".into());
                    }
                    tasks
                        .get_mut(&key)
                        .ok_or("completion for missing task")?
                        .complete = complete == 1;
                }
                (App::Tasks, 20) => {
                    let key = reader.text()?;
                    if tasks.remove(&key).is_none() {
                        return Err("delete for missing task".into());
                    }
                }
                _ => return Err("payload schema mismatch".into()),
            }
            reader.finish()?;
        }
    }
    let entries = match app {
        App::Transcript => messages,
        App::Tasks => tasks
            .into_iter()
            .map(|(key, task)| {
                format!(
                    "{}: {} [{}]",
                    key,
                    task.title,
                    if task.complete { "done" } else { "open" }
                )
            })
            .collect(),
    };
    Ok((checkpoint, entries))
}

fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn render(
    app: App,
    replica: &Path,
    status_path: &Path,
    receiver: &str,
) -> Result<String, AnyError> {
    let status = Status::read(status_path);
    let target = app.scope(receiver)?;
    let (checkpoint, entries) = project(app, replica, &target)?;
    let loaded = replica.exists() && (status.loaded || checkpoint > 0);
    let state = if !loaded {
        "not_loaded"
    } else if !status.loaded {
        "partial"
    } else if entries.is_empty() {
        "complete_empty"
    } else {
        "cached"
    };
    let freshness = if status.checked_ms == 0 {
        "Never checked".to_owned()
    } else {
        format!(
            "Last check {} s ago",
            now_ms().saturating_sub(status.checked_ms) / 1000
        )
    };
    let applied_age = if status.applied_ms == 0 {
        "Last applied time unknown".to_owned()
    } else {
        format!(
            "Last applied {} s ago",
            now_ms().saturating_sub(status.applied_ms) / 1000
        )
    };
    let list = entries
        .iter()
        .map(|item| format!("<li>{}</li>", html_escape(item)))
        .collect::<String>();
    let body = if !loaded {
        "<p>Not loaded yet. Connect and sync this device.</p>".to_owned()
    } else if entries.is_empty() {
        if status.loaded {
            "<p>Complete and empty.</p>".to_owned()
        } else {
            "<p>Cached records exist; the last source check is unknown.</p>".to_owned()
        }
    } else {
        format!(
            "{}<ul>{list}</ul>",
            if status.loaded {
                ""
            } else {
                "<p>Last source check is unknown.</p>"
            }
        )
    };
    Ok(format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"2\"><title>{}</title><style>body{{font:16px system-ui;max-width:720px;margin:3rem auto;padding:0 1rem;color:#17202a}}li{{padding:.6rem;border-bottom:1px solid #ddd}}small{{color:#52606d}}</style></head><body data-state=\"{}\"><h1>{}</h1><p>Device: {} · {} · {} · source: {}</p><small>Applied position {} · Last remote payload {} B · Protocol {} B · Head checks {}</small>{}</body></html>",
        app.name(), state, app.name(), html_escape(receiver), html_escape(&freshness), html_escape(&applied_age),
        if status.online { "reachable at last check" } else { "unavailable at last check" },
        checkpoint, status.payload_bytes, status.protocol_bytes, status.head_checks, body
    ))
}

fn web(
    app: App,
    replica: PathBuf,
    status: PathBuf,
    receiver: String,
    port: u16,
) -> Result<(), AnyError> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))?;
    eprintln!("local view on {}", listener.local_addr()?);
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                if let Err(error) = serve_view_connection(stream, app, &replica, &status, &receiver)
                {
                    eprintln!("local view request failed: {error}");
                }
            }
            Err(error) => eprintln!("local view accept failed: {error}"),
        }
    }
    Ok(())
}

fn serve_view_connection(
    mut stream: std::net::TcpStream,
    app: App,
    replica: &Path,
    status: &Path,
    receiver: &str,
) -> Result<(), AnyError> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut request = Vec::with_capacity(256);
    while request.len() < 8192 {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        request.push(byte[0]);
        if request.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let first = std::str::from_utf8(&request)
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("");
    let (status_line, body) = if first.starts_with("GET / HTTP/") {
        match render(app, replica, status, receiver) {
            Ok(page) => ("200 OK", page),
            Err(error) => (
                "500 Internal Server Error",
                format!("Projection error: {}", html_escape(&error.to_string())),
            ),
        }
    } else {
        ("404 Not Found", "Not found".to_owned())
    };
    write!(
        stream,
        "HTTP/1.1 {status_line}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    Ok(())
}

fn run() -> Result<(), AnyError> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("source") if args.len() == 7 => {
            let app = App::parse(&args[2])?;
            let port: u16 = args[4].parse()?;
            let server = LoopbackServer::bind(port, app.config(PathBuf::from(&args[3]), &args[5], &args[6])?)?;
            eprintln!("source on {}", server.local_addr()?);
            server.serve()?;
        }
        Some("mutate") if args.len() >= 7 => {
            let app = App::parse(&args[2])?;
            let bytes = payload(app, &args[6..])?;
            let position = client(&args[3], &args[4])?.append(&external_id(&args[5])?, &bytes).map_err(other)?;
            println!("{{\"position\":{position}}}");
        }
        Some("stats") if args.len() == 4 => {
            let (heads, pages, refused) = client(&args[2], &args[3])?
                .server_counters()
                .map_err(other)?;
            println!(
                "{{\"head_reads\":{heads},\"page_reads\":{pages},\"refused_reads\":{refused}}}"
            );
        }
        Some("sync") if args.len() == 8 => {
            sync(App::parse(&args[2])?, &args[3], Path::new(&args[4]), Path::new(&args[5]), &args[6], &args[7])?;
        }
        Some("view") if args.len() == 7 => {
            let page = render(App::parse(&args[2])?, Path::new(&args[3]), Path::new(&args[4]), &args[5])?;
            let port: u16 = args[6].parse()?;
            if port == 0 {
                println!("{page}");
            } else {
                web(App::parse(&args[2])?, PathBuf::from(&args[3]), PathBuf::from(&args[4]), args[5].clone(), port)?;
            }
        }
        _ => return Err("usage: local_apps source APP SOURCE PORT READ_TOKEN WRITE_TOKEN | mutate APP ADDR WRITE_TOKEN FACT_ID EVENT... | stats ADDR WRITE_TOKEN | sync APP ADDR REPLICA STATUS RECEIVER READ_TOKEN | view APP REPLICA STATUS RECEIVER PORT".into()),
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:?}");
        std::process::exit(1);
    }
}
