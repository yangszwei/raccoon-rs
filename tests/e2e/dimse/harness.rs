use std::fs::{self, File};
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::assertions::{assert_output_contains, command_succeeded};
use super::dcmtk::{self, Key, QueryModel};
use super::fixture::DicomFixture;

const LOCAL_AE: &str = "RACCOON";
const MOVE_DEST_AE: &str = "MOVESCP";

pub trait DimseEndpoint {
    fn host(&self) -> &str;
    fn port(&self) -> u16;
    fn called_ae(&self) -> &str;
    fn move_destination_ae(&self) -> &str;
    fn fixture(&self) -> &DicomFixture;
    fn path(&self, name: &str) -> PathBuf;
    fn wait_until_fixture_is_queryable(&self);
    fn start_storescp(&self, output_dir: &Path) -> ManagedChild;

    fn seed_fixture(&self) {
        let output = dcmtk::storescu(
            self.host(),
            self.port(),
            self.called_ae(),
            &self.fixture().path,
            &dcmtk::StoreOptions::default(),
        );
        command_succeeded("seed fixture with C-STORE", &output);
        self.wait_until_fixture_is_queryable();
    }

    fn expect_find_contains(
        &self,
        case_name: &str,
        model: QueryModel,
        keys: &[Key],
        expected: &str,
    ) {
        let output = dcmtk::findscu(self.host(), self.port(), self.called_ae(), model, keys);
        command_succeeded(case_name, &output);
        assert_output_contains(case_name, &output, expected);
    }
}

pub struct RaccoonDimseTestContext {
    pub root: PathBuf,
    pub local_port: u16,
    pub move_scp_port: u16,
    pub fixture: DicomFixture,
    raccoon: ManagedChild,
}

impl RaccoonDimseTestContext {
    pub fn start() -> Self {
        dcmtk::require_tools();
        require_sqlite3();
        let fixture = DicomFixture::from_file(&DicomFixture::default_file());
        let root = run_root();
        fs::create_dir_all(&root).expect("create e2e root");
        let local_port = free_port();
        let move_scp_port = free_port();
        write_config(&root, local_port, move_scp_port);
        let raccoon = start_raccoon(&root);
        wait_for_port("DIMSE listener", local_port, Duration::from_secs(15));

        Self {
            root,
            local_port,
            move_scp_port,
            fixture,
            raccoon,
        }
    }
}

impl DimseEndpoint for RaccoonDimseTestContext {
    fn host(&self) -> &str {
        "127.0.0.1"
    }

    fn port(&self) -> u16 {
        self.local_port
    }

    fn called_ae(&self) -> &str {
        LOCAL_AE
    }

    fn move_destination_ae(&self) -> &str {
        MOVE_DEST_AE
    }

    fn fixture(&self) -> &DicomFixture {
        &self.fixture
    }

    fn path(&self, name: &str) -> PathBuf {
        let path = self.root.join(name);
        fs::create_dir_all(&path).unwrap_or_else(|err| {
            panic!("failed to create test directory {}: {err}", path.display())
        });
        path
    }

    fn wait_until_fixture_is_queryable(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last = None;
        while Instant::now() < deadline {
            let output = read_model_instance_count(self);
            if output.trim() == "1" {
                return;
            }
            last = Some(output);
            thread::sleep(Duration::from_millis(500));
        }
        let output = last.expect("at least one polling query");
        panic!(
            "fixture did not appear in read model with an object key before timeout; last sqlite3 output: {output:?}"
        );
    }

    fn start_storescp(&self, output_dir: &Path) -> ManagedChild {
        fs::create_dir_all(output_dir).expect("create storescp output dir");
        let log = File::create(self.root.join("logs/storescp.log")).expect("storescp log");
        let err = log.try_clone().expect("clone storescp log");
        let child = Command::new("storescp")
            .arg("-v")
            .arg("-aet")
            .arg(MOVE_DEST_AE)
            .arg("+uf")
            .arg("-od")
            .arg(output_dir)
            .arg(self.move_scp_port.to_string())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err))
            .spawn()
            .expect("start storescp");
        wait_for_port(
            "storescp move destination",
            self.move_scp_port,
            Duration::from_secs(10),
        );
        ManagedChild::new("storescp", child)
    }
}

impl Drop for RaccoonDimseTestContext {
    fn drop(&mut self) {
        let _ = self.raccoon.kill_and_wait();
    }
}

pub struct ManagedChild {
    child: Child,
}

impl ManagedChild {
    fn new(_name: &'static str, child: Child) -> Self {
        Self { child }
    }

    pub fn kill_and_wait(&mut self) -> std::io::Result<()> {
        match self.child.try_wait()? {
            Some(_) => Ok(()),
            None => {
                let _ = self.child.kill();
                let _ = self.child.wait();
                Ok(())
            }
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let _ = self.kill_and_wait();
    }
}

fn run_root() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before unix epoch")
        .as_millis();
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/e2e/dimse")
        .join(format!("run-{}-{now}", std::process::id()))
}

fn write_config(root: &Path, raccoon_port: u16, move_scp_port: u16) {
    let config_dir = root.join("config");
    let logs_dir = root.join("logs");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::create_dir_all(&logs_dir).expect("create logs dir");
    let data_root = root.join("data");

    write_file(
        &config_dir.join("raccoon.toml"),
        &format!(
            r#"[app]
name = "raccoon-e2e"

[database]
type = "sqlite"

[filesystem]
root = "{}"

[runtime]
shutdown_timeout_seconds = 2
force_exit_on_timeout = true

[storage]
backend = "filesystem"

[telemetry]
log_level = "debug"
log_format = "json"
"#,
            toml_path(&data_root)
        ),
    );

    write_file(
        &config_dir.join("application-entities.toml"),
        &format!(
            r#"[[application_entities.local]]
title = "{LOCAL_AE}"
bind_address = "127.0.0.1:{raccoon_port}"
max_concurrent_associations = 8
read_timeout_seconds = 30
write_timeout_seconds = 30
max_pdu_length = 65536

[[application_entities.peer]]
title = "{MOVE_DEST_AE}"
address = "127.0.0.1:{move_scp_port}"
connect_timeout_seconds = 5
read_timeout_seconds = 30
write_timeout_seconds = 30
max_pdu_length = 65536
"#
        ),
    );
}

fn start_raccoon(root: &Path) -> ManagedChild {
    let log = File::create(root.join("logs/raccoon.log")).expect("raccoon log");
    let err = log.try_clone().expect("clone raccoon log");
    let child = Command::new(raccoon_bin())
        .current_dir(root)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("start raccoon binary");
    ManagedChild::new("raccoon", child)
}

fn raccoon_bin() -> PathBuf {
    std::env::var_os("RACCOON_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/raccoon")
        })
}

fn wait_for_port(name: &str, port: u16, timeout: Duration) {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("socket addr");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(150)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("{name} did not listen on {addr} within {timeout:?}");
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn require_sqlite3() {
    let status = Command::new("sqlite3")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap_or_else(|err| panic!("required sqlite3 tool is unavailable: {err}"));
    assert!(status.success(), "sqlite3 --version returned {status:?}");
}

fn read_model_instance_count(ctx: &RaccoonDimseTestContext) -> String {
    let db = ctx.root.join("data/read/read.db");
    if !db.exists() {
        return "missing-db".to_string();
    }
    let sql = format!(
        "select count(*) from instances where sop_instance_uid = '{}' and object_key is not null;",
        ctx.fixture.sop_instance_uid
    );
    let output = Command::new("sqlite3")
        .arg(db)
        .arg(sql)
        .output()
        .expect("run sqlite3 read-model poll");
    if output.status.success() {
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    } else {
        String::from_utf8_lossy(&output.stderr).trim().to_string()
    }
}

fn write_file(path: &Path, contents: &str) {
    let mut file = File::create(path)
        .unwrap_or_else(|err| panic!("failed to create {}: {err}", path.display()));
    file.write_all(contents.as_bytes())
        .unwrap_or_else(|err| panic!("failed to write {}: {err}", path.display()));
}

fn toml_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}
