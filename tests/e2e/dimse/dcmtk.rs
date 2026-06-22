use std::ffi::OsStr;
use std::fmt;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

#[derive(Debug)]
pub struct CommandOutput {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl CommandOutput {
    pub fn combined(&self) -> String {
        format!("{}\n{}", self.stdout, self.stderr)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QueryModel {
    PatientRoot,
    StudyRoot,
}

impl QueryModel {
    fn flag(self) -> &'static str {
        match self {
            Self::PatientRoot => "-P",
            Self::StudyRoot => "-S",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Priority {
    Default,
    High,
    Low,
}

impl Priority {
    fn key(self) -> Option<&'static str> {
        match self {
            Self::Default => None,
            Self::High => Some("0000,0700=1"),
            Self::Low => Some("0000,0700=2"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Key {
    name: String,
    value: String,
}

impl Key {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    fn as_dcmtk_arg(&self) -> String {
        format!("{}={}", self.name, self.value)
    }
}

#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub calling_ae: String,
    pub transfer_syntax_flag: Option<&'static str>,
    pub max_pdu: Option<u16>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            calling_ae: "STORESCU".to_string(),
            transfer_syntax_flag: None,
            max_pdu: None,
        }
    }
}

pub fn require_tools() {
    for tool in [
        "storescu", "findscu", "getscu", "movescu", "storescp", "dcmdump",
    ] {
        let output = Command::new(tool)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap_or_else(|err| panic!("required DCMTK tool {tool:?} is unavailable: {err}"));
        assert!(
            output.success(),
            "required DCMTK tool {tool:?} returned {output:?}"
        );
    }
}

pub fn dcmdump<P, I, S>(file: P, selectors: I) -> CommandOutput
where
    P: AsRef<Path>,
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = Command::new("dcmdump");
    for selector in selectors {
        cmd.arg("+P").arg(selector);
    }
    cmd.arg(file.as_ref());
    run(cmd)
}

pub fn storescu(
    host: &str,
    port: u16,
    called_ae: &str,
    file: &Path,
    options: &StoreOptions,
) -> CommandOutput {
    storescu_files(host, port, called_ae, [file], options)
}

pub fn storescu_files<'a, I>(
    host: &str,
    port: u16,
    called_ae: &str,
    files: I,
    options: &StoreOptions,
) -> CommandOutput
where
    I: IntoIterator<Item = &'a Path>,
{
    let mut cmd = Command::new("storescu");
    cmd.arg("-v")
        .arg("-aet")
        .arg(&options.calling_ae)
        .arg("-aec")
        .arg(called_ae);
    if let Some(flag) = options.transfer_syntax_flag {
        cmd.arg(flag);
    }
    if let Some(max_pdu) = options.max_pdu {
        cmd.arg("-pdu").arg(max_pdu.to_string());
    }
    cmd.arg(host).arg(port.to_string());
    for file in files {
        cmd.arg(file);
    }
    run(cmd)
}

pub fn findscu(
    host: &str,
    port: u16,
    called_ae: &str,
    model: QueryModel,
    keys: &[Key],
) -> CommandOutput {
    let mut cmd = Command::new("findscu");
    cmd.arg("-v")
        .arg("+sr")
        .arg("-aet")
        .arg("FINDSCU")
        .arg("-aec")
        .arg(called_ae)
        .arg(model.flag());
    add_keys(&mut cmd, keys);
    cmd.arg(host).arg(port.to_string());
    run(cmd)
}

pub fn getscu(
    host: &str,
    port: u16,
    called_ae: &str,
    model: QueryModel,
    keys: &[Key],
    priority: Priority,
    out_dir: &Path,
) -> CommandOutput {
    let mut cmd = Command::new("getscu");
    cmd.arg("-v")
        .arg("-aet")
        .arg("GETSCU")
        .arg("-aec")
        .arg(called_ae)
        .arg(model.flag())
        .arg("-od")
        .arg(out_dir);
    add_priority(&mut cmd, priority);
    add_keys(&mut cmd, keys);
    cmd.arg(host).arg(port.to_string());
    run(cmd)
}

pub fn movescu(
    host: &str,
    port: u16,
    called_ae: &str,
    move_destination: &str,
    model: QueryModel,
    keys: &[Key],
    priority: Priority,
) -> CommandOutput {
    let mut cmd = Command::new("movescu");
    cmd.arg("-v")
        .arg("--no-port")
        .arg("-aet")
        .arg("MOVESCU")
        .arg("-aec")
        .arg(called_ae)
        .arg("-aem")
        .arg(move_destination)
        .arg(model.flag());
    add_priority(&mut cmd, priority);
    add_keys(&mut cmd, keys);
    cmd.arg(host).arg(port.to_string());
    run(cmd)
}

fn add_priority(cmd: &mut Command, priority: Priority) {
    if let Some(key) = priority.key() {
        cmd.arg("-k").arg(key);
    }
}

fn add_keys(cmd: &mut Command, keys: &[Key]) {
    for key in keys {
        cmd.arg("-k").arg(key.as_dcmtk_arg());
    }
}

fn run(mut cmd: Command) -> CommandOutput {
    let display = CommandDisplay(&cmd).to_string();
    let output = cmd
        .output()
        .unwrap_or_else(|err| panic!("failed to run {display}: {err}"));
    CommandOutput {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

struct CommandDisplay<'a>(&'a Command);

impl fmt::Display for CommandDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
