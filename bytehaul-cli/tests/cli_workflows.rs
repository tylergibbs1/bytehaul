use std::process::Command;

fn bytehaul() -> Command {
    Command::new(env!("CARGO_BIN_EXE_bytehaul"))
}

#[test]
fn send_help_groups_common_advanced_and_experimental_flags() {
    let output = bytehaul()
        .args(["send", "--help"])
        .output()
        .expect("send --help should run");
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Common:"));
    assert!(stdout.contains("Advanced:"));
    assert!(stdout.contains("Experimental:"));
    assert!(stdout.contains("--profile"));
}

#[test]
fn pull_help_groups_common_and_advanced_flags() {
    let output = bytehaul()
        .args(["pull", "--help"])
        .output()
        .expect("pull --help should run");
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Common:"));
    assert!(stdout.contains("Advanced:"));
    assert!(stdout.contains("--profile"));
}

#[test]
fn doctor_help_lists_workflows() {
    let output = bytehaul()
        .args(["doctor", "--help"])
        .output()
        .expect("doctor --help should run");
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("send"));
    assert!(stdout.contains("pull"));
    assert!(stdout.contains("daemon"));
}

#[test]
fn doctor_daemon_invalid_address_fails_cleanly() {
    let output = bytehaul()
        .args(["doctor", "daemon", "not-a-socket"])
        .output()
        .expect("doctor daemon should run");
    assert!(!output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Daemon address"));
    assert!(stdout.contains("Invalid host:port"));
}
