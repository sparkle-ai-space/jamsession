use std::process::Command;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("cleanup") => run_cleanup(),
        Some(cmd) => {
            eprintln!("unknown xtask command: {cmd}");
            std::process::exit(1);
        }
        None => {
            eprintln!("usage: cargo xtask <command>");
            eprintln!("commands: cleanup");
            std::process::exit(1);
        }
    }
}

fn run_cleanup() {
    ensure_depub();
    run_depub();
    run_cargo_fmt();
}

fn ensure_depub() {
    if which("depub").is_none() {
        eprintln!("depub not found, installing...");
        let status = Command::new("cargo")
            .args(["install", "depub"])
            .status()
            .expect("failed to run cargo install");
        if !status.success() {
            eprintln!("failed to install depub");
            std::process::exit(1);
        }
    }
}

fn run_depub() {
    eprintln!("running depub...");

    let find = Command::new("find")
        .args(["src/jamsession", "-name", "*.rs"])
        .output()
        .expect("failed to run find");

    let files: Vec<&str> = std::str::from_utf8(&find.stdout)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .collect();

    if files.is_empty() {
        return;
    }

    let mut cmd = Command::new("depub");
    cmd.args(["-c", "cargo check --workspace --tests"]);
    cmd.args(&files);

    let status = cmd.status().expect("failed to run depub");
    if !status.success() {
        eprintln!("depub exited with non-zero status");
        std::process::exit(1);
    }
}

fn run_cargo_fmt() {
    eprintln!("running cargo fmt...");
    let status = Command::new("cargo")
        .args(["fmt", "--all"])
        .status()
        .expect("failed to run cargo fmt");
    if !status.success() {
        eprintln!("cargo fmt failed");
        std::process::exit(1);
    }
}

fn which(binary: &str) -> Option<std::path::PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let path = dir.join(binary);
            path.is_file().then_some(path)
        })
    })
}
