use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::process;

use anyhow::Result;
use clap::{Parser, Subcommand};
use mdbook_preprocessor::book::{Book, BookItem};
use mdbook_preprocessor::{Preprocessor, PreprocessorContext, parse_input};
use regex::Regex;
use walkdir::WalkDir;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Supports { renderer: String },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Supports { renderer }) => {
            if renderer == "html" || renderer == "markdown" {
                process::exit(0);
            } else {
                process::exit(1);
            }
        }
        None => {
            let (ctx, book) = parse_input(io::stdin())?;
            let preprocessor = AnchorPreprocessor;
            let processed = preprocessor.run(&ctx, book)?;
            serde_json::to_writer(io::stdout(), &processed)?;
            Ok(())
        }
    }
}

struct AnchorPreprocessor;

impl Preprocessor for AnchorPreprocessor {
    fn name(&self) -> &str {
        "anchor"
    }

    fn run(&self, ctx: &PreprocessorContext, mut book: Book) -> Result<Book> {
        let config = Config::from_context(ctx)?;
        let anchors = scan_anchors(&config.root, &config.scan_dirs)?;

        let mut missing: Vec<String> = Vec::new();

        book.for_each_mut(|item| {
            if let BookItem::Chapter(chapter) = item {
                let (content, chapter_missing) =
                    expand_anchors_checked(&chapter.content, &anchors, &config);
                chapter.content = content;
                missing.extend(chapter_missing);
            }
        });

        if !missing.is_empty() {
            missing.sort();
            missing.dedup();
            anyhow::bail!("unknown anchors: {}", missing.join(", "));
        }

        Ok(book)
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PreprocessorConfig {
    #[serde(default)]
    scan_dirs: Vec<String>,
    #[serde(default = "default_github_repo")]
    github_repo: String,
    #[serde(default = "default_github_branch")]
    github_branch: String,
}

fn default_github_repo() -> String {
    "nikomatsakis/jamsession".to_string()
}

fn default_github_branch() -> String {
    "main".to_string()
}

#[derive(Debug)]
struct Config {
    root: PathBuf,
    scan_dirs: Vec<PathBuf>,
    github_repo: String,
    github_ref: String,
}

impl Config {
    fn from_context(ctx: &PreprocessorContext) -> Result<Self> {
        let root = ctx.root.clone();

        let pp_config: PreprocessorConfig =
            ctx.config
                .get("preprocessor.anchor")?
                .unwrap_or(PreprocessorConfig {
                    scan_dirs: vec![],
                    github_repo: default_github_repo(),
                    github_branch: default_github_branch(),
                });

        let scan_dirs = if pp_config.scan_dirs.is_empty() {
            vec![root.join("src")]
        } else {
            pp_config.scan_dirs.iter().map(|s| root.join(s)).collect()
        };

        let (repo, git_ref) = detect_git_context(&root, &pp_config);

        Ok(Config {
            root,
            scan_dirs,
            github_repo: repo,
            github_ref: git_ref,
        })
    }
}

/// Detect the GitHub repo and ref from the git working tree.
/// Uses the tracking remote to find the correct fork, and the current
/// branch name for the ref (so links stay valid as commits move).
fn detect_git_context(root: &Path, fallback: &PreprocessorConfig) -> (String, String) {
    let tracking = tracking_remote_and_branch(root);

    let (remote_url, branch) = match &tracking {
        Some((url, branch)) => (Some(url.clone()), Some(branch.clone())),
        None => (origin_remote_url(root), None),
    };

    let repo = remote_url
        .and_then(|url| parse_github_repo(&url))
        .unwrap_or_else(|| fallback.github_repo.clone());

    let git_ref = branch.unwrap_or_else(|| fallback.github_branch.clone());

    (repo, git_ref)
}

/// Get the remote URL and branch name for the current branch's upstream.
/// Returns (remote_url, branch_name) — e.g. ("git@github.com:user/repo.git", "main").
fn tracking_remote_and_branch(root: &Path) -> Option<(String, String)> {
    // Returns something like "origin/main" or "nikomatsakis/conductor-like-arch"
    let upstream = std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
        .current_dir(root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())?;

    let (remote, branch) = upstream.split_once('/')?;
    let url = remote_url_by_name(root, remote)?;
    Some((url, branch.to_string()))
}

/// Get the URL for the "origin" remote.
fn origin_remote_url(root: &Path) -> Option<String> {
    remote_url_by_name(root, "origin")
}

fn remote_url_by_name(root: &Path, name: &str) -> Option<String> {
    std::process::Command::new("git")
        .args(["remote", "get-url", name])
        .current_dir(root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

/// Parse "owner/repo" from a GitHub remote URL.
/// Handles: `git@github.com:owner/repo.git`, `https://github.com/owner/repo.git`
fn parse_github_repo(url: &str) -> Option<String> {
    let path = if let Some(rest) = url.strip_prefix("git@github.com:") {
        rest
    } else if url.contains("github.com/") {
        url.split("github.com/").nth(1)?
    } else {
        return None;
    };
    let path = path.strip_suffix(".git").unwrap_or(path);
    let parts: Vec<&str> = path.splitn(3, '/').collect();
    if parts.len() >= 2 {
        Some(format!("{}/{}", parts[0], parts[1]))
    } else {
        None
    }
}

#[derive(Debug, Clone)]
struct Anchor {
    file: PathBuf,
    line_start: usize,
    line_end: usize,
    content: String,
}

impl Anchor {
    fn relative_path(&self, root: &Path) -> String {
        self.file
            .strip_prefix(root)
            .unwrap_or(&self.file)
            .to_string_lossy()
            .into_owned()
    }

    fn github_url(&self, config: &Config) -> String {
        let rel = self.relative_path(&config.root);
        format!(
            "https://github.com/{}/blob/{}/{}#L{}-L{}",
            config.github_repo, config.github_ref, rel, self.line_start, self.line_end,
        )
    }

    fn file_extension(&self) -> &str {
        self.file.extension().and_then(|e| e.to_str()).unwrap_or("")
    }
}

fn scan_anchors(root: &Path, scan_dirs: &[PathBuf]) -> Result<HashMap<String, Anchor>> {
    let mut anchors = HashMap::new();
    let anchor_start = Regex::new(r"//\s*ANCHOR:\s*(\w[\w-]*)").unwrap();
    let anchor_end = Regex::new(r"//\s*ANCHOR_END:\s*(\w[\w-]*)").unwrap();

    for dir in scan_dirs {
        if !dir.exists() {
            continue;
        }
        for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if !matches!(ext, "rs" | "toml" | "json" | "yaml" | "yml" | "ts" | "js") {
                continue;
            }

            let content = match std::fs::read_to_string(path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let mut open_anchors: HashMap<String, (usize, Vec<String>)> = HashMap::new();

            for (line_num, line) in content.lines().enumerate() {
                let line_1based = line_num + 1;

                if let Some(caps) = anchor_start.captures(line) {
                    let name = caps[1].to_string();
                    open_anchors.insert(name, (line_1based, Vec::new()));
                    continue;
                }

                if let Some(caps) = anchor_end.captures(line) {
                    let name = caps[1].to_string();
                    if let Some((start_line, lines)) = open_anchors.remove(&name) {
                        let anchor = Anchor {
                            file: path.to_path_buf(),
                            line_start: start_line + 1,
                            line_end: line_1based - 1,
                            content: dedent(&lines),
                        };
                        if anchors.contains_key(&name) {
                            eprintln!(
                                "warning: duplicate anchor `{}` in {}",
                                name,
                                path.strip_prefix(root).unwrap_or(path).display()
                            );
                        }
                        anchors.insert(name, anchor);
                    }
                    continue;
                }

                for (_, lines) in open_anchors.values_mut() {
                    lines.push(line.to_string());
                }
            }

            for (name, _) in open_anchors {
                eprintln!(
                    "warning: unclosed anchor `{}` in {}",
                    name,
                    path.strip_prefix(root).unwrap_or(path).display()
                );
            }
        }
    }

    Ok(anchors)
}

fn dedent(lines: &[String]) -> String {
    if lines.is_empty() {
        return String::new();
    }

    let min_indent = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);

    lines
        .iter()
        .map(|l| {
            if l.len() >= min_indent {
                &l[min_indent..]
            } else {
                l.trim()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn expand_anchors_checked(
    content: &str,
    anchors: &HashMap<String, Anchor>,
    config: &Config,
) -> (String, Vec<String>) {
    let mut missing = Vec::new();
    let mut result = expand_block_anchors_checked(content, anchors, config, &mut missing);
    result = expand_inline_anchors_checked(&result, anchors, config, &mut missing);
    (result, missing)
}

#[cfg(test)]
fn expand_inline_anchors(
    content: &str,
    anchors: &HashMap<String, Anchor>,
    config: &Config,
) -> String {
    expand_inline_anchors_checked(content, anchors, config, &mut Vec::new())
}

fn expand_inline_anchors_checked(
    content: &str,
    anchors: &HashMap<String, Anchor>,
    config: &Config,
    missing: &mut Vec<String>,
) -> String {
    let re = Regex::new(r"\{anchor\}`([^`]+)`").unwrap();
    // Collect missing names in a first pass
    for caps in re.captures_iter(content) {
        let name = &caps[1];
        if !anchors.contains_key(name) {
            missing.push(name.to_string());
        }
    }
    re.replace_all(content, |caps: &regex::Captures| {
        let name = &caps[1];
        match anchors.get(name) {
            Some(anchor) => {
                let rel = anchor.relative_path(&config.root);
                let url = anchor.github_url(config);
                format!(
                    "[`{}:{}-{}`]({})",
                    rel, anchor.line_start, anchor.line_end, url,
                )
            }
            None => {
                eprintln!("warning: unknown anchor `{name}`");
                format!("**⚠️ unknown anchor `{name}`**")
            }
        }
    })
    .into_owned()
}

#[cfg(test)]
fn expand_block_anchors(
    content: &str,
    anchors: &HashMap<String, Anchor>,
    config: &Config,
) -> String {
    expand_block_anchors_checked(content, anchors, config, &mut Vec::new())
}

fn expand_block_anchors_checked(
    content: &str,
    anchors: &HashMap<String, Anchor>,
    config: &Config,
    missing: &mut Vec<String>,
) -> String {
    let re = Regex::new(r"(?m)^```\{anchor\}\s*\n([\s\S]*?)^```\s*$").unwrap();
    for caps in re.captures_iter(content) {
        let body = caps[1].trim();
        let name = body.lines().next().unwrap_or("").trim();
        if !anchors.contains_key(name) {
            missing.push(name.to_string());
        }
    }
    re.replace_all(content, |caps: &regex::Captures| {
        let body = caps[1].trim();
        let name = body.lines().next().unwrap_or("").trim();
        match anchors.get(name) {
            Some(anchor) => {
                let rel = anchor.relative_path(&config.root);
                let url = anchor.github_url(config);
                let lang = anchor.file_extension();
                format!(
                    "```{lang}\n{content}\n```\n\n*[`{rel}:{start}-{end}`]({url})*",
                    content = anchor.content,
                    start = anchor.line_start,
                    end = anchor.line_end,
                )
            }
            None => {
                eprintln!("warning: unknown anchor `{name}`");
                format!("**⚠️ unknown anchor `{name}`**")
            }
        }
    })
    .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            root: PathBuf::from("/project"),
            scan_dirs: vec![],
            github_repo: "user/repo".to_string(),
            github_ref: "main".to_string(),
        }
    }

    fn test_anchors() -> HashMap<String, Anchor> {
        let mut m = HashMap::new();
        m.insert(
            "foo".to_string(),
            Anchor {
                file: PathBuf::from("/project/src/lib.rs"),
                line_start: 10,
                line_end: 15,
                content: "fn foo() {\n    println!(\"hello\");\n}".to_string(),
            },
        );
        m
    }

    #[test]
    fn inline_expansion() {
        let anchors = test_anchors();
        let config = test_config();
        let input = "See {anchor}`foo` for details.";
        let output = expand_inline_anchors(input, &anchors, &config);
        assert!(output.contains("[`src/lib.rs:10-15`]"));
        assert!(output.contains("https://github.com/user/repo/blob/main/src/lib.rs#L10-L15"));
    }

    #[test]
    fn block_expansion() {
        let anchors = test_anchors();
        let config = test_config();
        let input = "```{anchor}\nfoo\n```";
        let output = expand_block_anchors(input, &anchors, &config);
        assert!(output.contains("```rs"));
        assert!(output.contains("fn foo()"));
        assert!(output.contains("src/lib.rs:10-15"));
    }

    #[test]
    fn unknown_anchor_inline() {
        let anchors = HashMap::new();
        let config = test_config();
        let input = "{anchor}`missing`";
        let output = expand_inline_anchors(input, &anchors, &config);
        assert!(output.contains("unknown anchor"));
    }

    #[test]
    fn dedent_removes_common_whitespace() {
        let lines = vec![
            "        fn bar() {".to_string(),
            "            42".to_string(),
            "        }".to_string(),
        ];
        let result = dedent(&lines);
        assert_eq!(result, "fn bar() {\n    42\n}");
    }

    #[test]
    fn parse_github_repo_ssh() {
        assert_eq!(
            parse_github_repo("git@github.com:sparkle-ai-space/jamsession.git"),
            Some("sparkle-ai-space/jamsession".to_string())
        );
    }

    #[test]
    fn parse_github_repo_https() {
        assert_eq!(
            parse_github_repo("https://github.com/nikomatsakis/jamsession.git"),
            Some("nikomatsakis/jamsession".to_string())
        );
    }

    #[test]
    fn parse_github_repo_no_suffix() {
        assert_eq!(
            parse_github_repo("https://github.com/user/repo"),
            Some("user/repo".to_string())
        );
    }
}
