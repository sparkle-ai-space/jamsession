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

        book.for_each_mut(|item| {
            if let BookItem::Chapter(chapter) = item {
                chapter.content = expand_anchors(&chapter.content, &anchors, &config);
            }
        });

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
    github_branch: String,
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

        Ok(Config {
            root,
            scan_dirs,
            github_repo: pp_config.github_repo,
            github_branch: pp_config.github_branch,
        })
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
            config.github_repo, config.github_branch, rel, self.line_start, self.line_end,
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

fn expand_anchors(content: &str, anchors: &HashMap<String, Anchor>, config: &Config) -> String {
    let mut result = expand_block_anchors(content, anchors, config);
    result = expand_inline_anchors(&result, anchors, config);
    result
}

fn expand_inline_anchors(
    content: &str,
    anchors: &HashMap<String, Anchor>,
    config: &Config,
) -> String {
    let re = Regex::new(r"\{anchor\}`([^`]+)`").unwrap();
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

fn expand_block_anchors(
    content: &str,
    anchors: &HashMap<String, Anchor>,
    config: &Config,
) -> String {
    let re = Regex::new(r"(?m)^```\{anchor\}\s*\n([\s\S]*?)^```\s*$").unwrap();
    re.replace_all(content, |caps: &regex::Captures| {
        let body = caps[1].trim();
        let name = body.lines().next().unwrap_or("").trim();
        match anchors.get(name) {
            Some(anchor) => {
                let rel = anchor.relative_path(&config.root);
                let url = anchor.github_url(config);
                let lang = anchor.file_extension();
                format!(
                    "<figure>\n\n```{lang}\n{content}\n```\n\n<figcaption>\n\n[`{rel}:{start}-{end}`]({url})\n\n</figcaption>\n</figure>",
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
            github_branch: "main".to_string(),
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
        assert!(output.contains("figcaption"));
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
}
