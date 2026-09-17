//! Generate Markdown CLI docs from the clap definitions, grouped by operator
//! role so the site reads as a guide rather than a flat dump.
//!
//! The `index` subcommand is maintainer-only: it stays in the binary, but is
//! hidden from the public docs page (clap-markdown skips `hide = true`
//! commands). Run via `scripts/gen_cli_docs.sh`.
//!
//! Output layout:
//!   ## Overview                     (root `arkel` subcommand list + options)
//!   ## Using Arkel                  client, account
//!   ## Running a storage node       storage
//!   ## Operating the network        payment, repair
//!
//! Each subcommand's section header is demoted one level (## -> ###) so the
//! role headings stay the visible grouping.

use arkel::cli::Cli;
use clap::CommandFactory;
use clap_markdown::help_markdown_command;

/// Top-level command -> role group title, in display order.
const GROUPS: &[(&str, &[&str])] = &[
    ("Using Arkel", &["client", "account"]),
    ("Running a storage node", &["storage"]),
    ("Operating the network", &["payment", "repair"]),
];

fn main() {
    let cmd = Cli::command().mut_subcommand("index", |c| c.hide(true));
    let md = strip_clap_footer(&help_markdown_command(&cmd));
    println!("{}", regroup(&md));
}

/// Drop the trailing "This document was generated automatically by
/// clap-markdown" footer — the page already carries its own (hidden) note.
fn strip_clap_footer(md: &str) -> String {
    if let Some(pos) = md.find("generated automatically by") {
        if let Some(hr) = md[..pos].rfind("<hr/>") {
            return md[..hr].trim_end().to_string();
        }
    }
    md.to_string()
}

/// Split clap-markdown output into `## \`arkel ...\`` sections.
fn sections(md: &str) -> Vec<(String, String)> {
    let mut secs: Vec<(String, String)> = Vec::new();
    let mut cur: Option<(String, Vec<&str>)> = None;
    for line in md.lines() {
        if line.starts_with("## `arkel") {
            if let Some((h, body)) = cur.take() {
                secs.push((h, body.join("\n").trim().to_string()));
            }
            cur = Some((line.to_string(), Vec::new()));
        } else if let Some((_, body)) = cur.as_mut() {
            body.push(line);
        }
    }
    if let Some((h, body)) = cur {
        secs.push((h, body.join("\n").trim().to_string()));
    }
    secs
}

/// Top-level command name from a section header like "## `arkel client put`".
fn top_level(header: &str) -> &str {
    let rest = header.strip_prefix("## `arkel").unwrap_or_default().trim();
    rest.split(['`', ' ']).next().unwrap_or_default().trim()
}

fn regroup(md: &str) -> String {
    let secs = sections(md);
    let mut out = String::new();

    for (header, body) in &secs {
        if header == "## `arkel`" {
            out.push_str("## Overview\n\n");
            out.push_str("###### **Index:**\n\n");
            for (title, _) in GROUPS {
                let anchor = format!("#{}", title.to_lowercase().replace(' ', "-"));
                out.push_str(&format!("* [{title}]({anchor})\n"));
            }
            out.push('\n');
            out.push_str(&linkify_subcommands(body));
            out.push_str("\n\n");
        }
    }
    for (title, tops) in GROUPS {
        out.push_str("## ");
        out.push_str(title);
        out.push_str("\n\n");
        for (header, body) in &secs {
            if header == "## `arkel`" || !tops.contains(&top_level(header)) {
                continue;
            }
            out.push_str("### ");
            out.push_str(&header[3..]); // drop "## " so the command sits under its role
            out.push('\n');
            out.push_str(body);
            out.push_str("\n\n");
        }
    }
    out
}

/// Turn `* \`storage\` — desc` bullets into `* [\`storage\`](#arkel-storage) — desc`
/// so the Overview links down to each command section. Option bullets
/// (`* \`--data-dir ...\``) are left untouched.
fn linkify_subcommands(body: &str) -> String {
    let mut out = String::new();
    for line in body.lines() {
        if let Some(rest) = line.trim_start().strip_prefix("* `")
            && let Some(close) = rest.find('`')
            && !rest[..close].starts_with("--")
        {
            let (name, tail) = (&rest[..close], &rest[close + 1..]);
            out.push_str(&format!("* [`{name}`](#arkel-{name}){tail}\n"));
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}
