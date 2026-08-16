//! Generate Markdown CLI docs from the clap definitions.
//!
//! The `index` subcommand is maintainer-only: it stays in the binary, but is
//! hidden from the public docs page (clap-markdown skips `hide = true`
//! commands). Run via `scripts/gen_cli_docs.sh`.

use arkel::cli::Cli;
use clap::CommandFactory;
use clap_markdown::help_markdown_command;

fn main() {
    let cmd = Cli::command().mut_subcommand("index", |c| c.hide(true));
    println!("{}", help_markdown_command(&cmd));
}
