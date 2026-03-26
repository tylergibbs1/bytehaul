use std::io;

use anyhow::Result;
use clap::Args;
use clap_complete::{generate, Shell};

#[derive(Args)]
pub struct CompletionsArgs {
    /// Shell to generate completions for
    #[arg(value_enum)]
    pub shell: Shell,
}

pub fn run(args: CompletionsArgs, cmd: &mut clap::Command) -> Result<()> {
    generate(args.shell, cmd, "bytehaul", &mut io::stdout());
    Ok(())
}
