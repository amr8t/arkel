use anyhow::Result;

use crate::cli::AccountCmd;
use crate::identity::NodeIdentity;
use crate::index::remote;

/// Account & quota tooling: query the quota for an account (default: this identity).
pub async fn run(identity: &NodeIdentity, cmd: AccountCmd) -> Result<()> {
    match cmd {
        AccountCmd::Quota {
            account,
            index_addrs,
        } => {
            let account = account.unwrap_or_else(|| hex::encode(identity.node_id().as_bytes()));
            let http = reqwest::Client::new();
            let (total, used) =
                remote::account_quota(&http, &index_addrs, &account, identity.secret_key())
                    .await?;
            println!("{account}: {used} / {total} bytes used");
            Ok(())
        }
    }
}
