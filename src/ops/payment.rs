use anyhow::Result;

use crate::cli::PaymentCmd;
use crate::identity::NodeIdentity;
use crate::index::remote;

/// Payment-operator tooling: register this identity as the payment operator,
/// or credit quota to an account (the payment service in CLI form).
pub async fn run(identity: &NodeIdentity, cmd: PaymentCmd) -> Result<()> {
    let http = reqwest::Client::new();
    match cmd {
        PaymentCmd::Register { index_addrs } => {
            remote::set_payment_operator(&http, &index_addrs, identity.secret_key()).await?;
            println!("registered payment operator: {}", identity.node_id());
        }
        PaymentCmd::Credit {
            account,
            bytes,
            source,
            ref_id,
            index_addrs,
        } => {
            remote::credit_quota(
                &http,
                &index_addrs,
                &account,
                bytes,
                &source,
                &ref_id,
                identity.secret_key(),
            )
            .await?;
            println!("credited {bytes} bytes to {account} (ref {ref_id})");
        }
        PaymentCmd::SetDefaultQuota { bytes, index_addrs } => {
            let total = crate::config::parse_size(&bytes)?;
            remote::set_default_quota(&http, &index_addrs, total, identity.secret_key()).await?;
            println!("set default quota to {bytes} ({total} bytes)");
        }
    }
    Ok(())
}
