use anyhow::{Context, Result};
use iroh::{PublicKey as NodeId, SecretKey};
use std::path::Path;
use tokio::fs::{self};

pub struct NodeIdentity {
    secret_key: SecretKey,
}

impl NodeIdentity {
    pub async fn load_or_create(key_path: &Path) -> Result<Self> {
        if key_path.exists() {
            Self::load(key_path).await
        } else {
            let identity = Self::generate();
            identity.save(key_path).await?;
            tracing::info!("Generated new identity: {}", identity.node_id());
            Ok(identity)
        }
    }

    fn generate() -> Self {
        Self {
            secret_key: SecretKey::generate(),
        }
    }

    async fn save(&self, key_path: &Path) -> Result<()> {
        // 1. Extract the raw [u8; 32] array from the secret key
        let key_bytes: [u8; 32] = self.secret_key.to_bytes();

        // 2. Write the raw binary bytes straight to disk in a single operation
        fs::write(key_path, key_bytes)
            .await
            .context("Failed to write binary identity key to disk")?;

        // 3. Lock down file system access tightly to the file owner (0600)
        #[cfg(unix)]
        {
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(key_path, Permissions::from_mode(0o600)).await?;
        }

        Ok(())
    }

    // --- LOAD FUNCTION ---
    async fn load(key_path: &Path) -> Result<Self> {
        // 1. Read the raw binary file back into a vector
        let bytes = fs::read(key_path)
            .await
            .context("Failed to read node secret key file")?;

        // 2. Safely convert the dynamic vector slice into a fixed 32-byte array
        let byte_array: [u8; 32] = bytes.try_into().map_err(|_| {
            anyhow::anyhow!("Identity key on disk is corrupted (must be exactly 32 bytes)")
        })?;

        // 3. Reconstruct the Iroh SecretKey directly from the bytes
        let secret_key = SecretKey::from_bytes(&byte_array);

        Ok(Self { secret_key })
    }

    pub fn node_id(&self) -> NodeId {
        self.secret_key.public()
    }

    pub fn secret_key(&self) -> &SecretKey {
        &self.secret_key
    }
}
