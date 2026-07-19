use anyhow::{Result, bail};
use chacha20poly1305::{
    XChaCha20Poly1305,
    aead::{AeadInPlace, KeyInit, OsRng},
};
use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;

pub fn derive_key(master_key: &[u8; 32], object_hash: &blake3::Hash) -> [u8; 32] {
    let mut output = [0u8; 32];
    let hk = Hkdf::<Sha256>::new(Some(object_hash.as_bytes()), master_key);
    hk.expand(b"arkel-shard-key", &mut output)
        .expect("HKDF expansion failed (should never happen with SHA-256)");
    output
}

// XChaCha20-Poly1305, random 24-byte nonce, returns [nonce(24) || ciphertext || tag]
// Use chacha20poly1305 crate, OsRng for nonce generation.
pub fn encrypt_shard(data: &[u8], key: &[u8; 32]) -> Vec<u8> {
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut nonce);

    let cipher = XChaCha20Poly1305::new(key.into());
    let mut buf = data.to_vec();
    cipher
        .encrypt_in_place((&nonce).into(), &[], &mut buf)
        .unwrap();
    let mut result = Vec::with_capacity(24 + buf.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&buf);
    result
}

// pub fn decrypt_shard(encrypted: &[u8], key: &[u8; 32]) -> Result<Vec<u8>>
// Split first 24 bytes = nonce, rest = ciphertext+tag. Decrypt + verify.
pub fn decrypt_shard(encrypted: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if encrypted.len() < 24 {
        bail!("encrypted shard too short (missing nonce)");
    }
    let nonce = &encrypted[0..24];
    let ciphertext = &encrypted[24..];

    let cipher = XChaCha20Poly1305::new(key.into());
    let mut buf = ciphertext.to_vec();
    cipher
        .decrypt_in_place(nonce.into(), &[], &mut buf)
        .map_err(|_| anyhow::anyhow!("decryption failed: wrong key or corrupted data"))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn roundtrip() {
        let key = [0u8; 32];
        let data = b"hello world";
        let enc = encrypt_shard(data, &key);
        let dec = decrypt_shard(&enc, &key).unwrap();
        assert_eq!(dec, data);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];
        let data = b"test";
        let enc = encrypt_shard(data, &key1);
        assert!(decrypt_shard(&enc, &key2).is_err());
    }
}
