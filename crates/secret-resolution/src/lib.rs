//! Resolution of bounded, externally supplied secret references.
//!
//! This crate owns the supported `file:` reference scheme, bounded file reads, and redacted
//! credential values. It must not know `SQLx`, `PostgreSQL`, domain or application types,
//! telemetry,
//! cloud SDKs, or global process configuration.

use std::{fmt, fs::File, io, io::Read, path::Path};

use runtime_config::SecretReference;
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;

const FILE_SCHEME: &str = "file:";
const MAX_SECRET_BYTES: usize = 8 * 1024;

/// A file source used to resolve secret material with a strict byte limit.
pub trait SecretFileReader {
    /// Reads no more than `limit + 1` bytes so callers can detect oversize values.
    ///
    /// # Errors
    ///
    /// Returns an I/O error without adding the path or file contents to its message.
    fn read_bounded(&self, path: &Path, limit: usize) -> Result<Vec<u8>, io::Error>;
}

/// The operating-system file source used by process composition roots.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemSecretFileReader;

impl SecretFileReader for SystemSecretFileReader {
    fn read_bounded(&self, path: &Path, limit: usize) -> Result<Vec<u8>, io::Error> {
        let file = File::open(path)?;
        let maximum = u64::try_from(limit)
            .ok()
            .and_then(|value| value.checked_add(1))
            .unwrap_or(u64::MAX);
        let mut bytes = Vec::with_capacity(limit.saturating_add(1));
        file.take(maximum).read_to_end(&mut bytes)?;
        Ok(bytes)
    }
}

/// A resolved credential whose debug representation cannot expose its contents.
pub struct ResolvedCredential(SecretString);

impl ExposeSecret<str> for ResolvedCredential {
    fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for ResolvedCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResolvedCredential([REDACTED])")
    }
}

/// A bounded secret-resolution failure that never includes a reference or secret value.
#[derive(Debug, Error)]
pub enum SecretResolutionError {
    /// The opaque reference is empty or exceeds its configured bound.
    #[error("secret reference is invalid")]
    InvalidReference,
    /// The reference uses a provider other than the one supported by this checkpoint.
    #[error("secret reference uses an unsupported provider")]
    UnsupportedProvider,
    /// A file reference does not contain an absolute path.
    #[error("secret file path must be absolute")]
    RelativePath,
    /// The referenced file could not be read.
    #[error("secret file could not be read")]
    Read(#[source] io::Error),
    /// The file exceeds the strict 8 KiB bound.
    #[error("secret file exceeds the maximum size")]
    TooLarge,
    /// The file is not valid UTF-8.
    #[error("secret file is not valid UTF-8")]
    InvalidUtf8,
    /// The file is empty after permitted newline handling.
    #[error("secret file is empty")]
    Empty,
    /// The file contains an embedded NUL byte.
    #[error("secret file contains a forbidden NUL byte")]
    Nul,
}

/// Resolves a credential using the operating-system file source.
///
/// # Errors
///
/// Returns a safe [`SecretResolutionError`] without exposing the reference or file contents.
pub fn resolve(reference: &SecretReference) -> Result<ResolvedCredential, SecretResolutionError> {
    resolve_with(reference, &SystemSecretFileReader)
}

/// Parses and resolves an opaque reference without exposing composition-layer reference types.
///
/// This boundary is intended for non-deployable qualification tooling that is permitted to use
/// secret resolution but must not import runtime configuration.
///
/// # Errors
///
/// Returns a safe [`SecretResolutionError`] without exposing the reference or file contents.
pub fn resolve_reference(reference: &str) -> Result<ResolvedCredential, SecretResolutionError> {
    let reference = SecretReference::new(reference.to_owned())
        .map_err(|_| SecretResolutionError::InvalidReference)?;
    resolve(&reference)
}

/// Resolves a credential using an explicitly supplied file source.
///
/// # Errors
///
/// Returns a safe [`SecretResolutionError`] without exposing the reference or file contents.
pub fn resolve_with(
    reference: &SecretReference,
    reader: &impl SecretFileReader,
) -> Result<ResolvedCredential, SecretResolutionError> {
    let raw_reference = reference.as_str_for_resolution();
    let path_text = raw_reference
        .strip_prefix(FILE_SCHEME)
        .ok_or(SecretResolutionError::UnsupportedProvider)?;
    let path = Path::new(path_text);
    if !path.is_absolute() {
        return Err(SecretResolutionError::RelativePath);
    }

    let mut bytes = reader
        .read_bounded(path, MAX_SECRET_BYTES)
        .map_err(SecretResolutionError::Read)?;
    if bytes.len() > MAX_SECRET_BYTES {
        return Err(SecretResolutionError::TooLarge);
    }
    trim_one_final_line_ending(&mut bytes);
    if bytes.is_empty() {
        return Err(SecretResolutionError::Empty);
    }
    if bytes.contains(&0) {
        return Err(SecretResolutionError::Nul);
    }
    let value = String::from_utf8(bytes).map_err(|_| SecretResolutionError::InvalidUtf8)?;
    Ok(ResolvedCredential(SecretString::from(value)))
}

fn trim_one_final_line_ending(bytes: &mut Vec<u8>) {
    if bytes.ends_with(b"\r\n") {
        bytes.truncate(bytes.len().saturating_sub(2));
    } else if bytes.ends_with(b"\n") {
        bytes.truncate(bytes.len().saturating_sub(1));
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io, path::Path};

    use runtime_config::SecretReference;
    use secrecy::ExposeSecret;
    use tempfile::tempdir;

    use super::{
        MAX_SECRET_BYTES, ResolvedCredential, SecretFileReader, SecretResolutionError, resolve,
        resolve_with,
    };

    struct FailingReader;

    type ErrorPredicate = fn(&SecretResolutionError) -> bool;
    type InvalidCase<'a> = (&'a str, Vec<u8>, ErrorPredicate);

    impl SecretFileReader for FailingReader {
        fn read_bounded(&self, _path: &Path, _limit: usize) -> Result<Vec<u8>, io::Error> {
            Err(io::Error::new(io::ErrorKind::NotFound, "safe sentinel"))
        }
    }

    fn reference(value: String) -> Result<SecretReference, runtime_config::SecretReferenceError> {
        SecretReference::new(value)
    }

    #[test]
    fn valid_file_resolution_and_single_line_ending_handling() {
        let directory = tempdir();
        if let Ok(directory) = directory {
            for (suffix, expected) in [
                ("", "credential"),
                ("\n", "credential"),
                ("\r\n", "credential"),
                ("\n\n", "credential\n"),
            ] {
                let path = directory.path().join(format!("secret-{}", suffix.len()));
                assert!(fs::write(&path, format!("credential{suffix}")).is_ok());
                let resolved = reference(format!("file:{}", path.display()))
                    .ok()
                    .and_then(|value| resolve(&value).ok());
                assert_eq!(
                    resolved.as_ref().map(ExposeSecret::expose_secret),
                    Some(expected)
                );
            }
        } else {
            panic!("temporary directory must be available");
        }
    }

    #[test]
    fn empty_oversized_nul_and_relative_values_are_rejected() {
        let directory = tempdir();
        if let Ok(directory) = directory {
            let cases: [InvalidCase<'_>; 3] = [
                ("empty", Vec::new(), |error| {
                    matches!(error, SecretResolutionError::Empty)
                }),
                ("oversized", vec![b'x'; MAX_SECRET_BYTES + 1], |error| {
                    matches!(error, SecretResolutionError::TooLarge)
                }),
                ("nul", b"credential\0tail".to_vec(), |error| {
                    matches!(error, SecretResolutionError::Nul)
                }),
            ];
            for (name, bytes, predicate) in cases {
                let path = directory.path().join(name);
                assert!(fs::write(&path, bytes).is_ok());
                let result = reference(format!("file:{}", path.display()))
                    .ok()
                    .and_then(|value| resolve(&value).err());
                assert!(result.as_ref().is_some_and(predicate));
            }
        } else {
            panic!("temporary directory must be available");
        }

        let relative = reference(String::from("file:relative/secret"))
            .ok()
            .and_then(|value| resolve_with(&value, &FailingReader).err());
        assert!(matches!(
            relative,
            Some(SecretResolutionError::RelativePath)
        ));
    }

    #[test]
    fn missing_file_error_and_debug_are_redacted() {
        let sentinel = "postgresql://user:unique-password@secret-host/database";
        let reference = reference(String::from("file:/does/not/exist"));
        let error = reference
            .as_ref()
            .ok()
            .and_then(|value| resolve_with(value, &FailingReader).err());
        let display = error.as_ref().map(ToString::to_string).unwrap_or_default();
        let debug = format!("{error:?}");
        assert!(!display.contains(sentinel));
        assert!(!debug.contains(sentinel));

        let resolved = ResolvedCredential(secrecy::SecretString::from(String::from(sentinel)));
        let rendered = format!("{resolved:?}");
        assert_eq!(rendered, "ResolvedCredential([REDACTED])");
        assert!(!rendered.contains(sentinel));
    }

    #[test]
    fn unsupported_scheme_never_echoes_the_reference() {
        let sentinel = "secret-provider://unique-sensitive-reference";
        let error = reference(String::from(sentinel))
            .ok()
            .and_then(|value| resolve_with(&value, &FailingReader).err());
        let rendered = format!(
            "{error:?} {}",
            error.as_ref().map(ToString::to_string).unwrap_or_default()
        );
        assert!(matches!(
            error,
            Some(SecretResolutionError::UnsupportedProvider)
        ));
        assert!(!rendered.contains(sentinel));
    }
}
