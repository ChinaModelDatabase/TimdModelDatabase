//! Credential configuration.
//!
//! Authentication is mandatory: there is no mode in which the server comes up
//! with no users configured. [`load`] returns an error rather than an empty
//! table, so a missing configuration stops startup instead of silently
//! accepting every client.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::pg::scram::{self, Verifier};
use crate::pg::PasswordVerifier;

/// Iteration count used for entries created by `tmdb-users create`.
pub const DEFAULT_ITERATIONS: u32 = 15000;

/// A user table loaded from a verifier file, with an optional environment
/// fallback for development.
#[derive(Debug)]
pub struct Users {
    entries: HashMap<String, Verifier>,
}

impl Users {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn insert_verifier(&mut self, user: impl Into<String>, verifier: Verifier) {
        self.entries.insert(user.into(), verifier);
    }

    pub fn insert_password(
        &mut self,
        user: impl Into<String>,
        password: &str,
        salt: Vec<u8>,
        iterations: u32,
    ) -> Result<(), scram::Error> {
        self.insert_verifier(user, Verifier::derive(password, salt, iterations)?);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn names(&self) -> Vec<&str> {
        self.entries.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for Users {
    fn default() -> Self {
        Self::new()
    }
}

impl PasswordVerifier for Users {
    /// SCRAM verification needs the salted password, not the plaintext, so the
    /// lookup hands back the whole entry.
    fn verifier(&self, user: &str) -> Option<Verifier> {
        self.entries.get(user).cloned()
    }
}

#[derive(Debug)]
pub enum Error {
    NoCredentialsConfigured,
    Read { path: PathBuf, source: std::io::Error },
    Malformed { path: PathBuf, line: usize, detail: String },
    UnknownFields { line: usize, field: String },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoCredentialsConfigured => write!(
                f,
                "no credentials configured: set TMDB_USERS to a verifier file, \
                 or TMDB_USER and TMDB_PASSWORD for a single development account"
            ),
            Error::Read { path, source } => {
                write!(f, "cannot read {}: {}", path.display(), source)
            }
            Error::Malformed { path, line, detail } => {
                write!(f, "{}:{}: {}", path.display(), line, detail)
            }
            Error::UnknownFields { line, field } => write!(
                f,
                "line {}: unknown field `{}`; expected user, salt, iterations, salted_password",
                line, field
            ),
        }
    }
}

impl std::error::Error for Error {}

/// Where credentials come from.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Path to the verifier file.
    pub users_file: Option<PathBuf>,
    /// Development fallback pair.
    pub env_user: Option<String>,
    pub env_password: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Config {
            users_file: std::env::var_os("TMDB_USERS").map(PathBuf::from),
            env_user: std::env::var("TMDB_USER").ok().filter(|s| !s.is_empty()),
            env_password: std::env::var("TMDB_PASSWORD").ok().filter(|s| !s.is_empty()),
        }
    }

    pub fn from_file(path: impl Into<PathBuf>) -> Self {
        Config {
            users_file: Some(path.into()),
            ..Default::default()
        }
    }
}

/// Load the user table, or fail.
///
/// The environment fallback is deliberately narrow: it only applies when no
/// verifier file is configured, and only when both halves of the pair are
/// present. A half-configured pair is an error rather than a silent skip.
pub fn load(config: &Config) -> Result<Arc<Users>, Error> {
    if let Some(path) = &config.users_file {
        return Ok(Arc::new(read_file(path)?));
    }

    match (&config.env_user, &config.env_password) {
        (Some(user), Some(password)) => {
            let mut users = Users::new();
            let salt = random_salt();
            users
                .insert_password(user.clone(), password, salt, DEFAULT_ITERATIONS)
                .map_err(|_| Error::Malformed {
                    path: PathBuf::from("<environment>"),
                    line: 0,
                    detail: "TMDB_PASSWORD contains non-ASCII characters, which this
                             build cannot SASLprep"
                        .into(),
                })?;
            tracing::warn!(
                "loaded a single development account {:?} from the environment; \
                 set TMDB_USERS for anything but a local test",
                user
            );
            Ok(Arc::new(users))
        }
        (Some(_), None) => Err(Error::Malformed {
            path: PathBuf::from("<environment>"),
            line: 0,
            detail: "TMDB_USER is set but TMDB_PASSWORD is not".into(),
        }),
        (None, Some(_)) => Err(Error::Malformed {
            path: PathBuf::from("<environment>"),
            line: 0,
            detail: "TMDB_PASSWORD is set but TMDB_USER is not".into(),
        }),
        (None, None) => Err(Error::NoCredentialsConfigured),
    }
}

/// Parse a verifier file.
///
/// The format is one entry per line so it can be edited by hand:
///
/// ```text
/// # comment
/// user=alice salt=W22ZaJ0SNY7soEsUEjb6gQ== iterations=15000 salted_password=377deaa9...
/// ```
pub fn read_file(path: &Path) -> Result<Users, Error> {
    let text = std::fs::read_to_string(path)
        .map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;

    let mut users = Users::new();
    for (index, raw) in text.lines().enumerate() {
        let line_no = index + 1;
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }

        let mut user = None;
        let mut salt = None;
        let mut iterations = None;
        let mut salted_password = None;

        for field in line.split_whitespace() {
            let (key, value) = field.split_once('=').ok_or_else(|| Error::Malformed {
                path: path.to_path_buf(),
                line: line_no,
                detail: format!("expected key=value, got `{}`", field),
            })?;
            match key {
                "user" => user = Some(value.to_string()),
                "salt" => salt = Some(value.to_string()),
                "iterations" => iterations = Some(value.parse::<u32>().map_err(|_| {
                    Error::Malformed {
                        path: path.to_path_buf(),
                        line: line_no,
                        detail: format!("iterations is not a number: `{}`", value),
                    }
                })?),
                "salted_password" => salted_password = Some(value.to_string()),
                other => {
                    return Err(Error::UnknownFields {
                        line: line_no,
                        field: other.to_string(),
                    })
                }
            }
        }

        let missing = |what: &str| Error::Malformed {
            path: path.to_path_buf(),
            line: line_no,
            detail: format!("missing {}", what),
        };

        let user = user.ok_or_else(|| missing("user"))?;
        let salt = scram::base64_decode(&salt.ok_or_else(|| missing("salt"))?)
            .map_err(|e| Error::Malformed {
                path: path.to_path_buf(),
                line: line_no,
                detail: format!("salt is not valid base64: {}", e),
            })?;
        let iterations = iterations.ok_or_else(|| missing("iterations"))?;
        if iterations < 4096 {
            return Err(Error::Malformed {
                path: path.to_path_buf(),
                line: line_no,
                detail: format!(
                    "iterations {} is below the RFC 7677 minimum of 4096",
                    iterations
                ),
            });
        }
        let raw = scram::base64_decode(&salted_password.ok_or_else(|| missing("salted_password"))?)
            .map_err(|e| Error::Malformed {
                path: path.to_path_buf(),
                line: line_no,
                detail: format!("salted_password is not valid base64: {}", e),
            })?;
        if raw.len() != 32 {
            return Err(Error::Malformed {
                path: path.to_path_buf(),
                line: line_no,
                detail: format!(
                    "salted_password must be a 32-byte SHA-256 digest, got {} bytes",
                    raw.len()
                ),
            });
        }
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&raw);

        if users.names().contains(&user.as_str()) {
            return Err(Error::Malformed {
                path: path.to_path_buf(),
                line: line_no,
                detail: format!("user {:?} is defined twice", user),
            });
        }
        users.insert_verifier(
            user,
            Verifier {
                salt,
                iterations,
                salted_password: digest,
            },
        );
    }

    if users.is_empty() {
        return Err(Error::NoCredentialsConfigured);
    }
    Ok(users)
}

/// Render an entry in the format [`read_file`] accepts.
pub fn format_entry(
    user: &str,
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<String, scram::Error> {
    let verifier = Verifier::derive(password, salt.to_vec(), iterations)?;
    Ok(format!(
        "user={} salt={} iterations={} salted_password={}",
        user,
        scram::base64_encode(salt),
        iterations,
        scram::base64_encode(&verifier.salted_password)
    ))
}

pub fn random_salt() -> Vec<u8> {
    use rand::Rng;
    (0..16).map(|_| rand::thread_rng().gen()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

fn write(body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users");
        std::fs::write(&path, body).unwrap();
        (dir, path)
    }

    fn entry(user: &str, password: &str, salt: &[u8], iterations: u32) -> String {
        format_entry(user, password, salt, iterations).expect("ASCII password")
    }

    #[test]
    fn a_round_tripped_entry_never_stores_the_password() {
        let salt = vec![7u8; 16];
        let line = entry("alice", "s3cret", &salt, DEFAULT_ITERATIONS);
        assert!(!line.contains("s3cret"), "leaked the password: {}", line);

        let (_dir, path) = write(&line);
        let users = read_file(&path).unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users.names(), vec!["alice"]);
        // The stored digest is the PBKDF2 output, not the password.
        assert_eq!(
            users.entries["alice"].salted_password,
            scram::pbkdf2_hmac_sha256(b"s3cret", &salt, DEFAULT_ITERATIONS)
        );
    }

    #[test]
    fn a_non_ascii_password_is_refused_rather_than_silently_deriving_a_different_key() {
        let err = format_entry("alice", "密码", &[1u8; 16], 4096).unwrap_err();
        assert!(matches!(err, scram::Error::NonAsciiPassword));
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let salt = vec![1u8; 16];
        let body = format!(
            "# a comment\n\n   \n{}\n# trailing\n",
            entry("bob", "pw", &salt, 4096)
        );
        let (_dir, path) = write(&body);
        assert_eq!(read_file(&path).unwrap().names(), vec!["bob"]);
    }

    #[test]
    fn several_entries_load() {
        let (_dir, path) = write(&format!(
            "{}\n{}\n",
            entry("a", "pw1", &[1u8; 16], 4096),
            entry("b", "pw2", &[2u8; 16], 4096)
        ));
        let users = read_file(&path).unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn an_empty_file_is_an_error_not_an_open_server() {
        let (_dir, path) = write("\n# nothing here\n");
        assert!(matches!(
            read_file(&path),
            Err(Error::NoCredentialsConfigured)
        ));
    }

    #[test]
    fn a_missing_file_is_reported_with_its_path() {
        let err = read_file(Path::new("does/not/exist")).unwrap_err();
        assert!(matches!(err, Error::Read { .. }));
        assert!(err.to_string().contains("does/not/exist"));
    }

    #[test]
    fn a_low_iteration_count_is_refused() {
        let (_dir, path) = write(&entry("a", "pw", &[1u8; 16], 1000));
        let err = read_file(&path).unwrap_err().to_string();
        assert!(err.contains("4096"), "got {}", err);
    }

    #[test]
    fn a_short_digest_is_refused() {
        let (_dir, path) = write("user=a salt=AAAAAAAAAAAAAAAAAAAAAA== iterations=4096 salted_password=AAAA");
        let err = read_file(&path).unwrap_err().to_string();
        assert!(err.contains("32-byte"), "got {}", err);
    }

    #[test]
    fn a_duplicate_user_is_refused() {
        let (_dir, path) = write(&format!(
            "{}\n{}\n",
            entry("a", "pw", &[1u8; 16], 4096),
            entry("a", "other", &[2u8; 16], 4096)
        ));
        let err = read_file(&path).unwrap_err().to_string();
        assert!(err.contains("twice"), "got {}", err);
    }

    #[test]
    fn a_typo_in_a_field_name_is_reported_rather_than_ignored() {
        let (_dir, path) = write("user=a slat=AAAA iterations=4096 salted_password=AAAA");
        let err = read_file(&path).unwrap_err().to_string();
        assert!(err.contains("slat"), "got {}", err);
    }

    #[test]
    fn a_missing_field_names_the_field() {
        let (_dir, path) = write("user=a iterations=4096");
        let err = read_file(&path).unwrap_err().to_string();
        assert!(err.contains("salt"), "got {}", err);
    }

    #[test]
    fn a_field_without_an_equals_sign_is_refused() {
        let (_dir, path) = write("user=a salt");
        assert!(read_file(&path).is_err());
    }

    #[test]
    fn loading_without_any_configuration_fails() {
        let err = load(&Config::default()).unwrap_err();
        assert!(matches!(err, Error::NoCredentialsConfigured));
        assert!(err.to_string().contains("TMDB_USERS"));
    }

    #[test]
    fn the_environment_pair_produces_a_usable_account() {
        let users = load(&Config {
            users_file: None,
            env_user: Some("dev".into()),
            env_password: Some("pw".into()),
        })
        .unwrap();
        assert_eq!(users.names(), vec!["dev"]);
        let expected = scram::pbkdf2_hmac_sha256(
            b"pw",
            &users.entries["dev"].salt,
            DEFAULT_ITERATIONS,
        );
        assert_eq!(users.entries["dev"].salted_password, expected);
        assert_eq!(users.entries["dev"].iterations, DEFAULT_ITERATIONS);
    }

    #[test]
    fn a_half_configured_environment_pair_is_an_error() {
        for (user, password) in [(Some("dev"), None), (None, Some("pw"))] {
            let err = load(&Config {
                users_file: None,
                env_user: user.map(String::from),
                env_password: password.map(String::from),
            })
            .unwrap_err();
            assert!(matches!(err, Error::Malformed { .. }), "got {:?}", err);
        }
    }

    #[test]
    fn a_users_file_takes_precedence_over_the_environment() {
        let (_dir, path) = write(&entry("fromfile", "pw", &[1u8; 16], 4096));
        let users = load(&Config {
            users_file: Some(path),
            env_user: Some("fromenv".into()),
            env_password: Some("pw".into()),
        })
        .unwrap();
        assert_eq!(users.names(), vec!["fromfile"]);
    }
}