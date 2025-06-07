//! Client configuration data.

use std::{
    self,
    collections::BTreeMap,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use crate::{
    crypto::SecurityPolicy,
    types::{
        ApplicationDescription, ApplicationType, LocalizedText, MessageSecurityMode, UAString,
    },
};

pub const ANONYMOUS_USER_TOKEN_ID: &str = "ANONYMOUS";

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ClientUserToken {
    /// Username
    pub user: String,
    /// Password
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key_path: Option<String>,
}

impl ClientUserToken {
    /// Constructs a client token which holds a username and password.
    pub fn user_pass<S, T>(user: S, password: T) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        ClientUserToken {
            user: user.into(),
            password: Some(password.into()),
            cert_path: None,
            private_key_path: None,
        }
    }

    /// Constructs a client token which holds a username and paths to X509 certificate and private key.
    pub fn x509<S>(user: S, cert_path: &Path, private_key_path: &Path) -> Self
    where
        S: Into<String>,
    {
        // Apparently on Windows, a PathBuf can hold weird non-UTF chars but they will not
        // be stored in a config file properly in any event, so this code will lossily strip them out.
        ClientUserToken {
            user: user.into(),
            password: None,
            cert_path: Some(cert_path.to_string_lossy().to_string()),
            private_key_path: Some(private_key_path.to_string_lossy().to_string()),
        }
    }

    /// Test if the token, i.e. that it has a name, and either a password OR a cert path and key path.
    /// The paths are not validated.
    pub fn is_valid(&self) -> bool {
        let mut valid = true;
        if self.user.is_empty() {
            error!("User token has an empty name.");
            valid = false;
        }
        // A token must properly represent one kind of token or it is not valid
        if self.password.is_some() {
            if self.cert_path.is_some() || self.private_key_path.is_some() {
                error!(
                    "User token {} holds a password and certificate info - it cannot be both.",
                    self.user
                );
                valid = false;
            }
        } else {
            if self.cert_path.is_none() && self.private_key_path.is_none() {
                error!(
                    "User token {} fails to provide a password or certificate info.",
                    self.user
                );
                valid = false;
            } else if self.cert_path.is_none() || self.private_key_path.is_none() {
                error!("User token {} fails to provide both a certificate path and a private key path.", self.user);
                valid = false;
            }
        }
        valid
    }
}

/// Describes an endpoint, it's url security policy, mode and user token
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ClientEndpoint {
    /// Endpoint path
    pub url: String,
    /// Security policy
    pub security_policy: String,
    /// Security mode
    pub security_mode: String,
    /// User id to use with the endpoint
    #[serde(default = "ClientEndpoint::anonymous_id")]
    pub user_token_id: String,
}

impl ClientEndpoint {
    /// Makes a client endpoint
    pub fn new<T>(url: T) -> Self
    where
        T: Into<String>,
    {
        ClientEndpoint {
            url: url.into(),
            security_policy: SecurityPolicy::None.to_str().into(),
            security_mode: MessageSecurityMode::None.into(),
            user_token_id: Self::anonymous_id(),
        }
    }

    fn anonymous_id() -> String {
        ANONYMOUS_USER_TOKEN_ID.to_string()
    }

    // Returns the security policy
    pub fn security_policy(&self) -> SecurityPolicy {
        SecurityPolicy::from_str(&self.security_policy).unwrap()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DecodingOptions {
    /// Maximum size of a message chunk in bytes. 0 means no limit
    pub(crate) max_message_size: usize,
    /// Maximum number of chunks in a message. 0 means no limit
    pub(crate) max_chunk_count: usize,
    /// Maximum size of each individual sent message chunk.
    pub(crate) max_chunk_size: usize,
    /// Maximum size of each received chunk.
    pub(crate) max_incoming_chunk_size: usize,
    /// Maximum length in bytes (not chars!) of a string. 0 actually means 0, i.e. no string permitted
    pub(crate) max_string_length: usize,
    /// Maximum length in bytes of a byte string. 0 actually means 0, i.e. no byte string permitted
    pub(crate) max_byte_string_length: usize,
    /// Maximum number of array elements. 0 actually means 0, i.e. no array permitted
    pub(crate) max_array_length: usize,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Performance {
    /// Ignore clock skew allows the client to make a successful connection to the server, even
    /// when the client and server clocks are out of sync.
    pub(crate) ignore_clock_skew: bool,
    /// Maximum number of monitored items per request when recreating subscriptions on session recreation.
    pub(crate) recreate_monitored_items_chunk: usize,
    /// Maximum number of inflight messages.
    pub(crate) max_inflight_messages: usize,
}

/// Client OPC UA configuration
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    /// Name of the application that the client presents itself as to the server
    pub(crate) application_name: String,
    /// The application uri
    pub(crate) application_uri: String,
    /// Product uri
    pub(crate) product_uri: String,
    /// Autocreates public / private keypair if they don't exist. For testing/samples only
    /// since you do not have control of the values
    pub(crate) create_sample_keypair: bool,
    /// Custom certificate path, to be used instead of the default .der certificate path
    pub(crate) certificate_path: Option<PathBuf>,
    /// Custom private key path, to be used instead of the default private key path
    pub(crate) private_key_path: Option<PathBuf>,
    /// Auto trusts server certificates. For testing/samples only unless you're sure what you're
    /// doing.
    pub(crate) trust_server_certs: bool,
    /// Verify server certificates. For testing/samples only unless you're sure what you're
    /// doing.
    pub(crate) verify_server_certs: bool,
    /// PKI folder, either absolute or relative to executable
    pub(crate) pki_dir: PathBuf,
    /// Preferred locales
    pub(crate) preferred_locales: Vec<String>,
    /// Identifier of the default endpoint
    pub(crate) default_endpoint: String,
    /// User tokens
    pub(crate) user_tokens: BTreeMap<String, ClientUserToken>,
    /// List of end points
    pub(crate) endpoints: BTreeMap<String, ClientEndpoint>,
    /// Decoding options used for serialization / deserialization
    pub(crate) decoding_options: DecodingOptions,
    
    /// Interval between each keep-alive request sent to the server.
    pub(crate) keep_alive_interval: Duration,

    /// Timeout for each request sent to the server.
    pub(crate) request_timeout: Duration,
    /// Timeout for publish requests, separate from normal timeout since
    /// subscriptions are often more time sensitive.
    pub(crate) publish_timeout: Duration,
    /// Minimum publish interval. Setting this higher will make sure that subscriptions
    /// publish together, which may reduce the number of publish requests if you have a lot of subscriptions.
    pub(crate) min_publish_interval: Duration,
    /// Maximum number of inflight publish requests before further requests are skipped.
    pub(crate) max_inflight_publish: usize,

    /// Requested session timeout in milliseconds
    pub(crate) session_timeout: u32,

    /// Client performance settings
    pub(crate) performance: Performance,
    /// Session name
    pub(crate) session_name: String,
}

impl ClientConfig {
    pub(crate) fn application_description(&self) -> ApplicationDescription {
        ApplicationDescription {
            application_uri: self.application_uri(),
            application_name: LocalizedText::new("", self.application_name().as_ref()),
            application_type: self.application_type(),
            product_uri: self.product_uri(),
            gateway_server_uri: UAString::null(),
            discovery_profile_uri: UAString::null(),
            discovery_urls: None,
        }
    }

    fn application_name(&self) -> UAString {
        UAString::from(&self.application_name)
    }

    fn application_uri(&self) -> UAString {
        UAString::from(&self.application_uri)
    }

    fn product_uri(&self) -> UAString {
        UAString::from(&self.product_uri)
    }

    fn application_type(&self) -> ApplicationType {
        ApplicationType::Client
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new("", "")
    }
}

impl ClientConfig {
    /// The default PKI directory
    pub const PKI_DIR: &'static str = "pki";

    pub fn new(application_name: impl Into<String>, application_uri: impl Into<String>) -> Self {
        let mut pki_dir = std::env::current_dir().unwrap();
        pki_dir.push(Self::PKI_DIR);

        let decoding_options = crate::types::DecodingOptions::default();
        ClientConfig {
            application_name: application_name.into(),
            application_uri: application_uri.into(),
            create_sample_keypair: false,
            certificate_path: None,
            private_key_path: None,
            trust_server_certs: false,
            verify_server_certs: true,
            product_uri: String::new(),
            pki_dir,
            preferred_locales: Vec::new(),
            default_endpoint: String::new(),
            user_tokens: BTreeMap::new(),
            endpoints: BTreeMap::new(),
            keep_alive_interval: Duration::from_secs(10),
            request_timeout: Duration::from_secs(60),
            min_publish_interval: Duration::from_secs(1),
            publish_timeout: Duration::from_secs(60),
            max_inflight_publish: 2,
            session_timeout: 0,
            decoding_options: DecodingOptions {
                max_array_length: decoding_options.max_array_length,
                max_string_length: decoding_options.max_string_length,
                max_byte_string_length: decoding_options.max_byte_string_length,
                max_chunk_count: decoding_options.max_chunk_count,
                max_message_size: decoding_options.max_message_size,
                max_chunk_size: 65535,
                max_incoming_chunk_size: 65535,
            },
            performance: Performance {
                ignore_clock_skew: false,
                recreate_monitored_items_chunk: 1000,
                max_inflight_messages: 20,
            },
            session_name: "Rust OPC UA Client".into(),
        }
    }
}