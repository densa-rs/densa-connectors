//! What any read of a Flight service takes: where to connect, which locations an endpoint may
//! be read from, which headers go where, and what failed calls say.

use std::error::Error;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightClient, Location};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Code;

use crate::RequestError;

/// How long opening a connection may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// HTTP/2 pings on a quiet connection, and how long one may go unanswered: a peer that
/// vanished is noticed even while a call waits for rows.
const KEEP_ALIVE: Duration = Duration::from_secs(20);
/// The location that means "the connection this came from" (Flight 15+).
const REUSE_CONNECTION: &str = "arrow-flight-reuse-connection:";

/// Where to connect: an `http://` or `https://` URI for tonic.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub(crate) uri: String,
    pub(crate) tls: bool,
    /// Host and port, lowercase: whether two targets are one service.
    pub(crate) authority: String,
}

impl Target {
    /// A Flight URL (`grpc://`, `grpc+tcp://`, `grpc+tls://`, `http://`, `https://`) with a
    /// host and no credentials.
    pub(crate) fn parse(url: &str) -> Result<Self, String> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| format!("{url} has no scheme"))?;
        let tls = match scheme.to_ascii_lowercase().as_str() {
            "grpc" | "grpc+tcp" | "http" => false,
            "grpc+tls" | "https" => true,
            _ => {
                return Err(format!(
                    "{url} is not grpc://, grpc+tls://, http:// or https://"
                ))
            }
        };
        let uri = format!("{}://{rest}", if tls { "https" } else { "http" });
        let parsed: Uri = uri.parse().map_err(|error| format!("{url}: {error}"))?;
        let authority = parsed.authority().map(|a| a.as_str()).unwrap_or_default();
        if authority.contains('@') {
            // A URL ends up in messages: credentials go in headers.
            return Err("a Flight URL cannot hold credentials: give them as a header".to_owned());
        }
        let host = parsed.host().unwrap_or_default();
        if host.is_empty() {
            return Err(format!("{url} has no host"));
        }
        let port = parsed.port_u16().unwrap_or(if tls { 443 } else { 80 });
        Ok(Self {
            authority: format!("{}:{port}", host.to_ascii_lowercase()),
            uri,
            tls,
        })
    }

    /// The target of a service's URL, or why it cannot be read.
    pub(crate) fn origin(url: &str) -> Result<Self, RequestError> {
        let origin = Self::parse(url).map_err(RequestError::InvalidUrl)?;
        if origin.tls && cfg!(not(feature = "tls")) {
            return Err(RequestError::TlsDisabled);
        }
        Ok(origin)
    }

    /// Whether data from a service at `origin` may be read here: not in the clear after TLS,
    /// and not over TLS without the `tls` feature. Why not, if not.
    pub(crate) fn refused_after(&self, origin: &Target) -> Option<String> {
        if origin.tls && !self.tls {
            // Data meant to be private does not go on in the clear.
            Some(format!("{} is not TLS, unlike {}", self.uri, origin.uri))
        } else if self.tls && cfg!(not(feature = "tls")) {
            Some(format!("{} needs the tls feature", self.uri))
        } else {
            None
        }
    }

    /// How to open a connection to the target, or why it cannot be (fatal).
    pub(crate) fn endpoint(&self) -> Result<Endpoint, String> {
        let endpoint = Endpoint::from_shared(self.uri.clone())
            .map_err(|error| describe(&self.uri, &error))?
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(KEEP_ALIVE)
            .keep_alive_timeout(KEEP_ALIVE)
            .keep_alive_while_idle(true);
        secure(endpoint, self)
    }
}

/// Where an endpoint's data can be read.
pub(crate) enum Place {
    /// The service the request went to.
    Origin,
    /// Another service.
    At(Target),
}

/// The places an endpoint names, in the order given, or why it names none that can be used.
pub(crate) fn places(locations: &[Location]) -> Result<Vec<Place>, String> {
    if locations.is_empty() {
        return Ok(vec![Place::Origin]);
    }
    let mut places = Vec::new();
    let mut unusable = String::new();
    for location in locations {
        let reuse = location
            .uri
            .get(..REUSE_CONNECTION.len())
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case(REUSE_CONNECTION));
        if reuse {
            places.push(Place::Origin);
            continue;
        }
        match Target::parse(&location.uri) {
            Ok(target) => places.push(Place::At(target)),
            Err(reason) => unusable = reason,
        }
    }
    if places.is_empty() {
        return Err(format!("no usable location for an endpoint ({unusable})"));
    }
    Ok(places)
}

pub(crate) type Headers = Vec<(AsciiMetadataKey, AsciiMetadataValue)>;

pub(crate) fn headers(given: &[(String, String)]) -> Result<Headers, RequestError> {
    given
        .iter()
        .map(|(name, value)| {
            let key = AsciiMetadataKey::from_bytes(name.as_bytes())
                .map_err(|error| RequestError::InvalidHeader(format!("{name:?}: {error}")))?;
            // The value stays out of the message: it is often a secret.
            let value = AsciiMetadataValue::try_from(value.as_str()).map_err(|error| {
                RequestError::InvalidHeader(format!("the value of {name}: {error}"))
            })?;
            Ok((key, value))
        })
        .collect()
}

/// A client of the service at `target` over `channel`. The headers go only to the service the
/// request was made to (`origin`): a location elsewhere does not get its tokens.
pub(crate) fn client(
    channel: Channel,
    target: &Target,
    origin: &Target,
    headers: &Headers,
    max_message: usize,
) -> FlightClient {
    let inner = FlightServiceClient::new(channel).max_decoding_message_size(max_message);
    let mut client = FlightClient::new_from_inner(inner);
    if target.authority == origin.authority {
        for (key, value) in headers {
            client.metadata_mut().insert(key.clone(), value.clone());
        }
    }
    client
}

/// The descriptor `GetFlightInfo` takes for a Flight SQL query.
#[cfg(feature = "sql")]
pub(crate) fn statement(query: &str) -> arrow_flight::FlightDescriptor {
    use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
    use prost::Message;
    let command = CommandStatementQuery {
        query: query.to_owned(),
        transaction_id: None,
    };
    arrow_flight::FlightDescriptor::new_cmd(command.as_any().encode_to_vec())
}

/// `endpoint` with TLS when `target` asks for it.
fn secure(endpoint: Endpoint, target: &Target) -> Result<Endpoint, String> {
    if !target.tls {
        return Ok(endpoint);
    }
    #[cfg(feature = "tls")]
    {
        let config = tonic::transport::ClientTlsConfig::new().with_webpki_roots();
        endpoint
            .tls_config(config)
            .map_err(|error| describe("TLS", &error))
    }
    #[cfg(not(feature = "tls"))]
    {
        Err(format!("{} needs the tls feature", target.uri))
    }
}

/// Whether a call that ended with `code` may succeed when made again. A message over the
/// size limit is `OutOfRange` (tonic's own check): it would be over again.
pub(crate) fn passing(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Aborted
            | Code::Cancelled
            | Code::Unknown
            | Code::Internal
            | Code::ResourceExhausted
    )
}

/// `what`, then `error` and every error under it (tonic's own messages say little).
pub(crate) fn describe(what: &str, error: &dyn Error) -> String {
    let mut text = format!("{what}: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.ends_with(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

// TLS targets are refused outright without the `tls` feature.
#[cfg(all(test, feature = "tls"))]
mod tests {
    use super::Target;

    #[test]
    fn data_from_a_tls_service_is_not_read_in_the_clear() {
        let private = Target::parse("grpc+tls://flight.example.com").unwrap();
        let open = Target::parse("grpc://flight.example.com:8815").unwrap();
        let refused = open.refused_after(&private).unwrap();
        assert_eq!(
            refused,
            "http://flight.example.com:8815 is not TLS, unlike https://flight.example.com"
        );
        assert_eq!(private.refused_after(&private), None);
        assert_eq!(open.refused_after(&open), None);
        assert_eq!(private.refused_after(&open), None, "more private is fine");
    }
}
