// Copyright 2026 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use azure_core::{ClientOptions, Context, Policy, PolicyResult, Request, TransportOptions};

/// Replace the legacy SDK's credential-logging transport and redact the
/// signed request before retry logging (RUSTSEC-2026-0275). Authentication,
/// retries, timeouts and the HTTP client remain SDK-owned.
pub(crate) fn client_options() -> ClientOptions {
    client_options_with_transport(TransportOptions::default())
}

fn client_options_with_transport(transport: TransportOptions) -> ClientOptions {
    ClientOptions::new(transport.clone()).per_call_policies(vec![
        Arc::new(CredentialLoggingPolicy {
            transport: Arc::new(RedactingTransport(transport)),
        }) as Arc<dyn Policy>,
    ])
}

#[derive(Debug)]
struct CredentialLoggingPolicy {
    transport: Arc<dyn Policy>,
}

#[async_trait::async_trait]
impl Policy for CredentialLoggingPolicy {
    async fn send(
        &self,
        context: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        let mut next = next.to_vec();
        let last = next.last_mut().ok_or_else(|| {
            azure_core::Error::message(
                azure_core::error::ErrorKind::Other,
                "Azure request pipeline has no transport",
            )
        })?;
        *last = self.transport.clone();
        next[0].send(context, request, &next[1..]).await
    }
}

#[derive(Debug)]
struct RedactingTransport(TransportOptions);

#[async_trait::async_trait]
impl Policy for RedactingTransport {
    async fn send(
        &self,
        context: &Context,
        request: &mut Request,
        _next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        let request = SignedRequest(request);
        let response = self.0.send(context, request.0).await?;
        let listing = request
            .0
            .url()
            .query_pairs()
            .any(|(key, value)| key == "comp" && value == "list");
        let xml_error = !response.status().is_success()
            && response
                .headers()
                .get_optional_str(&azure_core::headers::CONTENT_TYPE)
                == Some("application/xml");
        if xml_error || (response.status().is_success() && listing) {
            let (status, headers, body) = response.deconstruct();
            Ok(azure_core::Response::new(
                status,
                headers,
                Box::pin(futures::stream::once(async move {
                    let body = body.collect().await?;
                    validate_service_xml(&body)?;
                    Ok(body)
                })),
            ))
        } else {
            // Blob downloads may contain arbitrary user XML and must remain streaming.
            Ok(response)
        }
    }
}

fn validate_service_xml(body: &[u8]) -> azure_core::Result<()> {
    use quick_xml::events::Event;

    // Bound fixed-schema service metadata before the legacy SDK's quadratic
    // duplicate-attribute checks. Do not apply this limit to blob contents.
    let mut reader = quick_xml::Reader::from_reader(body);
    loop {
        match reader.read_event() {
            Ok(Event::Start(element) | Event::Empty(element)) => {
                for (index, attribute) in element.attributes().enumerate() {
                    if index >= 256 {
                        return Err(azure_core::Error::message(
                            azure_core::error::ErrorKind::DataConversion,
                            "Azure service XML exceeds 256 attributes per element",
                        ));
                    }
                    attribute.map_err(|error| {
                        azure_core::Error::new(azure_core::error::ErrorKind::DataConversion, error)
                    })?;
                }
            }
            Ok(Event::Eof) => return Ok(()),
            Ok(_) => {}
            Err(error) => {
                return Err(azure_core::Error::new(
                    azure_core::error::ErrorKind::DataConversion,
                    error,
                ));
            }
        }
    }
}

struct SignedRequest<'a>(&'a mut Request);

impl Drop for SignedRequest<'_> {
    fn drop(&mut self) {
        // Runs on cancellation too. The SDK re-signs every retry.
        if self
            .0
            .headers()
            .get_optional_str(&azure_core::headers::AUTHORIZATION)
            .is_some()
        {
            self.0
                .insert_header(azure_core::headers::AUTHORIZATION, "<redacted>");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::{headers::Headers, HttpClient, Method, Response, StatusCode};
    use azure_storage::{clients::ServiceType, StorageCredentials};
    use futures::FutureExt;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };

    #[derive(Debug, Default)]
    struct Transport {
        authorization: Mutex<Option<String>>,
        pending: bool,
        body: Vec<u8>,
        body_polled: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl HttpClient for Transport {
        async fn execute_request(&self, request: &Request) -> azure_core::Result<Response> {
            *self.authorization.lock().unwrap() = Some(
                request
                    .headers()
                    .get_str(&azure_core::headers::AUTHORIZATION)?
                    .to_owned(),
            );
            if self.pending {
                futures::future::pending::<()>().await;
            }
            let mut headers = Headers::new();
            headers.insert(azure_core::headers::CONTENT_TYPE, "application/xml");
            let body = self.body.clone();
            let body_polled = self.body_polled.clone();
            Ok(Response::new(
                StatusCode::Ok,
                headers,
                Box::pin(futures::stream::once(async move {
                    body_polled.store(true, Ordering::SeqCst);
                    Ok(body.into())
                })),
            ))
        }
    }

    #[tokio::test]
    async fn request_authentication_is_delivered_then_redacted_on_return_or_cancellation() {
        for pending in [false, true] {
            let transport = Arc::new(Transport {
                pending,
                ..Default::default()
            });
            let options = client_options_with_transport(TransportOptions::new(transport.clone()));
            let credentials = StorageCredentials::access_key("testaccount", "dGVzdC1vbmx5LWtleQ==");
            let pipeline = azure_storage::clients::new_pipeline_from_options(options, credentials);
            let mut request = Request::new(
                "https://testaccount.blob.core.windows.net/items"
                    .parse()
                    .unwrap(),
                Method::Get,
            );
            request.insert_header(
                azure_core::headers::MS_DATE,
                "Thu, 29 Oct 2026 00:00:00 GMT",
            );
            let mut context = Context::new();
            context.insert(ServiceType::Blob);
            let result = pipeline.send(&context, &mut request).now_or_never();
            if pending {
                assert!(result.is_none());
            } else {
                assert!(result.unwrap().is_ok());
            }
            assert!(transport
                .authorization
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .starts_with("SharedKey testaccount:"));
            assert_eq!(
                request
                    .headers()
                    .get_str(&azure_core::headers::AUTHORIZATION)
                    .unwrap(),
                "<redacted>"
            );
        }
    }

    #[test]
    fn service_xml_rejects_unbounded_or_duplicate_attributes() {
        let xml = |count: usize| {
            format!(
                "<EnumerationResults {} />",
                (0..count)
                    .map(|i| format!("a{i}=\"v\""))
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        };
        assert!(validate_service_xml(xml(256).as_bytes()).is_ok());
        assert!(validate_service_xml(xml(257).as_bytes()).is_err());
        assert!(validate_service_xml(b"<Error a='one' a='two'/>").is_err());
    }

    #[tokio::test]
    async fn listings_are_guarded_but_arbitrary_xml_blobs_stay_streaming() {
        let body = format!(
            "<Data {} />",
            (0..257)
                .map(|i| format!("a{i}='v'"))
                .collect::<Vec<_>>()
                .join(" ")
        )
        .into_bytes();
        for listing in [false, true] {
            let transport = Arc::new(Transport {
                body: body.clone(),
                ..Default::default()
            });
            let pipeline = azure_storage::clients::new_pipeline_from_options(
                client_options_with_transport(TransportOptions::new(transport.clone())),
                StorageCredentials::access_key("testaccount", "dGVzdC1vbmx5LWtleQ=="),
            );
            let url = if listing {
                "https://testaccount.blob.core.windows.net/items?comp=list"
            } else {
                "https://testaccount.blob.core.windows.net/items/document.xml"
            };
            let mut request = Request::new(url.parse().unwrap(), Method::Get);
            request.insert_header(
                azure_core::headers::MS_DATE,
                "Thu, 29 Oct 2026 00:00:00 GMT",
            );
            let mut context = Context::new();
            context.insert(ServiceType::Blob);
            let response = pipeline.send(&context, &mut request).await.unwrap();
            assert!(!transport.body_polled.load(Ordering::SeqCst));
            let result = response.into_body().collect().await;
            if listing {
                assert!(result.is_err());
                assert!(transport.body_polled.load(Ordering::SeqCst));
            } else {
                assert_eq!(result.unwrap().as_ref(), body.as_slice());
            }
        }
    }
}
