//! A minimal in-memory apiserver for one namespace, driving the real
//! `kube::Client` request path (`tower::service_fn`, as the provider's
//! `cluster.rs` tests do). It models only what the reconciler touches and
//! records every request so tests can assert what was (never) written.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use http::{Method, Request, Response};
use kube::client::Body;
use kube::Client;
use serde_json::{json, Value};

/// One recorded request.
#[derive(Debug, Clone)]
pub struct Call {
    pub method: Method,
    pub path: String,
    pub body: Value,
}

pub struct State {
    /// Apiserver clock, served as the `Date` header.
    pub now: DateTime<Utc>,
    /// Omit the `Date` header.
    pub no_date: bool,
    pub sandboxes: BTreeMap<String, Value>,
    pub pods: BTreeMap<String, Value>,
    pub rv: u64,
    /// Applied to the named Sandbox (bumping its resourceVersion) just
    /// before the next PATCH or DELETE is evaluated: a concurrent writer.
    pub race: Option<(String, Value)>,
    pub calls: Vec<Call>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            now: DateTime::from_timestamp(1_800_000_000, 0).unwrap(),
            no_date: false,
            sandboxes: BTreeMap::new(),
            pods: BTreeMap::new(),
            rv: 100,
            race: None,
            calls: Vec::new(),
        }
    }
}

pub type Shared = Arc<Mutex<State>>;

fn status(code: u16, reason: &str) -> Response<Body> {
    let body = json!({"kind":"Status","apiVersion":"v1","status":"Failure",
        "message":reason,"reason":reason,"code":code});
    Response::builder()
        .status(code)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

/// JSON merge patch (RFC 7386).
pub fn merge(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                if value.is_null() {
                    target.remove(key);
                } else {
                    merge(target.entry(key.clone()).or_insert(Value::Null), value);
                }
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

fn matches_selector(object: &Value, query: &str) -> bool {
    let Some(selector) = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("labelSelector="))
    else {
        return true;
    };
    let selector = selector
        .replace("%3D", "=")
        .replace("%2C", ",")
        .replace("%2F", "/");
    selector.split(',').all(|term| {
        let (key, value) = term.split_once('=').unwrap_or((term, ""));
        object["metadata"]["labels"][key] == value
    })
}

impl State {
    fn ok(&self, value: &Value) -> Response<Body> {
        let mut response = Response::builder().status(200);
        if !self.no_date {
            response = response.header(
                http::header::DATE,
                self.now.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            );
        }
        response
            .body(Body::from(serde_json::to_vec(value).unwrap()))
            .unwrap()
    }

    fn list(&self, items: &BTreeMap<String, Value>, query: &str) -> Response<Body> {
        let items: Vec<&Value> = items
            .values()
            .filter(|o| matches_selector(o, query))
            .collect();
        self.ok(&json!({"apiVersion":"v1","kind":"List",
            "metadata":{"resourceVersion": self.rv.to_string()},"items":items}))
    }

    fn apply_race(&mut self) {
        if let Some((name, mutation)) = self.race.take() {
            self.rv += 1;
            let rv = self.rv.to_string();
            if let Some(current) = self.sandboxes.get_mut(&name) {
                merge(current, &mutation);
                current["metadata"]["resourceVersion"] = rv.into();
            }
        }
    }

    fn handle(&mut self, method: &Method, uri: &str, body: &[u8]) -> Response<Body> {
        let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
        let body: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        self.calls.push(Call {
            method: method.clone(),
            path: path.to_string(),
            body: body.clone(),
        });
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        match (method.clone(), segments.as_slice()) {
            (Method::GET, ["apis", "agents.x-k8s.io", "v1beta1", "namespaces", _, "sandboxes"]) => {
                self.list(&self.sandboxes, query)
            }
            (
                Method::GET,
                ["apis", "agents.x-k8s.io", "v1beta1", "namespaces", _, "sandboxes", name],
            ) => match self.sandboxes.get(*name) {
                Some(sandbox) => self.ok(sandbox),
                None => status(404, "NotFound"),
            },
            (
                Method::PATCH,
                ["apis", "agents.x-k8s.io", "v1beta1", "namespaces", _, "sandboxes", name],
            ) => {
                self.apply_race();
                self.rv += 1;
                let rv = self.rv.to_string();
                let Some(current) = self.sandboxes.get_mut(*name) else {
                    return status(404, "NotFound");
                };
                let expected = &body["metadata"]["resourceVersion"];
                if !expected.is_null() && *expected != current["metadata"]["resourceVersion"] {
                    return status(409, "Conflict");
                }
                merge(current, &body);
                current["metadata"]["resourceVersion"] = rv.into();
                let updated = current.clone();
                self.ok(&updated)
            }
            (
                Method::DELETE,
                ["apis", "agents.x-k8s.io", "v1beta1", "namespaces", _, "sandboxes", name],
            ) => {
                self.apply_race();
                let Some(current) = self.sandboxes.get(*name) else {
                    return status(404, "NotFound");
                };
                let pre = &body["preconditions"];
                for field in ["uid", "resourceVersion"] {
                    if pre[field].is_null() || pre[field] != current["metadata"][field] {
                        return status(409, "Conflict");
                    }
                }
                let removed = self.sandboxes.remove(*name).unwrap();
                self.ok(&removed)
            }
            (Method::GET, ["api", "v1", "namespaces", _, "pods"]) => self.list(&self.pods, query),
            (Method::GET, ["api", "v1", "namespaces", _, "pods", name]) => {
                match self.pods.get(*name) {
                    Some(pod) => self.ok(pod),
                    None => status(404, "NotFound"),
                }
            }
            (Method::POST, ["api", "v1", "namespaces", _, "events"]) => self.ok(&body),
            _ => status(405, "MethodNotAllowed"),
        }
    }
}

/// A client backed by `state`.
pub fn client(state: &Shared) -> Client {
    let state = Arc::clone(state);
    let service = tower::service_fn(move |request: Request<Body>| {
        let state = Arc::clone(&state);
        async move {
            let (parts, body) = request.into_parts();
            let bytes = http_body_util::BodyExt::collect(body)
                .await
                .map(|b| b.to_bytes().to_vec())
                .unwrap_or_default();
            let response =
                state
                    .lock()
                    .unwrap()
                    .handle(&parts.method, &parts.uri.to_string(), &bytes);
            Ok::<_, std::convert::Infallible>(response)
        }
    });
    Client::new(service, "agents")
}
