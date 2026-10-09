//! A minimal in-memory apiserver for one namespace, driving the real
//! `kube::Client` request path (`tower::service_fn`, as `cluster.rs` tests do).
//!
//! It models only what the Sandbox paths touch: Sandbox CRUD with
//! resourceVersion CAS and delete preconditions, a controller that creates
//! the Sandbox's Pod while it is Running, Pods whose agent container runs once
//! their `envFrom` Secret exists, and Secret create with failure injection.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use http::{Method, Request, Response};
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::client::Body;
use kube::Client;
use serde_json::{json, Value};

#[derive(Default)]
pub struct State {
    pub sandboxes: BTreeMap<String, Value>,
    pub secrets: BTreeMap<String, Value>,
    /// Pod UIDs by name; Pods are rendered from their Sandbox on read.
    pub pods: BTreeMap<String, String>,
    pub rv: u64,
    pub uid: u64,
    /// Fail this many Secret creates with a 500.
    pub fail_secret_creates: usize,
    /// Answer this many Sandbox patches with a 409 Conflict.
    pub conflict_patches: usize,
    /// Every merge patch body received.
    pub patches: Vec<Value>,
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

fn ok(value: &Value) -> Response<Body> {
    Response::builder()
        .status(200)
        .body(Body::from(serde_json::to_vec(value).unwrap()))
        .unwrap()
}

fn merge(target: &mut Value, patch: &Value) {
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

impl State {
    fn next_rv(&mut self) -> String {
        self.rv += 1;
        self.rv.to_string()
    }

    fn next_uid(&mut self, kind: &str) -> String {
        self.uid += 1;
        format!("{kind}-uid-{}", self.uid)
    }

    /// The controller: a Running Sandbox has a Pod; Suspended has none.
    fn reconcile(&mut self, name: &str) {
        let running = self
            .sandboxes
            .get(name)
            .is_some_and(|s| s["spec"]["operatingMode"] == "Running");
        if running && !self.pods.contains_key(name) {
            let uid = self.next_uid("pod");
            self.pods.insert(name.into(), uid);
        } else if !running {
            self.pods.remove(name);
        }
    }

    fn pod(&self, name: &str) -> Option<Value> {
        let uid = self.pods.get(name)?;
        let sandbox = self.sandboxes.get(name)?;
        let template = &sandbox["spec"]["podTemplate"];
        let secret = template["spec"]["containers"][0]["envFrom"][0]["secretRef"]["name"]
            .as_str()
            .unwrap_or_default();
        // `envFrom` names the generation's Secret; the container starts once
        // it exists.
        let started = self.secrets.contains_key(secret);
        let state = if started {
            json!({"running": {}})
        } else {
            json!({"waiting": {"reason": "CreateContainerConfigError"}})
        };
        Some(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": name, "uid": uid,
                "labels": template["metadata"]["labels"],
                "annotations": template["metadata"]["annotations"],
                "ownerReferences": [{"apiVersion": "agents.x-k8s.io/v1beta1",
                    "kind": "Sandbox", "name": name,
                    "uid": sandbox["metadata"]["uid"], "controller": true}],
            },
            "status": {"phase": "Pending", "containerStatuses": [{
                "name": "agent", "image": "i", "imageID": "", "ready": started,
                "restartCount": 0, "state": state}]},
        }))
    }

    fn handle(&mut self, method: &Method, path: &str, body: &[u8]) -> Response<Body> {
        let path = path.split('?').next().unwrap_or_default();
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        match segments.as_slice() {
            ["apis", "agents.x-k8s.io", "v1beta1", "namespaces", _, "sandboxes", rest @ ..] => {
                self.sandbox(method, rest.first().copied(), body)
            }
            ["api", "v1", "namespaces", _, "pods"] => {
                let names: Vec<String> = self.pods.keys().cloned().collect();
                let items: Vec<Value> = names.iter().filter_map(|n| self.pod(n)).collect();
                ok(&json!({"apiVersion":"v1","kind":"PodList","metadata":{},"items":items}))
            }
            ["api", "v1", "namespaces", _, "pods", name] => match self.pod(name) {
                Some(pod) => ok(&pod),
                None => status(404, "NotFound"),
            },
            ["api", "v1", "namespaces", _, "secrets", name] => match self.secrets.get(*name) {
                Some(secret) => ok(secret),
                None => status(404, "NotFound"),
            },
            ["api", "v1", "namespaces", _, "secrets"] if method == Method::POST => {
                if self.fail_secret_creates > 0 {
                    self.fail_secret_creates -= 1;
                    return status(500, "InternalError");
                }
                let mut secret: Secret = serde_json::from_slice(body).unwrap();
                let name = secret.metadata.name.clone().unwrap();
                if self.secrets.contains_key(&name) {
                    return status(409, "AlreadyExists");
                }
                secret.data = secret.string_data.take().map(|data| {
                    data.into_iter()
                        .map(|(k, v)| (k, ByteString(v.into_bytes())))
                        .collect()
                });
                secret.metadata.resource_version = Some(self.next_rv());
                let stored = serde_json::to_value(&secret).unwrap();
                self.secrets.insert(name, stored.clone());
                ok(&stored)
            }
            _ => status(404, "NotFound"),
        }
    }

    fn sandbox(&mut self, method: &Method, name: Option<&str>, body: &[u8]) -> Response<Body> {
        match (method.clone(), name) {
            (Method::GET, Some(name)) => match self.sandboxes.get(name) {
                Some(sandbox) => ok(sandbox),
                None => status(404, "NotFound"),
            },
            (Method::POST, None) => {
                let mut sandbox: Value = serde_json::from_slice(body).unwrap();
                let name = sandbox["metadata"]["name"].as_str().unwrap().to_string();
                if self.sandboxes.contains_key(&name) {
                    return status(409, "AlreadyExists");
                }
                sandbox["metadata"]["uid"] = self.next_uid("sandbox").into();
                sandbox["metadata"]["resourceVersion"] = self.next_rv().into();
                self.sandboxes.insert(name.clone(), sandbox.clone());
                self.reconcile(&name);
                ok(&sandbox)
            }
            (Method::PUT, Some(name)) => {
                let mut sandbox: Value = serde_json::from_slice(body).unwrap();
                let Some(current) = self.sandboxes.get(name) else {
                    return status(404, "NotFound");
                };
                if current["metadata"]["resourceVersion"] != sandbox["metadata"]["resourceVersion"]
                {
                    return status(409, "Conflict");
                }
                sandbox["metadata"]["resourceVersion"] = self.next_rv().into();
                self.sandboxes.insert(name.into(), sandbox.clone());
                self.reconcile(name);
                ok(&sandbox)
            }
            (Method::PATCH, Some(name)) => {
                let patch: Value = serde_json::from_slice(body).unwrap();
                self.patches.push(patch.clone());
                if self.conflict_patches > 0 {
                    self.conflict_patches -= 1;
                    return status(409, "Conflict");
                }
                let rv = self.next_rv();
                let Some(current) = self.sandboxes.get_mut(name) else {
                    return status(404, "NotFound");
                };
                let expected = &patch["metadata"]["resourceVersion"];
                if !expected.is_null() && *expected != current["metadata"]["resourceVersion"] {
                    return status(409, "Conflict");
                }
                merge(current, &patch);
                current["metadata"]["resourceVersion"] = rv.into();
                let updated = current.clone();
                self.reconcile(name);
                ok(&updated)
            }
            (Method::DELETE, Some(name)) => {
                let options: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                let Some(current) = self.sandboxes.get(name) else {
                    return status(404, "NotFound");
                };
                let pre = &options["preconditions"];
                for (field, key) in [("uid", "uid"), ("resourceVersion", "resourceVersion")] {
                    if !pre[field].is_null() && pre[field] != current["metadata"][key] {
                        return status(409, "Conflict");
                    }
                }
                let removed = self.sandboxes.remove(name).unwrap();
                self.pods.remove(name);
                ok(&removed)
            }
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
