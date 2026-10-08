# Pi adapter integration

Buzz uses the [buzz-pi-acp fork](https://github.com/salman1993/buzz-pi-acp).
Install Node.js 22 or newer, configure Pi, and install the latest development
adapter from the fork's `main` branch:

```sh
npm install -g @earendil-works/pi-coding-agent
pi
npm install -g --install-links=true 'git+https://github.com/salman1993/buzz-pi-acp.git#main'
```

This test setup intentionally tracks `main`; the Desktop runtime catalog pins a
reviewed adapter revision for users.

Make sure `pi` and `buzz-pi-acp` are on PATH, then restart Buzz.

## Tests

```sh
cargo test -p buzz-acp
```

The package suite also checks owner-only thread routing, ACP full-access negotiation,
redacted JSONL activity, and checkpoint failure propagation. Activity tests use the
real ACP client with a synthetic adapter and assert that test secrets never enter
the platform stream. Checkpoint tests cover validated receipts, command failures,
invalid and oversized output, missing helpers, and timeouts.
The image repository's `buzz-agent/test-checkpoint-storage.py` also stops the
packaged harness during startup while an adapter writes files. It checks that
recovery preserves the final edit and rejected uploads make shutdown fail.

Run the controller lifecycle test against the isolated local kind cluster:

```sh
BUZZ_SANDBOX_TEST_KUBECONFIG=/absolute/kind.kubeconfig \
BUZZ_SANDBOX_TEST_IMAGE=registry.example/uid-guard-fixture@sha256:<digest> \
  cargo test -p buzz-backend-kubernetes live_controller_reuses_scopes \
  -- --ignored --nocapture
```

The test requires context `kind-remote-agent-check`, the Agent Sandbox controller,
and a loaded fixture image that checks the Pod UID binding before sleeping.
It verifies concurrent reuse, separate threads, owner and relay refusals, and
refusal after the original Pod is replaced. It does not enroll real accounts.

Managed agent sessions may already export harness options. Clear them when
running the package suite: three CLI parsing tests assert the unset defaults,
and inherited values would change the inputs those tests exercise. Running the
package serially also avoids scheduling flakes in existing short-deadline tests.

```sh
env -u BUZZ_ACP_ALLOWED_RESPOND_TO \
  -u BUZZ_ACP_LAZY_POOL \
  -u BUZZ_ACP_IDLE_POOL_SLEEP \
  cargo test -p buzz-acp -- --test-threads=1
```

Run the ignored real-adapter test with a built fork checkout:

```sh
BUZZ_TEST_PI_ACP=/absolute/buzz-pi-acp/dist/index.js \
  cargo test -p buzz-acp real_pi_preserves -- --ignored
```

## Git bootstrap

`cargo test -p buzz-acp --test git_bootstrap` starts the actual harness with a
probe adapter, runs real signed commits/tags and scoped credential resolution,
and verifies key cleanup on startup failure and SIGTERM. No relay is contacted.

To exercise the real runtime boundaries on Unix:

```sh
cargo build -p buzz-acp -p buzz-agent -p buzz-dev-mcp
BUZZ_TEST_BIN_DIR="$PWD/target/debug" cargo test -p buzz-acp git_runtime_tests -- --ignored --nocapture
```

The Buzz Agent test uses a deterministic local OpenAI-compatible response to
invoke the actual MCP shell. The Goose test requires an installed, configured
Goose and uses its provider to invoke the native developer shell. Both operate
only on temporary local repositories, verify commit/tag signatures and identity,
check unrelated-remote credential scoping, and assert keyfile removal. They do
not replace authenticated relay clone/push/readback testing.
