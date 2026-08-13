# Janus Core

Production-oriented, async Python foundations for the
[Janus WebRTC Gateway](https://janus.conf.meetecho.com/): sessions, transports,
authentication, protocol envelopes, plugin-handle lifecycle, event routing, and
session-pool ownership. The separately installable operations server contains
the FastAPI, monitoring, administration, and persistence surfaces.

Janus Core intentionally contains **no named Janus plugin implementation**.
EchoTest, VideoCall, SIP, NoSIP, AudioBridge, VideoRoom, TextRoom, and
Record&Play are independent projects in `plugins/`; Streaming lives entirely
in `plugins/janus-streaming-plugin`. Applications can install only what
they use or implement their own client on the public `Plugin` base.

> The product is named **Janus Core**. The exact `janus-core` distribution and
> `janus_core` import namespace are already occupied on PyPI by an unrelated
> project, so this project uses the publishable distribution name
> `jrtc` and retains the stable `jrtc` Python namespace.

## Requirements and installation

- Python 3.12+
- Janus Gateway with at least one enabled API transport

```bash
pip install jrtc

# Plain HTTP/HTTPS transport
pip install "jrtc[http]"

# Explicit Kafka client support for the core broker adapter
pip install "jrtc[kafka]"

# Independent FastAPI monitoring and administration service
pip install "japi[ops]"
```

The core runtime uses Pydantic, WebSockets, LogVista, Dispio, and Broka. Broka
0.0.2 currently declares its Redis, RabbitMQ, and Kafka clients as required
dependencies, even when an in-process engine is selected. HTTP transport is the
core transport extra; the Kafka extra explicitly declares the client used by
the hardened adapter instead of relying on Broka's transitive metadata.
`japi` directly depends on core but core never imports FastAPI,
Starlette, Uvicorn, asyncpg, or the server package.

### Migrating from the bundled operations server

The 3.1 package boundary is intentionally explicit:

- Replace `jrtc.create_asgi_app` and `jrtc.api` imports with
  `japi.create_asgi_app` and `japi.api`.
- Replace `jrtc.contrib.admin.db.migrate` with
  `japi.contrib.admin.db.migrate`.
- Import `JanusSessionManager` from `jrtc` (or `jrtc.session`). The
  former mixed `jrtc.servers` namespace is gone.
- Remove callers of the former `/manager` API. The operations service neither
  owns nor exposes application session pools.
- Configure EventHandler delivery with `JANUS_EVENT_BROKER_*` settings. The
  legacy Kafka topic variable is accepted only as a destination fallback.

EventHandler records are now delivered using Broka's envelope wire format,
not the previous raw-JSON Kafka value. Native Kafka consumers must decode the
Broka envelope and deduplicate on its stable `janus-event-id` header; they must
not use the transport-generated envelope ID as the retry key. Migrate consumers
before routing an existing production topic through the new server.

The old operations flags are not read by the new distribution. Rename them as
part of the same deployment:

| Previous variable | Server variable |
|---|---|
| `JANUS_MOUNT_REST_API` | `JANUS_SERVER_MOUNT_REST_API` |
| `JANUS_ENABLE_ADMIN` | `JANUS_SERVER_ENABLE_ADMIN` |
| `JANUS_ENABLE_EVENTS` | `JANUS_SERVER_ENABLE_EVENTS` |
| `JANUS_MOUNT_LOGGING_APP` | `JANUS_SERVER_MOUNT_LOGGING_APP` |
| `JANUS_ALLOWED_ORIGINS` | `JANUS_SERVER_ALLOWED_ORIGINS` |
| `jrtc_ALLOW_CREDENTIALS` | `JANUS_SERVER_API_ALLOW_CREDENTIALS` |

## Client quick start

Install only the named plugin an application needs:

```bash
pip install janus-echotest-plugin
```

```python
import asyncio

from jrtc import JanusSession
from janus_echotest_plugin import EchoTestPlugin


async def main() -> None:
  async with JanusSession(url="ws://127.0.0.1:8188/janus") as session:
    async with EchoTestPlugin(session=session) as echo:
      reply = await echo.configure(audio=True, video=False)
      print(reply.data)


asyncio.run(main())
```

Use the same session API with Janus REST; long polling and REST path addressing
are managed by the HTTP transport:

```python
async with JanusSession(url="http://127.0.0.1:8088/janus") as session:
    ...
```

WebSocket disconnects invalidate every session and handle bound to that socket.
The manager creates fresh sessions rather than silently reusing stale Janus IDs.
Requests have bounded transaction tables and explicit timeouts; cancellation
always releases the pending transaction. Shutdown detaches handles with bounded
concurrency, reserves time for the Janus session destroy, and completes local
cleanup even if the calling task is cancelled.

WebSocket and HTTP are the built-in transports. RabbitMQ, MQTT, nanomsg, Unix
sockets, or an application-specific Janus transport can implement the small
`JanusTransport` protocol and be injected without core interpreting its URL:

```python
from jrtc import JanusSession

session = JanusSession(
  transport=my_transport,
  url="unix:///run/janus.sock",  # owned by the injected transport
)
await session.create()
```

Pass either `transport` or `transport_factory`, never both. A factory-created
transport is owned and closed by the session; a directly injected transport is
treated as shared and remains owned by the host application.

## Brokered WebRTC events

Inbound responses are coordinated once by a frozen Dispio dispatcher. ACK,
error, and transaction responses stay local; asynchronous Janus responses are
admitted directly from either transport to a bounded, ordered Broka publisher.
They do not pass through plugin callbacks on their way to third-party
applications, and ReactiveX is not part of the runtime.

Every event uses a logical type such as `janus.event` or `janus.media`, mapped
to one portable physical destination, `janus.events`. JSEP remains embedded in
the complete `janus.event` payload—there is no second SDP event.

The application that owns the Janus session also owns and injects its publisher.
The operations server deliberately creates neither one, which prevents duplicate
Janus sessions when both packages run in the same process:

```python
from jrtc import JanusSession
from jrtc.messaging import JanusEventPublisher, create_broker

broker = create_broker(
  engine="redis",
  engine_options={
    "mode": "streams",
    "url": "redis://localhost:6379/0",
  },
)

async with JanusEventPublisher(broker) as publisher:
  async with JanusSession(event_publisher=publisher) as session:
    ...
```

Third-party applications subscribe to the exact physical destination and route
on `delivery.envelope.type`:

```python
from jrtc.messaging import DEFAULT_PHYSICAL_ROUTE, create_broker

subscriber = create_broker(
  engine="redis",
  engine_options={
    "mode": "streams",
    "url": "redis://localhost:6379/0",
    "group": "analytics-janus-v1",
    "consumer_name": "analytics-01",
  },
)


async def receive(delivery) -> None:
  logical_type = delivery.envelope.type
  response = delivery.message
  jsep = response.get("jsep")
  await application_events.handle(logical_type, response, jsep=jsep)


async with subscriber:
  subscription = await subscriber.subscribe(DEFAULT_PHYSICAL_ROUTE, receive)
  try:
    await stop_event.wait()
  finally:
    await subscription.close()
```

Redis Pub/Sub, Redis Streams, RabbitMQ, Kafka, native wire consumers,
acknowledgement semantics, stable consumer identities, and current upstream
adapter limits are covered in the
[broker event guide](https://github.com/Leydotpy/Janus-API/blob/main/docs/broker-events.md).

## Authentication

Janus API tokens and API secrets belong to the outer envelope of every request.
Configure them once per session; representations and settings inspection redact
their values.

```python
from jrtc import JanusCredentials, JanusSession

credentials = JanusCredentials(token="client-token", api_secret="shared-secret")

async with JanusSession(credentials=credentials) as session:
  ...
```

A callable returning `JanusCredentials` may be supplied for credential rotation.
The default session manager reads `JANUS_TOKEN` and `jrtc_SECRET`.

## Plugin projects

The sibling `plugins/` workspace contains independent distributions:

| Janus plugin | Distribution | Import package | Entry-point name |
|---|---|---|---|
| EchoTest | `janus-echotest-plugin` | `janus_echotest_plugin` | `echotest` |
| VideoCall | `janus-videocall-plugin` | `janus_videocall_plugin` | `videocall` |
| SIP | `janus-sip-plugin` | `janus_sip_plugin` | `sip` |
| NoSIP | `janus-nosip-plugin` | `janus_nosip_plugin` | `nosip` |
| AudioBridge | `janus-audiobridge-plugin` | `janus_audiobridge_plugin` | `audiobridge` |
| VideoRoom | `janus-videoroom-plugin` | `janus_videoroom_plugin` | `videoroom` |
| TextRoom | `janus-textroom-plugin` | `janus_textroom_plugin` | `textroom` |
| Record&Play | `janus-recordplay-plugin` | `janus_recordplay_plugin` | `recordplay` |

Streaming remains in the `plugins/janus-streaming-plugin`
workspace as distribution `janus-api-streaming`, import package
`janus_streaming`, and entry-point name `streaming`.

Installed plugins are discovered lazily through the `jrtc.plugins` entry
point group. Importing Janus Core never scans or executes arbitrary local files
and never imports unrelated named plugins.

```python
from jrtc.lib import Plugin

# Resolves only the installed `echotest` entry point.
echo = Plugin(identifier="echotest", session=session)
await echo.attach()
```

Direct construction of the concrete class is preferred because it gives type
checkers the plugin-specific methods and result types.

## Implementing a custom plugin

Only the generic base and shared protocol primitives are required:

```python
from typing import Literal

from pydantic import BaseModel

from jrtc.lib import Plugin


class StatusRequest(BaseModel):
  request: Literal["status"] = "status"


class MyPlugin(Plugin):
  identifier = "my-plugin"
  name = "janus.plugin.my-plugin"

  async def status(self):
    return await self.send(StatusRequest())
```

To support lazy discovery from a separate distribution:

```toml
[project.entry-points."jrtc.plugins"]
my-plugin = "my_package.plugin:MyPlugin"
```

Plugin response bodies are opaque to core. A plugin package should use strict
outbound models, forward-compatible inbound models, typed plugin errors, and
golden protocol tests derived from its Janus documentation.

## Session ownership and the separate operations server

`JanusSessionManager` remains part of `jrtc` and owns a bounded,
process-local session pool. Each application owns its Janus connections and
passes the selected session to its own plugins. The operations server never
creates, installs, or tears down a session or manager.

```python
from jrtc import JanusSessionManager

async with JanusSessionManager(pool_size=2) as manager:
  session = manager.get_session(key="tenant-42")
  if session is None:
    raise RuntimeError("Janus is unavailable")
```

Install `japi` independently for monitoring and administration. Its
ASGI lifespan owns only server resources such as the Admin monitor, optional
Timescale storage, EventHandler broker sink, and log tailer:

```python
from japi import create_asgi_app

app = create_asgi_app(mount_rest_api=True)
```

```bash
JANUS_SERVER_MOUNT_REST_API=true \
uvicorn myapp:app --host 0.0.0.0 --port 8000
```

When enabled, the `/janus` mount can expose:

- `/admin/` — authenticated Admin/Monitor JSON, Prometheus, and WebSocket APIs
- `/events/janus-events` — bounded, Basic-authenticated EventHandler ingestion to Broka
- `/logs/` and `/logs/ws/logs` — bounded API-key-authenticated structured log access

There is intentionally no manager route. Monitoring the operations process and
managing application-owned Janus sessions are separate concerns.

The Admin surface is deliberately API-first (JSON, Prometheus text, and
WebSocket updates); core does not ship raw, uncompiled frontend source as a
production UI.

Admin, EventHandler, log viewer, and Timescale resources are disabled by
default. The service fails closed when an enabled component lacks credentials.
Schema migration is an explicit deployment action, never an import/startup side
effect:

```python
import asyncio

from japi.contrib.admin.db import migrate

asyncio.run(migrate())
```

EventHandler delivery uses the Broka abstraction with a stable
`janus-event-id` header, bounded per-application admission, finite publish and
batch deadlines, and partition/ordering keys derived from the Janus session or
emitter. Kafka deployments use the hardened core adapter, which forwards an
explicit allowlist of TLS/SASL and client options and requests idempotent,
`acks=all` delivery. Consumers should deduplicate by `janus-event-id` because an
HTTP batch can partially succeed before Janus retries it. Timescale queries have
statement, row, and time-bucket budgets; optional retention and compression
policies are installed only by the explicit migration.

Structured file logging is also opt-in. The handler installer is idempotent,
recursively redacts nested credentials, creates restrictive files, and supports
size-based or external watched rotation:

```python
from jrtc.core.logging import install_colored_logging

install_colored_logging(logfile="/var/log/myapp/janus.jsonl", rotation="watched")
```

## Core configuration

Configure these values through the host application's environment or secret
manager. The package itself deliberately does not load `.env` files.

| Variable | Default | Purpose |
|---|---:|---|
| `JANUS_SESSION_URL` | `ws://localhost:8188/janus` | WS/WSS or HTTP/HTTPS API endpoint |
| `JANUS_REQUEST_TIMEOUT` | `15` | Per-request timeout in seconds |
| `JANUS_SESSION_POOL_SIZE` | `1` | Sessions per process |
| `JANUS_KEEPALIVE_INTERVAL` | `25` | WebSocket session keepalive interval |
| `JANUS_KEEPALIVE_FAILURES` | `3` | Failures before a session is invalidated |
| `JANUS_SHUTDOWN_TIMEOUT` | `10` | Total bounded session-shutdown budget |
| `JANUS_DETACH_CONCURRENCY` | `16` | Concurrent handle detach limit |
| `JANUS_TOKEN` | unset | Janus token authentication |
| `jrtc_SECRET` | unset | Janus shared API secret |
| `JANUS_BROKER_ENGINE` | `memory` | `memory`, `local`, `redis`, `rabbitmq`, or `kafka` |
| `JANUS_BROKER_ROUTE` | `janus.events` | Exact physical event destination |
| `JANUS_BROKER_ENGINE_OPTIONS` | `{}` | JSON object passed to the selected Broka engine |
| `JANUS_BROKER_OPTIONS` | `{}` | JSON object passed to Broka configuration |
| `JANUS_BROKER_PUBLISH_WORKERS` | `4` | Ordered publisher worker shards |
| `JANUS_BROKER_QUEUE_CAPACITY` | `4096` | Process-wide bounded event admission |
| `JANUS_BROKER_ADMISSION_TIMEOUT` | `0.05` | Maximum queue-admission wait in seconds |
| `JANUS_BROKER_PUBLISH_TIMEOUT` | `5` | Hard deadline for each backend publication |
| `JANUS_BROKER_DRAIN_TIMEOUT` | `10` | Publisher shutdown/drain budget in seconds |

Use a custom typed module through `JANUS_SETTINGS_MODULE` for more complex core
deployments; explicit overrides are available through `jrtc.conf.configure`.

## Operations server configuration

The server loads its own settings independently through
`JANUS_SERVER_SETTINGS_MODULE` or `japi.configure`. Important
defaults are:

| Variable | Default | Purpose |
|---|---:|---|
| `JANUS_SERVER_MOUNT_REST_API` | `true` | Mount the REST application at `/janus` |
| `JANUS_SERVER_ENABLE_ADMIN` | `false` | Start Admin/Monitor integration |
| `JANUS_SERVER_ENABLE_EVENTS` | `false` | Start EventHandler broker ingestion |
| `JANUS_SERVER_MOUNT_LOGGING_APP` | `false` | Enable structured log query/streaming |
| `JANUS_SERVER_ALLOWED_ORIGINS` | unset | Exact CORS/WebSocket origins |
| `JANUS_SERVER_LIFESPAN_STARTUP_TIMEOUT` | `30` | Deadline for each external-resource startup |
| `JANUS_SERVER_LIFESPAN_SHUTDOWN_TIMEOUT` | `30` | Deadline for each external-resource shutdown |
| `JANUS_EVENT_BROKER_ENGINE` | unset | Required EventHandler Broka backend when ingestion is enabled |
| `JANUS_EVENT_BROKER_ENGINE_OPTIONS` | `{}` | Backend options, including secured Kafka options |
| `JANUS_EVENT_BROKER_OPTIONS` | `{}` | Broka reliability/serialization options |
| `JANUS_EVENT_HANDLER_LOGICAL_ROUTE` | `janus.eventhandler` | Logical Broka envelope type |
| `JANUS_EVENT_HANDLER_DESTINATION` | `janus.eventhandler` | Physical backend destination |
| `JANUS_EVENT_HANDLER_DELIVERY_CONCURRENCY` | `32` | Per-process concurrent deliveries |
| `JANUS_EVENT_HANDLER_MAX_INFLIGHT_BATCHES` | `16` | Per-process HTTP batch admission bound |
| `JANUS_EVENT_HANDLER_ADMISSION_TIMEOUT` | `0.1` | Capacity-admission deadline in seconds |
| `JANUS_EVENT_HANDLER_PUBLISH_TIMEOUT` | `5` | Per-event broker deadline in seconds |
| `JANUS_EVENT_HANDLER_BATCH_TIMEOUT` | `30` | Whole-batch deadline in seconds |
| `JANUS_TIMESCALE_QUERY_TIMEOUT` | `5` | Aggregate query/command timeout |
| `JANUS_TIMESCALE_MAX_QUERY_ROWS` | `10000` | Hard aggregate-result row limit |

Admin, EventHandler, log-viewer, and persistence credentials have no fallback
values. Enabled services fail startup when their required secrets are absent.
Redis Streams, RabbitMQ, or Kafka should back durable EventHandler ingestion;
Redis Pub/Sub is rejected because it cannot provide at-least-once delivery.

Scale the operations surfaces independently. EventHandler-only replicas are
stateless apart from bounded process-local admission and can scale horizontally
against one shared broker destination. Janus Admin polling, Timescale fallback
sampling, and file tailing are singleton activities for each Janus target or
log file: run those flags in a one-worker deployment (for example,
`uvicorn ... --workers 1`) or behind an external leader-election mechanism.
Enabling them in every worker would duplicate polling and persistence, while a
WebSocket can observe only the worker to which it is connected. Separate
event-only and admin/log deployments when both ingestion throughput and
monitoring availability need to scale.

## Development and verification

```bash
uv sync --all-packages --all-extras --group dev
uv run pytest
uv run ruff check src tests packages/japi/src packages/japi/tests
uv run ruff format --check src tests packages/japi/src packages/japi/tests
uv run mypy src/jrtc packages/japi/src/japi
uv build --package jrtc
uv build --package japi
```

The tracked tests cover response validation, local plugin lifecycle, complete
ReactiveX removal, bounded publisher concurrency and draining, real Broka
memory delivery, Dispio response selection, and one-envelope JSEP behavior for
both built-in transports. Live Janus and external broker tests should also run
against the exact versions used in each deployment.

See the
[broker event guide](https://github.com/Leydotpy/Janus-API/blob/main/docs/broker-events.md)
for ownership, event contracts, backend subscription topology, reliability,
and deployment limits.

## 3.1 messaging migration

- ReactiveX transport/plugin APIs were removed. Local plugin callbacks use
  bounded instance-owned queues; cross-process subscribers use Broka.
- Dispio now coordinates ACK, error, transaction, and asynchronous response
  handling without a transport `if`/`elif` decision tree.
- JSEP is delivered only inside its original response and is never dispatched
  as a separate SDP event.
- Python 3.12 or newer is required by the pinned messaging dependencies.

## 2.x migration notes

- Distribution: `janus-api` → `jrtc`; import namespace remains `jrtc`.
- `WebsocketSession` remains an alias of the transport-agnostic `JanusSession`.
- Named plugin models, clients, and the old VideoRoom facade moved out of core.
- Streaming moved completely to `janus-api-streaming` under `plugins`.
- Sessions and plugin managers are ordinary instances; process-global singleton
  handles, global Rx event routing, eager plugin scanning, and Redis RPC are gone.
- Plugin payloads are no longer part of a closed core request/response union.
- Root logging, database migrations, threads, sockets, and optional-service
  imports no longer happen at module import time.

## Protocol references

- [Janus transports and core protocol](https://janus.conf.meetecho.com/docs/rest.html)
- [Janus API authentication](https://janus.conf.meetecho.com/docs/auth.html)
- [Admin/Monitor API](https://janus.conf.meetecho.com/docs/admin.html)
- [Event handlers](https://janus.conf.meetecho.com/docs/eventhandlers.html)
- [Recordings](https://janus.conf.meetecho.com/docs/recordings.html)

## License

MIT
