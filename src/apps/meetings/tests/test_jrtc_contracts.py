"""Architecture contracts for Synq's JRTC 3 / jrtc-video migration."""

from __future__ import annotations

import asyncio
import importlib.metadata
import importlib.util
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from django.db import models
from django.test import SimpleTestCase, override_settings
from pydantic import ValidationError as PydanticValidationError

from jrtc.models.base import Jsep
from jrtc_video import (
    PublisherJoinAndConfigureRequest,
    SubscribeTarget,
    VideoRoomPlugin,
)

from apps.meetings.jrtc.handles import (
    BoundVideoRoomHandle,
    HandleBindingSpec,
    JrtcHandleRegistry,
)
from apps.meetings.jrtc.errors import (
    JrtcHandleOwnershipError,
    JrtcHandleUnavailable,
)
from apps.meetings.jrtc.config import load_event_config
from apps.meetings.jrtc.ids import (
    janus_event_to_wire,
    janus_id_from_wire,
    optional_janus_id_to_wire,
    require_janus_id,
)
from apps.meetings.jrtc.runtime import JanusProcessRuntime
from apps.meetings.jrtc.videoroom import VideoRoomAdapter
from apps.meetings.models import (
    MeetingSession,
    Participant,
    ParticipantMediaHandle,
    ParticipantStream,
)
from apps.meetings.services.janus import (
    NativeJanusIdVideoRoomPlugin,
    janus_room_id_for_session,
    serialize_janus_response,
)
from core.models.fields.janus import BoundPluginHandle, JanusPluginField


class DependencyContractTests(SimpleTestCase):
    def test_pyproject_and_environment_use_only_new_packages(self) -> None:
        project = Path(__file__).resolve().parents[4] / "pyproject.toml"
        dependencies = tomllib.loads(project.read_text(encoding="utf-8"))["project"][
            "dependencies"
        ]
        normalized = [dependency.lower() for dependency in dependencies]

        self.assertTrue(any(item.startswith("jrtc[") or item.startswith("jrtc>") for item in normalized))
        self.assertTrue(any(item.startswith("jrtc-video") for item in normalized))
        self.assertFalse(any("janus-api-core" in item for item in normalized))
        self.assertFalse(any("janus-videoroom-plugin" in item for item in normalized))
        self.assertIsNotNone(importlib.util.find_spec("jrtc"))
        self.assertIsNotNone(importlib.util.find_spec("jrtc_video"))
        self.assertIsNone(importlib.util.find_spec("janus_api"))
        self.assertIsNone(importlib.util.find_spec("janus_videoroom_plugin"))
        self.assertGreaterEqual(
            importlib.metadata.version("jrtc").split(".")[:2],
            ["3", "1"],
        )

    def test_historical_plugin_alias_is_exact_and_not_a_numeric_id_shim(self) -> None:
        self.assertIs(NativeJanusIdVideoRoomPlugin, VideoRoomPlugin)


class IdentifierContractTests(SimpleTestCase):
    def test_internal_ids_are_strict_positive_ints(self) -> None:
        class IntegerSubclass(int):
            pass

        self.assertEqual(require_janus_id(2**63 + 17), 2**63 + 17)
        for invalid in (True, False, "123", 123.0, 0, -1, None, IntegerSubclass(4)):
            with self.subTest(invalid=invalid), self.assertRaises(TypeError):
                require_janus_id(invalid)

    def test_jrtc_video_models_reject_strings_and_booleans(self) -> None:
        self.assertEqual(SubscribeTarget(feed=2**63 + 17).feed, 2**63 + 17)
        for invalid in ("123", True, 0, -1):
            with self.subTest(invalid=invalid), self.assertRaises(PydanticValidationError):
                SubscribeTarget(feed=invalid)

    def test_decimal_strings_are_parsed_only_at_an_explicit_boundary(self) -> None:
        self.assertEqual(janus_id_from_wire("900719925474099312345"), 900719925474099312345)
        for invalid in (9007199254740993, True, "0", "01", "+1", "1.0", "named"):
            with self.subTest(invalid=invalid), self.assertRaises(TypeError):
                janus_id_from_wire(invalid)

    def test_browser_boundary_stringifies_nested_janus_ids(self) -> None:
        payload = janus_event_to_wire(
            {
                "janus": "event",
                "transaction": "tx-1",
                "session_id": 2**63 + 1,
                "sender": 2**63 + 2,
                "plugin_id": 2**63 + 7,
                "plugindata": {
                    "data": {
                        "room": 2**63 + 3,
                        "id": 2**63 + 4,
                        "private_id": 2**63 + 5,
                        "streams": [{"feed_id": 2**63 + 6}],
                        "metadata": {
                            "session_id": "72689818-e020-4bcb-9638-2992a979d309",
                            "id": "domain-object-id",
                        },
                    }
                },
            }
        )
        self.assertEqual(payload["session_id"], str(2**63 + 1))
        self.assertEqual(payload["sender"], str(2**63 + 2))
        self.assertEqual(payload["plugin_id"], str(2**63 + 7))
        self.assertEqual(payload["plugindata"]["data"]["room"], str(2**63 + 3))
        self.assertEqual(payload["plugindata"]["data"]["id"], str(2**63 + 4))
        self.assertEqual(
            payload["plugindata"]["data"]["metadata"]["session_id"],
            "72689818-e020-4bcb-9638-2992a979d309",
        )
        self.assertEqual(payload["transaction"], "tx-1")
        self.assertIsNone(optional_janus_id_to_wire(None))

    def test_response_serialization_is_an_explicit_wire_boundary(self) -> None:
        payload = serialize_janus_response(
            {
                "janus": "event",
                "session_id": 9_007_199_254_740_993,
                "sender": 9_007_199_254_740_995,
                "plugindata": {"data": {"room": 9_007_199_254_740_997}},
            }
        )
        self.assertEqual(payload["session_id"], "9007199254740993")
        self.assertEqual(payload["sender"], "9007199254740995")
        self.assertEqual(payload["plugindata"]["data"]["room"], "9007199254740997")

    def test_active_database_fields_are_nullable_positive_bigints(self) -> None:
        fields = (
            MeetingSession._meta.get_field("control_handle_id"),
            MeetingSession._meta.get_field("janus_room_id"),
            Participant._meta.get_field("janus_publisher_id"),
            Participant._meta.get_field("janus_private_id"),
            ParticipantMediaHandle._meta.get_field("janus_session_id"),
            ParticipantMediaHandle._meta.get_field("janus_handle_id"),
            ParticipantStream._meta.get_field("janus_feed_id"),
        )
        for field in fields:
            with self.subTest(field=field.name):
                self.assertIsInstance(field, models.PositiveBigIntegerField)
                self.assertTrue(field.null)
        self.assertTrue(ParticipantMediaHandle._meta.get_field("runtime_owner_id").null)
        self.assertIn(
            ("janus_session_id", "janus_handle_id"),
            {tuple(index.fields) for index in ParticipantMediaHandle._meta.indexes},
        )

    def test_room_id_derivation_is_stable_and_strict(self) -> None:
        session = SimpleNamespace(pk=uuid4(), janus_room_id=None)
        first = janus_room_id_for_session(session)
        self.assertEqual(first, janus_room_id_for_session(session))
        self.assertGreater(first, 0)
        self.assertLess(first, 2**63)
        with self.assertRaises(PydanticValidationError):
            SubscribeTarget(feed=str(first))


class HistoricalFieldCompatibilityTests(SimpleTestCase):
    def test_field_replays_migrations_without_materializing_plugins(self) -> None:
        field = JanusPluginField(
            identifier="publisher",
            plugin_class="apps.meetings.services.janus.NativeJanusIdVideoRoomPlugin",
            null=True,
        )
        self.assertEqual(field.from_db_value(123, None, None), 123)
        with self.assertRaises(PydanticValidationError):
            SubscribeTarget(feed="123")
        wrapper = BoundPluginHandle(
            instance=SimpleNamespace(),
            field=field,
            raw_id=123,
        )
        self.assertEqual(wrapper.id, 123)
        with self.assertRaisesRegex(RuntimeError, "handle registry"):
            _ = wrapper.plugin


class _FakeSession:
    def __init__(self, session_id: int = 101) -> None:
        self.id = session_id
        self.ready = True
        self.plugins: dict[int, object] = {}


class _FakePlugin:
    next_id = 201
    constructor_kwargs: list[dict[str, object]] = []

    def __init__(self, *, session: _FakeSession, **kwargs: object) -> None:
        self.session = session
        self._id: int | None = None
        self.closed = False
        self.constructor_kwargs.append(dict(kwargs))

    @property
    def id(self) -> int:
        if self._id is None:
            raise RuntimeError("not attached")
        return self._id

    async def attach(self, *, opaque_id: str | None = None) -> None:
        del opaque_id
        self._id = self.next_id
        self.session.plugins[self.id] = self

    async def detach(self) -> None:
        if self._id is not None:
            self.session.plugins.pop(self._id, None)
        self._id = None

    async def aclose(self) -> None:
        self.closed = True


class HandleRegistryContractTests(SimpleTestCase):
    async def test_db_only_ids_are_replaced_without_plugin_id_adoption(self) -> None:
        registry = JrtcHandleRegistry("owner-a")
        session = _FakeSession()
        spec = HandleBindingSpec(
            model_id="domain-1",
            persisted_session_id=55,
            persisted_handle_id=66,
            persisted_owner_id=None,
        )
        _FakePlugin.constructor_kwargs.clear()
        with patch("apps.meetings.jrtc.handles.VideoRoomPlugin", _FakePlugin):
            result = await registry.resolve_or_attach(spec, session=session, recreate=True)

        self.assertTrue(result.recreated)
        self.assertTrue(result.replaced_stale)
        self.assertEqual(result.binding.session_id, 101)
        self.assertEqual(result.binding.handle_id, 201)
        self.assertNotIn("plugin_id", _FakePlugin.constructor_kwargs[0])
        self.assertIs(session.plugins[201], result.binding.plugin)

    async def test_foreign_runtime_ownership_is_never_silently_stolen(self) -> None:
        registry = JrtcHandleRegistry("owner-a")
        session = _FakeSession()
        spec = HandleBindingSpec(
            model_id="domain-1",
            persisted_session_id=55,
            persisted_handle_id=66,
            persisted_owner_id="owner-b",
        )
        _FakePlugin.constructor_kwargs.clear()
        with (
            patch("apps.meetings.jrtc.handles.VideoRoomPlugin", _FakePlugin),
            self.assertRaises(JrtcHandleOwnershipError),
        ):
            await registry.resolve_or_attach(spec, session=session, recreate=True)
        self.assertEqual(_FakePlugin.constructor_kwargs, [])

    async def test_lost_session_invalidates_live_binding(self) -> None:
        registry = JrtcHandleRegistry("owner-a")
        session = _FakeSession()
        with patch("apps.meetings.jrtc.handles.VideoRoomPlugin", _FakePlugin):
            result = await registry.resolve_or_attach(
                HandleBindingSpec(model_id="domain-1"),
                session=session,
                recreate=True,
            )
        session.ready = False
        self.assertIsNone(await registry.get("domain-1"))
        self.assertEqual(registry.stale_invalidations, 1)
        self.assertEqual(registry.active_count, 0)

    async def test_command_and_detach_share_one_per_handle_fence(self) -> None:
        registry = JrtcHandleRegistry("owner-a")
        session = _FakeSession()
        with patch("apps.meetings.jrtc.handles.VideoRoomPlugin", _FakePlugin):
            binding = (
                await registry.resolve_or_attach(
                    HandleBindingSpec(model_id="domain-1"),
                    session=session,
                    recreate=True,
                )
            ).binding

        entered = asyncio.Event()
        release = asyncio.Event()

        async def operation(plugin: object) -> str:
            self.assertIs(plugin, binding.plugin)
            entered.set()
            await release.wait()
            return "done"

        command_task = asyncio.create_task(registry.invoke(binding, operation))
        await entered.wait()
        detach_task = asyncio.create_task(
            registry.detach(binding.model_id, expected=binding)
        )
        await asyncio.sleep(0)
        self.assertFalse(detach_task.done())
        release.set()
        self.assertEqual(await command_task, "done")
        await detach_task
        self.assertEqual(registry.active_count, 0)
        self.assertNotIn(binding.model_id, registry._resolution_locks)

    async def test_clear_cannot_resurrect_an_inflight_attach(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        class BlockingPlugin(_FakePlugin):
            async def attach(self, *, opaque_id: str | None = None) -> None:
                started.set()
                await release.wait()
                await super().attach(opaque_id=opaque_id)

        registry = JrtcHandleRegistry("owner-a")
        session = _FakeSession()
        with patch("apps.meetings.jrtc.handles.VideoRoomPlugin", BlockingPlugin):
            resolution_task = asyncio.create_task(
                registry.resolve_or_attach(
                    HandleBindingSpec(model_id="domain-1"),
                    session=session,
                    recreate=True,
                )
            )
            await started.wait()
            clear_task = asyncio.create_task(registry.clear())
            await asyncio.sleep(0)
            release.set()
            with self.assertRaises(JrtcHandleUnavailable):
                await resolution_task
            await clear_task

        self.assertEqual(registry.active_count, 0)
        self.assertEqual(registry.snapshot(), ())


class CommandPlaneContractTests(SimpleTestCase):
    async def test_join_and_configure_stays_one_direct_plugin_command(self) -> None:
        adapter = VideoRoomAdapter(Mock(), Mock())
        adapter.invoke = AsyncMock(return_value=object())
        binding = BoundVideoRoomHandle(
            model_id="domain-1",
            session_id=101,
            handle_id=201,
            plugin=Mock(),
            owner_id="owner-a",
        )
        body = PublisherJoinAndConfigureRequest(room=301, display="Ada")
        offer = Jsep(type="offer", sdp="v=0\r\n")

        result = await adapter.join_and_configure(binding, body, offer)

        self.assertIsNotNone(result)
        adapter.invoke.assert_awaited_once_with(
            binding,
            "join_and_configure",
            body,
            offer,
        )

    async def test_hangup_does_not_implicitly_detach_the_plugin(self) -> None:
        adapter = VideoRoomAdapter(Mock(), Mock())
        adapter.invoke = AsyncMock(return_value="hung-up")
        binding = BoundVideoRoomHandle(
            model_id="domain-1",
            session_id=101,
            handle_id=201,
            plugin=Mock(),
            owner_id="owner-a",
        )

        self.assertEqual(await adapter.hangup(binding), "hung-up")
        adapter.invoke.assert_awaited_once_with(binding, "hangup")


class RuntimeLifecycleContractTests(SimpleTestCase):
    def test_event_timeouts_reject_bool_nan_and_infinity(self) -> None:
        for invalid in (True, float("nan"), float("inf"), float("-inf")):
            with (
                self.subTest(invalid=invalid),
                override_settings(JRTC_EVENT_PUBLISH_TIMEOUT=invalid),
                self.assertRaises(ValueError),
            ):
                load_event_config()

    @override_settings(
        JRTC_EVENT_BROKER_ENGINE="redis",
        JRTC_REDIS_MODE="streams",
        JRTC_REDIS_GROUP="stable-synq-group",
        JRTC_REDIS_CONSUMER_NAME="configured-name",
    )
    def test_redis_stream_identity_is_configured_on_the_engine(self) -> None:
        config = load_event_config(consumer_name="replica-a")
        self.assertEqual(config.consumer_group, "stable-synq-group")
        self.assertEqual(config.consumer_name, "replica-a")
        self.assertEqual(config.engine_options["group"], "stable-synq-group")
        self.assertEqual(config.engine_options["consumer_name"], "replica-a")

    async def test_start_order_is_publisher_then_manager_then_lease(self) -> None:
        events: list[str] = []
        runtime = JanusProcessRuntime()
        config = SimpleNamespace()

        class Publisher:
            async def start(self) -> None:
                events.append("publisher.start")

        publisher = Publisher()

        class Manager:
            async def start(self) -> None:
                events.append("manager.start")

            async def stop(self) -> None:
                events.append("manager.stop")

        manager = Manager()
        manager_factory = Mock(side_effect=lambda **kwargs: manager)

        with (
            patch("apps.meetings.jrtc.runtime.configure_jrtc_core", side_effect=lambda: events.append("configure")),
            patch("apps.meetings.jrtc.runtime.load_event_config", return_value=config),
            patch("apps.meetings.jrtc.runtime.build_event_publisher", return_value=publisher),
            patch("apps.meetings.jrtc.runtime.JanusSessionManager", manager_factory),
            patch(
                "apps.meetings.jrtc.runtime.Janus.install_manager",
                side_effect=lambda value: events.append("manager.install") or object(),
            ),
        ):
            returned_manager, returned_publisher, _, _ = await runtime._start_owned()

        self.assertIs(returned_manager, manager)
        self.assertIs(returned_publisher, publisher)
        self.assertEqual(
            events,
            ["configure", "publisher.start", "manager.start", "manager.install"],
        )
        self.assertIs(manager_factory.call_args.kwargs["event_publisher"], publisher)

    async def test_shutdown_stops_transports_before_draining_publisher(self) -> None:
        events: list[str] = []
        runtime = JanusProcessRuntime()

        class Manager:
            async def stop(self) -> None:
                events.append("manager.stop")

        class Publisher:
            async def stop(self, *, drain: bool, timeout: float) -> None:
                self.args = (drain, timeout)
                events.append("publisher.stop")

        publisher = Publisher()

        async def clear() -> None:
            events.append("registry.clear")

        runtime._registry.clear = clear
        await runtime._stop_owned(
            Manager(),
            publisher,
            SimpleNamespace(drain_timeout=7.0),
        )
        self.assertEqual(
            events,
            ["manager.stop", "publisher.stop", "registry.clear"],
        )
        self.assertEqual(publisher.args, (True, 7.0))
