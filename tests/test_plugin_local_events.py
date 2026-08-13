from __future__ import annotations

import asyncio

import pytest

from janus_api.lib.manager import PluginManager
from janus_api.lib.plugins.base import Plugin
from janus_api.models.response import DetachedResponse, MediaEventResponse


class ExamplePlugin(Plugin):
    identifier = "tests.local-events"
    name = "janus.plugin.tests"


class FakeSession:
    def __init__(self) -> None:
        self.plugins: PluginManager[Plugin] = PluginManager()


async def test_local_plugin_listeners_receive_generic_and_typed_events() -> None:
    session = FakeSession()
    on_event_called = asyncio.Event()
    generic_called = asyncio.Event()
    media_called = asyncio.Event()
    received: list[tuple[str, str]] = []

    async def on_event(event: MediaEventResponse) -> None:
        received.append(("on_event", event.janus))
        on_event_called.set()

    async def on_generic(event: MediaEventResponse) -> None:
        received.append(("event", event.janus))
        generic_called.set()

    async def on_media(event: MediaEventResponse) -> None:
        received.append(("media", event.janus))
        media_called.set()

    plugin = ExamplePlugin(session=session, plugin_id=99, on_event=on_event)
    await plugin.on("event", on_generic)
    await plugin.on("media", on_media)
    plugin._dispatch_event(
        MediaEventResponse(janus="media", sender=99, type="video", receiving=True)
    )

    await asyncio.wait_for(
        asyncio.gather(
            on_event_called.wait(),
            generic_called.wait(),
            media_called.wait(),
        ),
        timeout=1,
    )

    assert set(received) == {("on_event", "media"), ("event", "media"), ("media", "media")}
    await plugin.aclose()


async def test_plugin_listener_failures_are_isolated_and_event_order_is_preserved() -> None:
    session = FakeSession()
    delivered = asyncio.Event()
    received: list[bool] = []

    async def failing_listener(_: MediaEventResponse) -> None:
        raise RuntimeError("listener failure")

    async def ordered_listener(event: MediaEventResponse) -> None:
        received.append(event.receiving)
        if len(received) == 2:
            delivered.set()

    plugin = ExamplePlugin(session=session, plugin_id=100)
    await plugin.on("event", failing_listener)
    await plugin.on("event", ordered_listener)
    plugin._dispatch_event(
        MediaEventResponse(janus="media", sender=100, type="audio", receiving=True)
    )
    plugin._dispatch_event(
        MediaEventResponse(janus="media", sender=100, type="audio", receiving=False)
    )

    await asyncio.wait_for(delivered.wait(), timeout=1)

    assert received == [True, False]
    await plugin.aclose()


async def test_plugin_callbacks_keep_registration_and_typed_order() -> None:
    plugin = ExamplePlugin(session=FakeSession(), plugin_id=103)
    delivered = asyncio.Event()
    received: list[str] = []

    async def first(_: MediaEventResponse) -> None:
        received.append("generic-first")

    async def second(_: MediaEventResponse) -> None:
        received.append("generic-second")

    async def typed(_: MediaEventResponse) -> None:
        received.append("typed")
        delivered.set()

    await plugin.on("event", first)
    await plugin.on("event", second)
    await plugin.on("event", first)  # Duplicate registration remains idempotent.
    await plugin.on("media", typed)
    plugin._dispatch_event(
        MediaEventResponse(janus="media", sender=103, type="video", receiving=True)
    )

    await asyncio.wait_for(delivered.wait(), timeout=1)

    assert received == ["generic-first", "generic-second", "typed"]
    await plugin.aclose()


async def test_emit_and_off_remain_local_and_awaitable() -> None:
    plugin = ExamplePlugin(session=FakeSession(), plugin_id=101)

    async def async_listener(payload: int) -> int:
        return payload + 1

    def sync_listener(payload: int) -> int:
        return payload + 2

    await plugin.on("custom", async_listener)
    await plugin.on("custom", sync_listener)

    assert sorted(await plugin.emit("custom", 40, wait=True)) == [41, 42]

    await plugin.off("custom", async_listener)
    await plugin.off("custom", sync_listener)
    assert await plugin.emit("custom", 40, wait=True) == []
    await plugin.aclose()


async def test_detached_event_invalidates_the_local_handle() -> None:
    session = FakeSession()
    delivered = asyncio.Event()

    async def on_detached(_: DetachedResponse) -> None:
        delivered.set()

    plugin = ExamplePlugin(session=session, plugin_id=102)
    session.plugins.register(102, plugin)
    await plugin.on("detached", on_detached)
    plugin._dispatch_event(DetachedResponse(janus="detached", sender=102))

    await asyncio.wait_for(delivered.wait(), timeout=1)
    await asyncio.wait_for(plugin._event_queue.join(), timeout=1)

    assert session.plugins.get(102) is None
    with pytest.raises(RuntimeError, match="not been attached"):
        _ = plugin.id


def test_plugin_exposes_no_reactivex_compatibility_surface() -> None:
    for obsolete_name in ("rx", "subscribe_rx", "_set_rx_subject"):
        assert not hasattr(Plugin, obsolete_name)
