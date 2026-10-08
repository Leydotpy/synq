"""Use the installed VideoRoom service/plugin with a transport-only double."""
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from apps.meetings.jrtc.runtime import JanusProcessRuntime


class PluginRegistry(dict):
    def register(self, handle_id, plugin):
        self[handle_id] = plugin

    def unregister(self, handle_id):
        self.pop(handle_id, None)


class TransportSession:
    id = 101
    generation = 1
    ready = True

    def __init__(self):
        self.plugins = PluginRegistry()
        self.attaches = 0
        self.commands = 0
        self.detaches = 0
        self.detach_started = asyncio.Event()
        self.detach_release = None
        self.observers = []

    def add_loss_observer(self, callback):
        self.observers.append(callback)
        return lambda: self.observers.remove(callback)

    async def attach(self, name, **kwargs):
        self.attaches += 1
        return 200 + self.attaches

    async def detach(self, handle_id):
        self.detach_started.set()
        if self.detach_release is not None:
            await self.detach_release.wait()
        self.detaches += 1
        self.plugins.unregister(handle_id)
        return {"janus": "success"}

    async def send(self, message, **options):
        self.commands += 1
        return {"janus": "event", "transaction": message.transaction,
            "plugindata": {"plugin": "janus.plugin.videoroom", "data": {
                "videoroom": "participants", "room": 301, "participants": []}}}


class ServiceLifecycleTests(SimpleTestCase):
    async def test_reuse_and_replacement_with_recycled_integer_id(self):
        runtime = JanusProcessRuntime()
        first = TransportSession()
        runtime.session = Mock(return_value=first)
        runtime.observe_session = Mock()
        adapter = runtime.adapter
        for _ in range(5):
            await adapter.management_command(session_key="room", method_name="list_participants", args=(301,))
        old_service = next(iter(adapter._services.values()))
        self.assertEqual((first.attaches, first.commands, first.detaches), (1, 5, 0))
        first.ready = False
        replacement = TransportSession()  # Same numeric ID and generation, new owner.
        runtime.session.return_value = replacement
        await adapter.management_command(session_key="room", method_name="list_participants", args=(301,))
        self.assertTrue(old_service.closed)
        self.assertIs(old_service.session, first)
        self.assertEqual(len(adapter._services), 1)
        self.assertIs(next(iter(adapter._services.values())).session, replacement)
        await adapter.aclose()
        self.assertEqual(replacement.detaches, 1)
        self.assertEqual(len(replacement.plugins), 0)

    async def test_cancelled_shutdown_finishes_service_before_manager_and_publisher(self):
        runtime = JanusProcessRuntime()
        session = TransportSession()
        runtime.session = Mock(return_value=session)
        runtime.observe_session = Mock()
        await runtime.adapter.management_command(session_key="room", method_name="list_participants", args=(301,))
        session.detach_release = asyncio.Event()
        order = []

        async def stop_manager():
            self.assertEqual(session.detaches, 1)
            self.assertFalse(runtime.adapter._services)
            order.append("manager")

        async def stop_publisher(**kwargs):
            order.append("publisher")

        shutdown = asyncio.create_task(runtime._stop_owned(
            SimpleNamespace(stop=stop_manager), SimpleNamespace(stop=stop_publisher), None))
        await session.detach_started.wait()
        shutdown.cancel()
        await asyncio.sleep(0)
        shutdown.cancel()  # Repeated cancellation must not cancel the owned cleanup.
        await asyncio.sleep(0)
        self.assertFalse(shutdown.done())
        self.assertEqual(order, [])
        session.detach_release.set()
        with self.assertRaises(asyncio.CancelledError):
            await shutdown
        self.assertEqual(order, ["manager", "publisher"])

    async def test_loss_observer_is_bounded_coalesced_and_unsubscribed(self):
        runtime = JanusProcessRuntime()
        runtime._state = runtime.RUNNING
        runtime._loop = asyncio.get_running_loop()
        session = TransportSession()
        runtime._manager = SimpleNamespace(sessions=[session])
        runtime.observe_session(session)
        runtime.observe_session(session)
        self.assertEqual(len(session.observers), 1)
        task = runtime._loss_task
        for _ in range(1000):
            session.observers[0](None)
        self.assertIs(runtime._loss_task, task)
        self.assertTrue(runtime._loss_wakeup.is_set())
        self.assertEqual(runtime._observed_losses, 1000)
        # Stop without yielding to the DB worker; the reader callback did no I/O.
        await runtime._stop_loss_observation()
        self.assertEqual(session.observers, [])
        self.assertIsNone(runtime._loss_task)
