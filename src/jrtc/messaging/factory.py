"""Safe construction helpers for the Janus Broka broker."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from types import MappingProxyType
from typing import Literal

from broka import Broker, Destination, EngineRegistry, MetricsProvider, Router
from broka.engines.base import BaseEngine
from broka.exceptions import PluginLoadError
from logvista import VisualLogger

from jrtc.messaging.constants import DEFAULT_PHYSICAL_ROUTE, JANUS_LOGICAL_PATTERN
from jrtc.messaging.metrics import LogVistaMetrics

BrokerEngine = Literal["memory", "local", "redis", "rabbitmq", "kafka"]

_ENGINE_LOADERS = MappingProxyType(
    {
        "memory": ("broka.engines.memory", "MemoryEngine", -100),
        "local": ("broka.engines.local", "LocalEngine", 10),
        "redis": ("broka.engines.redis", "RedisEngine", 50),
        "rabbitmq": ("broka.engines.rabbitmq", "RabbitMQEngine", 40),
        "kafka": ("jrtc.messaging.engines.kafka", "KafkaEngine", 30),
    }
)


def _load_engine(module_name: str, attribute: str) -> type[BaseEngine]:
    value = getattr(import_module(module_name), attribute)
    if not isinstance(value, type) or not issubclass(value, BaseEngine):
        raise PluginLoadError(
            f"{module_name}:{attribute} did not provide a Broka BaseEngine subclass"
        )
    return value


def create_engine_registry() -> EngineRegistry:
    """Return an isolated registry containing only supported Broka engines.

    All implementations are registered lazily. This project-owned registry is
    intentional: Broka 0.0.2's default registry probes legacy ``pyev`` module
    paths for optional engines.
    """

    registry = EngineRegistry()
    for name, (module_name, attribute, priority) in _ENGINE_LOADERS.items():

        def loader(
            module_name: str = module_name,
            attribute: str = attribute,
        ) -> type[BaseEngine]:
            return _load_engine(module_name, attribute)

        registry.register_lazy(
            name,
            loader,
            priority=priority,
            source="jrtc.messaging",
        )
    return registry


def create_broker(
    *,
    engine: BrokerEngine = "memory",
    physical_route: str = DEFAULT_PHYSICAL_ROUTE,
    engine_options: Mapping[str, object] | None = None,
    broker_options: Mapping[str, object] | None = None,
    metrics: MetricsProvider | None = None,
    logger: VisualLogger | None = None,
) -> Broker:
    """Build an unstarted Broka broker for Janus event publication.

    ``janus.*`` remains the logical application namespace. Every logical Janus
    route is mapped to the one exact backend destination ``physical_route`` so
    subscribers use the same physical name on every supported engine.
    """

    if engine not in _ENGINE_LOADERS:
        supported = ", ".join(_ENGINE_LOADERS)
        raise ValueError(f"unsupported Broka engine {engine!r}; choose one of: {supported}")

    config = dict(broker_options or {})
    raw_engines = config.get("engines", {})
    if not isinstance(raw_engines, Mapping):
        raise TypeError("broker_options['engines'] must be a mapping")
    engines: dict[str, dict[str, object]] = {}
    for name, value in raw_engines.items():
        if not isinstance(value, Mapping):
            raise TypeError(f"broker_options['engines'][{name!r}] must be a mapping")
        engines[str(name)] = dict(value)
    selected_options = engines.get(engine, {})
    selected_options.update(engine_options or {})
    engines[engine] = selected_options
    config["engine"] = engine
    config["engines"] = engines

    router = Router()
    router.map_destination(JANUS_LOGICAL_PATTERN, Destination(physical_route))
    return Broker(
        config=config,
        registry=create_engine_registry(),
        router=router,
        metrics=metrics or LogVistaMetrics(logger),
    )


def configured_engine(broker: Broker) -> BrokerEngine | str | None:
    """Return the broker's configured engine without starting it."""

    return broker.config.engine


__all__ = ["BrokerEngine", "configured_engine", "create_broker", "create_engine_registry"]
