"""Disposable Redis presence with monotonic replacement and bounded expiry."""

import json
from datetime import timedelta

from asgiref.sync import sync_to_async
from django.db.models import Q
from django.utils import timezone
from redis.asyncio import Redis

from apps.meetings.models import MeetingSession
from apps.meetings.services.gateway_state import enabled, reconcile

# Preserve a short high-water fence after online data expires. Readers must also
# check each expires_at; key existence alone never means a participant is online.
REPLACE = """
local old = redis.call('GET', KEYS[1])
if old then
    local data = cjson.decode(old)
    if data.projection_version > tonumber(ARGV[1]) then return 0 end
    if data.projection_version == tonumber(ARGV[1]) and data.as_of > tonumber(ARGV[2]) then return 0 end
end
redis.call('SET', KEYS[1], ARGV[3], 'EX', 300)
return 1
"""


def meeting_batch(after):
    query = (
        MeetingSession.objects.filter(signalling_backend="gateway")
        .filter(
            ~Q(lifecycle_state__in=("ended", "failed"))
            | Q(updated_at__gte=timezone.now() - timedelta(minutes=5))
        )
        .order_by("pk")
    )
    # Clear recently terminal sessions; don't rescan lifetime meeting history.
    return (
        list(query.filter(pk__gt=after).values_list("pk", flat=True)[:10])
        if after
        else list(query.values_list("pk", flat=True)[:10])
    )


class PresenceProjector:
    def __init__(self, url):
        self.redis = Redis.from_url(url, socket_timeout=1, socket_connect_timeout=1)
        self.after = None

    async def publish(self, state):
        return await self.redis.eval(
            REPLACE,
            1,
            f"synq:gateway:presence:{state['meeting_id']}",
            state["projection_version"],
            state["as_of"],
            json.dumps(state, separators=(",", ":")),
        )

    async def tick(self):
        if not enabled():
            return
        ids = await sync_to_async(meeting_batch)(self.after)
        if not ids:
            self.after = None
            return
        for identity in ids:
            # Function exits its transaction before any Redis I/O.
            state = await sync_to_async(reconcile)(identity)
            await self.publish(state)
            self.after = identity

    async def close(self):
        await self.redis.aclose()
