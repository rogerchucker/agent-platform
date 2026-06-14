"""Topic-based pub/sub message queue for the control plane.

Agents subscribe to topics and receive messages via an asyncio.Queue (drained
by their WebSocket connection or long-poll). Publishing fans a message out to
every current subscriber of its topic. A bounded ring buffer keeps recent
history per topic so newly-connected agents can catch up.

Topics support a single trailing wildcard: subscribing to ``tasks.*`` (or
``tasks``) receives ``tasks.rootcause``, ``tasks.triage``, etc.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Optional

from .models import Message


class Subscriber:
    def __init__(self, subscriber_id: str, topics: set[str], maxsize: int = 1000):
        self.subscriber_id = subscriber_id
        self.topics = topics
        self.queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    def matches(self, topic: str) -> bool:
        for pattern in self.topics:
            if _topic_matches(pattern, topic):
                return True
        return False


def _topic_matches(pattern: str, topic: str) -> bool:
    if pattern in ("*", "#", topic):
        return True
    # Prefix wildcard: "tasks.*" or "tasks" matches "tasks.rootcause".
    base = pattern[:-2] if pattern.endswith(".*") else pattern
    return topic == base or topic.startswith(base + ".")


class MessageQueue:
    def __init__(self, history_per_topic: int = 100):
        self._subscribers: dict[str, Subscriber] = {}
        self._history: dict[str, deque[Message]] = defaultdict(
            lambda: deque(maxlen=history_per_topic)
        )
        self._lock = asyncio.Lock()

    async def subscribe(
        self, subscriber_id: str, topics: list[str], replay: int = 0
    ) -> Subscriber:
        async with self._lock:
            sub = Subscriber(subscriber_id, set(topics) or {"*"})
            self._subscribers[subscriber_id] = sub
        if replay > 0:
            for topic_msgs in list(self._history.values()):
                for msg in list(topic_msgs)[-replay:]:
                    if sub.matches(msg.topic):
                        await self._offer(sub, msg)
        return sub

    async def unsubscribe(self, subscriber_id: str) -> None:
        async with self._lock:
            self._subscribers.pop(subscriber_id, None)

    async def update_topics(self, subscriber_id: str, topics: list[str]) -> None:
        async with self._lock:
            sub = self._subscribers.get(subscriber_id)
            if sub:
                sub.topics = set(topics) or {"*"}

    async def publish(self, msg: Message) -> int:
        """Fan ``msg`` out to all matching subscribers. Returns the count delivered."""
        self._history[msg.topic].append(msg)
        async with self._lock:
            targets = [s for s in self._subscribers.values() if s.matches(msg.topic)]
        delivered = 0
        for sub in targets:
            if await self._offer(sub, msg):
                delivered += 1
        return delivered

    async def clear(self, topic: Optional[str] = None) -> int:
        """Flush retained message history (what the dashboard shows). Live
        subscriber queues are left intact so connected agents don't lose
        in-flight messages. Returns the number of messages dropped."""
        async with self._lock:
            if topic is not None:
                dropped = len(self._history.get(topic, []))
                self._history.pop(topic, None)
                return dropped
            dropped = sum(len(msgs) for msgs in self._history.values())
            self._history.clear()
            return dropped

    def history(self, topic: Optional[str] = None, limit: int = 50) -> list[Message]:
        if topic is not None:
            return list(self._history.get(topic, []))[-limit:]
        merged: list[Message] = []
        for msgs in self._history.values():
            merged.extend(msgs)
        merged.sort(key=lambda m: m.ts)
        return merged[-limit:]

    def topics(self) -> list[str]:
        return sorted(self._history.keys())

    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def _offer(self, sub: Subscriber, msg: Message) -> bool:
        try:
            sub.queue.put_nowait(msg)
            return True
        except asyncio.QueueFull:
            # Drop oldest to make room — keep the stream flowing for live agents.
            try:
                sub.queue.get_nowait()
                sub.queue.put_nowait(msg)
                sub.dropped += 1
                return True
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                sub.dropped += 1
                return False
