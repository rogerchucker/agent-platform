"""Tests for the control plane: registration, liveness, and the message queue."""
import asyncio

import pytest

from control_plane.message_queue import MessageQueue, _topic_matches
from control_plane.models import Message, RegisterRequest
from control_plane.registry import AgentRegistry


@pytest.mark.asyncio
async def test_register_and_list():
    reg = AgentRegistry()
    agent = await reg.register(RegisterRequest(name="rootcause-db", kind="rootcause",
                                                capabilities=["db-rootcause"]))
    assert agent.status == "live"
    assert agent.agent_id.startswith("agent_")
    listed = reg.list()
    assert len(listed) == 1 and listed[0].name == "rootcause-db"


@pytest.mark.asyncio
async def test_idempotent_reregister():
    reg = AgentRegistry()
    a1 = await reg.register(RegisterRequest(name="x", agent_id="fixed-1"))
    a2 = await reg.register(RegisterRequest(name="x-renamed", agent_id="fixed-1"))
    assert a1.agent_id == a2.agent_id == "fixed-1"
    assert len(reg.list()) == 1
    assert reg.get("fixed-1").name == "x-renamed"


@pytest.mark.asyncio
async def test_heartbeat_liveness_transition():
    reg = AgentRegistry(heartbeat_timeout=0.05, sweep_interval=0.02)
    agent = await reg.register(RegisterRequest(name="triage"))
    reg.start()
    await asyncio.sleep(0.12)  # exceed timeout without heartbeats
    assert reg.get(agent.agent_id).status == "inactive"
    await reg.heartbeat(agent.agent_id)
    assert reg.get(agent.agent_id).status == "live"
    await reg.stop()


@pytest.mark.asyncio
async def test_connected_keeps_live():
    reg = AgentRegistry(heartbeat_timeout=0.05, sweep_interval=0.02)
    agent = await reg.register(RegisterRequest(name="streamer"))
    await reg.set_connected(agent.agent_id, True)
    reg.start()
    await asyncio.sleep(0.12)
    assert reg.get(agent.agent_id).status == "live"  # ws connection overrides timeout
    await reg.stop()


@pytest.mark.asyncio
async def test_deregister():
    reg = AgentRegistry()
    agent = await reg.register(RegisterRequest(name="gone"))
    assert await reg.deregister(agent.agent_id)
    assert reg.list() == []
    assert await reg.heartbeat(agent.agent_id) is None  # cannot revive


def test_topic_matching():
    assert _topic_matches("*", "anything")
    assert _topic_matches("tasks", "tasks.rootcause")
    assert _topic_matches("tasks.*", "tasks.triage")
    assert _topic_matches("tasks.rootcause", "tasks.rootcause")
    assert not _topic_matches("tasks", "incidents")
    assert not _topic_matches("tasks.rootcause", "tasks.triage")


@pytest.mark.asyncio
async def test_pubsub_fanout():
    mq = MessageQueue()
    s1 = await mq.subscribe("a1", ["incidents"])
    s2 = await mq.subscribe("a2", ["tasks.*"])
    delivered = await mq.publish(Message(topic="incidents", payload={"sev": 1}))
    assert delivered == 1
    assert (await s1.queue.get()).payload == {"sev": 1}
    assert s2.queue.empty()

    delivered = await mq.publish(Message(topic="tasks.rootcause", payload={"id": 7}))
    assert delivered == 1
    assert (await s2.queue.get()).payload == {"id": 7}


@pytest.mark.asyncio
async def test_history_and_unsubscribe():
    mq = MessageQueue()
    await mq.publish(Message(topic="incidents", payload={"n": 1}))
    await mq.publish(Message(topic="incidents", payload={"n": 2}))
    hist = mq.history(topic="incidents")
    assert [m.payload["n"] for m in hist] == [1, 2]

    await mq.subscribe("a1", ["incidents"])
    assert mq.subscriber_count() == 1
    await mq.unsubscribe("a1")
    assert mq.subscriber_count() == 0
