"""Agent registry: registration, heartbeats, and liveness.

An agent is LIVE while it heartbeats (or holds an open WebSocket) within
``heartbeat_timeout`` seconds. A background sweeper flips silent agents to
INACTIVE so the dashboard reflects reality without the agent doing anything.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from .models import Agent, AgentStatus, RegisterRequest


class AgentRegistry:
    def __init__(self, heartbeat_timeout: float = 30.0, sweep_interval: float = 5.0):
        self.heartbeat_timeout = heartbeat_timeout
        self.sweep_interval = sweep_interval
        self._agents: dict[str, Agent] = {}
        self._lock = asyncio.Lock()
        self._sweeper: Optional[asyncio.Task] = None

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.create_task(self._sweep_loop())

    async def stop(self) -> None:
        if self._sweeper:
            self._sweeper.cancel()
            try:
                await self._sweeper
            except asyncio.CancelledError:
                pass
            self._sweeper = None

    # -- registration ------------------------------------------------------
    async def register(self, req: RegisterRequest) -> Agent:
        async with self._lock:
            # Idempotent re-registration: reuse the id if the agent supplies one
            # it has used before (e.g. after a restart).
            if req.agent_id and req.agent_id in self._agents:
                agent = self._agents[req.agent_id]
                agent.name = req.name
                agent.kind = req.kind
                agent.capabilities = req.capabilities
                agent.subscriptions = req.subscriptions
                agent.metadata = req.metadata
                agent.status = AgentStatus.LIVE
                agent.last_heartbeat = time.time()
                return agent

            agent = Agent(
                name=req.name,
                kind=req.kind,
                capabilities=req.capabilities,
                subscriptions=req.subscriptions,
                metadata=req.metadata,
            )
            if req.agent_id:
                agent.agent_id = req.agent_id
            self._agents[agent.agent_id] = agent
            return agent

    async def clone(self, source_id: str, new_id: Optional[str] = None, name: Optional[str] = None) -> Optional[Agent]:
        """Create a new agent from an existing one's config. Returns None if the
        source is missing; raises ValueError if new_id is already taken. The clone
        starts fresh (LIVE, not connected, no IdP provisioning yet)."""
        async with self._lock:
            src = self._agents.get(source_id)
            if not src:
                return None
            if new_id and new_id in self._agents:
                raise ValueError("agent_exists")
            agent = Agent(
                name=name or f"{src.name}-clone",
                kind=src.kind,
                capabilities=list(src.capabilities),
                subscriptions=list(src.subscriptions),
                metadata=dict(src.metadata),
            )
            if new_id:
                agent.agent_id = new_id
            self._agents[agent.agent_id] = agent
            return agent

    async def deregister(self, agent_id: str) -> bool:
        async with self._lock:
            agent = self._agents.get(agent_id)
            if not agent:
                return False
            agent.status = AgentStatus.DEREGISTERED
            agent.connected = False
            return True

    async def remove(self, agent_id: str) -> bool:
        """Hard-remove an agent from the registry (no tombstone). Used by a hard
        delete so the agent_id becomes reusable."""
        async with self._lock:
            return self._agents.pop(agent_id, None) is not None

    async def heartbeat(self, agent_id: str) -> Optional[Agent]:
        async with self._lock:
            agent = self._agents.get(agent_id)
            if not agent or agent.status == AgentStatus.DEREGISTERED:
                return None
            agent.last_heartbeat = time.time()
            agent.status = AgentStatus.LIVE
            return agent

    async def set_work_state(self, agent_id: str, state: str) -> Optional[Agent]:
        async with self._lock:
            agent = self._agents.get(agent_id)
            if not agent:
                return None
            agent.work_state = state
            # Reporting work proves liveness, so a busy (e.g. blocked-on-approval)
            # agent doesn't get swept to INACTIVE while it's clearly working.
            agent.last_heartbeat = time.time()
            if agent.status != AgentStatus.DEREGISTERED:
                agent.status = AgentStatus.LIVE
            return agent

    async def set_connected(self, agent_id: str, connected: bool) -> None:
        async with self._lock:
            agent = self._agents.get(agent_id)
            if not agent:
                return
            agent.connected = connected
            if connected:
                agent.last_heartbeat = time.time()
                agent.status = AgentStatus.LIVE

    # -- queries -----------------------------------------------------------
    def get(self, agent_id: str) -> Optional[Agent]:
        return self._agents.get(agent_id)

    def list(self, include_deregistered: bool = False) -> list[Agent]:
        agents = list(self._agents.values())
        if not include_deregistered:
            agents = [a for a in agents if a.status != AgentStatus.DEREGISTERED]
        return sorted(agents, key=lambda a: a.registered_at)

    # -- internals ---------------------------------------------------------
    def _evaluate(self, agent: Agent, now: float) -> None:
        if agent.status == AgentStatus.DEREGISTERED:
            return
        # An open WebSocket counts as live regardless of REST heartbeats.
        if agent.connected:
            agent.status = AgentStatus.LIVE
            return
        if agent.seconds_since_heartbeat(now) > self.heartbeat_timeout:
            agent.status = AgentStatus.INACTIVE
            # An agent that stopped heart-beating isn't working an incident any
            # more; clear its work state so the UI shows "inactive", not a stale
            # "blocked"/"investigating".
            agent.work_state = "idle"
        else:
            agent.status = AgentStatus.LIVE

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(self.sweep_interval)
            now = time.time()
            async with self._lock:
                for agent in self._agents.values():
                    self._evaluate(agent, now)
