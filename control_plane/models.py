"""Domain models for the SRE agent control plane.

These models describe agents, their liveness status, and the messages that
flow through the control plane's message queue.
"""
from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


def _now() -> float:
    return time.time()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class AgentStatus(str, Enum):
    """Liveness state derived from heartbeats."""

    LIVE = "live"
    INACTIVE = "inactive"
    DEREGISTERED = "deregistered"


class RuntimeBinding(BaseModel):
    """Where an agent runs — the IdP attests against this on token exchange."""

    kind: Literal["k8s", "spire", "cloud"] = "cloud"
    cluster: str
    namespace: Optional[str] = None
    service_account: Optional[str] = None
    spiffe_id: Optional[str] = None


class IdentitySpec(BaseModel):
    """Identity-provisioning details. When present on registration, the control
    plane provisions a matching agent identity in the IdP under the same id."""

    owner_principal: str = Field(..., description="Human/service that owns this agent.")
    tenant: str = "org:democorp"
    framework: Optional[str] = None
    target_application: Optional[str] = None
    trust_level: Literal["low", "medium", "high"] = "low"
    allowed_envs: list[str] = Field(default_factory=lambda: ["dev"])
    runtime_bindings: list[RuntimeBinding] = Field(default_factory=list)


class RegisterRequest(BaseModel):
    """Payload an agent sends to register with the control plane."""

    name: str = Field(..., description="Human-readable agent name, e.g. 'rootcause-db'")
    kind: str = Field(
        default="sre",
        description="Agent type, e.g. 'rootcause', 'remediation', 'triage'.",
    )
    capabilities: list[str] = Field(
        default_factory=list,
        description="What this agent can do, e.g. ['db-rootcause', 'k8s-triage'].",
    )
    # Topics the agent wants to subscribe to on registration.
    subscriptions: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    # Optional client-supplied id to make registration idempotent across restarts.
    agent_id: Optional[str] = None
    # When set (and the IdP is configured), registration provisions an identity.
    identity: Optional[IdentitySpec] = None


class Agent(BaseModel):
    """A registered agent and its live state."""

    agent_id: str = Field(default_factory=lambda: _new_id("agent"))
    name: str
    kind: str = "sre"
    capabilities: list[str] = Field(default_factory=list)
    subscriptions: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    status: AgentStatus = AgentStatus.LIVE
    registered_at: float = Field(default_factory=_now)
    last_heartbeat: float = Field(default_factory=_now)
    # True while a WebSocket connection is open for this agent.
    connected: bool = False

    # Identity: set when the IdP provisioned an identity for this agent.
    idp_provisioned: bool = False
    idp_error: Optional[str] = None

    def seconds_since_heartbeat(self, now: Optional[float] = None) -> float:
        return (now or _now()) - self.last_heartbeat


class Message(BaseModel):
    """A message published to a topic on the control plane queue."""

    message_id: str = Field(default_factory=lambda: _new_id("msg"))
    topic: str = Field(..., description="Topic/channel, e.g. 'incidents', 'tasks.rootcause'.")
    sender: str = Field(default="control-plane", description="agent_id or 'control-plane'.")
    payload: dict[str, Any] = Field(default_factory=dict)
    ts: float = Field(default_factory=_now)


class PublishRequest(BaseModel):
    topic: str
    payload: dict[str, Any] = Field(default_factory=dict)
    sender: Optional[str] = None


# --------------------------------------------------------------------------- #
# IdP broker request models — the agent-facing shapes for identity flows that
# the control plane forwards to the IdP (adding the admin/internal keys).
# --------------------------------------------------------------------------- #

class TokenRequest(BaseModel):
    """Attestation exchange → agent access token. The agent proves where it runs."""

    env: str = "dev"
    runtime: RuntimeBinding
    session_id: str = "sess"
    trace_id: str = "trace"


class GrantRequest(BaseModel):
    env: str = "dev"
    action: str
    resource: str
    purpose: str
    reason: str
    ticket: str
    granted_by: str = "control-plane"
    grant_type: Literal["human_approval", "policy_auto"] = "policy_auto"
    mfa: bool = False
    ttl_seconds: int = 1800


class CapabilityRequest(BaseModel):
    agent_access_token: str
    grant_id: str
    cap_action: str
    cap_resource: str
    purpose: str
    reason: str
    ticket: str
    session_id: str = "sess"
    trace_id: str = "trace"
    constraints: dict[str, Any] = Field(default_factory=dict)
    risk_level: Literal["low", "medium", "high"] = "low"
    limits: dict[str, Any] = Field(default_factory=dict)


class ExecuteRequest(BaseModel):
    capability_token: str
    tool: Literal["github", "kubernetes", "grafana", "aws"]
    action: str
    resource: str
    params: dict[str, Any] = Field(default_factory=dict)
    presenter: Optional[str] = None


class SkillAuthorizeRequest(BaseModel):
    skill_id: str
    action: Literal["read", "use"] = "read"
    login_hint: str
    binding_message: Optional[str] = None
    reason: Optional[str] = None


class SkillApprovalRequest(BaseModel):
    """An approver agent deciding a pending skill (CIBA) request. The control
    plane checks the approver is registered before brokering to the IdP."""

    auth_req_id: str
    approver_agent_id: str
    decision: Literal["approve", "deny"] = "approve"
    reason: Optional[str] = None


class IntrospectRequest(BaseModel):
    token: str
