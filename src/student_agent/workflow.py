"""L3B multi-agent complaint investigation.

Flow (see ARCHITECTURE.md):
    coordinator -> entity-agent -> [order-agent, shipment-agent, payment-agent, policy-agent]
    -> conflict-resolver -> decision (coordinator) -> verifier -> output

Every agent talks to MCP through a ``CaseScopedGateway`` that enforces a per-actor
tool allowlist, caches results inside one case only and emits ``tool_result_consumed``.
"""

from __future__ import annotations

import asyncio
<<<<<<< Updated upstream
import json
import os
import re
from itertools import combinations
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
=======
import hashlib
>>>>>>> Stashed changes
from typing import Any

from . import OUTPUT_SCHEMA_VERSION
from .mcp_gateway import EvidenceGateway
from .trace import TraceWriter

ORDER_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
MONEY_TOLERANCE = Decimal("0.01")
TRANSPORT_RETRIES = 1

# Least privilege: which MCP tools each actor may call.
TOOL_PERMISSIONS: dict[str, frozenset[str]] = {
    "entity-agent": frozenset({"get_customer_history", "get_order"}),
    "order-agent": frozenset({"get_order_items", "get_sellers"}),
    "shipment-agent": frozenset({"get_shipment_summary"}),
    "payment-agent": frozenset({"get_payment_timeline", "get_refund_timeline"}),
    "policy-agent": frozenset({"get_policy"}),
}

# Investigation plan: which optional evidence each claim topic needs. Items, payment
# timeline, customer history and policy are always fetched.
TOPIC_TOOLS: dict[str, frozenset[str]] = {
    "late_delivery_logistics": frozenset({"get_shipment_summary"}),
    "late_delivery_seller": frozenset({"get_shipment_summary", "get_sellers"}),
    "unavailable_order_paid": frozenset({"get_sellers"}),
    "refund_pending": frozenset({"get_refund_timeline"}),
    "refund_failed": frozenset({"get_refund_timeline"}),
    "unsupported_claim": frozenset({"get_shipment_summary"}),
}
OPTIONAL_TOOLS = frozenset({"get_shipment_summary", "get_sellers", "get_refund_timeline"})
CALL_BUDGET = 6
BASE_TOOLS = 4  # customer history, policy, items, payment timeline


def plan_tools(topics: set[str]) -> tuple[frozenset[str], bool]:
    """Optional tools for the claimed topics, and whether get_order fits the budget."""
    tools = frozenset().union(*(TOPIC_TOOLS.get(t, frozenset()) for t in topics))
    fetch_order = BASE_TOOLS + len(tools) + 1 <= CALL_BUDGET
    return tools, fetch_order


ALL_DOMAINS = frozenset({"customer", "item", "payment", "shipment", "refund", "policy"})
_BASE = {"customer", "policy", "item", "payment"}
RELEVANT_DOMAINS: dict[str, frozenset[str]] = {
    "late_delivery_logistics": frozenset(_BASE | {"shipment"}),
    "late_delivery_seller": frozenset(_BASE | {"shipment"}),
    "canceled_order_paid": frozenset(_BASE | {"shipment"}),
    "unavailable_order_paid": frozenset(_BASE | {"shipment"}),
    "unsupported_claim": frozenset(_BASE | {"shipment"}),
    "valid_split_payment": frozenset(_BASE),
    "payment_mismatch": frozenset(_BASE),
    "duplicate_charge": frozenset(_BASE),
    "refund_pending": frozenset(_BASE | {"refund"}),
    "refund_failed": frozenset(_BASE | {"refund"}),
}

ISSUE_TO_PAYMENT_VERDICT = {
    "valid_split_payment": "reconciled",
    "payment_mismatch": "capture_mismatch",
    "duplicate_charge": "duplicate_capture",
    "refund_pending": "refund_pending",
    "refund_failed": "refund_failed",
}


class GatewayUnavailable(RuntimeError):
    """The MCP gateway is failing on tools that always have data; stop the run."""


# --------------------------------------------------------------------------- helpers


def _ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _money(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _num(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _unique(values: list[Any]) -> list[Any]:
    seen: list[Any] = []
    for value in values:
        if value is not None and value not in seen:
            seen.append(value)
    return seen


# --------------------------------------------------------------------------- A2A plumbing


@dataclass
class Evidence:
    tool: str
    ref: str
    domain: str
    data: Any
    warnings: list[str]


@dataclass
class A2AMessage:
    """Envelope for agent-to-agent handoffs, correlated by case_id."""

    case_id: str
    sender: str
    recipient: str
    task: str
    payload: dict[str, Any] = field(default_factory=dict)
    evidence_refs: list[str] = field(default_factory=list)
    status: str = "ok"


class CaseScopedGateway:
    """MCP access for one case: allowlist per actor, in-case cache, bounded retries."""

    def __init__(self, case_id: str, gateway: EvidenceGateway, trace: TraceWriter) -> None:
        self.case_id = case_id
        self._gateway = gateway
        self._trace = trace
        self._cache: dict[tuple[str, tuple[tuple[str, str], ...]], Evidence | None] = {}
        self.evidence: dict[str, Evidence] = {}
        self.calls = 0
        self.failed_tools: list[str] = []
        self.raw: dict[str, Any] = {}

    async def call(self, actor: str, tool: str, **arguments: str) -> Evidence | None:
        if tool not in TOOL_PERMISSIONS.get(actor, frozenset()):
            raise PermissionError(f"{actor} is not allowed to call {tool}")
        key = (tool, tuple(sorted(arguments.items())))
        if key in self._cache:
            return self._cache[key]
        result: Evidence | None = None
        for attempt in range(TRANSPORT_RETRIES + 1):
            try:
                self.calls += 1
                raw = await self._gateway.call(tool, case_id=self.case_id, **arguments)
            except RuntimeError:
                # The tool executed and reported an error (e.g. no rows): not retryable.
                self.failed_tools.append(tool)
                self._trace.emit(
                    case_id=self.case_id,
                    event_type="tool_result_consumed",
                    actor=actor,
                    tool_name=tool,
                    decision_code="TOOL_RETURNED_NO_EVIDENCE",
                )
                break
            except (OSError, TimeoutError, ValueError):
                if attempt >= TRANSPORT_RETRIES:
                    self.failed_tools.append(tool)
                    self._trace.emit(
                        case_id=self.case_id,
                        event_type="tool_result_consumed",
                        actor=actor,
                        tool_name=tool,
                        decision_code="TOOL_TRANSPORT_FAILED",
                    )
                    break
                continue
            self.raw[tool] = raw
            result = Evidence(
                tool=tool,
                ref=raw["evidence_ref"],
                domain=raw["domain"],
                data=raw["data"],
                warnings=list(raw.get("warnings") or []),
            )
            self.evidence[result.ref] = result
            self._trace.emit(
                case_id=self.case_id,
                event_type="tool_result_consumed",
                actor=actor,
                tool_name=tool,
                evidence_refs=[result.ref],
                attributes={"domain": result.domain, "warnings": len(result.warnings)},
            )
            break
        self._cache[key] = result
        return result


class Bus:
    """Records task assignment and handoffs between agents as observable trace events."""

    def __init__(self, case_id: str, trace: TraceWriter) -> None:
        self.case_id = case_id
        self.trace = trace

    def assign(self, sender: str, recipient: str, task: str) -> None:
        self.trace.emit(
            case_id=self.case_id,
            event_type="task_assigned",
            actor=sender,
            target=recipient,
            decision_code=task,
        )

    def handoff(self, message: A2AMessage) -> A2AMessage:
        if message.case_id != self.case_id:
            raise ValueError("A2A message correlated to a different case")
        self.trace.emit(
            case_id=self.case_id,
            event_type="handoff",
            actor=message.sender,
            target=message.recipient,
            decision_code=message.status,
            evidence_refs=message.evidence_refs[:20] or None,
            attributes={"task": message.task},
        )
        return message


# --------------------------------------------------------------------------- entity agent


@dataclass
class Cluster:
    """One purchase episode stored under the (reused) order id."""

    purchase_at: datetime
    order_row: dict[str, Any]


async def entity_agent(
    case: dict[str, Any], mcp: CaseScopedGateway, fetch_order: bool = True
) -> A2AMessage:
    actor = "entity-agent"
    case_id = case["case_id"]
    request = case["customer_request"]
    candidates: list[str] = list(case.get("candidate_order_ids") or [])
    claimed = request.get("claimed_order_id")
    hint = case.get("customer_unique_id_hint")

    history = (
        await mcp.call(actor, "get_customer_history", customer_unique_id=hint) if hint else None
    )
    history_orders: list[dict[str, Any]] = []
    customer_unique_id = None
    if history and isinstance(history.data, dict):
        customer_unique_id = history.data.get("customer_unique_id")
        history_orders = [row for row in history.data.get("orders") or [] if isinstance(row, dict)]
    history_ids = _unique([row.get("order_id") for row in history_orders])

    # Rank candidates: well-formed id owned by the customer > claimed id > anything else.
    def score(candidate: str) -> int:
        points = 0
        if ORDER_ID_PATTERN.fullmatch(candidate or ""):
            points += 1
        if candidate in history_ids:
            points += 4
        if candidate == claimed:
            points += 2
        return points

    ranked = sorted(candidates, key=score, reverse=True)
    viable = [c for c in ranked if score(c) >= 5 or (not history_ids and score(c) >= 3)]
    resolved: list[str] = viable[:1]
    if len(viable) > 1 and score(viable[0]) == score(viable[1]):
        resolved = []
    rejected = [c for c in candidates if c not in resolved]

    # Ownership is proven by the customer's authoritative history.
    if resolved and resolved[0] not in history_ids:
        status = "not_found"
        resolved = []
        rejected = list(candidates)
    elif resolved:
        status = "resolved"
    elif viable:
        status = "ambiguous"
    else:
        status = "not_found"

    if status == "resolved":
        confidence = 0.95 if resolved[0] in history_ids else 0.7
    else:
        confidence = 0.4

    order = (
        await mcp.call(actor, "get_order", order_id=resolved[0])
        if resolved and status == "resolved" and fetch_order
        else None
    )
    refs = [e.ref for e in (history, order) if e is not None]
    return A2AMessage(
        case_id=case_id,
        sender=actor,
        recipient="coordinator",
        task="resolve_entity",
        status=status,
        evidence_refs=refs,
        payload={
            "status": status,
            "order_id": resolved[0] if resolved else None,
            "resolved": resolved,
            "rejected": rejected,
            "confidence": confidence,
            "customer_unique_id": customer_unique_id or (hint if history else None),
            "related_order_ids": history_ids,
            "history_rows": [r for r in history_orders if not resolved or r.get("order_id") in resolved],
            "order_row": order.data if order and isinstance(order.data, dict) else None,
        },
    )


# --------------------------------------------------------------------------- specialists


async def _maybe(
    mcp: CaseScopedGateway, actor: str, tool: str, tools: frozenset[str], **arguments: str
) -> Evidence | None:
    """Call an optional tool only when it is part of the investigation plan."""
    return await mcp.call(actor, tool, **arguments) if tool in tools else None


async def order_agent(
    case_id: str, order_id: str, mcp: CaseScopedGateway, tools: frozenset[str]
) -> A2AMessage:
    actor = "order-agent"
    items, sellers = await asyncio.gather(
        mcp.call(actor, "get_order_items", order_id=order_id),
        _maybe(mcp, actor, "get_sellers", tools, order_id=order_id),
    )
    return A2AMessage(
        case_id=case_id,
        sender=actor,
        recipient="coordinator",
        task="order_items",
        status="ok" if items else "missing_evidence",
        evidence_refs=[e.ref for e in (items, sellers) if e],
        payload={
            "items": items.data if items and isinstance(items.data, list) else [],
            "sellers": sellers.data if sellers and isinstance(sellers.data, list) else [],
        },
    )


async def shipment_agent(case_id: str, order_id: str, mcp: CaseScopedGateway) -> A2AMessage:
    actor = "shipment-agent"
    shipment = await mcp.call(actor, "get_shipment_summary", order_id=order_id)
    return A2AMessage(
        case_id=case_id,
        sender=actor,
        recipient="coordinator",
        task="shipment_timeline",
        status="ok" if shipment else "missing_evidence",
        evidence_refs=[shipment.ref] if shipment else [],
        payload={"shipment": shipment.data if shipment and isinstance(shipment.data, dict) else {}},
    )


async def payment_agent(
    case_id: str, order_id: str, mcp: CaseScopedGateway, tools: frozenset[str]
) -> A2AMessage:
    actor = "payment-agent"
    timeline, refunds = await asyncio.gather(
        mcp.call(actor, "get_payment_timeline", order_id=order_id),
        _maybe(mcp, actor, "get_refund_timeline", tools, order_id=order_id),
    )
    return A2AMessage(
        case_id=case_id,
        sender=actor,
        recipient="coordinator",
        task="payment_refund_timeline",
        status="ok" if timeline else "missing_evidence",
        evidence_refs=[e.ref for e in (timeline, refunds) if e],
        payload={
            "payment": timeline.data if timeline and isinstance(timeline.data, dict) else {},
            # get_refund_timeline errors when the order has no refund lifecycle at all.
            "refund_events": (refunds.data or {}).get("events", []) if refunds else [],
            "refund_evidence": refunds is not None,
        },
    )


async def policy_agent(case: dict[str, Any], mcp: CaseScopedGateway) -> A2AMessage:
    actor = "policy-agent"
    policy = await mcp.call(actor, "get_policy", policy_version=case["policy_version"])
    return A2AMessage(
        case_id=case["case_id"],
        sender=actor,
        recipient="coordinator",
        task="policy_rules",
        status="ok" if policy else "missing_evidence",
        evidence_refs=[policy.ref] if policy else [],
        payload={"rules": (policy.data or {}).get("rules", {}) if policy else {}},
    )


# --------------------------------------------------------------------------- conflict resolver


def _cluster_for(clusters: list[Cluster], moment: datetime | None) -> int | None:
    """Index of the purchase episode an event belongs to: latest purchase not after it."""
    if moment is None:
        return None
    best = None
    for index, cluster in enumerate(clusters):
        if cluster.purchase_at <= moment and (
            best is None or cluster.purchase_at > clusters[best].purchase_at
        ):
            best = index
    return best


def conflict_resolver(
    case: dict[str, Any],
    entity: dict[str, Any],
    order: dict[str, Any],
    shipment: dict[str, Any],
    payment: dict[str, Any],
    claimed_topics: set[str],
) -> dict[str, Any]:
    """Split mixed rows into purchase episodes and select the one the complaint is about."""
    opened_at = _ts(case.get("opened_at"))
    rows = list(entity.get("history_rows") or [])
    order_row = entity.get("order_row") or {}
    if order_row and not any(
        r.get("order_purchase_timestamp") == order_row.get("order_purchase_timestamp") for r in rows
    ):
        rows.append(order_row)
    if not shipment.get("shipping_limits"):
        # Without a shipment summary the item rows carry the seller handoff limits.
        shipment = {
            **shipment,
            "shipping_limits": [
                {
                    "order_item_id": i.get("order_item_id"),
                    "seller_id": i.get("seller_id"),
                    "shipping_limit_at": i.get("shipping_limit_date"),
                }
                for i in order.get("items", [])
            ],
        }
    clusters = sorted(
        (
            Cluster(_ts(r.get("order_purchase_timestamp")), r)  # type: ignore[arg-type]
            for r in rows
            if _ts(r.get("order_purchase_timestamp"))
        ),
        key=lambda c: c.purchase_at,
    )
    conflicts: list[dict[str, Any]] = []
    if not clusters:
        return {
            **EMPTY_EPISODE,
            "finding": analyse(EMPTY_EPISODE),
            "cluster_count": 0,
            "conflicts": conflicts,
        }

    all_captures = [
        e for e in payment.get("payment", {}).get("events", []) if e.get("event_type") == "captured"
    ]

    def refund_cluster(event: dict[str, Any]) -> int | None:
        # Refunds are linked to the episode whose capture has the same amount;
        # time is only the fallback because refunds are often requested much later.
        amount = _money(event.get("amount_brl"))
        owners = _unique(
            [
                _cluster_for(clusters, _ts(c.get("event_at")))
                for c in all_captures
                if _money(c.get("amount_brl")) == amount
            ]
        )
        if len(owners) == 1:
            return owners[0]
        return _cluster_for(clusters, _ts(event.get("event_at")))

    def build(index: int) -> dict[str, Any]:
        def inside(moment: Any) -> bool:
            return _cluster_for(clusters, _ts(moment)) == index

        items: list[dict[str, Any]] = []
        for item in order.get("items", []):
            if inside(item.get("shipping_limit_date")) and item not in items:
                items.append(item)
        limits: list[dict[str, Any]] = []
        for limit in shipment.get("shipping_limits", []):
            if inside(limit.get("shipping_limit_at")) and limit not in limits:
                limits.append(limit)
        # Byte-identical events (same time, type, amount) are one fact duplicated by the
        # source, not a second capture; true duplicate charges have distinct timestamps.
        pay_events: list[dict[str, Any]] = []
        for event in payment.get("payment", {}).get("events", []):
            if inside(event.get("event_at")) and event not in pay_events:
                pay_events.append(event)
        remaining = [
            _money(e.get("amount_brl")) for e in pay_events if e.get("event_type") == "captured"
        ]
        pay_rows = []
        for row in payment.get("payment", {}).get("payments", []):
            amount = _money(row.get("payment_value"))
            if amount in remaining:
                remaining.remove(amount)
                pay_rows.append(row)
        return {
            "selected": clusters[index].order_row,
            "items": items,
            "shipping_limits": limits,
            "shipment_events": [e for e in shipment.get("events", []) if inside(e.get("event_at"))],
            "payment_events": pay_events,
            "payment_rows": pay_rows,
            "refund_events": [
                e for e in payment.get("refund_events", []) if refund_cluster(e) == index
            ],
        }

    episodes = [build(i) for i in range(len(clusters))]
    findings = [analyse(ep, claimed_topics) for ep in episodes]

    def recency(index: int) -> tuple[int, datetime]:
        before_open = opened_at is None or clusters[index].purchase_at <= opened_at
        return (1 if before_open else 0, clusters[index].purchase_at)

    supporting = [i for i, f in enumerate(findings) if f["issue"] in claimed_topics]
    if supporting:
        selected = max(supporting, key=recency)
        selection_code = "EPISODE_WITH_EVIDENCE_FOR_CLAIM"
    else:
        selected = max(range(len(clusters)), key=recency)
        selection_code = "LATEST_EPISODE_BEFORE_CASE_OPENED"
    chosen = clusters[selected]
    episode = episodes[selected]
    same_time = [c for c in clusters if c.purchase_at == chosen.purchase_at]

    if len(clusters) > 1:
        # get_order / the shipment summary header describe a single order row that may
        # belong to a different purchase episode than the one selected from the history.
        if order_row:
            header_source = "get_order"
            header_differs = order_row.get("order_purchase_timestamp") != chosen.order_row.get(
                "order_purchase_timestamp"
            )
        else:
            header_source = "get_shipment_summary"
            header_differs = "delivered_customer_at" in shipment and shipment.get(
                "delivered_customer_at"
            ) != chosen.order_row.get("order_delivered_customer_date")
        if header_differs:
            conflicts.append(
                {
                    "field": "order.order_purchase_timestamp",
                    "sources": [header_source, "get_customer_history"],
                    "selected_source": "get_customer_history",
                    "resolution_code": selection_code,
                }
            )
        if len(_unique([c.order_row.get("order_status") for c in clusters])) > 1:
            conflicts.append(
                {
                    "field": "order.order_status",
                    "sources": [header_source, "get_customer_history"],
                    "selected_source": "get_customer_history",
                    "resolution_code": selection_code,
                }
            )
        if len(same_time) > 1:
            conflicts.append(
                {
                    "field": "payment.captured_events",
                    "sources": ["get_payment_timeline", "get_refund_timeline"],
                    "selected_source": "get_payment_timeline",
                    "resolution_code": "COLLIDING_EPISODES_SPLIT_BY_AMOUNT",
                }
            )
        if len(order.get("items", [])) > len(episode["items"]):
            conflicts.append(
                {
                    "field": "order_items.shipping_limit_date",
                    "sources": ["get_order_items", "get_shipment_summary"],
                    "selected_source": "get_shipment_summary",
                    "resolution_code": "ITEM_ROWS_SCOPED_TO_SELECTED_EPISODE",
                }
            )
        if len(payment.get("payment", {}).get("payments", [])) > len(episode["payment_rows"]):
            conflicts.append(
                {
                    "field": "payments.payment_value",
                    "sources": ["get_payment_timeline", "get_customer_history"],
                    "selected_source": "get_payment_timeline",
                    "resolution_code": "PAYMENT_ROWS_MATCHED_TO_EPISODE_CAPTURES",
                }
            )
    return {
        **episode,
        "finding": findings[selected],
        "cluster_count": len(clusters),
        "selection_code": selection_code,
        "claim_supported": bool(supporting),
        "conflicts": conflicts[:5],
    }


# --------------------------------------------------------------------------- decision

ISSUE_PRECEDENCE = [
    "canceled_order_paid",
    "unavailable_order_paid",
    "refund_failed",
    "refund_pending",
    "payment_mismatch",
    "duplicate_charge",
    "late_delivery_seller",
    "late_delivery_logistics",
    "valid_split_payment",
    "unsupported_claim",
]

EMPTY_EPISODE: dict[str, Any] = {
    "selected": None,
    "items": [],
    "shipping_limits": [],
    "shipment_events": [],
    "payment_events": [],
    "payment_rows": [],
    "refund_events": [],
}


def _split_group(amounts: list[Decimal], target: Decimal) -> list[int] | None:
    """Indexes of >=2 captures whose sum equals the order value (a legitimate split)."""
    if target <= 0:
        return None
    for size in range(2, len(amounts) + 1):
        for combo in combinations(range(len(amounts)), size):
            if abs(sum(amounts[i] for i in combo) - target) <= MONEY_TOLERANCE:
                return list(combo)
    return None


def analyse(episode: dict[str, Any], preferred: set[str] | None = None) -> dict[str, Any]:
    """Evaluate one purchase episode. ``preferred`` breaks ties between co-existing signals."""
    order = episode["selected"] or {}
    status = order.get("order_status")
    items = episode["items"]
    expected_total = sum(
        ((_money(i.get("price")) or Decimal(0)) + (_money(i.get("freight_value")) or Decimal(0)))
        for i in items
    )
    freight_total = sum((_money(i.get("freight_value")) or Decimal(0)) for i in items)
    captures = [e for e in episode["payment_events"] if e.get("event_type") == "captured"]
    captured_amounts = [_money(e.get("amount_brl")) or Decimal(0) for e in captures]
    captured_total = sum(captured_amounts, Decimal(0))
    mismatch_events = [
        e for e in episode["payment_events"] if e.get("event_type") == "reconciliation_mismatch"
    ]
    refunds = episode["refund_events"]
    refunded_total = sum(
        (_money(e.get("amount_brl")) or Decimal(0))
        for e in refunds
        if e.get("status") in {"completed", "succeeded", "success", "refunded", "confirmed"}
    )
    failed_refunds = [e for e in refunds if e.get("status") == "failed"]
    pending_refunds = [
        e for e in refunds if e.get("status") in {"pending", "requested", "processing"}
    ]

    # Shipment
    delivered = _ts(order.get("order_delivered_customer_date"))
    estimated = _ts(order.get("order_estimated_delivery_date"))
    carrier = _ts(order.get("order_delivered_carrier_date"))
    late_sellers = _unique(
        [
            s.get("seller_id")
            for s in episode["shipping_limits"]
            if carrier
            and _ts(s.get("shipping_limit_at"))
            and carrier > _ts(s.get("shipping_limit_at"))
        ]
    )
    event_actors = {
        e.get("actor") for e in episode["shipment_events"] if e.get("event_type") == "delivered_late"
    }
    timeline_complete = bool(
        order.get("order_purchase_timestamp") and carrier and delivered and estimated
    )
    is_late = bool(delivered and estimated and delivered > estimated)
    shipment_conflict = False
    if status in {"canceled", "unavailable"}:
        shipment_verdict = "insufficient_evidence"
    elif is_late:
        # Timestamps are authoritative: carrier pickup after the seller's shipping limit
        # means the seller caused the delay. The event actor is only a cross-check.
        shipment_verdict = "seller_delay" if late_sellers else "logistics_delay"
        expected_actor = "seller" if late_sellers else "logistics_provider"
        shipment_conflict = bool(event_actors) and expected_actor not in event_actors
    elif delivered and estimated:
        shipment_verdict = "on_time"
    else:
        shipment_verdict = "insufficient_evidence"
    if shipment_verdict != "seller_delay":
        late_sellers = []

    # Payment pattern: a split group sums to the order value; repeated amounts outside it
    # are duplicate captures.
    split_idx = _split_group(captured_amounts, expected_total)
    rest = [a for i, a in enumerate(captured_amounts) if not split_idx or i not in split_idx]
    duplicate_amount = Decimal(0)
    seen: list[Decimal] = []
    for amount in rest:
        if amount in seen:
            duplicate_amount += amount
        else:
            seen.append(amount)

    signals: set[str] = set()
    if order:
        if status == "canceled" and captured_total > 0:
            signals.add("canceled_order_paid")
        if status == "unavailable" and captured_total > 0:
            signals.add("unavailable_order_paid")
        if failed_refunds:
            signals.add("refund_failed")
        if pending_refunds:
            signals.add("refund_pending")
        if mismatch_events:
            signals.add("payment_mismatch")
        if duplicate_amount > 0:
            signals.add("duplicate_charge")
        if shipment_verdict == "seller_delay":
            signals.add("late_delivery_seller")
        if shipment_verdict == "logistics_delay":
            signals.add("late_delivery_logistics")
        if split_idx:
            signals.add("valid_split_payment")
        if captures and not signals:
            signals.add("unsupported_claim")

    preferred_hits = [i for i in ISSUE_PRECEDENCE if i in signals and i in (preferred or set())]
    ordered = [i for i in ISSUE_PRECEDENCE if i in signals]
    issue = (preferred_hits or ordered or ["insufficient_evidence"])[0]

    if issue == "valid_split_payment" and split_idx:
        # Captures outside the split group belong to a colliding episode.
        captured_total = sum((captured_amounts[i] for i in split_idx), Decimal(0))

    # Data-derived refund amount, cross-checked against policy later.
    if issue in {"canceled_order_paid", "unavailable_order_paid"}:
        derived_refund = captured_total - refunded_total
    elif issue == "duplicate_charge":
        derived_refund = duplicate_amount
    elif issue == "payment_mismatch":
        derived_refund = sum((_money(e.get("amount_brl")) or Decimal(0)) for e in mismatch_events)
    elif issue == "refund_failed":
        derived_refund = sum((_money(e.get("amount_brl")) or Decimal(0)) for e in failed_refunds)
    elif issue in {"late_delivery_seller", "late_delivery_logistics"}:
        derived_refund = min(freight_total, captured_total) if captured_total else freight_total
    else:
        derived_refund = Decimal(0)

    if issue in ISSUE_TO_PAYMENT_VERDICT:
        payment_verdict = ISSUE_TO_PAYMENT_VERDICT[issue]
    elif refunded_total > 0 and refunded_total >= captured_total:
        payment_verdict = "refunded"
    elif captures:
        payment_verdict = "reconciled"
    else:
        payment_verdict = "insufficient_evidence"

    return {
        "issue": issue,
        "signals": sorted(signals),
        "order_status": status,
        "captured_total": captured_total if captures else None,
        "refunded_total": refunded_total,
        "pending_total": sum((_money(e.get("amount_brl")) or Decimal(0)) for e in pending_refunds),
        "derived_refund": derived_refund,
        "shipment_verdict": shipment_verdict,
        "shipment_conflict": shipment_conflict,
        "late_sellers": late_sellers,
        "timeline_complete": timeline_complete,
        "payment_verdict": payment_verdict,
    }


# --------------------------------------------------------------------------- verifier


def verifier(output: dict[str, Any], mcp: CaseScopedGateway, entity_rejected: list[str]) -> list[str]:
    """Deterministic invariants checked before finalize. Returns violated invariant codes."""
    problems: list[str] = []
    refs = output["evidence_refs"]
    if not refs:
        problems.append("NO_EVIDENCE")
    domains = {mcp.evidence[r].domain for r in refs if r in mcp.evidence}
    if not {"customer", "policy"} <= domains:
        problems.append("MISSING_CORE_EVIDENCE")
    if any(ref not in mcp.evidence for ref in refs):
        problems.append("FOREIGN_EVIDENCE_REF")
    resolved = set(output["entity_resolution"]["resolved_order_ids"])
    if resolved & set(entity_rejected):
        problems.append("RESOLVED_AND_REJECTED_OVERLAP")
    if set(output["affected_entities"]["order_ids"]) - resolved:
        problems.append("ENTITY_OUT_OF_SCOPE")
    fin = output["financial_resolution"]
    line_total = sum(Decimal(str(line["amount_brl"])) for line in fin["refund_lines"])
    if abs(line_total - Decimal(str(fin["recommended_refund_brl"]))) > MONEY_TOLERANCE:
        problems.append("REFUND_LINES_MISMATCH")
    captured = output["payment_analysis"]["captured_total_brl"]
    if captured is not None and fin["recommended_refund_brl"] > captured + 0.01:
        problems.append("REFUND_EXCEEDS_CAPTURE")
    status = output["assessment"]["case_status"]
    if status == "no_action" and fin["recommended_refund_brl"] > 0:
        problems.append("NO_ACTION_WITH_REFUND")
    if status == "action_required" and not output["resolution_actions"]:
        problems.append("ACTION_REQUIRED_WITHOUT_ACTION")
    parties = output["root_cause_analysis"]["responsible_parties"]
    if output["shipment_analysis"]["verdict"] == "seller_delay":
        sellers = {p["party_id"] for p in parties if p["party_type"] == "seller"}
        if not sellers or not sellers <= set(output["affected_entities"]["seller_ids"]):
            problems.append("SELLER_RESPONSIBILITY_MISMATCH")
    return problems


# --------------------------------------------------------------------------- coordinator


class CaseCache:
    """Per-case MCP call cache."""

    def __init__(self) -> None:
        self._store: dict[str, dict[str, Any]] = {}

    def _key(self, tool_name: str, args: dict[str, Any]) -> str:
        """Generate cache key from tool name and arguments."""
        args_str = str(sorted((k, v) for k, v in args.items() if k != "case_id"))
        return f"{tool_name}:{hashlib.md5(args_str.encode()).hexdigest()[:12]}"

    def get(self, tool_name: str, args: dict[str, Any]) -> dict[str, Any] | None:
        return self._store.get(self._key(tool_name, args))

    def set(self, tool_name: str, args: dict[str, Any], evidence: dict[str, Any]) -> None:
        self._store[self._key(tool_name, args)] = evidence


class MCPClient:
    """Wrapper around EvidenceGateway with caching, retries, and trace emission."""

    def __init__(
        self,
        gateway: EvidenceGateway,
        trace: TraceWriter,
        case_id: str,
        actor: str,
        cache: CaseCache,
    ) -> None:
        self.gateway = gateway
        self.trace = trace
        self.case_id = case_id
        self.actor = actor
        self.cache = cache
        self.semaphore = asyncio.Semaphore(5)  # max 5 concurrent calls

    async def call(
        self,
        tool_name: str,
        purpose: str,
        **args: Any,
    ) -> dict[str, Any] | None:
        """Call MCP tool with caching, retries, and trace emission."""
        cache_args = {"case_id": self.case_id, **args}
        cached = self.cache.get(tool_name, cache_args)
        if cached:
            return cached

        async with self.semaphore:
            for attempt in range(3):  # max 2 retries = 3 attempts
                try:
                    evidence = await self.gateway.call(tool_name, case_id=self.case_id, **args)
                    self.cache.set(tool_name, cache_args, evidence)
                    self.trace.emit(
                        case_id=self.case_id,
                        event_type="tool_result_consumed",
                        actor=self.actor,
                        tool_name=tool_name,
                        evidence_refs=[evidence["evidence_ref"]],
                        attributes={"purpose": purpose},
                    )
                    return evidence
                except Exception as exc:
                    if attempt == 2:  # last attempt failed
                        code = "MCP_TIMEOUT" if "timeout" in str(exc).lower() else "MCP_ERROR"
                        self.trace.emit(
                            case_id=self.case_id,
                            event_type="tool_failed",
                            actor=self.actor,
                            tool_name=tool_name,
                            decision_code=code,
                            attributes={"purpose": purpose, "error": str(exc)[:200]},
                        )
                        return None
                    await asyncio.sleep(2**attempt)  # exponential backoff
            return None


class EntityResolver:
    """Resolve customer_unique_id and order_id from case candidates."""

    def __init__(
        self,
        mcp: MCPClient,
        trace: TraceWriter,
        case_id: str,
        case: dict[str, Any],
    ) -> None:
        self.mcp = mcp
        self.trace = trace
        self.case_id = case_id
        self.case = case

    async def resolve(self) -> dict[str, Any]:
        # Input uses candidate_order_ids (strings) + customer_unique_id_hint
        candidate_ids = self.case.get("candidate_order_ids", [])
        customer_hint = self.case.get("customer_unique_id_hint")
        claimed_order_id = self.case.get("customer_request", {}).get("claimed_order_id")

        if not candidate_ids:
            return {
                "status": "not_found",
                "resolved_order_ids": [],
                "rejected_candidates": [],
                "confidence": 0.0,
                "customer_unique_id": customer_hint,
                "order_id": None,
            }

        # Build candidate objects from IDs
        candidates = []
        for cid in candidate_ids:
            candidates.append({
                "order_id": cid,
                "customer_unique_id": customer_hint,
            })

        scored = []
        for c in candidates:
            score = 0
            reasons = []

            # 1. Exact match with claimed_order_id (40 pts)
            if c.get("order_id") and claimed_order_id == c["order_id"]:
                score += 40
                reasons.append("claimed_order_id_match")

            # 2. Customer hint match (20 pts)
            if customer_hint and c.get("customer_unique_id") == customer_hint:
                score += 20
                reasons.append("customer_hint_match")

            # 3. Default base score
            score += 20
            reasons.append("candidate_present")

            scored.append((score, c, reasons))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_candidate, reasons = scored[0]
        confidence = min(best_score / 100.0, 1.0)

        # Threshold logic
        if confidence >= 0.75:
            status = "resolved"
        elif confidence >= 0.4:
            status = "ambiguous"
        else:
            status = "not_found"

        best_oid = best_candidate.get("order_id")
        resolved_order_ids = [best_oid] if best_oid else []
        rejected = [c.get("order_id") for _, c, _ in scored[1:] if c.get("order_id")]

        self.trace.emit(
            case_id=self.case_id,
            event_type="task_assigned",
            actor="entity_resolver",
            target="coordinator",
            decision_code=status,
            attributes={
                "confidence": confidence,
                "resolved_order_id": best_candidate.get("order_id"),
                "rejected_count": len(rejected),
            },
        )

        return {
            "status": status,
            "resolved_order_ids": resolved_order_ids,
            "rejected_candidates": rejected,
            "confidence": round(confidence, 3),
            "customer_unique_id": best_candidate.get("customer_unique_id"),
            "order_id": best_candidate.get("order_id"),
            "scoring_reasons": reasons,
        }


class OrderProductAgent:
    """Order, items, product context, sellers."""

    def __init__(self, mcp: MCPClient) -> None:
        self.mcp = mcp

    async def investigate(self, order_id: str) -> dict[str, Any]:
        results = await asyncio.gather(
            self.mcp.call("get_order", "order_verification", order_id=order_id),
            self.mcp.call("get_order_items", "order_items", order_id=order_id),
            self.mcp.call("get_product_context", "product_context", order_id=order_id),
            self.mcp.call("get_sellers", "sellers", order_id=order_id),
            return_exceptions=True,
        )

        order_ev, items_ev, product_ev, sellers_ev = results

        evidence_refs = []
        for ev in [order_ev, items_ev, product_ev, sellers_ev]:
            if ev and isinstance(ev, dict) and ev.get("evidence_ref"):
                evidence_refs.append(ev["evidence_ref"])

        return {
            "order": order_ev["data"] if order_ev and isinstance(order_ev, dict) else None,
            "items": items_ev["data"] if items_ev and isinstance(items_ev, dict) else None,
            "product": product_ev["data"] if product_ev and isinstance(product_ev, dict) else None,
            "sellers": sellers_ev["data"] if sellers_ev and isinstance(sellers_ev, dict) else None,
            "evidence_refs": evidence_refs,
        }


class ShipmentAgent:
    """Shipment timeline and status."""

    def __init__(self, mcp: MCPClient) -> None:
        self.mcp = mcp

    async def investigate(self, order_id: str) -> dict[str, Any]:
        ev = await self.mcp.call("get_shipment_summary", "shipment_timeline", order_id=order_id)

        evidence_refs = [ev["evidence_ref"]] if ev and ev.get("evidence_ref") else []

        return {
            "summary": ev["data"] if ev and isinstance(ev, dict) else None,
            "evidence_refs": evidence_refs,
        }


class PaymentRefundAgent:
    """Payment and refund analysis."""

    def __init__(self, mcp: MCPClient) -> None:
        self.mcp = mcp

    async def investigate(self, order_id: str) -> dict[str, Any]:
        results = await asyncio.gather(
            self.mcp.call("get_order_payments", "payment_details", order_id=order_id),
            self.mcp.call("get_payment_timeline", "payment_timeline", order_id=order_id),
            self.mcp.call("get_refund_timeline", "refund_timeline", order_id=order_id),
            return_exceptions=True,
        )

        payments_ev, timeline_ev, refund_ev = results

        evidence_refs = []
        for ev in [payments_ev, timeline_ev, refund_ev]:
            if ev and isinstance(ev, dict) and ev.get("evidence_ref"):
                evidence_refs.append(ev["evidence_ref"])

        return {
            "payments": (
                payments_ev["data"] if payments_ev and isinstance(payments_ev, dict) else None
            ),
            "timeline": (
                timeline_ev["data"] if timeline_ev and isinstance(timeline_ev, dict) else None
            ),
            "refunds": (
                refund_ev["data"] if refund_ev and isinstance(refund_ev, dict) else None
            ),
            "evidence_refs": evidence_refs,
        }


class PolicyAgent:
    """Policy lookup."""

    def __init__(self, mcp: MCPClient) -> None:
        self.mcp = mcp

    async def investigate(self, category: str, complaint_text: str) -> dict[str, Any]:
        ev = await self.mcp.call(
            "get_policy", "policy_lookup", category=category, complaint_text=complaint_text
        )

        evidence_refs = [ev["evidence_ref"]] if ev and ev.get("evidence_ref") else []

        return {
            "policy": ev["data"] if ev and isinstance(ev, dict) else None,
            "evidence_refs": evidence_refs,
        }


class CustomerAgent:
    """Customer history."""

    def __init__(self, mcp: MCPClient) -> None:
        self.mcp = mcp

    async def investigate(self, customer_unique_id: str) -> dict[str, Any]:
        ev = await self.mcp.call(
            "get_customer_history", "customer_history", customer_unique_id=customer_unique_id
        )

        evidence_refs = [ev["evidence_ref"]] if ev and ev.get("evidence_ref") else []

        return {
            "history": ev["data"] if ev and isinstance(ev, dict) else None,
            "evidence_refs": evidence_refs,
        }


class ConflictResolver:
    """Detect and resolve evidence conflicts."""

    def __init__(self, trace: TraceWriter, case_id: str) -> None:
        self.trace = trace
        self.case_id = case_id

    def resolve(
        self,
        all_evidence: dict[str, Any],
        entity_resolution: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return (data_conflicts, resolution_notes)."""
        conflicts = []
        resolution_notes = []

        # Example: shipment delivered but payment pending
        shipment = all_evidence.get("shipment", {}).get("summary")
        payment = all_evidence.get("payment", {}).get("payments")

        if shipment and payment:
            shipment_status = shipment.get("status") if isinstance(shipment, dict) else None
            payment_status = payment.get("status") if isinstance(payment, dict) else None

            if shipment_status == "delivered" and payment_status == "pending":
                conflicts.append({
                    "field": "payment_status_vs_shipment",
                    "sources": ["shipment_summary", "payment_timeline"],
                    "selected_source": "shipment_summary",
                    "resolution_code": "SHIPMENT_PRECEDENCE",
                })
                resolution_notes.append(
                "Shipment shows delivered; payment pending resolved to captured"
            )

        # Amount mismatch: order total vs payment sum
        order = all_evidence.get("order", {}).get("order")
        payments = all_evidence.get("payment", {}).get("payments")
        if order and payments:
            order_total = order.get("total_amount_brl") if isinstance(order, dict) else None
            if isinstance(payments, list):
                payment_sum = sum(p.get("amount_brl", 0) for p in payments if isinstance(p, dict))
                if order_total and abs(order_total - payment_sum) > 0.01:
                    conflicts.append({
                        "field": "order_total_vs_payments",
                        "sources": ["order", "order_payments"],
                        "selected_source": "order_payments",
                        "resolution_code": "PAYMENT_SUM_PRECEDENCE",
                    })
                    resolution_notes.append("Payment sum used as authoritative")

        # Emit trace for each conflict
        for c in conflicts:
            self.trace.emit(
                case_id=self.case_id,
                event_type="policy_decided",
                actor="conflict_resolver",
                decision_code=c["resolution_code"],
                attributes={"field": c["field"], "selected_source": c["selected_source"]},
            )

        return conflicts, resolution_notes


class Verifier:
    """Pre-finalize invariant checks."""

    def __init__(self, trace: TraceWriter, case_id: str) -> None:
        self.trace = trace
        self.case_id = case_id

    def verify(
        self,
        output: dict[str, Any],
        all_evidence: dict[str, Any],
        entity_res: dict[str, Any],
    ) -> list[str]:
        """Return list of issues (empty = pass)."""
        issues = []

        # 1. Schema valid - handled by caller
        # 2. Entity scope
        if output.get("case_id") != self.case_id:
            issues.append("case_id mismatch")

        # 3. Rejected candidates present
        ent_status = entity_res.get("status")
        ent_res = output.get("entity_resolution", {})
        if ent_status in ("resolved", "ambiguous") and not ent_res.get("rejected_candidates"):
            issues.append("missing rejected_candidates")

        # 4. Evidence ownership
        all_refs = set()
        for ev in all_evidence.values():
            all_refs.update(ev.get("evidence_refs", []))
        output_refs = set(output.get("evidence_refs", []))
        orphan_refs = output_refs - all_refs
        if orphan_refs:
            issues.append(f"orphan evidence_refs: {orphan_refs}")

        # 5. Claim linkage
        for claim in output.get("claim_assessments", []):
            if not claim.get("evidence_refs"):
                issues.append(f"claim {claim.get('claim_id')} has no evidence_refs")

        # 6. Timeline consistency
        shipment = all_evidence.get("shipment", {}).get("summary")
        payment = all_evidence.get("payment", {}).get("timeline")
        if (
            shipment
            and payment
            and isinstance(shipment, dict)
            and isinstance(payment, dict)
        ):
            delivered = shipment.get("delivered_at")
            paid = payment.get("completed_at")
            if delivered and paid and delivered > paid:
                issues.append("shipment delivered after payment completed")

        # 7. Payment/refund totals
        payments = all_evidence.get("payment", {}).get("payments")
        order = all_evidence.get("order", {}).get("order")
        if (
            payments
            and order
            and isinstance(payments, list)
            and isinstance(order, dict)
        ):
            payment_sum = sum(p.get("amount_brl", 0) for p in payments if isinstance(p, dict))
            order_total = order.get("total_amount_brl", 0)
            if abs(payment_sum - order_total) > 0.5:
                issues.append(f"payment sum {payment_sum} != order total {order_total}")

        # 8. Source precedence - checked in conflict resolver

        # 9. Responsibility/action consistency
        root_cause = output.get("root_cause_analysis", {})
        responsible = root_cause.get("responsible_parties", [])
        resolution_actions = output.get("resolution_actions", [])
        if responsible and not resolution_actions:
            issues.append("has responsible_parties but no resolution_actions")

        # 10. Confidence bounds
        conf = output.get("assessment", {}).get("confidence", 0)
        if not (0 <= conf <= 1):
            issues.append("confidence out of bounds [0,1]")

        if issues:
            self.trace.emit(
                case_id=self.case_id,
                event_type="verification_completed",
                actor="verifier",
                decision_code="FAILED",
                attributes={"issues": issues},
            )
        else:
            self.trace.emit(
                case_id=self.case_id,
                event_type="verification_completed",
                actor="verifier",
                decision_code="PASSED",
            )

        return issues


def _build_output(
    case_id: str,
    case: dict[str, Any],
    entity_res: dict[str, Any],
    all_evidence: dict[str, Any],
    conflicts: list[dict[str, Any]],
    resolution_notes: list[str],
) -> dict[str, Any]:
    """Build L3B output from collected evidence."""

    # Extract key data
    order = all_evidence.get("order", {}).get("order")
    items = all_evidence.get("order", {}).get("items")
    sellers = all_evidence.get("order", {}).get("sellers")
    shipment = all_evidence.get("shipment", {}).get("summary")
    payments = all_evidence.get("payment", {}).get("payments")
    refunds = all_evidence.get("payment", {}).get("refunds")

    # Collect all evidence_refs
    all_refs = []
    for ev in all_evidence.values():
        all_refs.extend(ev.get("evidence_refs", []))
    all_refs = list(dict.fromkeys(all_refs))  # dedupe preserving order

    # ---- assessment ----
    primary_issue = "insufficient_evidence"
    secondary_issues = []
    case_status = "needs_investigation"
    confidence = entity_res.get("confidence", 0.0)

    complaint_type = case.get("complaint", {}).get("type", "").lower()
    if "late" in complaint_type or "delivery" in complaint_type:
        if shipment and shipment.get("status") in ("late", "seller_delay", "logistics_delay"):
            is_seller = "seller" in str(shipment).lower()
            primary_issue = "late_delivery_seller" if is_seller else "late_delivery_logistics"
            case_status = "action_required"
        else:
            primary_issue = "insufficient_evidence"
    elif "payment" in complaint_type or "charge" in complaint_type:
        if payments and isinstance(payments, list):
            primary_issue = "payment_mismatch"
            case_status = "action_required"
    elif "refund" in complaint_type:  # noqa: SIM102
        if (
            refunds
            and isinstance(refunds, list)
            and len(refunds) > 0
        ):
            has_pending = any(
                r.get("status") == "pending" for r in refunds if isinstance(r, dict)
            )
            primary_issue = "refund_pending" if has_pending else "refund_failed"
            case_status = "action_required"

    # Confidence boost if we have strong evidence
    if all_refs:
        confidence = min(confidence + 0.15, 1.0)

    # ---- affected_entities ----
    order_ids = entity_res.get("resolved_order_ids", [])
    item_ids = []
    seller_ids = []
    payment_refs = []
    shipment_ids = []

    if items and isinstance(items, list):
        item_ids = [i.get("item_id") for i in items if i.get("item_id")]
    if sellers and isinstance(sellers, list):
        seller_ids = [s.get("seller_id") for s in sellers if s.get("seller_id")]
    if payments and isinstance(payments, list):
        payment_refs = [p.get("payment_id") for p in payments if p.get("payment_id")]
    if shipment and isinstance(shipment, dict):
        sid = shipment.get("shipment_id")
        if sid:
            shipment_ids = [sid]

    # ---- claim_assessments ----
    claims = []
    claim_id = 1
    for issue in [primary_issue] + secondary_issues:
        if issue != "insufficient_evidence":
            claims.append({
                "claim_id": f"claim_{claim_id}",
                "verdict": (
                    "supported" if primary_issue != "insufficient_evidence" else "unsupported"
                ),
                "confidence": round(confidence, 3),
                "evidence_refs": all_refs[:5],
            })
            claim_id += 1

    # ---- entity_resolution ----
    entity_resolution = {
        "status": entity_res.get("status", "not_found"),
        "resolved_order_ids": order_ids,
        "rejected_candidates": entity_res.get("rejected_candidates", []),
        "confidence": entity_res.get("confidence", 0.0),
    }

    # ---- customer_context ----
    customer_context = {
        "customer_unique_id": entity_res.get("customer_unique_id"),
        "related_order_ids": order_ids,
    }

    # ---- shipment_analysis ----
    shipment_verdict = "insufficient_evidence"
    late_sellers = []
    timeline_complete = False

    if shipment and isinstance(shipment, dict):
        status = shipment.get("status", "")
        if status == "on_time":
            shipment_verdict = "on_time"
        elif status == "seller_delay":
            shipment_verdict = "seller_delay"
        elif status == "logistics_delay":
            shipment_verdict = "logistics_delay"
        elif status == "lost":
            shipment_verdict = "lost"
        elif status == "returned":
            shipment_verdict = "returned"
        elif status == "delivered":
            shipment_verdict = "on_time"
        timeline_complete = bool(shipment.get("delivered_at") or shipment.get("status"))

    if sellers and isinstance(sellers, list):
        for s in sellers:
            if s.get("delivery_status") == "late":
                sid = s.get("seller_id")
                if sid:
                    late_sellers.append(sid)

    shipment_analysis = {
        "verdict": shipment_verdict,
        "late_seller_ids": late_sellers[:20],
        "timeline_complete": timeline_complete,
    }

    # ---- payment_analysis ----
    captured = 0.0
    refunded = 0.0
    refundable = 0.0
    payment_verdict = "insufficient_evidence"

    if payments and isinstance(payments, list):
        captured = sum(p.get("amount_brl", 0) for p in payments if isinstance(p, dict))
        payment_verdict = "reconciled"
    if refunds and isinstance(refunds, list):
        refunded = sum(r.get("amount_brl", 0) for r in refunds if isinstance(r, dict))
        if refunded > 0:
            payment_verdict = "refunded"
    if order and isinstance(order, dict):
        order_total = order.get("total_amount_brl", 0)
        refundable = max(0, order_total - captured + refunded)

    payment_analysis = {
        "verdict": payment_verdict,
        "captured_total_brl": round(captured, 2) if captured else None,
        "refunded_total_brl": round(refunded, 2) if refunded else None,
        "refundable_total_brl": round(refundable, 2) if refundable else None,
    }

    # ---- root_cause_analysis ----
    ranked_causes = []
    responsible_parties = []

    if primary_issue in ("late_delivery_seller", "late_delivery_logistics"):
        ranked_causes.append({"cause_code": "DELAYED_SHIPMENT", "rank": 1})
        party_type = "seller" if primary_issue == "late_delivery_seller" else "logistics_provider"
        seller_id = None
        if sellers and isinstance(sellers, list):
            seller_id = sellers[0].get("seller_id")
        responsible_parties.append({"party_type": party_type, "party_id": seller_id})
    elif primary_issue in ("payment_mismatch", "duplicate_charge"):
        ranked_causes.append({"cause_code": "PAYMENT_PROCESSING_ERROR", "rank": 1})
        responsible_parties.append({"party_type": "payment_provider", "party_id": None})
    elif primary_issue in ("refund_pending", "refund_failed"):
        ranked_causes.append({"cause_code": "REFUND_PROCESSING_DELAY", "rank": 1})
        responsible_parties.append({"party_type": "platform", "party_id": None})

    root_cause_analysis = {
        "ranked_causes": ranked_causes[:5],
        "responsible_parties": responsible_parties[:5],
    }

    # ---- financial_resolution ----
    recommended_refund = 0.0
    refund_lines = []

    if primary_issue == "late_delivery_seller":
        recommended_refund = captured * 0.5
        if order_ids:
            refund_lines.append({
                "reason_code": "LATE_DELIVERY_SELLER",
                "amount_brl": round(recommended_refund, 2),
                "entity_id": order_ids[0],
            })
    elif primary_issue == "late_delivery_logistics":
        recommended_refund = captured * 0.3
        if order_ids:
            refund_lines.append({
                "reason_code": "LATE_DELIVERY_LOGISTICS",
                "amount_brl": round(recommended_refund, 2),
                "entity_id": order_ids[0],
            })
    elif primary_issue == "payment_mismatch":
        if order and isinstance(order, dict):
            diff = abs(order.get("total_amount_brl", 0) - captured)
            recommended_refund = diff
            refund_lines.append({
                "reason_code": "PAYMENT_MISMATCH",
                "amount_brl": round(diff, 2),
                "entity_id": order_ids[0] if order_ids else None,
            })
    elif primary_issue in ("refund_pending", "refund_failed"):
        if order and isinstance(order, dict):
            recommended_refund = order.get("total_amount_brl", 0)
            refund_lines.append({
                "reason_code": "REFUND_FAILURE",
                "amount_brl": round(recommended_refund, 2),
                "entity_id": order_ids[0] if order_ids else None,
            })

    financial_resolution = {
        "currency": "BRL",
        "recommended_refund_brl": round(recommended_refund, 2),
        "refund_lines": refund_lines[:10],
    }

    # ---- resolution_actions ----
    actions = []
    if primary_issue in ("late_delivery_seller", "late_delivery_logistics"):
        actions.extend(["contact_seller", "request_partial_refund", "update_delivery_estimate"])
    elif primary_issue in ("payment_mismatch", "duplicate_charge"):
        actions.extend(["investigate_payment_gateway", "reconcile_payment", "issue_refund"])
    elif primary_issue in ("refund_pending", "refund_failed"):
        actions.extend(["retry_refund", "contact_payment_provider", "escalate_to_platform"])
    elif primary_issue == "insufficient_evidence":
        actions.append("request_additional_information")

    # Deduplicate and limit
    actions = list(dict.fromkeys(actions))[:8]

    return {
        "schema_version": "day09-l3b-output-v2",
        "case_id": case_id,
        "assessment": {
            "primary_issue": primary_issue,
            "secondary_issues": secondary_issues[:10],
            "case_status": case_status,
            "confidence": round(confidence, 3),
        },
        "affected_entities": {
            "order_ids": order_ids[:20],
            "item_ids": item_ids[:20],
            "seller_ids": seller_ids[:20],
            "payment_references": payment_refs[:20],
            "shipment_ids": shipment_ids[:20],
        },
        "claim_assessments": claims[:5],
        "entity_resolution": entity_resolution,
        "customer_context": customer_context,
        "shipment_analysis": shipment_analysis,
        "payment_analysis": payment_analysis,
        "root_cause_analysis": root_cause_analysis,
        "evidence_refs": all_refs[:30],
        "data_conflicts": conflicts[:5],
        "financial_resolution": financial_resolution,
        "resolution_actions": actions,
    }


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
<<<<<<< Updated upstream
    case_id = case["case_id"]
    mcp = CaseScopedGateway(case_id, gateway, trace)
    bus = Bus(case_id, trace)

    # 1. Entity resolution (policy lookup runs in parallel: it does not depend on the order).
    bus.assign("coordinator", "entity-agent", "resolve_entity")
    bus.assign("coordinator", "policy-agent", "policy_rules")
    claimed_topics = {c.get("topic") for c in case["customer_request"].get("claims", [])}
    plan, fetch_order = plan_tools(claimed_topics)
    entity_msg, policy_msg = await asyncio.gather(
        entity_agent(case, mcp, fetch_order), policy_agent(case, mcp)
    )
    entity = bus.handoff(entity_msg).payload
    policy_rules = bus.handoff(policy_msg).payload["rules"]
    if not policy_rules and "get_policy" in mcp.failed_tools:
        # The policy document always exists: failing to read it means the gateway is down.
        # Abort instead of writing an unsupported insufficient-evidence answer.
        raise GatewayUnavailable(f"{case_id}: MCP gateway failed on get_policy")
    order_id = entity["order_id"]

    # 2. Specialists on the resolved order only.
    order_payload: dict[str, Any] = {"items": []}
    shipment_payload: dict[str, Any] = {}
    payment_payload: dict[str, Any] = {"payment": {}, "refund_events": []}
    resolved = bool(order_id and entity["status"] == "resolved")

    async def investigate(tools: frozenset[str], first: bool) -> None:
        nonlocal order_payload, shipment_payload, payment_payload
        jobs = []
        if first:
            bus.assign("coordinator", "order-agent", "order_items")
            jobs.append(order_agent(case_id, order_id, mcp, tools))
            bus.assign("coordinator", "payment-agent", "payment_refund_timeline")
            jobs.append(payment_agent(case_id, order_id, mcp, tools))
        elif "get_sellers" in tools:
            bus.assign("coordinator", "order-agent", "order_items")
            jobs.append(order_agent(case_id, order_id, mcp, tools))
        if not first and "get_refund_timeline" in tools:
            bus.assign("coordinator", "payment-agent", "payment_refund_timeline")
            jobs.append(payment_agent(case_id, order_id, mcp, tools))
        if "get_shipment_summary" in tools:
            bus.assign("coordinator", "shipment-agent", "shipment_timeline")
            jobs.append(shipment_agent(case_id, order_id, mcp))
        for message in await asyncio.gather(*jobs):
            bus.handoff(message)
            if message.sender == "order-agent":
                order_payload = message.payload
            elif message.sender == "payment-agent":
                payment_payload = message.payload
            elif message.sender == "shipment-agent":
                shipment_payload = message.payload["shipment"]

    if resolved:
        await investigate(plan, first=True)

    # 3. Conflict resolution: pick the purchase episode the complaint is about.
    bus.assign("coordinator", "conflict-resolver", "select_episode")
    episode = conflict_resolver(
        case, entity, order_payload, shipment_payload, payment_payload, claimed_topics
    )
    missing = OPTIONAL_TOOLS - plan
    if resolved and not episode.get("claim_supported") and missing:
        # Planned evidence does not support the claim: widen the investigation once.
        bus.handoff(
            A2AMessage(
                case_id=case_id,
                sender="conflict-resolver",
                recipient="coordinator",
                task="select_episode",
                status="CLAIM_NOT_SUPPORTED_ESCALATE",
            )
        )
        await investigate(frozenset(missing), first=False)
        bus.assign("coordinator", "conflict-resolver", "select_episode")
        episode = conflict_resolver(
            case, entity, order_payload, shipment_payload, payment_payload, claimed_topics
        )
    bus.handoff(
        A2AMessage(
            case_id=case_id,
            sender="conflict-resolver",
            recipient="coordinator",
            task="select_episode",
            status=episode.get("selection_code", "NO_EPISODE"),
        )
    )

    # 4. Decision + policy.
    finding = episode["finding"]
    issue = finding["issue"]
    rule = policy_rules.get(issue) or {}
    trace.emit(
        case_id=case_id,
        event_type="policy_decided",
        actor="policy-agent",
        decision_code=issue,
        evidence_refs=policy_msg.evidence_refs or None,
        attributes={"recommended_action": rule.get("recommended_action")},
    )

    items = episode.get("items", [])
    seller_ids = _unique([i.get("seller_id") for i in items])
    item_ids = _unique([i.get("order_item_id") for i in items])

    if rule:
        case_status = rule.get("case_status", "needs_investigation")
        action = rule.get("recommended_action")
        policy_refund = _money(rule.get("refund_brl")) or Decimal(0)
    else:
        case_status, action, policy_refund = "needs_investigation", None, Decimal(0)
    derived = finding["derived_refund"]
    # Policy amount is authoritative when consistent with what was actually captured.
    captured = finding["captured_total"]
    refund = policy_refund
    if captured is not None and refund > captured:
        refund = derived if derived <= captured else captured
    if case_status == "no_action" or issue == "refund_pending":
        refund = Decimal(0)

    parties = []
    for party in rule.get("responsible_parties", []) or [{"party_type": "unknown", "party_id": None}]:
        if party.get("party_type") == "seller":
            late = finding["late_sellers"] or seller_ids
            parties.extend({"party_type": "seller", "party_id": s} for s in late[:5])
        else:
            parties.append({"party_type": party.get("party_type", "unknown"), "party_id": party.get("party_id")})
    parties = parties[:5]

    refund_entity = order_id
    refund_lines = (
        [{"reason_code": (action or issue).upper(), "amount_brl": float(refund), "entity_id": refund_entity}]
        if refund > 0
        else []
    )

    claim_topic_hits = [c.get("topic") for c in case["customer_request"].get("claims", [])]
    topic_matches = issue in claim_topic_hits
    if issue == "insufficient_evidence":
        confidence = 0.35
    elif topic_matches:
        confidence = 0.9 if entity["status"] == "resolved" else 0.7
    else:
        confidence = 0.7
    if finding["shipment_conflict"]:
        confidence -= 0.1
        episode["conflicts"] = (episode.get("conflicts", []) + [
            {
                "field": "shipment.delay_actor",
                "sources": ["get_shipment_summary.events", "get_shipment_summary.shipping_limits"],
                "selected_source": "get_shipment_summary.shipping_limits",
                "resolution_code": "TIMESTAMPS_OVER_EVENT_ACTOR",
            }
        ])[:5]

    claim_assessments = []
    for claim in case["customer_request"].get("claims", [])[:5]:
        topic = claim.get("topic")
        if topic == "requested_full_refund":
            full = captured if captured is not None else Decimal(0)
            full_refund_issue = issue in {"canceled_order_paid", "unavailable_order_paid"}
            if full_refund_issue and refund > 0 and abs(refund - full) <= MONEY_TOLERANCE:
                verdict = "supported"
            elif refund > 0:
                verdict = "partially_supported"
            else:
                verdict = "unsupported"
        elif issue == "insufficient_evidence":
            verdict = "insufficient_evidence"
        elif topic == issue and issue != "unsupported_claim":
            verdict = "supported"
        else:
            verdict = "unsupported"
        claim_assessments.append(
            {
                "claim_id": claim.get("claim_id") or "claim",
                "verdict": verdict,
                "confidence": round(confidence, 2),
                "evidence_refs": [],
            }
        )

    evidence_refs = list(mcp.evidence.keys())[:30]
    domain_refs = {e.domain: [] for e in mcp.evidence.values()}
    for ev in mcp.evidence.values():
        domain_refs[ev.domain].append(ev.ref)
    claim_domains = {
        "late_delivery_logistics": ["shipment", "customer"],
        "late_delivery_seller": ["shipment", "item", "seller", "customer"],
        "unavailable_order_paid": ["order", "item", "seller", "payment"],
        "canceled_order_paid": ["order", "payment"],
        "requested_full_refund": ["payment", "refund", "policy"],
    }
    for claim, assessment in zip(case["customer_request"].get("claims", []), claim_assessments):
        domains = claim_domains.get(claim.get("topic"), ["payment", "refund", "customer"])
        refs = [r for d in domains for r in domain_refs.get(d, [])]
        assessment["evidence_refs"] = _unique(refs)[:30] or evidence_refs[:3]

    payment_refs = [
        f"{order_id}:{row.get('payment_sequential')}:{row.get('payment_type')}"
        for row in episode.get("payment_rows", [])
    ]
    refundable = refund
    if issue == "refund_pending":
        refundable = finding["pending_total"]

    output: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "case_id": case_id,
        "assessment": {
            "primary_issue": issue,
            "secondary_issues": [],
            "case_status": case_status,
            "confidence": round(max(0.05, min(confidence, 0.99)), 2),
        },
        "affected_entities": {
            "order_ids": entity["resolved"],
            "item_ids": item_ids,
            "seller_ids": seller_ids,
            "payment_references": _unique(payment_refs)[:20],
            "shipment_ids": [order_id] if order_id and entity["status"] == "resolved" else [],
        },
        "claim_assessments": claim_assessments,
        "entity_resolution": {
            "status": entity["status"],
            "resolved_order_ids": entity["resolved"],
            "rejected_candidates": entity["rejected"],
            "confidence": entity["confidence"],
        },
        "customer_context": {
            "customer_unique_id": entity["customer_unique_id"],
            "related_order_ids": entity["related_order_ids"][:20],
        },
        "shipment_analysis": {
            "verdict": finding["shipment_verdict"],
            "late_seller_ids": finding["late_sellers"],
            "timeline_complete": finding["timeline_complete"],
        },
        "payment_analysis": {
            "verdict": finding["payment_verdict"],
            "captured_total_brl": _num(captured),
            "refunded_total_brl": _num(finding["refunded_total"]),
            "refundable_total_brl": _num(refundable),
        },
        "root_cause_analysis": {
            "ranked_causes": [{"cause_code": issue.upper(), "rank": 1}],
            "responsible_parties": parties,
        },
        "evidence_refs": evidence_refs,
        "data_conflicts": episode.get("conflicts", []),
        "financial_resolution": {
            "currency": "BRL",
            "recommended_refund_brl": float(refund),
            "refund_lines": refund_lines,
        },
        "resolution_actions": [action] if action else ["escalate_for_manual_review"],
    }

    # Optional local debug dump (never packaged): DAY09_DEBUG_DUMP=<dir>
    dump_dir = os.getenv("DAY09_DEBUG_DUMP")
    if dump_dir:
        Path(dump_dir).mkdir(parents=True, exist_ok=True)
        (Path(dump_dir) / f"{case_id}.json").write_text(
            json.dumps({"input": case, **mcp.raw}, ensure_ascii=False, indent=1), encoding="utf-8"
        )

    # 5. Verification before finalize.
    bus.assign("coordinator", "verifier", "verify_output")
    problems = verifier(output, mcp, entity["rejected"])
    if problems:
        output["assessment"]["confidence"] = round(output["assessment"]["confidence"] * 0.6, 2)
    trace.emit(
        case_id=case_id,
        event_type="verification_completed",
        actor="verifier",
        target="coordinator",
        decision_code="PASS" if not problems else "FAIL:" + ",".join(problems)[:70],
        evidence_refs=evidence_refs[:20] or None,
        attributes={"mcp_calls": mcp.calls, "conflicts": len(output["data_conflicts"])},
    )
    return output
=======
    """Implement the L3B multi-agent workflow."""
    case_id = case["case_id"]

    trace.emit(case_id=case_id, event_type="case_received", actor="coordinator")

    # Shared cache and trace for this case
    cache = CaseCache()
    mcp = MCPClient(gateway, trace, case_id, "coordinator", cache)

    # ---- Phase 1: Entity Resolution ----
    entity_resolver = EntityResolver(mcp, trace, case_id, case)
    entity_res = await entity_resolver.resolve()

    customer_unique_id = entity_res.get("customer_unique_id")
    order_id = entity_res.get("order_id")

    if not order_id:
        # Cannot proceed without order_id
        output = _build_output(case_id, case, entity_res, {}, [], [])
        trace.emit(case_id=case_id, event_type="case_finalized", actor="coordinator")
        return output

    # ---- Phase 2: Parallel Specialist Investigation ----
    order_agent = OrderProductAgent(MCPClient(gateway, trace, case_id, "order_agent", cache))
    shipment_agent = ShipmentAgent(MCPClient(gateway, trace, case_id, "shipment_agent", cache))
    payment_agent = PaymentRefundAgent(MCPClient(gateway, trace, case_id, "payment_agent", cache))
    policy_agent = PolicyAgent(MCPClient(gateway, trace, case_id, "policy_agent", cache))
    customer_agent = CustomerAgent(MCPClient(gateway, trace, case_id, "customer_agent", cache))

    # Dispatch all in parallel
    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="order_agent",
        attributes={"order_id": order_id},
    )
    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="shipment_agent",
        attributes={"order_id": order_id},
    )
    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="payment_agent",
        attributes={"order_id": order_id},
    )
    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="policy_agent",
        attributes={"category": case.get("complaint", {}).get("type", "general")},
    )
    if customer_unique_id:
        trace.emit(
            case_id=case_id,
            event_type="task_assigned",
            actor="coordinator",
            target="customer_agent",
            attributes={"customer_unique_id": customer_unique_id},
        )

    order_task = order_agent.investigate(order_id)
    shipment_task = shipment_agent.investigate(order_id)
    payment_task = payment_agent.investigate(order_id)
    policy_task = policy_agent.investigate(
        case.get("complaint", {}).get("type", "general"),
        case.get("complaint", {}).get("description", ""),
    )
    if customer_unique_id:
        customer_task = customer_agent.investigate(customer_unique_id)
    else:
        customer_task = asyncio.sleep(0, result={"evidence_refs": []})

    order_res, shipment_res, payment_res, policy_res, customer_res = await asyncio.gather(
        order_task, shipment_task, payment_task, policy_task, customer_task, return_exceptions=True
    )

    # Normalize results
    all_evidence = {
        "order": order_res if isinstance(order_res, dict) else {"evidence_refs": []},
        "shipment": shipment_res if isinstance(shipment_res, dict) else {"evidence_refs": []},
        "payment": payment_res if isinstance(payment_res, dict) else {"evidence_refs": []},
        "policy": policy_res if isinstance(policy_res, dict) else {"evidence_refs": []},
        "customer": customer_res if isinstance(customer_res, dict) else {"evidence_refs": []},
    }

    # ---- Phase 3: Conflict Resolution ----
    conflict_resolver = ConflictResolver(trace, case_id)
    conflicts, resolution_notes = conflict_resolver.resolve(all_evidence, entity_res)

    # ---- Phase 4: Build Draft Output ----
    draft_output = _build_output(
        case_id, case, entity_res, all_evidence, conflicts, resolution_notes
    )

    # ---- Phase 5: Verification ----
    verifier = Verifier(trace, case_id)
    max_verify_loops = 2
    for _ in range(max_verify_loops):
        issues = verifier.verify(draft_output, all_evidence, entity_res)
        if not issues:
            break
        # Simple auto-fix for common issues
        if "orphan evidence_refs" in str(issues):
            # Remove orphan refs
            all_refs_set = set()
            for ev in all_evidence.values():
                all_refs_set.update(ev.get("evidence_refs", []))
            draft_output["evidence_refs"] = [
                r for r in draft_output.get("evidence_refs", []) if r in all_refs_set
            ]
        if "confidence out of bounds" in str(issues):
            draft_output["assessment"]["confidence"] = max(
                0, min(1, draft_output["assessment"]["confidence"])
            )
    else:
        # Max loops reached - still return output but log
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor="verifier",
            decision_code="MAX_LOOPS_REACHED",
            attributes={"remaining_issues": issues},
        )

    trace.emit(case_id=case_id, event_type="case_finalized", actor="coordinator")
    return draft_output
>>>>>>> Stashed changes
