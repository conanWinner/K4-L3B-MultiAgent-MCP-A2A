"""L3B multi-agent complaint investigation.

Flow (see ARCHITECTURE.md):
    coordinator -> entity-agent -> [order-agent, shipment-agent, payment-agent, policy-agent]
    -> conflict-resolver -> decision (coordinator) -> verifier -> output

Every agent talks to MCP through a ``CaseScopedGateway`` that enforces a per-actor
tool allowlist, caches results inside one case only and emits ``tool_result_consumed``.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from itertools import combinations
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
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


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
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
