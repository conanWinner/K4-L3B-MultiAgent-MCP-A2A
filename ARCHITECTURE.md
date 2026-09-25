# L3B Architecture Record

Team phải cập nhật tài liệu này cùng source. Mục tiêu là mô tả quyết định có thể kiểm chứng, không ghi prompt bí mật hoặc chain-of-thought.

Mã nguồn: `src/student_agent/workflow.py` (agents, resolver, verifier), `src/student_agent/cli.py` (vòng đời case, reconnect).
Hệ thống là **deterministic, rule-based** — không dùng LLM, không có randomness.

## 1. System overview

```text
                 ┌──────────────── policy-agent ── get_policy ───────────────┐
Input ─► coordinator                                                          ▼
           │  task_assigned                                          policy_decided
           ▼
     entity-agent ── get_customer_history, get_order
           │  handoff (status, resolved/rejected candidates)
           ▼
     coordinator ──► order-agent     ── get_order_items, get_product_context ─┐
               ├──► shipment-agent  ── get_shipment_summary                   ├─ handoff
               └──► payment-agent   ── get_payment_timeline, get_refund_timeline ┘
           ▼
     conflict-resolver  (tách các "purchase episode" bị trộn dưới cùng order_id, chọn episode)
           ▼
     decision (coordinator + policy rule) ─► verifier ─► output JSON
                                                │
     mọi MCP call ─► tool_result_consumed ──────┴──► traces/trace.jsonl
```

Các specialist chạy song song (`asyncio.gather`) sau khi entity đã được resolve; policy-agent chạy song song với entity-agent vì không phụ thuộc order.

## 2. Agent ownership

| Actor | Input | Trách nhiệm | Tool permission | Output/handoff |
| --- | --- | --- | --- | --- |
| Entity/customer (`entity-agent`) | candidate_order_ids, claimed_order_id, customer_unique_id_hint | Xác minh customer, xếp hạng/loại candidate, lấy order row | `get_customer_history`, `get_order` | status, resolved/rejected ids, history rows → coordinator |
| Coordinator | case input, mọi handoff | Giao task, gom kết quả, áp policy, dựng output | không gọi MCP trực tiếp | `task_assigned`, output cuối |
| Order/product (`order-agent`) | resolved order_id | Item, seller, giá/freight, category | `get_order_items`, `get_product_context` | items, products → coordinator |
| Shipment (`shipment-agent`) | resolved order_id | Mốc giao hàng, shipping limit, shipment events | `get_shipment_summary` | shipment summary → coordinator |
| Payment/refund (`payment-agent`) | resolved order_id | Capture/mismatch events, refund lifecycle | `get_payment_timeline`, `get_refund_timeline` | payment + refund events → coordinator |
| Policy (`policy-agent`) | policy_version, primary issue | Tra rule: case_status, action, refund, responsible party | `get_policy` | rules → coordinator, `policy_decided` |
| Conflict resolver | handoffs của entity + specialists | Tách episode, gán event/item/payment vào episode, chọn episode, ghi `data_conflicts` | không | selected episode + finding |
| Verifier | output nháp + evidence registry | Kiểm invariant (mục 6) trước finalize | không | `verification_completed` PASS/FAIL |

Least privilege được **enforce trong code**: `CaseScopedGateway.call` raise `PermissionError` nếu actor gọi tool ngoài `TOOL_PERMISSIONS`. `get_order_payments` và `get_sellers` không được cấp cho ai vì dữ liệu của chúng đã có trong `get_payment_timeline` / `get_order_items` (tránh call thừa).

## 3. Entity resolution và A2A protocol

**Xếp hạng candidate** (điểm cộng dồn):

- +4 nếu order_id xuất hiện trong `get_customer_history` của `customer_unique_id_hint` (xác minh độc lập);
- +2 nếu trùng `claimed_order_id`;
- +1 nếu đúng định dạng order id (32 hex).

Candidate có điểm ≥ 5 (hoặc ≥ 3 khi không có history) là viable. Một viable duy nhất → `resolved` (confidence 0.95 nếu có trong history, 0.7 nếu không); hai viable bằng điểm → `ambiguous`; không có → `not_found`. Mọi candidate còn lại vào `rejected_candidates`. Candidate placeholder (vd `candidate-001`) bị reject **mà không gọi MCP** — không tốn call. Khi không `resolved`, specialist không được giao task và output ở trạng thái `insufficient_evidence` / `needs_investigation`.

**Message envelope** (`A2AMessage`): `case_id`, `sender`, `recipient`, `task`, `status`, `payload`, `evidence_refs`. `Bus.handoff` từ chối message có `case_id` khác case hiện tại (correlation). Mỗi handoff emit một event `handoff` gồm actor → target, `decision_code` = status và evidence refs liên quan.

**Chống vòng lặp/timeout**: luồng là DAG cố định (entity → specialists → resolver → verifier), không có agent nào gọi lại agent trước đó; mỗi agent chạy đúng một lần mỗi case. Timeout HTTP 300s ở gateway; lỗi transport làm rớt session được `cli.py` xử lý bằng reconnect (mục 5).

## 4. Evidence và conflict lifecycle

1. **Validate**: mọi MCP response được kiểm theo `mcp-evidence-response-v1.schema.json` trong `EvidenceGateway.call` trước khi dùng.
2. **Lưu**: `CaseScopedGateway` giữ registry `evidence_ref → (tool, domain, data)` **riêng cho từng case**; object này bị huỷ khi case kết thúc nên evidence không thể dùng chéo case. `evidence_ref` được copy nguyên văn, không bao giờ tự tạo.
3. **Trace**: mỗi call thành công emit `tool_result_consumed` (actor, tool_name, evidence_refs, domain). Call lỗi emit `tool_result_consumed` với `decision_code` = `TOOL_RETURNED_NO_EVIDENCE` và không có ref.
4. **Chọn episode (source conflict chính)**: dữ liệu L3B trộn nhiều lần mua dưới cùng `order_id` (history có 2 row khác timestamp; items/payments/events lẫn lộn; `get_order` có thể trả row của episode khác). Resolver:
   - tách episode theo `order_purchase_timestamp` từ `get_customer_history` (+ row của `get_order`);
   - gán item/shipping limit/shipment & payment event vào episode có purchase gần nhất **không sau** mốc thời gian của nó;
   - gán refund event theo **số tiền khớp với capture** của episode (refund thường xảy ra muộn nên thời gian không tin cậy), fallback theo thời gian;
   - payment row (không có timestamp) được ghép với capture event cùng số tiền;
   - event giống hệt nhau (cùng time/type/amount) được coi là bản ghi lặp của nguồn, không phải capture thứ hai;
   - phân tích từng episode; chọn episode có evidence **xác nhận** claim của khách; nếu không có, chọn episode mới nhất trước `opened_at`.
5. **Source precedence**: timeline có timestamp (history, shipment/payment/refund timelines) > order row đơn lẻ; timestamp (carrier vs shipping limit) > actor của event `delivered_late`. Mỗi xung đột được ghi vào `data_conflicts` với `sources`, `selected_source`, `resolution_code`.
6. **Claim linkage**: `claim_assessments[].evidence_refs` được lấy theo domain liên quan tới topic (shipment/order cho late delivery; payment/refund/policy cho yêu cầu hoàn tiền…); `evidence_refs` tổng gồm mọi ref đã consume trong case.

**Business rules** trong episode được chọn (thứ tự ưu tiên khi nhiều tín hiệu cùng có; tín hiệu trùng claim được ưu tiên):
`canceled_order_paid` (status canceled + có capture) → `unavailable_order_paid` → `refund_failed` → `refund_pending` → `payment_mismatch` (event `reconciliation_mismatch`) → `duplicate_charge` (capture trùng số tiền ngoài nhóm split) → `late_delivery_seller` (carrier date > shipping limit) → `late_delivery_logistics` (giao trễ nhưng seller bàn giao đúng hạn) → `valid_split_payment` (≥2 capture có tổng = giá + freight) → `unsupported_claim`.
Refund/status/action/responsible party lấy từ `get_policy`; `party_id` của seller được thay bằng seller thật của đơn; refund bị chặn trên bởi tổng tiền đã capture.

## 5. Failure and efficiency policy

| Failure | Retry budget | Fallback | Trace event/code |
| --- | ---: | --- | --- |
| MCP timeout / transport lỗi trong call | 1 | bỏ evidence đó, không phỏng đoán | `tool_result_consumed` / `TOOL_TRANSPORT_FAILED` |
| Session MCP bị rớt (ReadError) | 5 reconnect / run | xoá trace dở của case, kết nối lại, chạy lại case đó | trace của case được viết lại sạch |
| Tool trả lỗi (vd không có refund) | 0 (deterministic) | coi là "không có dữ liệu" | `tool_result_consumed` / `TOOL_RETURNED_NO_EVIDENCE` |
| Entity not found/ambiguous | 0 | không giao specialist; `insufficient_evidence`, `needs_investigation` | `handoff` status `not_found`/`ambiguous` |
| Source conflict | 0 | resolver chọn theo precedence, ghi `data_conflicts` | `handoff` từ conflict-resolver, `decision_code` = mã chọn episode |
| Invalid specialist result | 0 | thiếu field → coi như không có evidence; verifier FAIL → giảm confidence ×0.6 | `verification_completed` `FAIL:<codes>` |

**Budget**: đúng **8 call/case** (history, order, items, product, shipment, payment timeline, refund timeline, policy), mỗi tool tối đa 1 lần. Cache `(tool, args)` trong phạm vi case chặn gọi lặp. Không quét rộng: chỉ gọi trên order đã resolve; candidate placeholder không được query.

## 6. Verification invariants

`verifier()` kiểm trước khi finalize:

- output hợp lệ theo JSON Schema (cli validate lần nữa khi ghi file);
- `evidence_refs` không rỗng và **mọi ref thuộc registry của chính case** (evidence ownership);
- `resolved_order_ids ∩ rejected_candidates = ∅`; `affected_entities.order_ids ⊆ resolved` (entity scope);
- tổng `refund_lines` = `recommended_refund_brl`; refund ≤ captured total;
- `no_action` ⇒ refund = 0; `action_required` ⇒ có ít nhất một action;
- `seller_delay` ⇒ responsible party là seller nằm trong `affected_entities.seller_ids`.

Kết quả được emit trong `verification_completed` (`PASS` hoặc `FAIL:<codes>`, kèm số MCP call và số conflict).

**Confidence**: 0.9 khi entity resolved và episode được evidence xác nhận claim; 0.7 khi kết luận khác claim; 0.35 khi `insufficient_evidence`; −0.1 nếu shipment event mâu thuẫn timestamp; ×0.6 nếu verifier FAIL.

## 7. Reproducibility

- Python 3.11; dependency theo `pyproject.toml` (`mcp>=2,<3`, cài được 2.2.0 — gateway hỗ trợ cả `is_error` lẫn `isError`).
- Không LLM, không random seed; kết quả chỉ phụ thuộc MCP responses.
- Concurrency: case chạy tuần tự; trong một case tối đa 3 call song song.
- Lệnh:

  ```bash
  day09 run
  day09 validate
  day09 package --output dist/submission.zip
  ```

- Debug: `DAY09_DEBUG_DUMP=debug/evidence day09 run` lưu raw MCP responses mỗi case vào `debug/` (đã gitignore, không đưa vào submission).
