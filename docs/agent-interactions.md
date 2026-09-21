# Agent and bot interactions

> **Status: design proposal.** Nothing here is implemented yet. It fixes the shape of the
> exchanges so that implementations can change without changing the audit trail.

## 1. Principle: a graph at run time, a chain at read time

Agents do not form a pipeline. Several detectors can raise a case, several agents can
investigate competing hypotheses at once, and a human can reject a recommendation, which sends
the case back. A fixed `A -> B -> C` chain cannot express any of that.

What *is* linear is the audit trail:

```
evidence -> model/version -> decision -> recommendation -> approval -> action -> result
```

The trail is **reconstructed on read** from message links (section 3). It is never used as the
execution order.

## 2. Rules

1. **No direct calls between agents.** Agents exchange typed messages through the event spine and
   the evidence store. No agent depends on another agent's implementation.
2. **Agents propose; the workflow executes.** Only the durable workflow engine performs actions,
   and only after the policy gate. An agent output is never an action.
3. **Fan-out is parallel, decision is a join.** One investigation agent per hypothesis; policy
   and human approval aggregate them.
4. **Confidence is a vector, not a scalar.** Policy never reasons on a model's self-reported score.
5. **Every message is immutable and attributable**: producer identity, model and version, inputs.
6. **Fail closed.** A missing approval, an expired message or an unknown type blocks the action.

## 3. Message envelope

Every message carries:

| Field | Purpose |
|---|---|
| `message_id` | Unique, immutable. |
| `case_id` | The exchange this message belongs to (the `correlation_id`). |
| `caused_by` | `message_id`(s) that triggered this one (the `causation_id`). Several for a join. |
| `type` | One of the types in section 4. |
| `producer` | Agent, bot, detector or human identity, with model and version when relevant. |
| `evidence_refs` | Pointers to evidence, never a copy. |
| `confidence` | Vector (per dimension), when the type is a claim. |
| `expires_at` | After this, the message cannot justify an action. |

`caused_by` makes the exchange a DAG. Walking it backwards from any action yields the linear
audit trail.

## 4. Message types

| Type | Sent by | Meaning |
|---|---|---|
| `SignalRaised` | detector (business, tracing, kernel, SLO) | Something deviates. Opens or joins a case. |
| `EvidenceRequested` | investigation agent | Needs more data before concluding. |
| `EvidenceProvided` | data/correlation service | Answers a request, with references. |
| `HypothesisProposed` | investigation agent | One candidate cause with its contribution and confidence vector. |
| `HypothesisRetracted` | investigation agent | Withdraws its own earlier claim. |
| `RecommendationIssued` | policy service | Aggregates hypotheses into a proposed action. |
| `ApprovalDecided` | human or delegated approver | Approve, reject or request changes. |
| `ActionExecuted` | workflow engine | The action ran; carries the outcome. |
| `ActionCompensated` | workflow engine | The action was rolled back or corrected. |
| `CaseClosed` | workflow engine | Terminal. |

## 5. Case lifecycle

```
             SignalRaised (any detector, any time)
                   |
                   v
   +--------> INVESTIGATING <------------------+
   |               |  HypothesisProposed x N   |
   |               v                           |
   |          RECOMMENDED                      |
   |               |                           |
   |      ApprovalDecided                      |
   |        /       |         \                |
   |  reject   changes-requested   approve     |
   |     |          |                |         |
   |     v          +----------------+---------+   (back to INVESTIGATING)
   |  CLOSED_NO_ACTION               v
   |                             EXECUTING
   |                              /     \
   |                        success   failure
   |                            |         |
   |                            v         v
   |                        CLOSED    COMPENSATING -> CLOSED
   |
   +-- new SignalRaised on an open case (joins it, may reopen hypotheses)
```

Transitions are owned by the workflow engine. An agent that emits a message out of state is
ignored and the attempt is recorded.

## 6. Concurrency and conflicts

- **Competing hypotheses coexist.** Two agents may propose different causes for the same case.
  Both stay visible with their own confidence vector; policy weighs them, agents do not vote.
- **Concurrent causes are legitimate.** A release regression during an external outage yields two
  hypotheses that both hold, not one winner.
- **Idempotent delivery.** Messages are delivered at least once; consumers deduplicate on
  `message_id`.
- **Loop guard.** A case has a bounded number of `EvidenceRequested` rounds and a wall-clock
  budget. When exhausted, the case escalates to a human instead of looping.

## 7. Bots and automated clients

Bots that act on the platform (chat bots, ticketing integrations, scheduled jobs) are producers
like any other: they authenticate, carry an identity in `producer`, and may emit only the types
their role allows. A bot never emits `ApprovalDecided` unless it has been explicitly delegated
that authority, and that delegation is itself recorded.

## 8. Open questions

- Delivery substrate for messages: the event spine directly, or a dedicated case topic.
- Delegation model for automated approvers and its expiry.
- Retention of retracted hypotheses.
