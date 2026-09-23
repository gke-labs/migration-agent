# Assessment Step 2 — Resolve Blockers

**DAG state:** `STATE_BLOCKER_RESOLUTION` · **Expected tool call:** `assign_blocker_owner`

The readiness report is written and it names at least one blocker. Landing zone design is
locked until **every blocker has an owner and a target close date**. That rule is enforced
on the server, not here: you cannot talk your way past it, and you do not need to police
it yourself.

## What to do

1. Call `list_blockers` to see the current checklist and which entries are still
   outstanding.
2. Present the outstanding blockers to the user. For each, give the title, the affected
   workloads, and the resolution path — enough for them to decide who should own it.
3. Ask the user to assign owners and target close dates. They will often do several at
   once, in prose: *"Assign the webhook blocker to @jane.doe, target close date this
   Friday."*
4. For each assignment, call `assign_blocker_owner` with the blocker `id`, the owner's
   email address, and `target_close_date` as `YYYY-MM-DD`.

Resolve relative dates ("this Friday", "end of next week") against today's date before
calling, and state the absolute date you resolved to when you confirm back to the user, so
a misreading is visible immediately.

## Owners who are not in the ledger

If the owner is not registered in the workspace, the server will not silently add them and
neither should you. `assign_blocker_owner` routes to a `STATE_CONFIRM_NEW_MEMBER`
elicitation that asks which team the person is on:

- **platform** — registered as a platform engineer, which carries write access to the
  `platform/` prefix of the ledger.
- **application** — registered as a developer.

Answering registers them, grants their ledger access, and applies the assignment in one
step. Declining leaves the blocker unassigned.

This is a real access-control decision, not a formality: the answer determines what that
person can write. If you do not know which team they are on, ask the user rather than
guessing from their email domain.

## What happens next

- Every blocker owned and dated → `STATE_LZ_DESIGN` (landing zone design).
- Any blocker still outstanding → you stay on this state. Keep going.

## Rules

- Never assign an owner the user has not named. Inventing an owner to clear the gate
  defeats the gate.
- Never pass a `target_close_date` the user has not agreed to. If they name an owner but no
  date, ask for the date.
- Report the remaining count after each assignment so the user can see the gate closing.
- If the tool returns an `ERROR`, report it to the user verbatim and stop.
