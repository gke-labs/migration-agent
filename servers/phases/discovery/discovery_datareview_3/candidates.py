# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Ranking the workloads that might own an unattributed data service.

Pure logic. This is the one place name similarity is allowed anywhere near
the workload→data-service mapping, and only in one direction: it orders a
list a human then chooses from. `consumers.py` refuses it as a source of
facts because a wrong consumer stops the wrong team's release; the same signal
is fine as a ranker, because the human is the one who decides and they can see
every candidate, including none of them.

Two rules the shape of the prompt has to keep, both from `browse_component_seed`
(workload_scope_1/seed.py), which does the same job for component scoping:

  Always a ranked LIST with an explicit "none of these", never a yes/no on the
  top candidate. A single named guess anchors a tired reviewer into confirming
  it, and the failure that produces — a plausible wrong owner — is silent.

  A match is always labeled a guess, and no match is stated honestly: zero
  candidates means the names do not resemble each other, never that nobody
  uses the resource.

The pool comes from the scan (`consumers.attach_consumers`), so it is every
workload the same walk saw at the same commit — not a re-read of a checkout
that may have moved on.
"""

import re

# Tokens that identify nothing. Terraform and AWS scaffolding, environment
# names, and the generic halves of resource names: "orders-db" and
# "checkout-db" are both databases, and "db" matching both is what turns a
# ranking into noise. Service words stay out of the entry's own token set for
# the same reason.
_NOISE = frozenset({
    "module", "resource", "aws", "this", "main", "default", "primary",
    "cluster", "instance", "database", "data", "store", "storage", "table",
    "bucket", "queue", "topic", "stream", "cache", "secret", "parameter",
    "prod", "production", "dev", "development", "stage", "staging", "test",
    "qa", "uat", "sandbox", "shared", "common", "core", "app", "apps",
    "service", "services", "svc", "eks", "k8s", "kubernetes", "helm", "chart",
    "release", "role", "policy", "account", "the", "and", "for",
})

# Under four characters a substring match is an accident ("api" inside
# "rapid"), so only whole-token equality counts there.
_MIN_FUZZY = 4


def tokens(*values) -> list:
    """Distinct, meaning-carrying, lowercase tokens of the given strings."""
    found = []
    for value in values:
        if not isinstance(value, str):
            continue
        for token in re.split(r"[^A-Za-z0-9]+", value.lower()):
            if len(token) > 2 and token not in _NOISE and not token.isdigit():
                found.append(token)
    return list(dict.fromkeys(found))


def _overlap(entry_tokens: list, candidate_tokens: list) -> list:
    matched = []
    for token in entry_tokens:
        for other in candidate_tokens:
            if token == other or (
                    len(token) >= _MIN_FUZZY and len(other) >= _MIN_FUZZY
                    and (token in other or other in token)):
                matched.append(token)
                break
    return matched


def rank(entry: dict, pool: list, limit: int = 5,
         chain_was_cut: bool = False,
         reading_incomplete: bool = False) -> tuple[list, str]:
    """(ranked candidates, the note that must be shown with them).

    A candidate is `{workload, kind, namespace, source_path, matched}`. The
    note is written for the user, not the agent, and says what the ranking is
    and is not.

    Why an entry has no consumer is three states, not two — the same three
    `consumers.note_unattributed` keeps apart. `chain_was_cut` is a reviewer's
    decision; `reading_incomplete` is a file the scan could not read to the
    end, which is the one case where "none reached it" is not a finding but a
    refusal to answer, and stating it as a finding is what `TRUNCATED_NOTE`
    exists to prevent. Cut wins over incomplete: a chain that reached this
    entry reached it whatever else went unread.
    """
    pool = pool or []
    # A chain DID reach this one; the reviewer cut it. Saying otherwise
    # contradicts their own note four lines above, in text the agent is told
    # to relay verbatim.
    because = ("the scan follows references, and the one that reached this was "
               "rejected at the review" if chain_was_cut else
               "part of the Terraform could not be read to the end, so the "
               "chains never got to answer for this one" if reading_incomplete
               else "the scan follows references, and none reached this one")
    entry_tokens = tokens(entry.get("identifier"), entry.get("address"))
    identity = entry.get("identifier") or entry.get("address") or "this entry"
    if not pool:
        return [], (
            f"Candidates for {identity}: the scan saw no workload at all in "
            "the Terraform it read, so there is nobody to rank. That is a "
            "statement about the scanned files, not about the estate — the "
            "workloads may be deployed from somewhere this scan does not read "
            "(a pipeline, a separate repository, the console).")
    if not entry_tokens:
        # The pool itself, unranked. This fires on exactly the entries that
        # need it most — `module.this`, an identifier of `db` — because the
        # noise list eats the whole name, and returning nothing here left the
        # agent relaying a sentence that pointed at an absent list.
        shown = [dict(c, matched=[]) for c in pool[:max(limit, 0)]]
        return shown, (
            f"Candidates for {identity} (a GUESS, not a fact): its name "
            "yields no usable tokens (nothing in it distinguishes one "
            f"workload from another), so nothing is ranked. {len(shown)} of "
            f"the {len(pool)} workload(s) the scan saw are listed below in "
            "the order it found them — that order means nothing, and no "
            "evidence connects any of them to this entry. Put the list to "
            "the user WITH 'none of these' as an equal option; asking the "
            "owner is better than picking one.")

    scored = []
    for candidate in pool:
        matched = _overlap(entry_tokens,
                           tokens(candidate.get("workload"),
                                  candidate.get("namespace"),
                                  candidate.get("source_path")))
        if matched:
            scored.append((len(matched), candidate.get("workload") or "",
                           dict(candidate, matched=matched)))
    scored.sort(key=lambda row: (-row[0], row[1]))
    ranked = [row[2] for row in scored[:max(limit, 0)]]

    shown = ", ".join(repr(t) for t in entry_tokens)
    if not ranked:
        # Nothing is listed under this note, so it must not point at a list —
        # the same dangling promise the token-less branch used to make.
        return [], (
            f"Candidates for {identity} (a GUESS, not a fact): no workload the "
            f"scan saw shares a name token with {shown}, so none is offered. "
            "That does NOT mean nothing uses it — "
            + ("the link a chain did find was rejected at the review, and no "
               "name here resembles it either. Ask "
               if chain_was_cut else
               "part of the Terraform could not be read to the end, so the "
               "chains never got to answer, and the names do not resemble "
               "each other either. Ask "
               if reading_incomplete else
               "the two reference chains missed it and the names do not "
               "resemble each other either. Ask ")
            + f"the platform team who owns {identity}; there is nothing here "
            "to pick from.")
    # The cap is disclosed. "None of these" is only an honest option when the
    # reviewer knows what "these" was: offering it over a silently truncated
    # list invites them to rule out a candidate they were never shown.
    capped = ("" if len(ranked) == len(scored) else
              f" Only the top {len(ranked)} are listed — say so, and narrow the "
              "question rather than letting the user rule out what they cannot "
              "see.")
    return ranked, (
        f"Candidates for {identity} (a GUESS from name similarity, not a "
        f"fact — {because}): "
        f"{len(scored)} workload(s) share a name token with {shown}, ranked by "
        "how many. Put the list to the user WITH 'none of these' as an equal "
        "option, and attach only what they confirm. Never ask them to confirm "
        "the top one alone." + capped)


def lines(candidates: list) -> list:
    """One display line per ranked candidate."""
    out = []
    for candidate in candidates:
        where = candidate.get("namespace") or "namespace not stated literally"
        matched = ", ".join(candidate.get("matched") or [])
        line = f"    {candidate.get('workload')} ({candidate.get('kind')}, {where})"
        # No "shares" clause on an unranked listing: there is no shared token,
        # and writing "shares " with nothing after it would read as a match.
        if matched:
            line += f" — shares {matched}"
        if candidate.get("source_path"):
            line += f"; chart at {candidate['source_path']}"
        out.append(line)
    return out
