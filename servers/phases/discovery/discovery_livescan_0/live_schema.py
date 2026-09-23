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

"""The Live IR's contract: servers/dag/server/schema/live_discovery.json.

The IR is code-authored, so its schema is the structure the walk owns —
the cluster record, the in-cluster sections, the reduced records (nodes,
IRSA bindings) — held closed, while the projected objects inside it stay
open: a Deployment's spec is Kubernetes' contract, not this one's. One
invariant the schema states outright: a Secret record carries key names,
and under data/stringData nothing but the `<omitted>` marker.

The module is stdlib plus jsonschema, imported lazily, so the walk's own
tests can hold every IR they produce to the schema without the server's GCS
or MCP imports — the module's purity, not the import path's: the package's
`__init__` pulls in the tool layer, and with it MCP and GCS. Same shape as
state_management.validate_inventory: draft 2020-12, ValueError on the first
violation with the path to it — and never the value at it: the message is
persisted as a coverage note, and a value that landed in the wrong field is
a value still.
"""

import json
import os

SCHEMA_NAME = "live_discovery.json"
SCHEMA_PATH = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "dag", "server", "schema", SCHEMA_NAME))


def load_schema() -> dict:
    """The Live IR contract, as a dict."""
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_live_ir(ir: dict) -> None:
    """Validates a Live IR against server/schema/live_discovery.json.

    Raises ValueError on the first schema violation, naming where it is.
    """
    import jsonschema

    validator = jsonschema.Draft202012Validator(load_schema())
    for err in sorted(validator.iter_errors(ir),
                      key=lambda e: list(e.absolute_path)):
        where = "/".join(str(p) for p in err.absolute_path) or "<root>"
        raise ValueError(
            f"Live IR failed schema validation at {where}: {_describe(err)}")


# Violations whose jsonschema message names property names only; every
# other message quotes the instance — type, enum, const and pattern
# outright, and the count keywords (`minItems`, `maxProperties`,
# `uniqueItems`) by repeating the array or object they measured.
_MESSAGE_SAFE_VALIDATORS = frozenset({
    "required", "additionalProperties", "dependentRequired"})


def _describe(err) -> str:
    """The violation without the value: the keyword that failed and the
    constraint it set, both from the schema's side."""
    if err.validator in _MESSAGE_SAFE_VALIDATORS:
        return err.message
    return (f"does not satisfy {err.validator} "
            f"{json.dumps(err.validator_value, default=str)}")
