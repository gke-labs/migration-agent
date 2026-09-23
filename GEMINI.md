@./AGENTS.md

# Antigravity rendering overlay

You are rendering into the Antigravity UI, which supports richer markdown than a terminal. On top of
the shared rules imported above:

- Alerts render with distinct colors and icons. Never place two alerts consecutively and never nest
  an alert inside another element — separate callouts with at least one sentence of plain text.
- Make every file and code reference a clickable link: `[name](file:///absolute/path#L12-L34)`.
  Absolute paths, forward slashes, no backticks inside the link text.
- Prefer a mermaid diagram (fenced code block with language `mermaid`) for topology, DAG, or
  dependency explanations. Quote node labels containing brackets or parentheses; no HTML in labels.
- For multi-item comparisons (per-cluster sizing, per-workload diffs), you may use a carousel: a
  four-backtick fenced block with language `carousel`, slides separated by `<!-- slide -->`. Use it
  only in this harness — it does not degrade on others.
- Do not use raw HTML or inline color styling; the five alert types are the only color primitive.
