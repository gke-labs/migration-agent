/* GKE Agentic Migration review frontend.
 *
 * Read-only: polls /api/overview, draws the DAG as SVG, and shows per-step
 * details + artifacts from /api/state and /api/blob. All dynamic content is
 * inserted with textContent (never innerHTML) so ledger/LLM-produced text
 * cannot inject markup.
 */

"use strict";

const POLL_MS = 4000;
const SVG_NS = "http://www.w3.org/2000/svg";

const LAYOUT = {
  nodeWidth: 300,
  nodeHeight: 42,
  nodeX: 130,
  vGap: 26,
  top: 26,
  laneGap: 13,
};

let overview = null;
let selectedState = null;
let pollSeq = 0;          // discards out-of-order /api/overview responses
let dagFingerprint = "";  // skips SVG rebuilds when nothing visible changed
let phaseOverrides = {};  // phase -> expanded? (user toggles; cleared when the workspace moves)
let activeTab = "dag";
let liveRefresher = null; // open live artifact (extraction progress) re-fetched each poll
let auditFingerprint = "";

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function svgEl(tag, attrs) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs || {})) node.setAttribute(key, value);
  return node;
}

const HEADER_H = 30;   // clickable phase header row
const HEADER_GAP = 8;  // space between a header and its first node

/* ---------------- header ---------------- */

function renderHeader(data) {
  const identity = data.identity || {};
  const roles = data.roles || {};

  document.getElementById("dag-name").textContent =
    (data.dag_name ? data.dag_name : "") + (data.dag_version ? " v" + data.dag_version : "");

  const mode = document.getElementById("mode-badge");
  mode.textContent = "live ledger";

  const current = document.getElementById("current-chip");
  current.textContent = data.current_state ? "at " + data.current_state : "no state";

  const who = document.getElementById("who-user");
  who.textContent = identity.user_email || "(user unknown)";

  const roleBits = [];
  if (identity.acting_role) roleBits.push("acting as " + identity.acting_role);
  if (roles.registered_role) roleBits.push("registered: " + roles.registered_role);
  else if (data.registry_ok === false) roleBits.push("workspace registry unavailable");
  else if (identity.user_email) roleBits.push("not in workspace registry");
  if (roles.workspace_name) roleBits.push("workspace: " + roles.workspace_name);
  document.getElementById("who-role").textContent = roleBits.join(" · ");
}

function renderBanner(data) {
  const banner = document.getElementById("error-banner");
  const identity = data.identity || {};
  const problems = [];
  if (data.error) problems.push(data.error);
  if (identity.identity_error) problems.push("Identity: " + identity.identity_error);
  banner.hidden = problems.length === 0;
  banner.textContent = problems.join("  •  ");
}

/* ---------------- DAG rendering ---------------- */

function assignLanes(edges) {
  // Greedy interval lanes so long skip/back edges don't overlap.
  const spans = edges
    .map((edge, i) => ({ edge, i }))
    .sort((a, b) =>
      Math.abs(a.edge._span) - Math.abs(b.edge._span) || a.i - b.i);
  const lanes = [];
  for (const { edge } of spans) {
    const lo = Math.min(edge._fromIdx, edge._toIdx);
    const hi = Math.max(edge._fromIdx, edge._toIdx);
    let lane = 0;
    while ((lanes[lane] || []).some(([a, b]) => lo <= b && hi >= a)) lane++;
    (lanes[lane] = lanes[lane] || []).push([lo, hi]);
    edge._lane = lane;
  }
  return edges;
}

function renderDag(data) {
  const dag = data.dag;
  if (!dag) return;

  const currentNode = dag.nodes.find((n) => n.name === data.current_state);
  const currentPhase = currentNode ? currentNode.phase : null;

  // Rebuild only when something the SVG shows actually changed — a rebuild
  // on every 4s poll would kill open tooltips and restart animations.
  const fingerprint = JSON.stringify([
    data.current_state, data.visited, data.dag_version, selectedState,
    dag.nodes.map((n) => n.name), phaseOverrides,
  ]);
  if (fingerprint === dagFingerprint) return;
  dagFingerprint = fingerprint;

  const svg = document.getElementById("dag-svg");
  svg.textContent = "";

  const nodes = dag.nodes;
  const visited = new Set(data.visited || []);
  const width = 560;

  // Row layout: every phase gets a clickable header row; its nodes are laid
  // out beneath only while the phase is expanded. The phase holding the
  // current step is always expanded; the rest start minimized so the DAG
  // fits the panel, and clicking a header toggles it.
  const bands = [];
  const nodeYs = {};   // node name -> y
  const rowIdx = {};   // node name -> visible ordinal (for edge spans/lanes)
  let y = 8;
  let ordinal = 0;
  for (const band of dag.phases || []) {
    const bandNodes = nodes.slice(band.start, band.end + 1);
    const hasCurrent = bandNodes.some((n) => n.name === data.current_state);
    const expanded = hasCurrent ||
      (band.phase in phaseOverrides ? phaseOverrides[band.phase] : band.phase === currentPhase);
    const info = {
      phase: band.phase, expanded, hasCurrent,
      count: bandNodes.length,
      allVisited: bandNodes.every((n) => visited.has(n.name)),
      anyVisited: bandNodes.some((n) => visited.has(n.name)),
      y0: y,
    };
    y += HEADER_H;
    if (expanded) {
      y += HEADER_GAP;
      for (const node of bandNodes) {
        nodeYs[node.name] = y;
        rowIdx[node.name] = ordinal++;
        y += LAYOUT.nodeHeight + LAYOUT.vGap;
      }
      y -= LAYOUT.vGap / 2;
    }
    info.y1 = y;
    y += 10;
    bands.push(info);
  }
  const height = y + 6;
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

  const defs = svgEl("defs");
  for (const [id, color] of [["arrow", "#9aa5b4"], ["arrow-back", "#d97706"]]) {
    const marker = svgEl("marker", {
      id, viewBox: "0 0 10 10", refX: 9, refY: 5,
      markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse",
    });
    marker.appendChild(svgEl("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: color }));
    defs.appendChild(marker);
  }
  svg.appendChild(defs);

  // Phase bands + headers behind everything.
  bands.forEach((band, i) => {
    svg.appendChild(svgEl("rect", {
      x: 2, y: band.y0, width: width - 4, height: band.y1 - band.y0,
      class: "phase-band" + (i % 2 ? " alt" : ""), rx: 6,
    }));
    const header = svgEl("g", {
      class: "phase-header" + (band.hasCurrent ? " has-current" : ""),
    });
    header.appendChild(svgEl("rect", {
      x: 2, y: band.y0, width: width - 4, height: HEADER_H, class: "phase-header-hit", rx: 6,
    }));
    const arrow = svgEl("text", { x: 12, y: band.y0 + 20, class: "phase-arrow" });
    arrow.textContent = band.expanded ? "▾" : "▸";
    header.appendChild(arrow);
    const label = svgEl("text", { x: 30, y: band.y0 + 20, class: "phase-label" });
    label.textContent = band.phase;
    header.appendChild(label);
    const status = band.hasCurrent ? "in progress"
      : band.allVisited ? "done"
      : band.anyVisited ? "partly done" : "not started";
    const meta = svgEl("text", {
      x: width - 12, y: band.y0 + 20, class: "phase-meta", "text-anchor": "end",
    });
    meta.textContent = band.count + " step" + (band.count === 1 ? "" : "s") + " · " + status;
    header.appendChild(meta);
    const title = svgEl("title");
    title.textContent = band.hasCurrent
      ? "current phase (always expanded)"
      : "click to " + (band.expanded ? "minimize" : "expand");
    header.appendChild(title);
    if (!band.hasCurrent) {
      header.addEventListener("click", () => {
        phaseOverrides[band.phase] = !band.expanded;
        if (overview) renderDag(overview);
      });
    }
    svg.appendChild(header);
  });

  // Edges — only between nodes that are actually visible; flows in or out
  // of a minimized phase are implied by its header.
  const centerX = LAYOUT.nodeX + LAYOUT.nodeWidth / 2;
  const rightX = LAYOUT.nodeX + LAYOUT.nodeWidth;
  const edges = dag.edges
    .filter((e) => nodeYs[e.from] !== undefined && nodeYs[e.to] !== undefined)
    .map((e) => ({
      ...e,
      _fromIdx: rowIdx[e.from],
      _toIdx: rowIdx[e.to],
      _span: rowIdx[e.to] - rowIdx[e.from],
    }));
  assignLanes(edges.filter((e) => e.back && !e.self));            // right channel
  assignLanes(edges.filter((e) => !e.back && !e.self && e._span > 1)); // left channel

  for (const edge of edges) {
    let path;
    if (edge.self) {
      const y0 = nodeYs[edge.from] + LAYOUT.nodeHeight / 2;
      path = svgEl("path", {
        d: `M ${rightX} ${y0 - 8} C ${rightX + 34} ${y0 - 14}, ${rightX + 34} ${y0 + 14}, ${rightX} ${y0 + 8}`,
        class: "edge self", "marker-end": "url(#arrow)",
      });
    } else if (edge._span === 1) {
      const y0 = nodeYs[edge.from] + LAYOUT.nodeHeight;
      const y1 = nodeYs[edge.to];
      path = svgEl("path", {
        d: `M ${centerX} ${y0} L ${centerX} ${y1 - 2}`,
        class: "edge", "marker-end": "url(#arrow)",
      });
    } else if (edge.back) {
      const x = rightX + 26 + edge._lane * LAYOUT.laneGap;
      const y0 = nodeYs[edge.from] + LAYOUT.nodeHeight / 2;
      const y1 = nodeYs[edge.to] + LAYOUT.nodeHeight / 2;
      path = svgEl("path", {
        d: `M ${rightX} ${y0} H ${x} V ${y1} H ${rightX + 4}`,
        class: "edge back", "marker-end": "url(#arrow-back)",
      });
      const label = svgEl("text", {
        x: x + 3, y: (y0 + y1) / 2, class: "edge-label",
        transform: `rotate(90 ${x + 3} ${(y0 + y1) / 2})`, "text-anchor": "middle",
      });
      label.textContent = edge.trigger;
      svg.appendChild(label);
    } else {
      const x = LAYOUT.nodeX - 22 - edge._lane * LAYOUT.laneGap;
      const y0 = nodeYs[edge.from] + LAYOUT.nodeHeight / 2;
      const y1 = nodeYs[edge.to] + LAYOUT.nodeHeight / 2;
      path = svgEl("path", {
        d: `M ${LAYOUT.nodeX} ${y0} H ${x} V ${y1} H ${LAYOUT.nodeX - 4}`,
        class: "edge", "marker-end": "url(#arrow)",
      });
    }
    const title = svgEl("title");
    title.textContent = `${edge.from} —${edge.trigger}→ ${edge.to}`;
    path.appendChild(title);
    svg.appendChild(path);
  }

  // Nodes on top.
  for (const node of nodes) {
    if (nodeYs[node.name] === undefined) continue; // phase minimized
    const y = nodeYs[node.name];
    const group = svgEl("g", {
      class:
        "node t-" + node.type +
        (node.name === data.current_state ? " current" : "") +
        (visited.has(node.name) ? " visited" : "") +
        (node.name === selectedState ? " selected" : ""),
    });
    group.appendChild(svgEl("rect", {
      x: LAYOUT.nodeX, y, width: LAYOUT.nodeWidth, height: LAYOUT.nodeHeight, rx: 8,
    }));
    group.appendChild(svgEl("rect", {
      x: LAYOUT.nodeX + 6, y: y + 7, width: 4, height: LAYOUT.nodeHeight - 14, class: "accent",
    }));

    const label = svgEl("text", { x: LAYOUT.nodeX + 20, y: y + 18 });
    label.textContent = node.display_name;
    group.appendChild(label);

    const sub = svgEl("text", { x: LAYOUT.nodeX + 20, y: y + 33, class: "subtitle" });
    sub.textContent = node.expected_tool_call
      ? "tool: " + node.expected_tool_call + (node.hitl ? " · human sign-off" : "")
      : node.action ? "server: " + node.action
      : node.type === "HITL_ELICITATION" ? "human approval"
      : node.terminal_status ? "terminal: " + node.terminal_status : node.type;
    group.appendChild(sub);

    if (node.name === data.current_state) {
      const marker = svgEl("text", {
        x: LAYOUT.nodeX - 8, y: y + LAYOUT.nodeHeight / 2 + 3,
        class: "current-marker", "text-anchor": "end",
      });
      marker.textContent = "YOU ARE HERE ▶";
      group.appendChild(marker);
    }

    const title = svgEl("title");
    title.textContent = node.name;
    group.appendChild(title);
    group.addEventListener("click", () => selectState(node.name));
    svg.appendChild(group);
  }
}

/* ---------------- detail panel ---------------- */

function metaRow(table, key, value, asCode) {
  if (value === undefined || value === null || value === "") return;
  const row = el("tr");
  row.appendChild(el("td", null, key));
  const cell = el("td");
  if (asCode) cell.appendChild(el("code", null, String(value)));
  else cell.textContent = String(value);
  row.appendChild(cell);
  table.appendChild(row);
}

function prettyJson(value) {
  return JSON.stringify(value, null, 2);
}

function contentPre(text, prose) {
  const pre = el("pre", "content" + (prose ? " prose" : ""));
  pre.textContent = text;
  return pre;
}

async function fetchBlob(path) {
  const response = await fetch("/api/blob?path=" + encodeURIComponent(path));
  if (!response.ok) throw new Error("blob fetch failed: " + response.status);
  return response.json();
}

/* ---------------- safe markdown rendering ----------------
 * Built entirely with DOM nodes and textContent — ledger/LLM text can never
 * inject markup. Covers what the readiness report and design docs use:
 * headings, lists, tables, fenced code, blockquotes, bold, inline code,
 * http(s) links. */

function mdInline(target, text) {
  const pattern = /(\*\*[^*]+\*\*|`[^`]+`|\[[^\]]+\]\(https?:\/\/[^\s)]+\))/g;
  let last = 0;
  for (const match of text.matchAll(pattern)) {
    if (match.index > last) {
      target.appendChild(document.createTextNode(text.slice(last, match.index)));
    }
    const token = match[0];
    if (token.startsWith("**")) {
      target.appendChild(el("strong", null, token.slice(2, -2)));
    } else if (token.startsWith("`")) {
      target.appendChild(el("code", "md-code", token.slice(1, -1)));
    } else {
      const split = token.indexOf("](");
      const anchor = el("a", "ext", token.slice(1, split));
      anchor.href = token.slice(split + 2, -1);
      anchor.target = "_blank";
      anchor.rel = "noopener noreferrer";
      target.appendChild(anchor);
    }
    last = match.index + token.length;
  }
  if (last < text.length) target.appendChild(document.createTextNode(text.slice(last)));
}

function renderMarkdown(container, text) {
  const root = el("div", "markdown");
  const lines = String(text).split("\n");
  let i = 0;
  let paragraph = [];
  const flushParagraph = () => {
    if (!paragraph.length) return;
    const p = el("p");
    mdInline(p, paragraph.join(" "));
    root.appendChild(p);
    paragraph = [];
  };
  while (i < lines.length) {
    const trimmed = lines[i].trim();

    if (trimmed.startsWith("```")) {
      flushParagraph();
      const code = [];
      i++;
      while (i < lines.length && !lines[i].trim().startsWith("```")) code.push(lines[i++]);
      i++; // closing fence
      root.appendChild(contentPre(code.join("\n")));
      continue;
    }
    const heading = /^(#{1,4})\s+(.*)$/.exec(trimmed);
    if (heading) {
      flushParagraph();
      const h = el("h" + Math.min(6, heading[1].length + 2), "md-h");
      mdInline(h, heading[2]);
      root.appendChild(h);
      i++;
      continue;
    }
    if (/^(-{3,}|\*{3,}|_{3,})$/.test(trimmed)) {
      flushParagraph();
      root.appendChild(el("hr"));
      i++;
      continue;
    }
    if (/^[-*]\s+/.test(trimmed) || /^\d+[.)]\s+/.test(trimmed)) {
      flushParagraph();
      const ordered = /^\d/.test(trimmed);
      const list = el(ordered ? "ol" : "ul", "md-list");
      while (i < lines.length) {
        const item = lines[i].trim();
        const m = ordered ? /^\d+[.)]\s+(.*)$/.exec(item) : /^[-*]\s+(.*)$/.exec(item);
        if (!m) break;
        let bodyText = m[1];
        i++;
        // hanging indented continuation lines belong to the same item
        while (i < lines.length && /^\s{2,}\S/.test(lines[i])
               && !/^\s*([-*]|\d+[.)])\s/.test(lines[i])) {
          bodyText += " " + lines[i].trim();
          i++;
        }
        const li = el("li");
        mdInline(li, bodyText);
        list.appendChild(li);
      }
      root.appendChild(list);
      continue;
    }
    if (trimmed.startsWith("|") && i + 1 < lines.length
        && /^\|?[\s:|-]+\|?$/.test(lines[i + 1].trim()) && lines[i + 1].includes("-")) {
      flushParagraph();
      const table = el("table", "md-table");
      const parseRow = (row) => row.trim().replace(/^\||\|$/g, "").split("|").map((c) => c.trim());
      const thead = el("thead");
      const headRow = el("tr");
      for (const cell of parseRow(trimmed)) {
        const th = el("th");
        mdInline(th, cell);
        headRow.appendChild(th);
      }
      thead.appendChild(headRow);
      table.appendChild(thead);
      i += 2;
      const tbody = el("tbody");
      while (i < lines.length && lines[i].trim().startsWith("|")) {
        const tr = el("tr");
        for (const cell of parseRow(lines[i])) {
          const td = el("td");
          mdInline(td, cell);
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
        i++;
      }
      table.appendChild(tbody);
      root.appendChild(table);
      continue;
    }
    if (trimmed.startsWith(">")) {
      flushParagraph();
      const quote = el("blockquote", "md-quote");
      const quoteLines = [];
      while (i < lines.length && lines[i].trim().startsWith(">")) {
        quoteLines.push(lines[i].trim().replace(/^>\s?/, ""));
        i++;
      }
      mdInline(quote, quoteLines.join(" "));
      root.appendChild(quote);
      continue;
    }
    if (!trimmed) {
      flushParagraph();
      i++;
      continue;
    }
    paragraph.push(trimmed);
    i++;
  }
  flushParagraph();
  container.appendChild(root);
}

function renderBlobInto(container, payload, render) {
  if (render === "inventory") { renderInventory(container, payload); return; }
  if (render === "validation-report") { renderValidationReport(container, payload); return; }
  if (render === "comparisons") { renderComparisons(container, payload); return; }
  container.textContent = "";
  if (payload.truncated) {
    container.appendChild(el("p", "muted", "(truncated at 2 MB)"));
  }
  if (render === "markdown") {
    renderMarkdown(container, payload.content);
    return;
  }
  if (render === "json") {
    try {
      container.appendChild(contentPre(prettyJson(JSON.parse(payload.content))));
      return;
    } catch (err) { /* fall through to raw */ }
  }
  container.appendChild(contentPre(payload.content));
}

function summarizeInventoryItem(item) {
  if (typeof item !== "object" || item === null) return String(item);
  const nameKeys = ["service", "identifier", "name", "id", "cluster", "path", "namespace", "kind", "version"];
  const parts = [];
  for (const key of nameKeys) if (item[key]) parts.push(String(item[key]));
  return parts.length ? parts.join(" · ") : JSON.stringify(item).slice(0, 100);
}

function renderInventory(container, payload) {
  container.textContent = "";
  let inv;
  try { inv = JSON.parse(payload.content); } catch (err) {
    container.appendChild(contentPre(payload.content));
    return;
  }
  renderInventoryObject(container, inv);
}

function renderInventoryObject(container, inv) {
  // The discovered estate, resource by resource: every top-level inventory
  // category as a count + expandable item list, raw JSON one click away.
  for (const [key, value] of Object.entries(inv)) {
    if (Array.isArray(value)) {
      const details = el("details", "file-block");
      const summary = el("summary");
      summary.appendChild(el("code", null, key + " (" + value.length + ")"));
      details.appendChild(summary);
      if (value.length) {
        const list = el("ul", "plain");
        value.forEach((item) => list.appendChild(el("li", null, summarizeInventoryItem(item))));
        details.appendChild(list);
        const raw = el("details", "file-block");
        raw.appendChild(el("summary", null, "raw json"));
        raw.appendChild(contentPre(prettyJson(value)));
        details.appendChild(raw);
      }
      container.appendChild(details);
    } else if (typeof value === "object" && value !== null) {
      container.appendChild(el("div", "section-h", key));
      const chips = el("div", "chip-row");
      let scalars = 0;
      for (const [k, v] of Object.entries(value)) {
        if (typeof v !== "object" || v === null) {
          chips.appendChild(el("span", "chip", k + ": " + String(v)));
          scalars += 1;
        }
      }
      if (scalars) container.appendChild(chips);
      if (scalars < Object.keys(value).length) {
        const raw = el("details", "file-block");
        raw.appendChild(el("summary", null, "details"));
        raw.appendChild(contentPre(prettyJson(value)));
        container.appendChild(raw);
      }
    } else {
      const chips = el("div", "chip-row");
      chips.appendChild(el("span", "chip", key + ": " + String(value)));
      container.appendChild(chips);
    }
  }
}

function renderExtractionProgress(holder, data) {
  // Live view while extraction runs: chunk progress bar plus the resources
  // merged from the fragments persisted so far.
  holder.textContent = "";
  if (!data.available) {
    holder.appendChild(el("p", "muted",
      "Extraction has not started for this scope yet — progress appears here "
      + "the moment the first worker finishes."));
    return;
  }
  const total = data.total || 0;
  const done = data.done || 0;
  const bar = el("div", "progress-bar");
  const fill = el("div", "progress-fill");
  fill.style.width = total ? Math.round((100 * done) / total) + "%" : "0%";
  bar.appendChild(fill);
  holder.appendChild(bar);
  let label = done + " of " + total + " chunks extracted";
  if (data.reused) label += " (" + data.reused + " reused from a previous run)";
  holder.appendChild(el("p", "muted", label));
  if (data.inventory) {
    holder.appendChild(el("div", "section-h", "Resources found so far"));
    const invHolder = el("div", null);
    renderInventoryObject(invHolder, data.inventory);
    holder.appendChild(invHolder);
  }
}


function renderTranslationProgress(holder, data) {
  // Live view while translation runs: a bar over the units, plus which of this
  // run's units have landed. The generated code itself shows in the 'Translated
  // units' tab as each worker finishes.
  holder.textContent = "";
  if (!data.available) {
    holder.appendChild(el("p", "muted",
      "Translation has not started yet — progress appears here the moment the "
      + "first unit finishes."));
    return;
  }
  const total = data.total || 0;
  const done = data.done || 0;
  const bar = el("div", "progress-bar");
  const fill = el("div", "progress-fill");
  fill.style.width = total ? Math.round((100 * done) / total) + "%" : "0%";
  bar.appendChild(fill);
  holder.appendChild(bar);
  let label = done + " of " + total + " units translated";
  if (data.failed) label += ", " + data.failed + " failed";
  if (data.reused) label += " (" + data.reused + " kept from a previous run)";
  holder.appendChild(el("p", "muted", label));

  const doneSet = new Set(data.done_ids || []);
  const errorSet = new Set(data.error_ids || []);
  const pending = data.pending_ids || [];
  if (pending.length) {
    holder.appendChild(el("div", "section-h", "Units this run"));
    const list = el("ul", "plain");
    pending.forEach((id) => {
      const item = el("li", null);
      // A finished unit is either a success or an error — an errored unit gets a
      // distinct red chip, never the green "done" that would hide failures.
      if (errorSet.has(id)) {
        item.appendChild(el("span", "chip bad", "failed"));
      } else if (doneSet.has(id)) {
        item.appendChild(el("span", "chip ok", "done"));
      } else {
        item.appendChild(el("span", "chip", "translating…"));
      }
      item.appendChild(document.createTextNode(" " + id));
      list.appendChild(item);
    });
    holder.appendChild(list);
  }
}

function renderUnitBundle(container, payload) {
  container.textContent = "";
  let blob;
  try {
    blob = JSON.parse(payload.content);
  } catch (err) {
    container.appendChild(contentPre(payload.content));
    return;
  }
  // run_translation persists {"unit": <plan unit>, "result": <worker output>};
  // status/kind/feedback/error live on the nested plan unit.
  const unit = blob.unit || blob;
  const result = blob.result || unit.result || {};
  const meta = el("div", "chip-row");
  if (unit.status) meta.appendChild(el("span", "chip", "status: " + unit.status));
  if (unit.kind) meta.appendChild(el("span", "chip", unit.kind));
  container.appendChild(meta);

  for (const file of result.files || []) {
    const details = el("details", "file-block");
    const summary = el("summary");
    summary.appendChild(el("code", null, file.path));
    details.appendChild(summary);
    details.appendChild(contentPre(file.content));
    container.appendChild(details);
  }
  if (result.tradeoffs) {
    container.appendChild(el("div", "section-h", "Tradeoffs"));
    container.appendChild(contentPre(result.tradeoffs, true));
  }
  for (const [key, label] of [["assumptions", "Assumptions"], ["open_questions", "Open questions"]]) {
    const items = result[key] || [];
    if (!items.length) continue;
    container.appendChild(el("div", "section-h", label));
    const list = el("ul", "plain");
    items.forEach((item) => list.appendChild(el("li", null, String(item))));
    container.appendChild(list);
  }
  if (unit.feedback) {
    container.appendChild(el("div", "section-h", "Reviewer feedback"));
    container.appendChild(contentPre(String(unit.feedback), true));
  }
  if (unit.error) {
    container.appendChild(el("div", "section-h", "Worker error"));
    container.appendChild(contentPre(String(unit.error)));
  }
}

function renderValidationReport(container, payload) {
  // run_generated_validation's outcome: the per-directory terraform-validate
  // result for the landing-zone draft at the clone root ('.') and each
  // translated unit directory (clean / auto-fixed / still-failing with its
  // error text), plus the structural check over the units' K8s manifests.
  container.textContent = "";
  let report;
  try {
    report = JSON.parse(payload.content);
  } catch (err) {
    container.appendChild(contentPre(payload.content));
    return;
  }
  const clean = (report.clean || []).length;
  const fixed = (report.fixed || []).length;
  const remaining = (report.remaining || []).length;
  const manifests = report.manifests || {};
  const manifestsChecked = manifests.checked || 0;
  const manifestsInvalid = (manifests.invalid || []).length;

  const failures = [];
  if (remaining) {
    failures.push(remaining + (remaining === 1 ? " directory is" : " directories are")
      + " still failing after the auto-fix pass");
  }
  if (manifestsInvalid) {
    failures.push(manifestsInvalid + (manifestsInvalid === 1 ? " manifest is" : " manifests are")
      + " structurally invalid");
  }
  container.appendChild(el("p", report.all_valid ? null : "muted caveat",
    report.all_valid
      ? "✔ Every directory validated" + (fixed ? " (" + fixed + " after an auto-fix)" : "")
        + (manifestsChecked
            ? ", " + manifestsChecked + " manifest" + (manifestsChecked === 1 ? "" : "s")
              + " structurally valid."
            : ".")
      : "⚠ " + failures.join("; ") + "."));

  const chips = el("div", "chip-row");
  chips.appendChild(el("span", "chip ok", clean + " clean"));
  chips.appendChild(el("span", "chip", fixed + " auto-fixed"));
  chips.appendChild(el("span", "chip" + (remaining ? " bad" : ""), remaining + " failing"));
  if (manifestsChecked) {
    chips.appendChild(el("span", "chip" + (manifestsInvalid ? " bad" : " ok"),
      (manifestsChecked - manifestsInvalid) + "/" + manifestsChecked + " manifests"));
  }
  container.appendChild(chips);

  const dirs = report.dirs || [];
  if (!dirs.length) {
    container.appendChild(el("p", "muted", "No directories were validated."));
    return;
  }
  container.appendChild(el("div", "section-h", "Directories"));
  dirs.forEach((entry) => {
    const label = entry.dir === "." ? "(landing-zone root)" : entry.dir;
    const status = entry.status || "unknown";
    if (status === "clean") {
      const row = el("div", "chip-row");
      row.appendChild(el("span", "chip ok", "clean"));
      row.appendChild(document.createTextNode(" " + label));
      container.appendChild(row);
      return;
    }
    const block = el("details", "file-block");
    const summary = el("summary");
    summary.appendChild(el("span", "chip" + (status === "failed" ? " bad" : ""),
      status === "failed" ? "failing" : "auto-fixed"));
    const attempts = entry.attempts ? " · " + entry.attempts + " attempt" + (entry.attempts === 1 ? "" : "s") : "";
    summary.appendChild(document.createTextNode(" " + label + attempts));
    block.appendChild(summary);
    if (entry.original_error) {
      block.appendChild(el("div", "section-h",
        status === "failed" ? "First error" : "Original error (now fixed)"));
      block.appendChild(contentPre(entry.original_error));
    }
    if (status === "failed" && entry.final_error) {
      block.appendChild(el("div", "section-h", "Still failing"));
      block.appendChild(contentPre(entry.final_error));
    }
    container.appendChild(block);
  });

  if (manifestsInvalid) {
    container.appendChild(el("div", "section-h", "Manifests"));
    (manifests.invalid || []).forEach((entry) => {
      const block = el("details", "file-block");
      const summary = el("summary");
      summary.appendChild(el("span", "chip bad", "invalid"));
      summary.appendChild(document.createTextNode(" " + entry.file));
      block.appendChild(summary);
      if (entry.error) block.appendChild(contentPre(entry.error));
      container.appendChild(block);
    });
  }
}

function renderComparisons(container, payload) {
  // The before/after the reviewer signs off before the PR opens: discovered
  // AWS inputs on one side, the generated (and possibly auto-fixed) GCP
  // code on the other, with each unit's tradeoffs and open questions.
  container.textContent = "";
  let comps;
  try {
    comps = JSON.parse(payload.content);
  } catch (err) {
    container.appendChild(contentPre(payload.content));
    return;
  }
  if (!Array.isArray(comps) || !comps.length) {
    container.appendChild(el("p", "muted", "No before/after comparison produced yet."));
    return;
  }
  comps.forEach((c) => {
    const card = el("details", "unit-card");
    const summary = el("summary");
    summary.appendChild(el("code", null, c.title || c.unit_id || "unit"));
    if (c.kind) summary.appendChild(el("span", "chip", c.kind));
    if (c.autofix_attempts) summary.appendChild(el("span", "chip", "auto-fixed ×" + c.autofix_attempts));
    card.appendChild(summary);
    const body = el("div", "body");

    body.appendChild(el("div", "section-h", "Before — discovered AWS inputs"));
    const before = c.before || {};
    if (before.inputs && Object.keys(before.inputs).length) {
      body.appendChild(contentPre(prettyJson(before.inputs)));
    } else {
      body.appendChild(el("p", "muted", "No inputs recorded."));
    }
    if ((before.notes || []).length) {
      const list = el("ul", "plain");
      before.notes.forEach((n) => list.appendChild(el("li", null, String(n))));
      body.appendChild(list);
    }

    const after = c.after || {};
    body.appendChild(el("div", "section-h",
      "After — generated GCP code" + (after.dir ? " (" + after.dir + ")" : "")));
    const files = after.files || [];
    if (!files.length) body.appendChild(el("p", "muted", "No files."));
    files.forEach((f) => {
      const fb = el("details", "file-block");
      const fs = el("summary");
      fs.appendChild(el("code", null, f.path));
      fb.appendChild(fs);
      fb.appendChild(contentPre(f.content));
      body.appendChild(fb);
    });

    if (c.tradeoffs) {
      body.appendChild(el("div", "section-h", "Tradeoffs"));
      body.appendChild(contentPre(String(c.tradeoffs), true));
    }
    for (const [key, label] of [["assumptions", "Assumptions"], ["open_questions", "Open questions"]]) {
      const items = c[key] || [];
      if (!items.length) continue;
      body.appendChild(el("div", "section-h", label));
      const list = el("ul", "plain");
      items.forEach((item) => list.appendChild(el("li", null, String(item))));
      body.appendChild(list);
    }
    card.appendChild(body);
    container.appendChild(card);
  });
}

function artifactBody(artifact) {
  const body = el("div", "body");

  if (artifact.endpoint) {
    // Server-computed live artifact: fetched now, then again on every
    // overview poll while it stays open, so progress updates in place. The
    // renderer is chosen by artifact.render so each live endpoint draws its
    // own view from the same poll machinery.
    const ENDPOINT_RENDERERS = {
      "extraction-progress": renderExtractionProgress,
      "translation-progress": renderTranslationProgress,
    };
    const renderProgress = ENDPOINT_RENDERERS[artifact.render] || renderExtractionProgress;
    const holder = el("div", null);
    holder.appendChild(el("p", "muted", "Loading…"));
    let lastFingerprint = "";
    let inflight = false; // one fetch at a time — also rules out stale reordering
    const update = () => {
      if (inflight) return;
      inflight = true;
      fetch(artifact.endpoint)
        .then((response) => {
          if (!response.ok) throw new Error("progress fetch failed: " + response.status);
          return response.json();
        })
        .then((data) => {
          const fp = JSON.stringify([data.available, data.done, data.failed, data.total, data.reused]);
          if (fp === lastFingerprint) return; // don't clobber open sections
          lastFingerprint = fp;
          renderProgress(holder, data);
        })
        .catch((err) => {
          lastFingerprint = "";
          holder.textContent = "";
          holder.appendChild(el("p", "muted", String(err)));
        })
        .finally(() => { inflight = false; });
    };
    update();
    liveRefresher = () => {
      if (!holder.isConnected) { liveRefresher = null; return; } // panel rebuilt
      const details = holder.closest("details");
      if (details && !details.open) return; // collapsed: stop hitting the server
      update();
    };
    body.appendChild(holder);
    return body;
  }

  if (artifact.content !== undefined) {
    if (artifact.render === "link" && typeof artifact.content === "string") {
      const url = artifact.content;
      if (/^https?:\/\//.test(url)) {
        const anchor = el("a", "ext", url);
        anchor.href = url;
        anchor.target = "_blank";
        anchor.rel = "noopener noreferrer";
        body.appendChild(anchor);
      } else {
        body.appendChild(contentPre(url));
      }
    } else {
      body.appendChild(contentPre(prettyJson(artifact.content)));
    }
    return body;
  }

  if (artifact.note) {
    body.appendChild(el("p", "muted", artifact.note));
    return body;
  }

  if (artifact.blob) {
    const holder = el("div", null);
    holder.appendChild(el("p", "muted", "Loading…"));
    fetchBlob(artifact.blob)
      .then((payload) => renderBlobInto(holder, payload, artifact.render))
      .catch((err) => { holder.textContent = ""; holder.appendChild(el("p", "muted", String(err))); });
    body.appendChild(holder);
    return body;
  }

  if (artifact.items) {
    // Prefix-listing artifact (extraction fragments, translated units): one
    // lazy-loading card per ledger path. During the producing *_RUNNING state the workers persist
    // blobs one at a time, so re-poll /api/state and append cards for newly-landed
    // paths as they appear — the tab fills in live instead of freezing at the set
    // it had when first opened. Only unseen paths are appended, so an expanded
    // card the reviewer is reading is never rebuilt or collapsed.
    const seen = new Set();
    const emptyNote = el("p", "muted", "No items yet.");

    const addCard = (path) => {
      if (seen.has(path)) return;
      seen.add(path);
      const item = el("details", "unit-card");
      const name = path.split("/").pop();
      item.appendChild(el("summary", null, name));
      const holder = el("div", "body");
      item.appendChild(holder);
      let loaded = false;
      item.addEventListener("toggle", () => {
        if (!item.open || loaded) return;
        loaded = true;
        holder.appendChild(el("p", "muted", "Loading…"));
        fetchBlob(path)
          .then((payload) => {
            // The artifact's own `render`, not a hardcoded "json". Every
            // prefix artifact was JSON until the data migration procedures
            // arrived, so a markdown one under a prefix rendered as a
            // monospace dump of its own markup — headings, tables and
            // command lines unwrapped, in the document an operator reads to
            // run a database migration.
            if (artifact.render === "unit-bundle") renderUnitBundle(holder, payload);
            else { renderBlobInto(holder, payload, artifact.render || "json"); }
          })
          .catch((err) => { holder.textContent = ""; holder.appendChild(el("p", "muted", String(err))); });
      });
      body.appendChild(item);
    };

    (artifact.items || []).forEach(addCard);
    if (!seen.size) body.appendChild(emptyNote);

    liveRefresher = () => {
      if (!body.isConnected) { liveRefresher = null; return; } // panel rebuilt
      // Only the current, still-running step is actively producing new blobs.
      if (!(overview && overview.current_state === selectedState
            && /_RUNNING$/.test(selectedState))) return;
      fetch("/api/state/" + encodeURIComponent(selectedState))
        .then((response) => (response.ok ? response.json() : null))
        .then((detail) => {
          if (!detail || !body.isConnected) return;
          const fresh = (detail.artifacts || []).find((a) => a.id === artifact.id);
          ((fresh && fresh.items) || []).forEach(addCard);
          if (seen.size && emptyNote.isConnected) emptyNote.remove();
        })
        .catch(() => {}); // best-effort; the next poll retries
    };
    return body;
  }

  body.appendChild(el("p", "muted", "Not produced yet."));
  return body;
}

function renderArtifactTabs(panel, artifacts) {
  if (!artifacts.length) {
    panel.appendChild(el("p", "muted", "No artifacts are registered for this step."));
    return;
  }
  const bar = el("div", "subtabs");
  const content = el("div", "artifact-content");
  const buttons = [];
  let active = -1;
  const select = (idx) => {
    if (idx === active) return;
    active = idx;
    buttons.forEach((b, j) => b.classList.toggle("active", j === idx));
    liveRefresher = null; // a live artifact re-registers when its tab builds
    content.textContent = "";
    const artifact = artifacts[idx];
    if (artifact.caveat) content.appendChild(el("p", "muted caveat", "⚠ " + artifact.caveat));
    if (!artifact.available && !artifact.endpoint) {
      content.appendChild(el("p", "muted", "Not produced yet."));
      return;
    }
    content.appendChild(artifactBody(artifact));
  };
  artifacts.forEach((artifact, idx) => {
    const button = el("button", "subtab", artifact.label);
    button.type = "button";
    if (!artifact.available) button.appendChild(el("span", "subtab-badge", "not yet"));
    else if (artifact.items) button.appendChild(el("span", "subtab-badge yes", String(artifact.items.length)));
    button.addEventListener("click", () => select(idx));
    buttons.push(button);
    bar.appendChild(button);
  });
  panel.appendChild(bar);
  panel.appendChild(content);
  // The server puts the state's primary artifact first; fall back to the
  // first available one (e.g. report not generated yet), else just the first.
  const firstAvailable = artifacts.findIndex((a) => a.available);
  select(artifacts[0].available ? 0 : Math.max(firstAvailable, 0));
}

async function selectState(name) {
  selectedState = name;
  liveRefresher = null; // the panel is being rebuilt; a new live artifact re-registers
  if (overview) renderDag(overview);

  const head = document.getElementById("step-head");
  const panel = document.getElementById("detail-content");
  head.textContent = "";
  head.appendChild(el("p", "muted", "Loading " + name + "…"));

  let detail;
  try {
    const response = await fetch("/api/state/" + encodeURIComponent(name));
    detail = await response.json();
    if (!response.ok) throw new Error(detail.error || response.status);
  } catch (err) {
    if (selectedState !== name) return; // user clicked elsewhere meanwhile
    head.textContent = "";
    head.appendChild(el("p", "muted", "Failed to load state: " + err));
    // Clear the right column too — otherwise a stale panel (with an enabled
    // sign-off bar) lingers under the error. It reloads on the next poll.
    panel.textContent = "";
    panel.appendChild(el("p", "muted", "Could not load this step — retrying on the next refresh."));
    return;
  }
  if (selectedState !== name) return; // user clicked elsewhere meanwhile

  head.textContent = "";
  panel.textContent = "";
  const node = detail.node;

  // --- left column: what this step is, and where it can go ---
  head.appendChild(el("h2", "state-title", node.display_name));
  const chips = el("div", "chip-row");
  chips.appendChild(el("span", "chip type-" + node.type, node.type));
  if (node.hitl) chips.appendChild(el("span", "chip type-HITL_ELICITATION", "human sign-off"));
  chips.appendChild(el("span", "chip phase", "phase: " + node.phase));
  if (overview && overview.current_state === name) {
    chips.appendChild(el("span", "chip current", "current step"));
  }
  if (overview && (overview.visited || []).includes(name)) {
    chips.appendChild(el("span", "chip ok", "visited"));
  }
  head.appendChild(chips);

  const table = el("table", "meta-table");
  metaRow(table, "State", node.name, true);
  metaRow(table, "Expected tool call", node.expected_tool_call, true);
  metaRow(table, "Server action", node.action, true);
  metaRow(table, "Approval prompt", node.prompt_template);
  metaRow(table, "Terminal status", node.terminal_status);
  head.appendChild(table);

  head.appendChild(el("div", "section-h", "Transitions"));
  const transitions = detail.transitions || {};
  if (!Object.keys(transitions).length) {
    head.appendChild(el("p", "muted", "None (terminal state)."));
  }
  for (const [trigger, target] of Object.entries(transitions)) {
    const row = el("div", "transition-row");
    row.appendChild(el("code", null, trigger));
    row.appendChild(document.createTextNode(" → "));
    const jump = el("span", "jump", target);
    jump.addEventListener("click", () => selectState(target));
    row.appendChild(jump);
    head.appendChild(row);
  }

  // --- right column: artifacts ---
  renderArtifactTabs(panel, detail.artifacts || []);
}

/* ---------------- tabs + audit ---------------- */

function setTab(tab) {
  activeTab = tab;
  document.getElementById("view-dag").hidden = tab !== "dag";
  document.getElementById("view-audit").hidden = tab !== "audit";
  for (const button of document.querySelectorAll("#tabs .tab")) {
    button.classList.toggle("active", button.dataset.tab === tab);
  }
  if (tab === "audit" && overview) renderAudit(overview, true);
}

function classifyHistoryLine(line) {
  // Anchored prefixes first: a "Transitioned ... -> STATE_..._REVIEW" line
  // would otherwise match the human-keyword regex and mislabel plain transitions.
  if (/^Transitioned /.test(line)) return "transition";
  if (/^Action '/.test(line)) return "server";
  if (/HITL|approved|rejected|declined|cancelled|human/i.test(line)) return "human";
  if (/decision|Scope amended|resolved/i.test(line)) return "input";
  return "note";
}

function renderAudit(data, force) {
  const lines = data.history || [];
  // Fingerprint the whole window: at the server's 100-entry cap the window
  // slides, so length + last line alone can miss a byte-identical repeat.
  const fingerprint = JSON.stringify(lines);
  if (!force && fingerprint === auditFingerprint) return;
  auditFingerprint = fingerprint;

  document.getElementById("audit-note").textContent =
    lines.length >= 100 ? "last 100 entries" : lines.length + " entries";
  const list = document.getElementById("audit-list");
  list.textContent = "";
  if (!lines.length) {
    list.appendChild(el("p", "muted", "No actions recorded yet."));
    return;
  }
  lines.forEach((line, i) => {
    const text = String(line);
    const row = el("div", "audit-row");
    row.appendChild(el("span", "audit-idx", String(i + 1)));
    const kind = classifyHistoryLine(text);
    row.appendChild(el("span", "audit-badge " + kind, kind));
    row.appendChild(el("span", "audit-text", text));
    list.appendChild(row);
  });
}

for (const button of document.querySelectorAll("#tabs .tab")) {
  button.addEventListener("click", () => setTab(button.dataset.tab));
}

/* ---------------- poll loop ---------------- */

async function refresh() {
  const seq = ++pollSeq;
  let data;
  try {
    const response = await fetch("/api/overview");
    data = await response.json();
  } catch (err) {
    if (seq !== pollSeq) return;
    const banner = document.getElementById("error-banner");
    banner.hidden = false;
    banner.textContent = "Frontend server unreachable: " + err;
    return;
  }
  if (seq !== pollSeq) return; // a newer poll already resolved

  renderBanner(data);
  if (data.error) {
    renderHeader(data);
    return;
  }

  const previousCurrent = overview ? overview.current_state : null;
  const stateChanged = previousCurrent !== data.current_state;
  const firstLoad = overview === null;
  overview = data;
  renderHeader(data);

  if (stateChanged && !firstLoad) {
    // The workspace moved on: drop manual expand/collapse choices so the
    // view follows the migration into its new phase.
    phaseOverrides = {};
  }

  renderDag(data);
  if (activeTab === "audit") renderAudit(data);
  if (liveRefresher) liveRefresher();
  document.getElementById("updated-at").textContent =
    "updated " + new Date().toLocaleTimeString();

  if (firstLoad && data.current_state) {
    selectState(data.current_state);
  } else if (stateChanged && data.current_state &&
             (selectedState === previousCurrent || selectedState === data.current_state)) {
    // Follow the workspace when the reviewer was looking at the old tip, and
    // re-render when the step they're already inspecting just became the live
    // tip (so its human sign-off bar appears) — but never yank the panel away
    // from some other step they chose to inspect.
    selectState(data.current_state);
  }
}

refresh();
setInterval(refresh, POLL_MS);
