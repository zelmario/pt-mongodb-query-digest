(function () {
  "use strict";
  const DATA = JSON.parse(document.getElementById("data").textContent);
  const tbody = document.querySelector("#classes tbody");
  const search = document.getElementById("search");
  const opFilter = document.getElementById("op-filter");
  const flagFilter = document.getElementById("flag-filter");
  const sortSel = document.getElementById("sort");
  const resultCount = document.getElementById("result-count");
  const detail = document.getElementById("detail");
  const backdrop = document.getElementById("detail-backdrop");

  const ops = new Set();
  for (const c of DATA.classes) ops.add(c.op);
  for (const op of Array.from(ops).sort()) {
    const o = document.createElement("option");
    o.value = op; o.textContent = op;
    opFilter.appendChild(o);
  }

  const histLabels = ["<1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", "100-1000s", "1000s+"];

  function fmtMs(ms) {
    if (!ms) return "0";
    if (ms < 1) return (ms * 1000).toFixed(0) + "us";
    if (ms < 1000) return ms.toFixed(1) + "ms";
    if (ms < 60_000) return (ms / 1000).toFixed(2) + "s";
    return (ms / 60_000).toFixed(1) + "m";
  }
  function fmtRatio(r) {
    if (!r || r === 0) return "-";
    if (r > 1e17) return "inf";
    return r.toFixed(1) + ":1";
  }
  function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, c => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    })[c]);
  }
  function prettyJSON(s) {
    try { return JSON.stringify(JSON.parse(s), null, 2); }
    catch { return s; }
  }

  function getSortKey(c) {
    switch (sortSel.value) {
      case "count": return c.count;
      case "p95": return c.p95_ms;
      case "max": return c.max_ms;
      case "ratio": return c.exam_return_ratio || 0;
      default: return c.total_ms;
    }
  }

  function currentView() {
    const q = search.value.trim().toLowerCase();
    const op = opFilter.value;
    const flag = flagFilter.value;
    let rows = DATA.classes.filter(c => {
      if (op && c.op !== op) return false;
      if (flag && !(c.flags || []).includes(flag)) return false;
      if (q) {
        const hay = (c.namespace + " " + c.shape + " " + (c.flags || []).join(" ")).toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
    rows.sort((a, b) => getSortKey(b) - getSortKey(a));
    return rows;
  }

  function render() {
    const rows = currentView();
    resultCount.textContent = rows.length + " / " + DATA.classes.length + " classes";
    tbody.innerHTML = "";
    if (rows.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="9" class="hint">No classes match the current filters.</td>`;
      tbody.appendChild(tr);
      return;
    }
    for (let i = 0; i < rows.length; i++) {
      const c = rows[i];
      const tr = document.createElement("tr");
      tr.dataset.id = c.id;
      tr.innerHTML =
        `<td>${i + 1}</td>` +
        `<td class="id">${escapeHTML(c.id)}</td>` +
        `<td><span class="op">${escapeHTML(c.op)}</span></td>` +
        `<td>${escapeHTML(c.namespace)}</td>` +
        `<td class="num">${c.count.toLocaleString()}</td>` +
        `<td class="num">${fmtMs(c.total_ms)}</td>` +
        `<td class="num">${fmtMs(c.p95_ms)}</td>` +
        `<td class="num">${fmtRatio(c.exam_return_ratio)}</td>` +
        `<td>${(c.flags || []).map(f => `<span class="badge">${escapeHTML(f)}</span>`).join("")}</td>`;
      tr.addEventListener("click", () => openDetail(c.id));
      tbody.appendChild(tr);
    }
  }

  function openDetail(id) {
    const c = DATA.classes.find(x => x.id === id);
    if (!c) return;
    const flagsHTML = (c.flags || []).map(f => {
      const d = (DATA.flag_descriptions || {})[f] || {};
      return `<div class="flag-card">
        <div class="name">${escapeHTML(f)}</div>
        <div class="title">${escapeHTML(d.title || "")}</div>
        <div class="why">${escapeHTML(d.why || "")}</div>
        <div class="fix">Fix: ${escapeHTML(d.fix || "")}</div>
      </div>`;
    }).join("") || `<div class="hint">No anti-patterns detected.</div>`;

    const hist = c.histogram || [];
    const max = Math.max(1, ...hist);
    const histHTML = histLabels.map((lbl, i) => {
      const n = hist[i] || 0;
      const w = (n / max) * 100;
      return `<span class="label">${lbl}</span>` +
             `<span class="bar" style="width:${w}%;${n===0?'opacity:.15':''}"></span>` +
             `<span class="count">${n}</span>`;
    }).join("");

    detail.innerHTML =
      `<h2>${escapeHTML(c.op)} — ${escapeHTML(c.namespace)}
        <button class="close" aria-label="Close">×</button></h2>
      <dl class="kv">
        <dt>ID</dt><dd>${escapeHTML(c.id)}</dd>
        <dt>Calls</dt><dd>${c.count.toLocaleString()}</dd>
        <dt>Total exec</dt><dd>${fmtMs(c.total_ms)}</dd>
        <dt>min / avg / max</dt><dd>${fmtMs(c.min_ms)} / ${fmtMs(c.avg_ms)} / ${fmtMs(c.max_ms)}</dd>
        <dt>median / p95 / p99</dt><dd>${fmtMs(c.median_ms)} / ${fmtMs(c.p95_ms)} / ${fmtMs(c.p99_ms)}</dd>
        <dt>stddev</dt><dd>${fmtMs(c.stddev_ms)}</dd>
        <dt>examined/returned/keys avg</dt><dd>${c.avg_docs_examined.toFixed(1)} / ${c.avg_docs_returned.toFixed(1)} / ${c.avg_keys_examined.toFixed(1)} (${fmtRatio(c.exam_return_ratio)})</dd>
        ${c.plan_summary ? `<dt>Plan</dt><dd>${escapeHTML(c.plan_summary)}${c.distinct_plans > 1 ? ` (${c.distinct_plans} distinct)` : ""}</dd>` : ""}
        ${c.query_hash ? `<dt>queryHash</dt><dd>${escapeHTML(c.query_hash)}</dd>` : ""}
        ${c.first_seen ? `<dt>First seen</dt><dd>${escapeHTML(c.first_seen)}</dd>` : ""}
        ${c.last_seen ? `<dt>Last seen</dt><dd>${escapeHTML(c.last_seen)}</dd>` : ""}
      </dl>

      <h3>Anti-patterns</h3>
      ${flagsHTML}

      <h3>Duration distribution</h3>
      <div class="hist">${histHTML}</div>

      <h3>Shape</h3>
      <pre>${escapeHTML(prettyJSON(c.shape))}</pre>
      `;
    detail.hidden = false;
    backdrop.hidden = false;
    detail.querySelector(".close").addEventListener("click", closeDetail);
  }

  function closeDetail() {
    detail.hidden = true;
    backdrop.hidden = true;
  }

  search.addEventListener("input", render);
  opFilter.addEventListener("change", render);
  flagFilter.addEventListener("change", render);
  sortSel.addEventListener("change", render);
  backdrop.addEventListener("click", closeDetail);
  document.addEventListener("keydown", e => { if (e.key === "Escape") closeDetail(); });

  render();
})();
