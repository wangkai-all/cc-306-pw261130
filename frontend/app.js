/* 点态留存分析前端：无框架、无构建步骤。
 * 全局状态：basis（口径）+ pinnedVersion（报告版本）。所有报告请求都带同一个版本，
 * 保证矩阵、钻取、导出看到的是同一份版本快照。
 */
"use strict";

const state = {
  basis: "entity",
  versions: [],
  currentVersion: 0,
  report: null,
  drillCtx: null,
};

const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const mono = (s) => `<span class="mono">${esc(s)}</span>`;

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

function toast(msg, isErr = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.className = "toast" + (isErr ? " err" : "");
  setTimeout(() => el.classList.add("hidden"), 3600);
}

const kindTag = (k) => `<span class="kind-tag kind-${k}">${esc(k)}</span>`;

// ----------------------------------------------------------------------------- 初始化
async function init() {
  const meta = await api("/api/meta");
  state.currentVersion = meta.current_version;
  $("#basis-select").addEventListener("change", (e) => {
    state.basis = e.target.value;
    loadReport();
  });
  $("#version-select").addEventListener("change", (e) => loadReport());
  $("#btn-latest").addEventListener("click", async () => {
    await refreshVersions();
    $("#version-select").value = state.currentVersion;
    loadReport();
  });
  $("#btn-snapshot").addEventListener("click", saveSnapshot);
  $("#btn-export-matrix").addEventListener("click", exportMatrix);
  $("#btn-diff").addEventListener("click", runDiff);
  $("#btn-recompute").addEventListener("click", startRecompute);
  $("#btn-close-modal").addEventListener("click", () => $("#modal").classList.add("hidden"));
  $("#btn-export-drill").addEventListener("click", exportDrill);
  bindForms();
  await refreshVersions();
  await loadReport();
  await loadJobsAndSnapshots();
  setInterval(pollBackground, 2500);
}

async function refreshVersions() {
  const data = await api("/api/versions");
  state.versions = data.versions;
  state.currentVersion = data.current_version;
  const opts = data.versions.map((v) =>
    `<option value="${v.version}">v${v.version} · ${esc(v.kind)} · ${esc(v.summary.slice(0, 38))}</option>`
  ).join("");
  $("#version-select").innerHTML = opts;
  $("#version-select").value = state.currentVersion;
  $("#diff-old").innerHTML = opts;
  $("#diff-new").innerHTML = opts;
  $("#diff-new").value = state.currentVersion;
  $("#diff-old").value = Math.max(1, state.currentVersion - 1);
  renderVersionsTable();
}

// ----------------------------------------------------------------------------- 报告
async function loadReport() {
  const version = Number($("#version-select").value) || state.currentVersion;
  state.report = await api(`/api/report?basis=${state.basis}&version=${version}&horizon=6`);
  renderBadge();
  renderStats();
  renderMatrix();
  renderUncertain();
}

function renderBadge() {
  const r = state.report;
  const badge = $("#version-badge");
  if (r.version === r.latest_version) {
    badge.textContent = `当前修正视图 · v${r.version}`;
    badge.className = "badge";
  } else {
    badge.textContent = `当时已知视图 · v${r.version}（最新 v${r.latest_version}）`;
    badge.className = "badge stale";
  }
  $("#basis-note").textContent = r.basis_note;
}

function renderStats() {
  const t = state.report.totals;
  const items = [
    ["身份节点", t.nodes, "账号 + 设备 + 匿名"],
    ["稳定实体", t.entities, "当前版本身份图连通分量"],
    ["队列成员数", t.cohort_members, "当前口径下有过活动的成员"],
    ["可见事件", t.events_visible, `ingest_version ≤ v${state.report.version}`],
    ["不确定事件", t.uncertain_events, "拆分后无法唯一归属", t.uncertain_events > 0],
  ];
  $("#stat-grid").innerHTML = items.map(([k, v, sub, warn]) =>
    `<div class="stat"><div class="k">${k}</div>
      <div class="v ${warn ? "warn" : ""}">${v}</div><div class="sub">${esc(sub)}</div></div>`
  ).join("");
}

function heatClass(rate) {
  if (rate == null) return "heat-0";
  return "heat-" + Math.min(4, Math.floor(rate * 5));
}

function renderMatrix() {
  const r = state.report;
  const head = `<thead><tr><th>队列周</th><th>队列人数</th>` +
    r.cohorts[0]?.cells.map((c) => `<th>W${c.offset}<br><span style="font-weight:400">${esc(c.week_key)}</span></th>`).join("") +
    `</tr></thead>`;
  const body = r.cohorts.map((cohort) => {
    const tds = cohort.cells.map((cell) => {
      const interval = cell.lower_rate === cell.upper_rate || cell.upper_rate == null
        ? fmtPct(cell.lower_rate)
        : `${fmtPct(cell.lower_rate)} ~ ${fmtPct(cell.upper_rate)}`;
      return `<td class="cell ${heatClass(cell.lower_rate)} ${cell.uncertain_event_count ? "uncertain" : ""}"
        data-week="${esc(cohort.week_start)}" data-offset="${cell.offset}"
        title="下界 ${cell.active}/${cell.cohort_size}；不确定事件 ${cell.uncertain_event_count} 个">
        <strong>${cell.active}/${cell.cohort_size}</strong>
        ${cell.uncertain_event_count ? `<span class="pill" title="不确定事件数">?${cell.uncertain_event_count}</span>` : ""}
        <span class="rate">${interval}</span></td>`;
    }).join("");
    return `<tr><td><strong>${esc(cohort.week_key)}</strong></td>
      <td>${cohort.cohort_size}</td>${tds}</tr>`;
  }).join("");
  $("#matrix-table").innerHTML = head + `<tbody>${body}</tbody>`;
  document.querySelectorAll("#matrix-table td.cell").forEach((td) => {
    td.addEventListener("click", () => openDrill(td.dataset.week, Number(td.dataset.offset)));
  });
}

function fmtPct(x) {
  return x == null ? "—" : (x * 100).toFixed(1) + "%";
}

function renderUncertain() {
  const rows = state.report.uncertain_by_week;
  if (!rows.length) {
    $("#uncertain-table").innerHTML =
      `<tbody><tr><td class="note">当前版本没有不确定事件（所有历史事件都可唯一归属）。</td></tr></tbody>`;
    return;
  }
  const head = `<thead><tr><th>周</th><th>事件ID</th><th>发生时间</th><th>类型</th>
    <th>到达版本</th><th>挂载节点</th><th>不确定原因</th></tr></thead>`;
  const body = rows.flatMap((item) => item.events.map((ev) =>
    `<tr><td>${esc(item.week_key)}</td><td>${mono(ev.event_id)}</td>
      <td class="mono">${esc(ev.event_time)}</td><td>${esc(ev.event_type)}</td>
      <td>v${ev.ingest_version}</td><td>${ev.attached_ids.map(mono).join(" ")}</td>
      <td>${esc(ev.reason)}</td></tr>`
  )).join("");
  $("#uncertain-table").innerHTML = head + `<tbody>${body}</tbody>`;
}

// ----------------------------------------------------------------------------- 钻取
async function openDrill(cohortWeekStart, offset) {
  const v = state.report.version;
  $("#modal").classList.remove("hidden");
  $("#modal-title").textContent = "加载中…";
  $("#modal-content").innerHTML = "";
  const data = await api(
    `/api/drill?basis=${state.basis}&version=${v}&cohort_week=${encodeURIComponent(cohortWeekStart)}&offset=${offset}`);
  state.drillCtx = { cohortWeekStart, offset };
  $("#modal-title").textContent =
    `钻取：队列 ${data.cohort_week_key} → ${data.target_week_key}（v${data.version}，${state.basis} 口径）`;
  const memberBlock = (title, list, active) =>
    `<div class="drill-section"><h3>${title}（${list.length}）</h3>` +
    list.map((m) => `<div class="member-card ${active ? "" : "inactive"}">
      <div class="head">${mono(m.member)} <span class="pill">实体 ${esc(m.entity_root || "—")}</span>
      <span class="pill">${active ? "确认活跃" : "未确认活跃"}</span></div>
      ${m.events.length ? `<ul class="events">` + m.events.map((ev) =>
        `<li>${mono(ev.event_id)} · ${esc(ev.event_type)} · ${esc(ev.event_time)} ·
          发射 ${mono(ev.emitter_id || "—")} · 到达 v${ev.ingest_version} · 依据 ${esc(ev.allocation_reason)}</li>`
      ).join("") + "</ul>" : ""}</div>`).join("") + "</div>";

  const uncertainBlock = data.uncertain_events.length
    ? `<div class="drill-section"><h3>不确定事件（${data.uncertain_events.length}，不计入确认活跃）</h3>` +
      data.uncertain_events.map((ev) => `<div class="member-card">
        <div class="head">${mono(ev.event_id)} · ${esc(ev.event_type)} · ${esc(ev.reason)}</div>
        <div class="events">发生 ${esc(ev.event_time)}，到达 v${ev.ingest_version}；
          候选成员：${ev.candidate_members.map(mono).join(" ") || "（不在本队列）"}</div></div>`).join("") +
      "</div>"
    : "";
  $("#modal-content").innerHTML =
    memberBlock("确认活跃成员", data.active_members, true) +
    memberBlock("未确认活跃成员", data.inactive_members, false) + uncertainBlock;
}

// ----------------------------------------------------------------------------- 版本时间线 / diff
function renderVersionsTable() {
  const head = `<thead><tr><th>版本</th><th>时间(UTC)</th><th>类型</th><th>摘要</th></tr></thead>`;
  const body = state.versions.map((v) =>
    `<tr><td><strong>v${v.version}</strong></td><td class="mono">${esc(v.created_at.slice(11, 19))}</td>
      <td><span class="kind-tag kind-${v.kind}">${esc(v.kind)}</span></td>
      <td>${esc(v.summary)}</td></tr>`).join("");
  $("#versions-table").innerHTML = head + `<tbody>${body}</tbody>`;
}

async function runDiff() {
  const vOld = $("#diff-old").value, vNew = $("#diff-new").value;
  const d = await api(`/api/diff?basis=${state.basis}&v_old=${vOld}&v_new=${vNew}`);
  const logs = d.version_logs.map((l) =>
    `<li>v${l.version} <span class="kind-tag kind-${l.kind}">${esc(l.kind)}</span> ${esc(l.summary)}</li>`
  ).join("") || "<li>区间内无版本</li>";
  const cohort = d.cohort_changes.map((c) =>
    `<li class="change-${c.change}">${mono(c.member)}：${esc(c.old_first_week || "不存在")}
      → ${esc(c.new_first_week || "移除")}（${esc(c.change)}）</li>`).join("")
    || "<li>队列成员首活跃周无变化</li>";
  const cells = d.cell_changes.map((c) =>
    `<li>${esc(c.cohort_week)} W${c.offset}（${esc(c.target_week)}）：
      人数 ${c.old_cohort_size}→${c.new_cohort_size}，
      确认活跃 ${c.old_active}→${c.new_active}，
      不确定 ${c.old_uncertain}→${c.new_uncertain}</li>`).join("")
    || "<li>矩阵单元格无变化</li>";
  const gc = d.graph_changes;
  const graph = [
    ...gc.nodes_born.map((n) => `<li>节点出生：${mono(n.id)} ${kindTag(n.kind)}（v${n.born_version}）</li>`),
    ...gc.edges_opened.map((e) => `<li class="kind-edge_open">合并：${mono(e.a)} ⇔ ${mono(e.b)}（v${e.version}，${esc(e.reason)}）</li>`),
    ...gc.edges_closed.map((e) => `<li class="kind-edge_close">拆分：${mono(e.a)} ⊘ ${mono(e.b)}（v${e.version}，${esc(e.reason)}）</li>`),
  ].join("") || "<li>身份图结构无变化</li>";
  $("#diff-panel").classList.remove("hidden");
  $("#diff-panel").innerHTML = `
    <div><h4>区间版本日志（v${d.v_old} → v${d.v_new}）</h4><ul class="diff-list">${logs}</ul></div>
    <div><h4>身份图结构变化</h4><ul class="diff-list">${graph}</ul></div>
    <div><h4>队列成员变化</h4><ul class="diff-list">${cohort}</ul></div>
    <div><h4>矩阵单元格变化（不确定事件总数 ${d.uncertain_total_old} → ${d.uncertain_total_new}）</h4>
      <ul class="diff-list">${cells}</ul></div>`;
}

// ----------------------------------------------------------------------------- 重算 / 快照
async function startRecompute() {
  try {
    const job = await api("/api/recompute", { method: "POST", body: { basis: state.basis, simulated_seconds: 2.5 } });
    toast(`重算任务 #${job.job_id} 已启动，钉住 v${job.pinned_version}；期间可继续操作制造新版本`);
    await loadJobsAndSnapshots();
  } catch (e) { toast(e.message, true); }
}

async function loadJobsAndSnapshots() {
  const [{ jobs }, { snapshots }] = await Promise.all([
    api("/api/jobs"), api("/api/snapshots")]);
  $("#jobs-table").innerHTML = `<thead><tr><th>任务</th><th>状态</th><th>钉住版本</th>
    <th>完成时最新</th><th>口径</th><th>开始</th><th>结束</th><th>说明</th></tr></thead>
    <tbody>${jobs.map((j) => `<tr>
      <td>#${j.job_id}</td><td><span class="status-pill status-${j.status}">${esc(j.status)}</span></td>
      <td>v${j.pinned_version}</td><td>${j.latest_at_end != null ? "v" + j.latest_at_end : "—"}</td>
      <td>${esc(j.basis)}</td><td class="mono">${esc(j.started_at.slice(11, 19))}</td>
      <td class="mono">${j.finished_at ? esc(j.finished_at.slice(11, 19)) : "—"}</td>
      <td>${esc(j.note)}</td></tr>`).join("")}</tbody>`;
  $("#snapshots-table").innerHTML = `<thead><tr><th>快照</th><th>钉住版本</th><th>口径</th>
    <th>标签</th><th>创建时间</th><th></th></tr></thead>
    <tbody>${snapshots.map((s) => `<tr>
      <td>#${s.snapshot_id}</td><td>v${s.pinned_version}${s.is_current ? " <span class='pill'>最新</span>" : ""}</td>
      <td>${esc(s.basis)}</td><td>${esc(s.label)}</td>
      <td class="mono">${esc(s.created_at.slice(5, 19).replace("T", " "))}</td>
      <td><button class="ghost" data-snapshot="${s.snapshot_id}">查看</button></td></tr>`).join("")}</tbody>`;
  document.querySelectorAll("[data-snapshot]").forEach((btn) =>
    btn.addEventListener("click", () => viewSnapshot(Number(btn.dataset.snapshot))));
}

async function viewSnapshot(id) {
  const s = await api(`/api/snapshot?id=${id}`);
  state.report = s.payload;
  $("#version-select").value = s.pinned_version;
  renderBadge(); renderStats(); renderMatrix(); renderUncertain();
  toast(`已载入快照 #${id}（钉版 v${s.pinned_version}，只读历史视图）`);
}

async function saveSnapshot() {
  try {
    const r = await api("/api/snapshot", {
      method: "POST",
      body: { version: state.report.version, basis: state.basis, label: `手动快照 v${state.report.version}` },
    });
    toast(`快照 #${r.snapshot_id} 已保存`);
    loadJobsAndSnapshots();
  } catch (e) { toast(e.message, true); }
}

async function pollBackground() {
  const before = JSON.stringify(await api("/api/jobs"));
  await loadJobsAndSnapshots();
  const after = before; // 表格已刷新；再检查版本是否被外部推进
  const meta = await api("/api/meta");
  if (meta.current_version !== state.currentVersion) {
    const keep = $("#version-select").value;
    await refreshVersions();
    $("#version-select").value = keep; // 用户正在看历史版本时不强行跳走
    state.currentVersion = meta.current_version;
  }
}

// ----------------------------------------------------------------------------- 表单
function bindForms() {
  const handle = async (formId, url, buildBody, okMsg) => {
    $(formId).addEventListener("submit", async (e) => {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        const body = buildBody(fd);
        const r = await api(url, { method: "POST", body });
        const out = $("#op-result");
        out.className = "op-result ok";
        out.textContent = `${okMsg} → 新版本 v${r.version}（历史报告保持不变）`;
        e.target.reset();
        await refreshVersions();
        $("#version-select").value = state.currentVersion;
        await loadReport();
        await loadJobsAndSnapshots();
      } catch (err) {
        const out = $("#op-result");
        out.className = "op-result err";
        out.textContent = "操作被拒绝：" + err.message;
      }
    });
  };

  handle("#form-event", "/api/ingest/event", (fd) => ({
    event_id: fd.get("event_id").trim(),
    event_time: fd.get("event_time").trim(),
    emitter_id: fd.get("emitter_id").trim() || null,
    emitter_kind: fd.get("emitter_kind") || null,
    attached_ids: fd.get("attached_ids").split(",").map((s) => s.trim()).filter(Boolean),
    event_type: fd.get("event_type"),
  }), "事件已回补");

  handle("#form-merge", "/api/identity/merge", (fd) => ({
    a: fd.get("a").trim(), b: fd.get("b").trim(),
    reason: fd.get("reason") || "人工合并",
  }), "身份已合并");

  handle("#form-split", "/api/identity/split", (fd) => ({
    a: fd.get("a").trim(), b: fd.get("b").trim(),
    reason: fd.get("reason") || "人工撤销",
  }), "合并已撤销");
}

// ----------------------------------------------------------------------------- 导出
function exportMatrix() {
  const v = state.report.version;
  window.location = `/api/export/matrix?basis=${state.basis}&version=${v}`;
}
function exportDrill() {
  if (!state.drillCtx) return;
  const v = state.report.version;
  const { cohortWeekStart, offset } = state.drillCtx;
  window.location = `/api/export/drill?basis=${state.basis}&version=${v}` +
    `&cohort_week=${encodeURIComponent(cohortWeekStart)}&offset=${offset}`;
}

init().catch((e) => toast("初始化失败：" + e.message, true));
