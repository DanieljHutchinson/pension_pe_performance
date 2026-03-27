// State
let currentPage = 1;
let currentSort = { by: "fund_name", dir: "asc" };
let scatterChart = null;

// Helpers
function qs(sel) { return document.querySelector(sel); }
function fmtUSD(v) { return v != null ? "$" + Number(v).toLocaleString("en-US", { maximumFractionDigits: 0 }) : "-"; }
function fmtPct(v) { return v != null ? v.toFixed(2) + "%" : "-"; }
function fmtX(v) { return v != null ? v.toFixed(2) + "x" : "-"; }

function getFilters() {
  const f = {};
  const v = (id) => qs(id).value;
  if (v("#f-pension")) f.pension_fund = v("#f-pension");
  if (v("#f-gp")) f.gp_name = v("#f-gp");
  if (v("#f-strategy")) f.strategy = v("#f-strategy");
  if (v("#f-vy-min")) f.vintage_min = v("#f-vy-min");
  if (v("#f-vy-max")) f.vintage_max = v("#f-vy-max");
  if (v("#f-irr-min")) f.irr_min = v("#f-irr-min");
  if (v("#f-irr-max")) f.irr_max = v("#f-irr-max");
  if (v("#f-tvpi-min")) f.tvpi_min = v("#f-tvpi-min");
  if (v("#f-tvpi-max")) f.tvpi_max = v("#f-tvpi-max");
  return f;
}

function buildQS(filters, extra = {}) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries({ ...filters, ...extra })) {
    if (v !== "" && v != null) params.set(k, v);
  }
  return params.toString();
}

// Data fetching
async function fetchFunds() {
  const filters = getFilters();
  const qs_str = buildQS(filters, {
    sort_by: currentSort.by,
    sort_dir: currentSort.dir,
    page: currentPage,
    page_size: 50,
  });
  const res = await fetch(`/api/funds?${qs_str}`);
  const data = await res.json();
  renderTable(data);
  renderScatter(data.data);
}

async function fetchStats() {
  const filters = getFilters();
  const qs_str = buildQS(filters);
  const res = await fetch(`/api/stats?${qs_str}`);
  const data = await res.json();
  renderStats(data);
}

async function loadFilterOptions() {
  try {
    const [pensions, strategies] = await Promise.all([
      fetch("/api/pension-funds").then(r => r.json()),
      fetch("/api/strategies").then(r => r.json()),
    ]);
    const pSel = qs("#f-pension");
    pensions.forEach(p => {
      const opt = document.createElement("option");
      opt.value = p; opt.textContent = p;
      pSel.appendChild(opt);
    });
    const sSel = qs("#f-strategy");
    strategies.forEach(s => {
      const opt = document.createElement("option");
      opt.value = s; opt.textContent = s;
      sSel.appendChild(opt);
    });
  } catch (e) { /* filters will just show "All" */ }
}

// Rendering
function renderTable(data) {
  const { total, page, page_size, data: funds } = data;
  const tbody = qs("#funds-body");
  tbody.innerHTML = "";

  funds.forEach(f => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${esc(f.fund_name)}</td>
      <td>${esc(f.gp_name || "")}</td>
      <td>${esc(f.pension_fund)}</td>
      <td>${f.vintage_year || "-"}</td>
      <td>${esc(f.strategy || "")}</td>
      <td class="num">${fmtUSD(f.commitment_usd)}</td>
      <td class="num">${fmtX(f.tvpi)}</td>
      <td class="num">${fmtX(f.dpi)}</td>
      <td class="num">${fmtPct(f.net_irr)}</td>
      <td>${f.as_of_date || "-"}</td>
    `;
    tbody.appendChild(tr);
  });

  const totalPages = Math.ceil(total / page_size) || 1;
  qs("#page-info").textContent = `Page ${page} of ${totalPages} (${total} funds)`;
  qs("#table-count").textContent = `(${total})`;
  qs("#prev-btn").disabled = page <= 1;
  qs("#next-btn").disabled = page >= totalPages;
}

function renderStats(data) {
  qs("#s-total").textContent = data.total_funds;
  qs("#s-med-irr").textContent = data.median_irr != null ? fmtPct(data.median_irr) : "-";
  qs("#s-mean-irr").textContent = data.mean_irr != null ? fmtPct(data.mean_irr) : "-";
  qs("#s-med-tvpi").textContent = data.median_tvpi != null ? fmtX(data.median_tvpi) : "-";
  qs("#s-mean-tvpi").textContent = data.mean_tvpi != null ? fmtX(data.mean_tvpi) : "-";

  // Vintage breakdown
  const vDiv = qs("#vintage-stats");
  if (data.by_vintage && data.by_vintage.length) {
    let html = "<h3>By Vintage Year</h3><table><tr><th>Vintage</th><th>Count</th><th>Med IRR</th><th>Mean IRR</th><th>Med TVPI</th></tr>";
    data.by_vintage.forEach(v => {
      html += `<tr><td>${v.vintage_year}</td><td>${v.count}</td><td>${fmtPct(v.median_irr)}</td><td>${fmtPct(v.mean_irr)}</td><td>${fmtX(v.median_tvpi)}</td></tr>`;
    });
    html += "</table>";
    vDiv.innerHTML = html;
  }

  // Strategy breakdown
  const sDiv = qs("#strategy-stats");
  if (data.by_strategy && data.by_strategy.length) {
    let html = "<h3>By Strategy</h3><table><tr><th>Strategy</th><th>Count</th><th>Med IRR</th><th>Mean IRR</th><th>Med TVPI</th></tr>";
    data.by_strategy.forEach(s => {
      html += `<tr><td>${esc(s.group)}</td><td>${s.count}</td><td>${fmtPct(s.median_irr)}</td><td>${fmtPct(s.mean_irr)}</td><td>${fmtX(s.median_tvpi)}</td></tr>`;
    });
    html += "</table>";
    sDiv.innerHTML = html;
  }
}

function renderScatter(funds) {
  const canvas = qs("#scatter-chart");
  if (scatterChart) scatterChart.destroy();

  // Group by strategy with colors
  const colors = {
    "Buyout": "#0d6efd", "Venture Capital": "#198754", "Growth Equity": "#ffc107",
    "Credit/Distressed": "#dc3545", "Secondaries": "#6f42c1", "Fund of Funds": "#fd7e14",
    "Co-Investment": "#20c997", "Energy/Infrastructure": "#6c757d",
    "Real Estate": "#d63384", "Other": "#adb5bd",
  };

  const groups = {};
  funds.forEach(f => {
    if (f.net_irr == null || f.tvpi == null) return;
    const s = f.strategy || "Other";
    if (!groups[s]) groups[s] = [];
    groups[s].push({ x: f.net_irr, y: f.tvpi, label: `${f.fund_name} (${f.pension_fund})` });
  });

  const datasets = Object.entries(groups).map(([strategy, points]) => ({
    label: strategy,
    data: points,
    backgroundColor: colors[strategy] || "#adb5bd",
    pointRadius: 4,
    pointHoverRadius: 6,
  }));

  scatterChart = new Chart(canvas, {
    type: "scatter",
    data: { datasets },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: "Net IRR (%)" } },
        y: { title: { display: true, text: "TVPI (x)" } },
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const p = ctx.raw;
              return `${p.label}: IRR ${p.x.toFixed(1)}%, TVPI ${p.y.toFixed(2)}x`;
            },
          },
        },
      },
    },
  });
}

// Actions
function applyFilters() {
  currentPage = 1;
  fetchFunds();
  fetchStats();
}

function clearFilters() {
  ["#f-pension", "#f-strategy"].forEach(s => qs(s).value = "");
  ["#f-gp", "#f-vy-min", "#f-vy-max", "#f-irr-min", "#f-irr-max", "#f-tvpi-min", "#f-tvpi-max"]
    .forEach(s => qs(s).value = "");
  applyFilters();
}

function prevPage() { if (currentPage > 1) { currentPage--; fetchFunds(); } }
function nextPage() { currentPage++; fetchFunds(); }

function exportCSV() {
  const qs_str = buildQS(getFilters());
  window.open(`/api/export/csv?${qs_str}`, "_blank");
}

function exportJSON() {
  const qs_str = buildQS(getFilters());
  window.open(`/api/export/json?${qs_str}`, "_blank");
}

async function handleUpload(e) {
  e.preventDefault();
  const form = new FormData();
  form.append("file", qs("#u-file").files[0]);
  form.append("pension_fund", qs("#u-pension").value);
  form.append("as_of_date", qs("#u-date").value);

  qs("#upload-status").textContent = "Uploading...";
  try {
    const res = await fetch("/api/upload", { method: "POST", body: form });
    const data = await res.json();
    if (res.ok) {
      qs("#upload-status").textContent = `Ingested ${data.records_ingested} records`;
      applyFilters();
    } else {
      qs("#upload-status").textContent = `Error: ${data.detail}`;
    }
  } catch (err) {
    qs("#upload-status").textContent = `Error: ${err.message}`;
  }
}

async function refreshSource(source) {
  qs("#refresh-status").textContent = "Refreshing...";
  try {
    const res = await fetch(`/api/refresh?source=${source}`, { method: "POST" });
    const data = await res.json();
    const msgs = Object.entries(data).map(([k, v]) => `${k}: ${v.status} (${v.records || v.message || 0})`);
    qs("#refresh-status").textContent = msgs.join(", ");
    applyFilters();
    loadFilterOptions();
  } catch (err) {
    qs("#refresh-status").textContent = `Error: ${err.message}`;
  }
}

// Sorting
document.querySelectorAll("#funds-table th.sortable").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (currentSort.by === col) {
      currentSort.dir = currentSort.dir === "asc" ? "desc" : "asc";
    } else {
      currentSort = { by: col, dir: "asc" };
    }
    fetchFunds();
  });
});

// Escape HTML
function esc(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

// Init
loadFilterOptions();
fetchFunds();
fetchStats();
