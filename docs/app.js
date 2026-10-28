/* ─────────────────────────────────────────────────────────────
   F1 RACE ANALYTICS – DASHBOARD APPLICATION LOGIC
   Loads BigQuery Gold JSON, renders Chart.js visualizations
───────────────────────────────────────────────────────────── */

/* ─────────────────────────────────────────────────────────────
   PURE FORMATTING & CONTRACT HELPERS
───────────────────────────────────────────────────────────── */

function formatDuration(seconds) {
  if (seconds == null || isNaN(seconds)) return "—";
  const totalMs = Math.round(Number(seconds) * 1000);
  const ms = totalMs % 1000;
  const totalS = Math.floor(totalMs / 1000);
  const s = totalS % 60;
  const totalM = Math.floor(totalS / 60);
  const m = totalM % 60;
  const h = Math.floor(totalM / 60);
  const msStr = String(ms).padStart(3, "0");
  const sStr = String(s).padStart(2, "0");
  if (h > 0) {
    const mStr = String(m).padStart(2, "0");
    return `${h}:${mStr}:${sStr}.${msStr}`;
  }
  return `${m}:${sStr}.${msStr}`;
}

function formatGap(gapSeconds) {
  if (gapSeconds == null || isNaN(gapSeconds)) return "—";
  return `+${Number(gapSeconds).toFixed(3)} s`;
}

function formatLapsDown(lapsDown) {
  if (lapsDown == null || isNaN(lapsDown)) return "—";
  const n = Number(lapsDown);
  return `+${n} ${n === 1 ? "LAP" : "LAPS"}`;
}

function formatTimeGap(row) {
  if (!row) return "—";
  if (row.final_position === 1) {
    return formatDuration(row.duration_seconds);
  }
  if (row.gap_to_leader_seconds != null) {
    return formatGap(row.gap_to_leader_seconds);
  }
  if (row.laps_down != null) {
    return formatLapsDown(row.laps_down);
  }
  return "—";
}

function formatPoints(pts) {
  if (pts == null || isNaN(pts)) return "—";
  const n = Number(pts);
  return n % 1 === 0 ? String(Math.trunc(n)) : n.toFixed(1);
}

function formatPositionsGained(gained) {
  if (gained == null || isNaN(gained)) {
    return { text: "—", html: "—", label: "grid position not available" };
  }
  const n = Number(gained);
  if (n > 0) {
    return {
      text: `▲ ${n}`,
      html: `<span class="pos-gain" aria-label="gained ${n} position${n === 1 ? "" : "s"}">▲ ${n}</span>`,
      label: `gained ${n} position${n === 1 ? "" : "s"}`,
    };
  }
  if (n < 0) {
    const abs = Math.abs(n);
    return {
      text: `▼ ${abs}`,
      html: `<span class="pos-loss" aria-label="lost ${abs} position${abs === 1 ? "" : "s"}">▼ ${abs}</span>`,
      label: `lost ${abs} position${abs === 1 ? "" : "s"}`,
    };
  }
  return {
    text: "0",
    html: `<span class="pos-same" aria-label="no change in position">0</span>`,
    label: "no change in position",
  };
}

function formatStatusBadge(status, lapsDown) {
  const normStatus = (status || "Unclassified").trim();
  if (normStatus === "Finished" && lapsDown != null && !isNaN(lapsDown)) {
    const n = Number(lapsDown);
    const badgeText = `+${n} ${n === 1 ? "LAP" : "LAPS"}`;
    return `<span class="status-badge finished">${badgeText}</span>`;
  }
  const cls = normStatus.toLowerCase();
  return `<span class="status-badge ${cls}">${normStatus}</span>`;
}

function formatFinalPosition(pos) {
  return pos != null ? String(pos) : "—";
}

function formatGridPosition(pos) {
  return pos != null ? String(pos) : "—";
}

let goldData = null;
let tyreChart = null;
let weatherChart = null;
let selectedSessionKey = null;
let resultsSortField = null;
let resultsSortDirection = "asc";

// Initialize on DOM load
document.addEventListener("DOMContentLoaded", async () => {
  setupTabs();
  setupResultsSorting();
  await loadData();
});

// Setup tab navigation
function setupTabs() {
  const tabs = document.querySelectorAll(".tab-btn");
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      tabs.forEach((t) => t.classList.remove("active"));
      document.querySelectorAll(".view-content").forEach((v) => v.classList.remove("active"));

      tab.classList.add("active");
      const targetId = tab.dataset.target;
      const targetView = document.getElementById(targetId);
      if (targetView) targetView.classList.add("active");

      // Trigger resize for Chart.js canvases if hidden during init
      if (targetId === "view-tyre" && tyreChart) tyreChart.resize();
      if (targetId === "view-weather" && weatherChart) weatherChart.resize();
    });
  });
}

function setupResultsSorting() {
  const resetBtn = document.getElementById("reset-results-order");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      resultsSortField = null;
      resultsSortDirection = "asc";
      renderRaceResults();
    });
  }

  document.querySelectorAll("#results-thead-tr th.sortable").forEach((th) => {
    const handleSort = () => {
      const field = th.dataset.sort;
      if (resultsSortField === field) {
        resultsSortDirection = resultsSortDirection === "asc" ? "desc" : "asc";
      } else {
        resultsSortField = field;
        resultsSortDirection = (field === "final_position" || field === "grid_position") ? "asc" : "desc";
      }
      renderRaceResults();
    };

    th.addEventListener("click", handleSort);
    th.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        handleSort();
      }
    });
  });
}

// Load Gold dataset from static JSON
async function loadData() {
  try {
    const res = await fetch("gold_data.json");
    goldData = await res.json();
    initRaceSelector();
    renderSelectedRace();
  } catch (err) {
    console.error("Failed to load gold_data.json:", err);
    document.getElementById("leaderboard-tbody").innerHTML = `
      <tr><td colspan="7" style="color: #ef4444; text-align: center;">
        Failed to load Gold dataset. Ensure gold_data.json exists in the dashboard directory.
      </td></tr>
    `;
  }
}

function renderStrategyChips(driverName, raceStints) {
  if (!raceStints || !raceStints.length) return "—";
  const driverStints = raceStints
    .filter((s) => s.driver_name === driverName)
    .sort((a, b) => (a.stint_number || 0) - (b.stint_number || 0));

  if (!driverStints.length) return "—";

  return driverStints.map((stint, idx) => {
    const comp = (stint.compound || "UNKNOWN").toUpperCase();
    const initial = comp.charAt(0) || "?";
    let cls = "soft";
    if (comp.includes("MEDIUM")) cls = "medium";
    else if (comp.includes("HARD")) cls = "hard";
    else if (comp.includes("INTER")) cls = "intermediate";
    else if (comp.includes("WET")) cls = "wet";

    const chipHtml = `<span class="compound-chip ${cls}" title="${comp} (Stint ${stint.stint_number || idx + 1})" aria-label="${comp}">${initial}</span>`;
    return idx === 0 ? chipHtml : `<span class="strategy-sep" aria-hidden="true">&rsaquo;</span>${chipHtml}`;
  }).join("");
}

function initRaceSelector() {
  const select = document.getElementById("race-select");
  const races = [...(goldData.races || [])].sort((a, b) =>
    a.year - b.year || String(a.date || "").localeCompare(String(b.date || ""))
  );
  select.innerHTML = "";
  races.forEach((race) => {
    const option = document.createElement("option");
    option.value = race.session_key;
    const suffix = race.has_results === false ? " (no results)" : "";
    option.textContent = `${race.name}${suffix}`;
    select.appendChild(option);
  });
  if (!races.length) return;
  const hash = window.location.hash;
  const raceMatch = hash.match(/race=(\d+)/);
  if (raceMatch && races.some((r) => r.session_key === Number(raceMatch[1]))) {
    selectedSessionKey = Number(raceMatch[1]);
  } else {
    selectedSessionKey = (races.find((race) => race.name.includes("Abu Dhabi")) || races[races.length - 1]).session_key;
  }
  select.value = selectedSessionKey;
  select.addEventListener("change", () => {
    selectedSessionKey = Number(select.value);
    resultsSortField = null;
    resultsSortDirection = "asc";
    renderSelectedRace();
  });
}

function raceRows(key) {
  return (goldData?.[key] || []).filter(
    (row) => Number(row.session_key) === Number(selectedSessionKey)
  );
}

function renderSelectedRace() {
  const race = (goldData.races || []).find(
    (item) => Number(item.session_key) === Number(selectedSessionKey)
  );
  if (!race) return;

  const sourceMap = {
    bigquery: "BigQuery Gold",
    api: "OpenF1 snapshot (API mode)",
  };
  const metaSource = goldData.meta?.source || "bigquery";
  const sourceLabel = sourceMap[metaSource] || metaSource;
  const raceDateStr = race.date ? ` (${race.date})` : "";
  document.getElementById("race-subtitle").textContent =
    `${race.name}${raceDateStr} • ${sourceLabel}`;

  const pill = document.getElementById("header-status-pill");
  if (pill) {
    const genAt = goldData.meta?.generated_at;
    pill.setAttribute("title", genAt ? `Generated at: ${genAt}` : "OpenF1 Gold Snapshot");
  }

  renderRaceResults();
  renderOverviewMetrics();
  renderLeaderboard();
  initTyreDegradationControls();
  renderStintTimelines();
  renderWeatherChart();
}

// Render Official Race Results Table
function renderRaceResults() {
  const tbody = document.getElementById("results-tbody");
  const tabBtn = document.getElementById("tab-results");
  const viewSection = document.getElementById("view-results");
  const noResultsBanner = document.getElementById("leaderboard-no-results-banner");
  const podiumContainer = document.getElementById("results-podium");
  const resetBtn = document.getElementById("reset-results-order");
  if (!tbody || !goldData) return;

  const results = raceRows("race_results");

  // If no results for this race, hide the tab/view and switch to leaderboard if active
  if (!results.length) {
    if (tabBtn) tabBtn.style.display = "none";
    if (noResultsBanner) noResultsBanner.style.display = "block";
    if (podiumContainer) podiumContainer.style.display = "none";
    if (viewSection && viewSection.classList.contains("active")) {
      viewSection.classList.remove("active");
      if (tabBtn) tabBtn.classList.remove("active");
      const lbTab = document.querySelector('[data-target="view-leaderboard"]');
      const lbView = document.getElementById("view-leaderboard");
      if (lbTab) lbTab.classList.add("active");
      if (lbView) lbView.classList.add("active");
    }
    return;
  }

  if (tabBtn) tabBtn.style.display = "flex";
  if (noResultsBanner) noResultsBanner.style.display = "none";

  // Check if race has any starting grid data
  const hasGrid = results.some((r) => r.grid_position != null);
  document.querySelectorAll(".col-grid").forEach((th) => {
    th.style.display = hasGrid ? "" : "none";
  });

  // Podium Strip (P1 to P3)
  const p1 = results.find((r) => r.final_position === 1);
  const p2 = results.find((r) => r.final_position === 2);
  const p3 = results.find((r) => r.final_position === 3);

  if (podiumContainer) {
    if (p1 && p2 && p3) {
      podiumContainer.style.display = "grid";
      const makePodiumCard = (pRow, posNum, posClass, badgeClass) => {
        const teamColour = pRow.team_colour ? `#${pRow.team_colour}` : "#94a3b8";
        const timeOrGap = posNum === 1 ? formatDuration(pRow.duration_seconds) : formatGap(pRow.gap_to_leader_seconds);
        return `
          <div class="podium-card ${posClass}">
            <div class="podium-header">
              <span class="podium-badge ${badgeClass}">P${posNum}</span>
              <span class="podium-time">${timeOrGap}</span>
            </div>
            <div class="podium-driver">
              <span class="team-stripe" style="background: ${teamColour};"></span>
              <span>${pRow.driver_name}</span>
              <span style="color: var(--text-muted); font-size: 11px;">(${pRow.name_acronym || pRow.driver_number})</span>
            </div>
            <div class="podium-team">${pRow.team_name || "—"}</div>
          </div>
        `;
      };
      podiumContainer.innerHTML =
        makePodiumCard(p1, 1, "p1", "p1") +
        makePodiumCard(p2, 2, "p2", "p2") +
        makePodiumCard(p3, 3, "p3", "p3");
    } else {
      podiumContainer.style.display = "none";
      podiumContainer.innerHTML = "";
    }
  }

  // Row order: preserved by default, or stable sort if header clicked
  let rowsToRender = [...results];
  if (resultsSortField) {
    const field = resultsSortField;
    const dir = resultsSortDirection;
    rowsToRender = [...results].map((r, i) => ({ r, i })).sort((a, b) => {
      let va = a.r[field];
      let vb = b.r[field];
      if (va == null && vb == null) return a.i - b.i;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (va !== vb) {
        return dir === "asc" ? (va < vb ? -1 : 1) : (va > vb ? -1 : 1);
      }
      return a.i - b.i;
    }).map((x) => x.r);
  }

  // Update sort headers aria-sort
  document.querySelectorAll("#results-thead-tr th.sortable").forEach((th) => {
    if (th.dataset.sort === resultsSortField) {
      th.setAttribute("aria-sort", resultsSortDirection === "asc" ? "ascending" : "descending");
    } else {
      th.setAttribute("aria-sort", "none");
    }
  });

  if (resetBtn) {
    resetBtn.style.display = resultsSortField ? "inline-flex" : "none";
  }

  tbody.innerHTML = "";
  document.getElementById("results-desc").textContent =
    `Official classified finishing results for ${results.length} drivers.`;

  const raceStints = raceRows("pit_strategy");

  rowsToRender.forEach((r) => {
    const tr = document.createElement("tr");
    const teamColour = r.team_colour ? `#${r.team_colour}` : "#94a3b8";
    const gained = formatPositionsGained(r.positions_gained);
    const gridCells = hasGrid ? `
      <td>${formatGridPosition(r.grid_position)}</td>
      <td>${gained.html}</td>
    ` : "";
    const stratChips = renderStrategyChips(r.driver_name, raceStints);
    const pitStopsDisplay = r.total_pit_stops != null ? `
      <div style="font-weight: 700;">${r.total_pit_stops}</div>
      ${stratChips !== "—" ? `<div class="strategy-chips" style="margin-top: 4px;">${stratChips}</div>` : ""}
    ` : "—";

    tr.innerHTML = `
      <td style="font-weight: 800; color: #fff;">${formatFinalPosition(r.final_position)}</td>
      <td>
        <div class="team-pill">
          <span class="team-stripe" style="background: ${teamColour};"></span>
          <span>${r.driver_name || `Driver ${r.driver_number}`}</span>
          <span style="color: var(--text-muted); font-size: 11px;">(${r.name_acronym || r.driver_number})</span>
        </div>
      </td>
      <td>${r.team_name || "—"}</td>
      ${gridCells}
      <td>${formatStatusBadge(r.status, r.laps_down)}</td>
      <td>${pitStopsDisplay}</td>
      <td style="font-family: var(--font-mono); font-size: 13px;">${formatTimeGap(r)}</td>
      <td>${r.number_of_laps != null ? r.number_of_laps : "—"}</td>
      <td style="font-weight: 800; color: #38bdf8;">${formatPoints(r.points)}</td>
    `;
    tbody.appendChild(tr);
  });

  renderGridVsFinish(results);
}

// Render Grid vs Finish Slope Chart and Biggest Movers (Task 5.2)
function renderGridVsFinish(results) {
  const panel = document.getElementById("grid-vs-finish-panel");
  if (!panel) return;

  const hasGrid = results && results.some((r) => r.grid_position != null);
  if (!hasGrid) {
    panel.style.display = "none";
    return;
  }
  panel.style.display = "block";

  const svg = document.getElementById("slope-chart-svg");
  const tooltip = document.getElementById("slope-tooltip");
  const notShownEl = document.getElementById("slope-not-shown");
  const gainsList = document.getElementById("movers-gains-list");
  const lossesList = document.getElementById("movers-losses-list");

  const validDrivers = results.filter((r) => r.grid_position != null && r.final_position != null);

  if (svg) {
    svg.innerHTML = "";
    const minPos = 1;
    const maxPos = 20;
    const topY = 30;
    const bottomY = 430;
    const x1 = 70;
    const x2 = 430;
    const getY = (p) => topY + ((p - minPos) / (maxPos - minPos)) * (bottomY - topY);

    // Background horizontal guide lines (every 5 positions)
    [1, 5, 10, 15, 20].forEach((p) => {
      const y = getY(p);
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", String(x1));
      line.setAttribute("y1", String(y));
      line.setAttribute("x2", String(x2));
      line.setAttribute("y2", String(y));
      line.setAttribute("stroke", "rgba(255, 255, 255, 0.05)");
      line.setAttribute("stroke-dasharray", "3 3");
      svg.appendChild(line);

      const labelL = document.createElementNS("http://www.w3.org/2000/svg", "text");
      labelL.setAttribute("x", String(x1 - 28));
      labelL.setAttribute("y", String(y + 3));
      labelL.setAttribute("text-anchor", "end");
      labelL.setAttribute("font-size", "10");
      labelL.setAttribute("font-family", "var(--font-mono)");
      labelL.setAttribute("fill", "rgba(148, 163, 184, 0.4)");
      labelL.textContent = `P${p}`;
      svg.appendChild(labelL);

      const labelR = document.createElementNS("http://www.w3.org/2000/svg", "text");
      labelR.setAttribute("x", String(x2 + 28));
      labelR.setAttribute("y", String(y + 3));
      labelR.setAttribute("text-anchor", "start");
      labelR.setAttribute("font-size", "10");
      labelR.setAttribute("font-family", "var(--font-mono)");
      labelR.setAttribute("fill", "rgba(148, 163, 184, 0.4)");
      labelR.textContent = `P${p}`;
      svg.appendChild(labelR);
    });

    const allGroups = [];

    validDrivers.forEach((r) => {
      const y1 = getY(r.grid_position);
      const y2 = getY(r.final_position);
      const teamColour = r.team_colour ? `#${r.team_colour}` : "#94a3b8";

      const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
      g.setAttribute("class", "slope-line-group");
      g.setAttribute("tabindex", "0");
      g.setAttribute("role", "img");
      const gainedNum = r.positions_gained || 0;
      const gainedStr = gainedNum > 0 ? `+${gainedNum}` : (gainedNum < 0 ? String(gainedNum) : "0");
      g.setAttribute(
        "aria-label",
        `${r.driver_name}: grid P${r.grid_position} -> finish P${r.final_position} (${gainedStr})`
      );

      // Smooth bezier curve connecting grid and finish
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", `M ${x1} ${y1} C ${x1 + 130} ${y1}, ${x2 - 130} ${y2}, ${x2} ${y2}`);
      path.setAttribute("stroke", teamColour);
      path.setAttribute("stroke-width", "2.5");
      path.setAttribute("fill", "none");
      path.setAttribute("opacity", "0.75");
      g.appendChild(path);

      // Circles at endpoints
      const c1 = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      c1.setAttribute("cx", String(x1));
      c1.setAttribute("cy", String(y1));
      c1.setAttribute("r", "3.5");
      c1.setAttribute("fill", teamColour);
      g.appendChild(c1);

      const c2 = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      c2.setAttribute("cx", String(x2));
      c2.setAttribute("cy", String(y2));
      c2.setAttribute("r", "3.5");
      c2.setAttribute("fill", teamColour);
      g.appendChild(c2);

      // Acronym labels
      const t1 = document.createElementNS("http://www.w3.org/2000/svg", "text");
      t1.setAttribute("x", String(x1 - 8));
      t1.setAttribute("y", String(y1 + 4));
      t1.setAttribute("text-anchor", "end");
      t1.setAttribute("font-size", "10");
      t1.setAttribute("font-family", "var(--font-mono)");
      t1.setAttribute("font-weight", "600");
      t1.setAttribute("fill", "#cbd5e1");
      t1.textContent = r.name_acronym || String(r.driver_number);
      g.appendChild(t1);

      const t2 = document.createElementNS("http://www.w3.org/2000/svg", "text");
      t2.setAttribute("x", String(x2 + 8));
      t2.setAttribute("y", String(y2 + 4));
      t2.setAttribute("text-anchor", "start");
      t2.setAttribute("font-size", "10");
      t2.setAttribute("font-family", "var(--font-mono)");
      t2.setAttribute("font-weight", "600");
      t2.setAttribute("fill", "#cbd5e1");
      t2.textContent = r.name_acronym || String(r.driver_number);
      g.appendChild(t2);

      const onHighlight = () => {
        allGroups.forEach((other) => {
          if (other !== g) other.classList.add("dimmed");
        });
        g.classList.add("highlight");
        svg.appendChild(g);
        if (tooltip) {
          tooltip.textContent = `${r.driver_name}: grid P${r.grid_position} -> finish P${r.final_position} (${gainedStr})`;
          tooltip.style.display = "block";
        }
      };

      const onUnHighlight = () => {
        allGroups.forEach((other) => other.classList.remove("dimmed"));
        g.classList.remove("highlight");
        if (tooltip) tooltip.style.display = "none";
      };

      g.addEventListener("mouseenter", onHighlight);
      g.addEventListener("mouseleave", onUnHighlight);
      g.addEventListener("focus", onHighlight);
      g.addEventListener("blur", onUnHighlight);

      svg.appendChild(g);
      allGroups.push(g);
    });
  }

  // Drivers omitted
  if (notShownEl) {
    const notShown = results.filter((r) => r.grid_position == null || r.final_position == null);
    if (notShown.length > 0) {
      const noFinish = notShown.filter((r) => r.final_position == null).map((r) => r.name_acronym || r.driver_name);
      const noGrid = notShown.filter((r) => r.grid_position == null).map((r) => r.name_acronym || r.driver_name);
      const parts = [];
      if (noFinish.length) parts.push(`${noFinish.join(", ")} (no classified finish)`);
      if (noGrid.length) parts.push(`${noGrid.join(", ")} (no grid position)`);
      notShownEl.textContent = `Not shown: ${parts.join("; ")}`;
      notShownEl.style.display = "block";
    } else {
      notShownEl.style.display = "none";
    }
  }

  // Biggest movers
  const classifiedMovers = results.filter((r) => r.final_position != null && r.positions_gained != null);
  const gains = [...classifiedMovers]
    .sort((a, b) => b.positions_gained - a.positions_gained || (a.driver_number || 0) - (b.driver_number || 0))
    .slice(0, 3);
  const losses = [...classifiedMovers]
    .sort((a, b) => a.positions_gained - b.positions_gained || (a.driver_number || 0) - (b.driver_number || 0))
    .slice(0, 3);

  const renderMoverItem = (r, isGain) => {
    const teamColour = r.team_colour ? `#${r.team_colour}` : "#94a3b8";
    const badgeClass = isGain ? "pos-gain" : "pos-loss";
    const badgeText = isGain ? `▲ ${r.positions_gained}` : `▼ ${Math.abs(r.positions_gained)}`;
    return `
      <div class="mover-item">
        <div class="mover-driver">
          <span class="team-stripe" style="background: ${teamColour};"></span>
          <span>${r.driver_name}</span>
          <span style="color: var(--text-muted); font-size: 11px;">(${r.name_acronym || r.driver_number})</span>
        </div>
        <span class="mover-badge ${badgeClass}">${badgeText}</span>
      </div>
    `;
  };

  if (gainsList) {
    gainsList.innerHTML = gains.length
      ? gains.map((r) => renderMoverItem(r, true)).join("")
      : '<div style="color: var(--text-muted); font-size: 12px;">No position gains</div>';
  }

  if (lossesList) {
    lossesList.innerHTML = losses.length
      ? losses.map((r) => renderMoverItem(r, false)).join("")
      : '<div style="color: var(--text-muted); font-size: 12px;">No position losses</div>';
  }
}



// Render Top Overview Cards (Task 5.3)
function renderOverviewMetrics() {
  if (!goldData) return;

  const results = raceRows("race_results");
  const drivers = raceRows("driver_summary");

  // 1. Race Winner (from race_results where final_position === 1)
  const winnerCard = document.getElementById("metric-winner-card");
  const winnerRow = results.find((r) => r.final_position === 1);
  if (winnerCard) {
    if (winnerRow) {
      winnerCard.style.display = "block";
      document.getElementById("metric-race-winner").textContent = winnerRow.driver_name;
      const winTime = formatDuration(winnerRow.duration_seconds);
      document.getElementById("metric-race-winner-sub").textContent =
        `${winnerRow.team_name || ""} • ${winTime}`;
    } else {
      winnerCard.style.display = "none";
    }
  }

  // 2. Fastest Lap (from driver_summary)
  const fastestCard = document.getElementById("metric-fastest-card");
  if (fastestCard) {
    if (drivers.length > 0) {
      fastestCard.style.display = "block";
      const fastest = drivers.reduce(
        (prev, curr) => (curr.fastest_lap_seconds < prev.fastest_lap_seconds ? curr : prev),
        drivers[0]
      );
      document.getElementById("metric-fastest-lap").textContent = `${fastest.fastest_lap_seconds.toFixed(3)}s`;
      document.getElementById("metric-fastest-sub").textContent =
        `${fastest.driver_name} • ${fastest.team_name || ""}`;
    } else {
      fastestCard.style.display = "none";
    }
  }

  // 3. Points Awarded (sum of points in race_results)
  const pointsCard = document.getElementById("metric-points-card");
  if (pointsCard) {
    const totalPoints = results.reduce((sum, r) => sum + (r.points || 0), 0);
    if (results.length > 0 && totalPoints > 0) {
      pointsCard.style.display = "block";
      document.getElementById("metric-points").textContent = `${formatPoints(totalPoints)} pts`;
      document.getElementById("metric-points-sub").textContent = "Championship points distributed";
    } else {
      pointsCard.style.display = "none";
    }
  }

  // 4. Field Pit Stops
  const stopsCard = document.getElementById("metric-stops-card");
  if (stopsCard) {
    if (drivers.length > 0) {
      stopsCard.style.display = "block";
      const totalStops = drivers.reduce((sum, d) => sum + (d.total_pit_stops || 0), 0);
      document.getElementById("metric-stops").textContent = `${totalStops} Stops`;
      document.getElementById("metric-stops-sub").textContent = `${(totalStops / drivers.length).toFixed(1)} stops / driver`;
    } else {
      stopsCard.style.display = "none";
    }
  }

  const raceLaps = Math.max(...drivers.map((driver) => driver.total_laps || 0), 0);
  const totalStops = drivers.reduce((sum, d) => sum + (d.total_pit_stops || 0), 0);
  document.getElementById("leaderboard-desc").textContent =
    `Ranked by fastest lap, not race finishing position (${raceLaps} laps, ${totalStops} pit events).`;
  document.getElementById("strategy-title").textContent = `Pit Stop & Tyre Stint Timeline (${raceLaps} Laps)`;
  document.getElementById("weather-desc").textContent = `Track and ambient weather observations for ${raceRows("weather_impact").length} timestamps.`;
}

// Render Pace Ranking Table (Task 5.3)
function renderLeaderboard() {
  const tbody = document.getElementById("leaderboard-tbody");
  if (!tbody || !goldData) return;

  const drivers = [...raceRows("driver_summary")].sort(
    (a, b) => (a.fastest_lap_seconds || Infinity) - (b.fastest_lap_seconds || Infinity)
  );
  const raceStints = raceRows("pit_strategy");
  tbody.innerHTML = "";

  drivers.forEach((d, idx) => {
    const tr = document.createElement("tr");
    const teamColour = d.team_colour ? `#${d.team_colour}` : "#94a3b8";
    const stratChips = renderStrategyChips(d.driver_name, raceStints);

    tr.innerHTML = `
      <td style="font-weight: 800; color: #94a3b8;">#${idx + 1}</td>
      <td>
        <div class="team-pill">
          <span class="team-stripe" style="background: ${teamColour};"></span>
          <span>${d.driver_name}</span>
          <span style="color: var(--text-muted); font-size: 11px;">(${d.name_acronym || d.driver_number})</span>
        </div>
      </td>
      <td>${d.team_name || "—"}</td>
      <td><div class="strategy-chips">${stratChips}</div></td>
      <td>${d.total_laps || "—"}</td>
      <td style="font-family: var(--font-mono); font-weight: 700; color: #38bdf8;">
        ${d.fastest_lap_seconds ? d.fastest_lap_seconds.toFixed(3) + "s" : "—"}
      </td>
      <td>${d.total_pit_stops != null ? d.total_pit_stops : "—"}</td>
    `;
    tbody.appendChild(tr);
  });
}

// Initialize Tyre Degradation Controls & Chart
function initTyreDegradationControls() {
  const select = document.getElementById("driver-select");
  if (!select || !goldData) return;

  select.innerHTML = "";
  const tyreRows = raceRows("tyre_degradation");
  const uniqueDrivers = [...new Set(tyreRows.map(d => d.driver_name))];

  uniqueDrivers.forEach((name, i) => {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    if (i === 0) opt.selected = true;
    select.appendChild(opt);
  });

  select.onchange = () => updateTyreChart(select.value);

  if (uniqueDrivers.length > 0) {
    updateTyreChart(uniqueDrivers[0]);
  }
}

// Update Tyre Degradation Chart for selected driver
function updateTyreChart(selectedDriver) {
  const ctx = document.getElementById("tyreDegChart");
  if (!ctx || !goldData) return;

  const laps = raceRows("tyre_degradation")
    .filter(d => d.driver_name === selectedDriver && d.lap_time_seconds != null)
    .sort((a, b) => a.lap_number - b.lap_number);

  // Group by stint
  const stints = [...new Set(laps.map(l => l.stint_number))];
  const datasets = stints.map((stintNum) => {
    const stintLaps = laps.filter(l => l.stint_number === stintNum);
    const compound = stintLaps[0]?.compound || "SOFT";
    
    let color = "#ef4444"; // Soft
    if (compound.includes("MEDIUM")) color = "#eab308";
    if (compound.includes("HARD")) color = "#38bdf8";

    return {
      label: `Stint ${stintNum} (${compound})`,
      data: stintLaps.map(l => ({ x: l.tyre_age, y: l.lap_time_seconds })),
      borderColor: color,
      backgroundColor: color,
      borderWidth: 2.5,
      pointRadius: 4,
      pointHoverRadius: 6,
      tension: 0.25,
    };
  });

  if (tyreChart) tyreChart.destroy();

  tyreChart = new Chart(ctx, {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: "#f8fafc", font: { family: "Outfit", size: 12 } }
        },
        tooltip: {
          callbacks: {
            label: (ctx) => `Tyre Lap ${ctx.parsed.x}: ${ctx.parsed.y.toFixed(3)}s`
          }
        }
      },
      scales: {
        x: {
          type: "linear",
          title: { display: true, text: "Tyre Age (Laps on Set)", color: "#94a3b8" },
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: { color: "#94a3b8" }
        },
        y: {
          title: { display: true, text: "Lap Time (Seconds)", color: "#94a3b8" },
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: { color: "#94a3b8" }
        }
      }
    }
  });
}

// Render Stint Timelines (Gantt-style, Task 5.3 & Amendment 3)
function renderStintTimelines() {
  const container = document.getElementById("timeline-list");
  if (!container || !goldData) return;

  container.innerHTML = "";
  const stints = raceRows("pit_strategy");
  const results = raceRows("race_results");

  const uniqueStintDrivers = [...new Set(stints.map((s) => s.driver_name))];

  let orderedDrivers = [];
  if (results.length > 0) {
    // Order drivers by official race finishing order (final_position, nulls last)
    const resultsDrivers = results.map((r) => r.driver_name);
    orderedDrivers = resultsDrivers.filter((name) => uniqueStintDrivers.includes(name));
    uniqueStintDrivers.forEach((name) => {
      if (!orderedDrivers.includes(name)) orderedDrivers.push(name);
    });
  } else {
    // Order by driver_number
    const driverNumMap = {};
    stints.forEach((s) => {
      driverNumMap[s.driver_name] = s.driver_number || 999;
    });
    orderedDrivers = [...uniqueStintDrivers].sort(
      (a, b) => (driverNumMap[a] || 999) - (driverNumMap[b] || 999)
    );
  }

  if (!orderedDrivers.length) {
    container.innerHTML = '<div style="color: var(--text-muted); font-size: 13px;">No stint strategy data available.</div>';
    return;
  }

  const totalRaceLaps = Math.max(...stints.map((stint) => stint.lap_end || 0), 1);

  orderedDrivers.forEach((driver) => {
    const driverStints = stints
      .filter((s) => s.driver_name === driver)
      .sort((a, b) => a.stint_number - b.stint_number);
    const row = document.createElement("div");
    row.className = "driver-timeline-row";

    const nameLabel = document.createElement("div");
    nameLabel.className = "timeline-driver-name";
    nameLabel.textContent = driver;

    const track = document.createElement("div");
    track.className = "timeline-track";

    driverStints.forEach((stint) => {
      const len = Math.max(stint.stint_length || (stint.lap_end - stint.lap_start + 1), 1);
      const widthPct = (len / totalRaceLaps) * 100;
      const compClass = (stint.compound || "soft").toLowerCase();

      const seg = document.createElement("div");
      seg.className = `stint-segment ${compClass}`;
      seg.style.width = `${widthPct}%`;
      seg.title = `${stint.compound}: Laps ${stint.lap_start}-${stint.lap_end} (${len} laps)`;
      seg.textContent = `${stint.compound.substring(0, 1)} (${len})`;
      track.appendChild(seg);
    });

    row.appendChild(nameLabel);
    row.appendChild(track);
    container.appendChild(row);
  });
}

// Render Weather & Track Condition Impact Chart
function renderWeatherChart() {
  const ctx = document.getElementById("weatherChart");
  if (!ctx || !goldData || !goldData.weather_impact) return;

  const wData = raceRows("weather_impact");
  if (!wData.length) return;
  const labels = wData.map((_, i) => `T+${i * 2}m`);
  const trackTemps = wData.map(d => d.track_temperature);
  const airTemps = wData.map(d => d.air_temperature);

  if (weatherChart) weatherChart.destroy();

  weatherChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Track Temp (°C)",
          data: trackTemps,
          borderColor: "#ef4444",
          backgroundColor: "rgba(239, 68, 68, 0.1)",
          fill: true,
          tension: 0.3,
        },
        {
          label: "Air Temp (°C)",
          data: airTemps,
          borderColor: "#38bdf8",
          backgroundColor: "transparent",
          tension: 0.3,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "#f8fafc", font: { family: "Outfit" } } }
      },
      scales: {
        x: { grid: { color: "rgba(255, 255, 255, 0.05)" }, ticks: { color: "#94a3b8" } },
        y: {
          title: { display: true, text: "Temperature (°C)", color: "#94a3b8" },
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: { color: "#94a3b8" }
        }
      }
    }
  });
}
