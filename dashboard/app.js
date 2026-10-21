/* ─────────────────────────────────────────────────────────────
   F1 RACE ANALYTICS – DASHBOARD APPLICATION LOGIC
   Loads BigQuery Gold JSON, renders Chart.js visualizations
───────────────────────────────────────────────────────────── */

let goldData = null;
let tyreChart = null;
let weatherChart = null;
let selectedSessionKey = null;

// Initialize on DOM load
document.addEventListener("DOMContentLoaded", async () => {
  setupTabs();
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

function initRaceSelector() {
  const select = document.getElementById("race-select");
  const races = [...(goldData.races || [])].sort((a, b) =>
    a.year - b.year || String(a.date || "").localeCompare(String(b.date || ""))
  );
  select.innerHTML = "";
  races.forEach((race) => {
    const option = document.createElement("option");
    option.value = race.session_key;
    option.textContent = race.name;
    select.appendChild(option);
  });
  if (!races.length) return;
  selectedSessionKey = (races.find((race) => race.name.includes("Abu Dhabi")) || races[races.length - 1]).session_key;
  select.value = selectedSessionKey;
  select.addEventListener("change", () => {
    selectedSessionKey = Number(select.value);
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
  document.getElementById("race-subtitle").textContent =
    `${race.name} • GCP Medallion Pipeline (OpenF1 → GCS → PySpark → BigQuery)`;
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
  if (!tbody || !goldData) return;

  const results = raceRows("race_results");

  // If no results for this race, hide the tab/view and switch to leaderboard if active
  if (!results.length) {
    if (tabBtn) tabBtn.style.display = "none";
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

  // Ordering contract:
  // 1. final_position ascending with nulls last
  // 2. Among null positions: DNF (0), DSQ (1), DNS (2), Unclassified (3)
  const statusRank = { DNF: 0, DSQ: 1, DNS: 2, Unclassified: 3 };
  const sorted = [...results].sort((a, b) => {
    const aHasPos = a.final_position != null;
    const bHasPos = b.final_position != null;
    if (aHasPos && bHasPos) return a.final_position - b.final_position;
    if (aHasPos && !bHasPos) return -1;
    if (!aHasPos && bHasPos) return 1;
    const aRank = statusRank[a.status] ?? 4;
    const bRank = statusRank[b.status] ?? 4;
    if (aRank !== bRank) return aRank - bRank;
    return (a.driver_number || 0) - (b.driver_number || 0);
  });

  tbody.innerHTML = "";
  document.getElementById("results-desc").textContent =
    `Official classified finishing results for ${sorted.length} drivers.`;

  sorted.forEach((r) => {
    const tr = document.createElement("tr");
    const teamColour = r.team_colour ? `#${r.team_colour}` : "#94a3b8";
    const statusClass = (r.status || "unclassified").toLowerCase();

    // Position display: keeps position number if not null, otherwise "-"
    const posDisplay = r.final_position != null ? `#${r.final_position}` : "—";

    // Grid display
    const gridDisplay = r.grid_position != null ? `P${r.grid_position}` : (r.grid_position_missing ? "PL" : "—");

    // Positions gained
    let gainedHtml = "—";
    if (r.positions_gained != null) {
      if (r.positions_gained > 0) {
        gainedHtml = `<span class="pos-gain">▲ ${r.positions_gained}</span>`;
      } else if (r.positions_gained < 0) {
        gainedHtml = `<span class="pos-loss">▼ ${Math.abs(r.positions_gained)}</span>`;
      } else {
        gainedHtml = `<span class="pos-same">0</span>`;
      }
    }

    // Gap / Time display
    let timeGap = "—";
    if (r.final_position === 1 && r.duration_seconds) {
      const mins = Math.floor(r.duration_seconds / 60);
      const secs = (r.duration_seconds % 60).toFixed(3);
      timeGap = `${mins}:${secs.padStart(6, "0")}`;
    } else if (r.gap_to_leader_seconds != null) {
      timeGap = `+${r.gap_to_leader_seconds.toFixed(3)}s`;
    } else if (r.laps_down != null) {
      timeGap = `+${r.laps_down} ${r.laps_down === 1 ? "lap" : "laps"}`;
    }

    // Points display
    const ptsDisplay = r.points != null ? (Number.isInteger(r.points) ? r.points : r.points.toFixed(1)) : "0";

    tr.innerHTML = `
      <td style="font-weight: 800; color: #fff;">${posDisplay}</td>
      <td>
        <div class="team-pill">
          <span class="team-stripe" style="background: ${teamColour};"></span>
          <span>${r.driver_name || `Driver ${r.driver_number}`}</span>
          <span style="color: var(--text-muted); font-size: 11px;">(${r.name_acronym || r.driver_number})</span>
        </div>
      </td>
      <td>${r.team_name || "N/A"}</td>
      <td><span class="status-badge ${statusClass}">${r.status}</span></td>
      <td>${gridDisplay}</td>
      <td>${gainedHtml}</td>
      <td>${r.number_of_laps != null ? r.number_of_laps : "—"}</td>
      <td style="font-family: var(--font-mono); font-size: 13px;">${timeGap}</td>
      <td style="font-weight: 800; color: #38bdf8;">${ptsDisplay}</td>
    `;
    tbody.appendChild(tr);
  });
}

// Render Top Overview Cards
function renderOverviewMetrics() {
  if (!goldData || !goldData.driver_summary) return;

  const drivers = raceRows("driver_summary");
  if (!drivers.length) return;
  const fastest = drivers.reduce((prev, curr) =>
    (curr.fastest_lap_seconds < prev.fastest_lap_seconds ? curr : prev), drivers[0]);

  document.getElementById("metric-winner").textContent = fastest.driver_name;
  document.getElementById("metric-fastest-lap").textContent = `${fastest.fastest_lap_seconds.toFixed(3)}s`;
  document.getElementById("metric-winner-sub").textContent = fastest.team_name || "Fastest lap in selected race";
  document.getElementById("metric-fastest-sub").textContent = `${fastest.name_acronym || ""} • ${fastest.team_name || ""}`;

  // Totals and row counts for the selected session.
  const totalStops = drivers.reduce((sum, d) => sum + (d.total_pit_stops || 0), 0);
  document.getElementById("metric-stops").textContent = `${totalStops} Stops`;
  document.getElementById("metric-stops-sub").textContent = `${(totalStops / drivers.length).toFixed(1)} stops / driver`;
  const rows = ["driver_summary", "pit_strategy", "tyre_degradation", "weather_impact", "race_results"]
    .reduce((sum, key) => sum + raceRows(key).length, 0);
  document.getElementById("metric-rows").textContent = rows.toLocaleString();
  const raceLaps = Math.max(...drivers.map((driver) => driver.total_laps || 0));
  document.getElementById("leaderboard-desc").textContent =
    `PySpark Gold metrics for ${raceLaps} laps and ${totalStops} pit events. Drivers are ordered by fastest lap.`;
  document.getElementById("strategy-title").textContent = `Pit Stop & Tyre Stint Timeline (${raceLaps} Laps)`;
  document.getElementById("weather-desc").textContent = `Track and ambient weather observations for ${raceRows("weather_impact").length} timestamps.`;
}

// Render Leaderboard Table
function renderLeaderboard() {
  const tbody = document.getElementById("leaderboard-tbody");
  if (!tbody || !goldData) return;

  const drivers = [...raceRows("driver_summary")].sort(
    (a, b) => (a.fastest_lap_seconds || Infinity) - (b.fastest_lap_seconds || Infinity)
  );
  tbody.innerHTML = "";

  drivers.forEach((d, idx) => {
    const tr = document.createElement("tr");
    const teamColour = d.team_colour ? `#${d.team_colour}` : "#94a3b8";
    const compoundClass = (d.starting_compound || "soft").toLowerCase();

    tr.innerHTML = `
      <td style="font-weight: 800; color: #94a3b8;">#${idx + 1}</td>
      <td>
        <div class="team-pill">
          <span class="team-stripe" style="background: ${teamColour};"></span>
          <span>${d.driver_name}</span>
          <span style="color: var(--text-muted); font-size: 11px;">(${d.name_acronym || d.driver_number})</span>
        </div>
      </td>
      <td>${d.team_name || "N/A"}</td>
      <td><span class="compound-badge ${compoundClass}">${d.starting_compound || "SOFT"}</span></td>
      <td>${d.total_laps || 57}</td>
      <td style="font-family: var(--font-mono); font-weight: 700; color: #38bdf8;">
        ${d.fastest_lap_seconds ? d.fastest_lap_seconds.toFixed(3) + "s" : "N/A"}
      </td>
      <td>${d.total_pit_stops || 0}</td>
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

// Render Stint Timelines (Gantt-style)
function renderStintTimelines() {
  const container = document.getElementById("timeline-list");
  if (!container || !goldData) return;

  container.innerHTML = "";
  const stints = raceRows("pit_strategy");
  const drivers = [...new Set(stints.map(s => s.driver_name))].slice(0, 10); // top 10

  drivers.forEach((driver) => {
    const driverStints = stints.filter(s => s.driver_name === driver).sort((a, b) => a.stint_number - b.stint_number);
    const row = document.createElement("div");
    row.className = "driver-timeline-row";

    const nameLabel = document.createElement("div");
    nameLabel.className = "timeline-driver-name";
    nameLabel.textContent = driver;

    const track = document.createElement("div");
    track.className = "timeline-track";

    const totalRaceLaps = Math.max(...driverStints.map((stint) => stint.lap_end || 0), 1);
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
