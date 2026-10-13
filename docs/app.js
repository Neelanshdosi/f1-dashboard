/* ─────────────────────────────────────────────────────────────
   F1 RACE ANALYTICS – DASHBOARD APPLICATION LOGIC
   Loads BigQuery Gold JSON, renders Chart.js visualizations
───────────────────────────────────────────────────────────── */

let goldData = null;
let tyreChart = null;
let weatherChart = null;

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

    renderOverviewMetrics();
    renderLeaderboard();
    initTyreDegradationControls();
    renderStintTimelines();
    renderWeatherChart();
  } catch (err) {
    console.error("Failed to load gold_data.json:", err);
    document.getElementById("leaderboard-tbody").innerHTML = `
      <tr><td colspan="7" style="color: #ef4444; text-align: center;">
        Failed to load Gold dataset. Ensure gold_data.json exists in the dashboard directory.
      </td></tr>
    `;
  }
}

// Render Top Overview Cards
function renderOverviewMetrics() {
  if (!goldData || !goldData.driver_summary) return;

  const drivers = goldData.driver_summary;
  const fastest = drivers.reduce((prev, curr) => 
    (curr.fastest_lap_seconds < prev.fastest_lap_seconds ? curr : prev), drivers[0]);

  document.getElementById("metric-winner").textContent = fastest.driver_name;
  document.getElementById("metric-fastest-lap").textContent = `${fastest.fastest_lap_seconds.toFixed(3)}s`;

  // Total pit stops
  const totalStops = drivers.reduce((sum, d) => sum + (d.total_pit_stops || 0), 0);
  document.getElementById("metric-stops").textContent = `${totalStops} Stops`;

  // Telemetry points
  document.getElementById("metric-rows").textContent = "51,415";
}

// Render Leaderboard Table
function renderLeaderboard() {
  const tbody = document.getElementById("leaderboard-tbody");
  if (!tbody || !goldData) return;

  const drivers = goldData.driver_summary;
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
  const uniqueDrivers = [...new Set(goldData.tyre_degradation.map(d => d.driver_name))];

  uniqueDrivers.forEach((name, i) => {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    if (i === 0) opt.selected = true;
    select.appendChild(opt);
  });

  select.addEventListener("change", () => {
    updateTyreChart(select.value);
  });

  if (uniqueDrivers.length > 0) {
    updateTyreChart(uniqueDrivers[0]);
  }
}

// Update Tyre Degradation Chart for selected driver
function updateTyreChart(selectedDriver) {
  const ctx = document.getElementById("tyreDegChart");
  if (!ctx || !goldData) return;

  const laps = goldData.tyre_degradation
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
  const stints = goldData.pit_strategy;
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

    const totalRaceLaps = 57;
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

  const wData = goldData.weather_impact;
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
