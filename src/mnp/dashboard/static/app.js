// Renders <canvas data-chart="bar|stacked|line|scatter" data-source="json-script-id">.
(function () {
  function css(name) { return getComputedStyle(document.body).getPropertyValue(name).trim(); }

  // Explicit palette: changing Chart.js defaults (for dark mode) disables its automatic colors.
  const PALETTE = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4",
                   "#ec4899", "#84cc16", "#f97316", "#64748b"];
  function colorize(spec, kind) {
    spec.datasets.forEach((ds, i) => {
      const c = PALETTE[i % PALETTE.length];
      if (kind === "scatter") { ds.backgroundColor = c + "99"; ds.pointRadius = 4; ds.pointHoverRadius = 6; }
      else if (spec.datasets.length === 1 && kind === "bar") { ds.backgroundColor = c + "cc"; }
      else { ds.backgroundColor = c + "cc"; ds.borderColor = c; }
      ds.borderWidth = ds.borderWidth ?? 0;
    });
  }

  function build(canvas) {
    if (canvas.dataset.rendered || typeof Chart === "undefined") return;
    const source = document.getElementById(canvas.dataset.source);
    if (!source) return;
    const spec = JSON.parse(source.textContent);
    const kind = canvas.dataset.chart;
    colorize(spec, kind);
    const color = css("--pico-color") || "#888";
    const grid = css("--pico-muted-border-color") || "rgba(128,128,128,.2)";
    Chart.defaults.color = color;
    Chart.defaults.borderColor = grid;
    const options = {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { display: spec.datasets.length > 1, position: "bottom" } },
      scales: {},
    };
    if (kind === "stacked") options.scales = { x: { stacked: true }, y: { stacked: true, beginAtZero: true } };
    if (kind === "bar" && spec.horizontal) {
      options.indexAxis = "y";
      options.scales = { x: { beginAtZero: true, ticks: { precision: 0 } } };  // counts
      spec.datasets.forEach((ds) => { ds.maxBarThickness = 22; });
    }
    if (spec.links) {
      options.onClick = (_e, items) => { if (items.length) location.href = spec.links[items[0].index]; };
      options.onHover = (e, items) => { e.native.target.style.cursor = items.length ? "pointer" : ""; };
    }
    if (kind === "scatter") {
      options.scales = {
        x: { min: -1, max: 1, title: { display: true, text: spec.xLabel || "" } },
        y: { min: 0, max: 1, title: { display: true, text: spec.yLabel || "" } },
      };
      options.plugins.tooltip = { callbacks: { label: (ctx) => ctx.raw.label || "" } };
      options.onClick = (_e, items) => {
        if (items.length) { const p = items[0].element.$context.raw; if (p.id) location.href = "/ui/articles/" + p.id; }
      };
    }
    new Chart(canvas, { type: kind === "stacked" ? "bar" : kind, data: spec, options });
    canvas.dataset.rendered = "1";
  }

  function renderAll(root) { (root || document).querySelectorAll("canvas[data-chart]").forEach(build); }
  document.addEventListener("DOMContentLoaded", () => renderAll());
  document.addEventListener("htmx:afterSettle", (e) => renderAll(e.target));
})();
