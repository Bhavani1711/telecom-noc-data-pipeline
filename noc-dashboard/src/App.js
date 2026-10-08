import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import {
  MapContainer,
  TileLayer,
  GeoJSON,
} from "react-leaflet";
import "./App.css";

function App() {
  const API_URL =
    process.env.REACT_APP_API_URL || "http://localhost:8000";

  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [page, setPage] = useState("overview");

  // Grid Explorer
  const [gridId, setGridId] = useState("6321");
  const [gridData, setGridData] = useState(null);
  const [gridLoading, setGridLoading] = useState(false);
  const [gridError, setGridError] = useState("");

  // Hotspots & Alerts
  const [hotspots, setHotspots] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [geoData, setGeoData] = useState(null);
  const [hotspotLimit, setHotspotLimit] = useState(10);
  const [severity, setSeverity] = useState("ALL");

  // Predictive Risk
  const [riskGrid, setRiskGrid] = useState("6321");
  const [riskFeatures, setRiskFeatures] = useState(null);
  const [riskResult, setRiskResult] = useState(null);
  const [riskLoading, setRiskLoading] = useState(false);
  const [riskError, setRiskError] = useState("");

  /*
   * =========================================================
   * INITIAL NETWORK SUMMARY
   * =========================================================
   */
  useEffect(() => {
    axios
      .get(`${API_URL}/network/summary`)
      .then((response) => {
        setSummary(response.data);
        setError("");
      })
      .catch(() => {
        setError("Network API is currently unavailable.");
      })
      .finally(() => {
        setLoading(false);
      });
  }, [API_URL]);

  /*
   * =========================================================
   * HOTSPOTS / ALERTS / GEOJSON
   * =========================================================
   */
  useEffect(() => {
    if (page !== "alerts") return;

    axios
      .get("/reference/milano-grid.geojson")
      .then((response) => {
        setGeoData(response.data);
      })
      .catch(() => {
        setGeoData(null);
      });

    axios
      .get(`${API_URL}/network/hotspots?limit=${hotspotLimit}`)
      .then((response) => {
        setHotspots(response.data.hotspots || []);
      })
      .catch(() => {
        setHotspots([]);
      });

    axios
      .get(`${API_URL}/network/alerts`)
      .then((response) => {
        setAlerts(response.data.alerts || []);
      })
      .catch(() => {
        setAlerts([]);
      });
  }, [page, hotspotLimit, API_URL]);

  /*
   * =========================================================
   * GRID EXPLORER
   * =========================================================
   */
  const loadGrid = (selectedGridId = gridId) => {
    if (!selectedGridId) return;

    const numericGridId = Number(selectedGridId);

    if (
      !Number.isInteger(numericGridId) ||
      numericGridId < 1 ||
      numericGridId > 10000
    ) {
      setGridError(
        "Grid ID must be an integer between 1 and 10,000."
      );
      return;
    }

    setGridLoading(true);
    setGridError("");
    setGridData(null);

    axios
      .get(`${API_URL}/network/grid/${numericGridId}`)
      .then((response) => {
        setGridData(response.data);
      })
      .catch((err) => {
        if (err.response?.status === 404) {
          setGridError(
            `Grid ${numericGridId} was not found.`
          );
        } else {
          setGridError("Unable to load grid activity.");
        }
      })
      .finally(() => {
        setGridLoading(false);
      });
  };

  /*
   * =========================================================
   * ML3 PREDICTIVE RISK
   * =========================================================
   *
   * The feature endpoint is authoritative.
   * The returned ML2 features are passed to ML3.
   */
  const loadRisk = async () => {
    if (!riskGrid) return;

    const numericGridId = Number(riskGrid);

    if (
      !Number.isInteger(numericGridId) ||
      numericGridId < 1 ||
      numericGridId > 10000
    ) {
      setRiskError(
        "Grid ID must be an integer between 1 and 10,000."
      );
      return;
    }

    setRiskLoading(true);
    setRiskError("");
    setRiskResult(null);
    setRiskFeatures(null);

    try {
      const featureResponse = await axios.get(
        `${API_URL}/network/grid/${numericGridId}/features`
      );

      const features = featureResponse.data;

      setRiskFeatures(features);

      const predictionResponse = await axios.post(
        `${API_URL}/network/predict-risk`,
        {
          grid_id: numericGridId,
          avg_activity: features.avg_activity,
          activity_growth: features.activity_growth,
          active_hours: features.active_hours,
          peak_ratio: features.peak_ratio,
          variability: features.variability,
          internet_share: features.internet_share,
          feature_timestamp: features.feature_timestamp,
        }
      );

      setRiskResult(predictionResponse.data);
    } catch (err) {
      const detail = err.response?.data?.detail;

      setRiskError(
        typeof detail === "string"
          ? detail
          : "Unable to get risk prediction."
      );
    } finally {
      setRiskLoading(false);
    }
  };

  /*
   * =========================================================
   * ACTUAL ALERT LOOKUP
   * =========================================================
   *
   * Confirmed/generated alerts are kept separate from
   * activity hotspot priority.
   */
  const getActualAlertSeverity = (grid) => {
    const alert = alerts.find(
      (item) =>
        Number(item.grid_id) === Number(grid)
    );

    if (!alert) return "NORMAL";

    const alertType = String(
      alert.alert_type ||
        alert.severity ||
        ""
    ).toUpperCase();

    if (
      alertType.includes("HIGH") ||
      alertType.includes("SPIKE")
    ) {
      return "HIGH";
    }

    return "ATTENTION";
  };

  /*
   * =========================================================
   * HOTSPOT PRIORITY
   * =========================================================
   *
   * IMPORTANT:
   * These are ACTIVITY priorities, not confirmed faults.
   *
   * Top 20%  -> HIGH
   * Next 30% -> MEDIUM
   * Rest     -> LOW
   */
  const hotspotPriorityMap = useMemo(() => {
    const map = {};
    const total = hotspots.length;

    if (total === 0) return map;

    hotspots.forEach((item, index) => {
      const percentile = (index + 1) / total;

      if (percentile <= 0.2) {
        map[item.grid_id] = "HIGH";
      } else if (percentile <= 0.5) {
        map[item.grid_id] = "MEDIUM";
      } else {
        map[item.grid_id] = "LOW";
      }
    });

    return map;
  }, [hotspots]);

  const getHotspotPriority = (grid) => {
    return hotspotPriorityMap[grid] || "LOW";
  };

  const filteredHotspots = hotspots.filter((item) => {
    if (severity === "ALL") return true;

    return (
      getHotspotPriority(item.grid_id) === severity
    );
  });

  const highHotspotCount = hotspots.filter(
    (item) =>
      getHotspotPriority(item.grid_id) === "HIGH"
  ).length;

  const mediumHotspotCount = hotspots.filter(
    (item) =>
      getHotspotPriority(item.grid_id) === "MEDIUM"
  ).length;

  const lowHotspotCount = hotspots.filter(
    (item) =>
      getHotspotPriority(item.grid_id) === "LOW"
  ).length;

  const highAlertCount = alerts.filter(
    (alert) =>
      getActualAlertSeverity(alert.grid_id) === "HIGH"
  ).length;

  const attentionCount = alerts.filter(
    (alert) =>
      getActualAlertSeverity(alert.grid_id) === "ATTENTION"
  ).length;

  /*
   * =========================================================
   * ML4 ANOMALY PARSER
   * =========================================================
   */
  const parseAnomaly = (note) => {
    if (!note) {
      return {
        status: "UNKNOWN",
        score: null,
        flag: null,
      };
    }

    const match = String(note).match(
      /ML4 anomaly:\s*([A-Z_]+)\s*\(score=([-+]?\d*\.?\d+),\s*flag=(\d+)\)/i
    );

    if (!match) {
      return {
        status: "UNKNOWN",
        score: null,
        flag: null,
      };
    }

    return {
      status: match[1].toUpperCase(),
      score: Number(match[2]),
      flag: Number(match[3]),
    };
  };

  const anomaly = riskResult
    ? parseAnomaly(riskResult.explanation_note)
    : null;

  /*
   * =========================================================
   * NAVIGATION
   * =========================================================
   */
  const navigate = (targetPage) => {
    setPage(targetPage);
  };

  /*
   * =========================================================
   * LOADING SCREEN
   * =========================================================
   */
  if (loading) {
    return (
      <div className="loading-screen">
        <div className="loading-spinner"></div>

        <div>
          <h2>
            Loading Network Intelligence System
          </h2>

          <p>
            Connecting to network intelligence services...
          </p>
        </div>
      </div>
    );
  }

  /*
   * =========================================================
   * MAIN APPLICATION
   * =========================================================
   */
  return (
    <div className="app-shell">

      {/* ================= SIDEBAR ================= */}
      <aside className="sidebar">

        <div className="brand">

          <div className="brand-mark">
            N
          </div>

          <div>
            <div className="brand-name">
              NETWORK INTELLIGENCE
            </div>

            <div className="brand-subtitle">
              OPERATIONS & PREDICTIVE ANALYTICS
            </div>
          </div>

        </div>

        <div className="sidebar-section-label">
          MONITORING
        </div>

        <nav className="sidebar-nav">

          <button
            className={`nav-item ${
              page === "overview" ? "active" : ""
            }`}
            onClick={() => navigate("overview")}
          >
            <span className="nav-icon">
              ◉
            </span>

            <span>
              Overview
            </span>
          </button>

          <button
            className={`nav-item ${
              page === "grid" ? "active" : ""
            }`}
            onClick={() => navigate("grid")}
          >
            <span className="nav-icon">
              ▦
            </span>

            <span>
              Grid Explorer
            </span>
          </button>

          <button
            className={`nav-item ${
              page === "alerts" ? "active" : ""
            }`}
            onClick={() => navigate("alerts")}
          >
            <span className="nav-icon">
              △
            </span>

            <span>
              Hotspots & Alerts
            </span>

            {highAlertCount > 0 && (
              <span className="nav-count">
                {highAlertCount}
              </span>
            )}
          </button>

          <button
            className={`nav-item ${
              page === "risk" ? "active" : ""
            }`}
            onClick={() => navigate("risk")}
          >
            <span className="nav-icon">
              ◈
            </span>

            <span>
              Predictive Risk
            </span>
          </button>

        </nav>

        <div className="sidebar-bottom">

          <div className="system-card">

            <div className="system-card-header">
              <span className="status-dot"></span>
              SYSTEM HEALTH
            </div>

            <strong>
              Operational
            </strong>

            <span className="system-small">
              Core network intelligence
              services responding
            </span>

          </div>

          <div className="version">
            NETWORK INTELLIGENCE · v1.0
          </div>

        </div>

      </aside>

      {/* ================= MAIN ================= */}
      <main className="main-content">

        {/* ================= TOP BAR ================= */}
        <header className="topbar">

          <div>

            <div className="breadcrumb">
              NETWORK /{" "}
              {page === "overview"
                ? "OVERVIEW"
                : page === "grid"
                ? "GRID EXPLORER"
                : page === "alerts"
                ? "HOTSPOTS & ALERTS"
                : "PREDICTIVE RISK"}
            </div>

            <h1>
              {page === "overview"
                ? "Network Intelligence System"
                : page === "grid"
                ? "Grid Explorer"
                : page === "alerts"
                ? "Hotspots & Alerts"
                : "Predictive Risk"}
            </h1>

          </div>

          <div className="topbar-status">

            <div className="status-pill healthy">
              <span className="status-dot"></span>
              SYSTEM HEALTHY
            </div>

            <div className="status-pill pipeline">
              PIPELINE CURRENT
            </div>

          </div>

        </header>

        {error && (
          <div className="error-banner">
            <span>!</span>
            {error}
          </div>
        )}

        {/* =====================================================
            OVERVIEW
            ===================================================== */}
        {page === "overview" && summary && (
          <div className="content-area">

            <div className="page-intro">

              <div>

                <p className="eyebrow">
                  LIVE NETWORK MONITORING
                </p>

                <h2>
                  Operational Overview
                </h2>

                <p>
                  Network activity, operational signals
                  and predictive intelligence across
                  reporting grid cells.
                </p>

              </div>

              <div className="timestamp-card">

                <span>
                  REPORTING TIMESTAMP
                </span>

                <strong>
                  {summary.as_of}
                </strong>

              </div>

            </div>

            {/* KPI CARDS */}
            <section className="kpi-grid">

              <div className="kpi-card">

                <div className="kpi-top">
                  <span className="kpi-label">
                    TOTAL ACTIVITY
                  </span>

                  <span className="kpi-icon">
                    ↗
                  </span>
                </div>

                <div className="kpi-value">
                  {Number(
                    summary.total_activity || 0
                  ).toLocaleString()}
                </div>

                <div className="kpi-footer">
                  Aggregate network activity
                </div>

              </div>

              <div className="kpi-card">

                <div className="kpi-top">
                  <span className="kpi-label">
                    ACTIVE GRIDS
                  </span>

                  <span className="kpi-icon">
                    ▦
                  </span>
                </div>

                <div className="kpi-value">
                  {Number(
                    summary.active_grids || 0
                  ).toLocaleString()}
                </div>

                <div className="kpi-footer">
                  Reporting network cells
                </div>

              </div>

              <div className="kpi-card">

                <div className="kpi-top">
                  <span className="kpi-label">
                    PEAK HOUR
                  </span>

                  <span className="kpi-icon">
                    ◷
                  </span>
                </div>

                <div className="kpi-value kpi-hour">
                  {summary.peak_hour || "—"}
                </div>

                <div className="kpi-footer">
                  Highest observed activity
                </div>

              </div>

              <div className="kpi-card highlight">

                <div className="kpi-top">
                  <span className="kpi-label">
                    TOP GRID
                  </span>

                  <span className="kpi-icon">
                    ◆
                  </span>
                </div>

                <div className="kpi-value">
                  {summary.top_grid || "—"}
                </div>

                <div className="kpi-footer">
                  Highest activity grid
                </div>

              </div>

            </section>

            {/* OPERATIONAL STATUS */}
            <section className="section-grid two-columns">

              <div className="panel attention-panel">

                <div className="panel-header">

                  <div>
                    <span className="eyebrow">
                      NOC SIGNALS
                    </span>

                    <h3>
                      Operational Attention
                    </h3>
                  </div>

                  <span className="live-indicator">
                    LIVE
                  </span>

                </div>

                <div className="attention-content">

                  <div className="attention-metric high">

                    <div className="attention-number">
                      {highAlertCount}
                    </div>

                    <div>
                      <strong>
                        High Priority Alerts
                      </strong>

                      <span>
                        Confirmed alert signals
                      </span>
                    </div>

                  </div>

                  <div className="attention-divider"></div>

                  <div className="attention-metric warning">

                    <div className="attention-number">
                      {attentionCount}
                    </div>

                    <div>
                      <strong>
                        Attention Alerts
                      </strong>

                      <span>
                        Generated alert signals
                      </span>
                    </div>

                  </div>

                  <div className="attention-divider"></div>

                  <div className="attention-metric normal">

                    <div className="attention-number">
                      {Math.max(
                        Number(summary.active_grids || 0) -
                          highAlertCount -
                          attentionCount,
                        0
                      )}
                    </div>

                    <div>
                      <strong>
                        Grids Without Generated Alerts
                      </strong>

                      <span>
                        No generated alert signal
                      </span>
                    </div>

                  </div>

                </div>

              </div>

              <div className="panel quick-actions">

                <div className="panel-header">

                  <div>
                    <span className="eyebrow">
                      OPERATIONS
                    </span>

                    <h3>
                      Quick Actions
                    </h3>
                  </div>

                </div>

                <button
                  className="action-button"
                  onClick={() => navigate("grid")}
                >
                  <span className="action-icon">
                    ▦
                  </span>

                  <div>
                    <strong>
                      Explore a Grid
                    </strong>

                    <span>
                      Drill into hourly activity
                    </span>
                  </div>

                  <span className="arrow">
                    →
                  </span>
                </button>

                <button
                  className="action-button"
                  onClick={() => navigate("alerts")}
                >
                  <span className="action-icon">
                    △
                  </span>

                  <div>
                    <strong>
                      Review Hotspots
                    </strong>

                    <span>
                      Inspect activity priorities
                    </span>
                  </div>

                  <span className="arrow">
                    →
                  </span>
                </button>

                <button
                  className="action-button"
                  onClick={() => navigate("risk")}
                >
                  <span className="action-icon">
                    ◈
                  </span>

                  <div>
                    <strong>
                      Run Risk Analysis
                    </strong>

                    <span>
                      Check next-interval risk
                    </span>
                  </div>

                  <span className="arrow">
                    →
                  </span>
                </button>

              </div>

            </section>

            {/* NETWORK INTELLIGENCE */}
            <section className="section-grid two-columns">

              <div className="panel">

                <div className="panel-header">

                  <div>
                    <span className="eyebrow">
                      NETWORK INTELLIGENCE
                    </span>

                    <h3>
                      What the NOC is seeing
                    </h3>
                  </div>

                </div>

                <div className="intelligence-list">

                  <div className="intel-row">

                    <span className="intel-icon blue">
                      ●
                    </span>

                    <div>
                      <strong>
                        Network data available
                      </strong>

                      <span>
                        {Number(
                          summary.active_grids || 0
                        ).toLocaleString()}{" "}
                        active grids are reporting
                        activity.
                      </span>
                    </div>

                  </div>

                  <div className="intel-row">

                    <span className="intel-icon amber">
                      ▲
                    </span>

                    <div>
                      <strong>
                        Activity hotspots prioritized
                      </strong>

                      <span>
                        High-activity grids are ranked
                        separately from confirmed
                        network alerts.
                      </span>
                    </div>

                  </div>

                  <div className="intel-row">

                    <span className="intel-icon green">
                      ✓
                    </span>

                    <div>
                      <strong>
                        Predictive layer online
                      </strong>

                      <span>
                        ML3 risk scoring and ML4 anomaly
                        detection are available.
                      </span>
                    </div>

                  </div>

                </div>

              </div>

              <div className="panel">

                <div className="panel-header">

                  <div>
                    <span className="eyebrow">
                      DATA PIPELINE
                    </span>

                    <h3>
                      Pipeline Status
                    </h3>
                  </div>

                  <span className="badge success">
                    CURRENT
                  </span>

                </div>

                <div className="pipeline-list">

                  <div className="pipeline-row">
                    <span className="pipeline-dot done"></span>

                    <span>
                      Data ingestion
                    </span>

                    <strong>
                      READY
                    </strong>
                  </div>

                  <div className="pipeline-row">
                    <span className="pipeline-dot done"></span>

                    <span>
                      Feature engineering
                    </span>

                    <strong>
                      READY
                    </strong>
                  </div>

                  <div className="pipeline-row">
                    <span className="pipeline-dot done"></span>

                    <span>
                      Risk classifier
                    </span>

                    <strong>
                      ONLINE
                    </strong>
                  </div>

                  <div className="pipeline-row">
                    <span className="pipeline-dot done"></span>

                    <span>
                      Anomaly detection
                    </span>

                    <strong>
                      ONLINE
                    </strong>
                  </div>

                </div>

              </div>

            </section>

          </div>
        )}

        {/* =====================================================
            GRID EXPLORER
            ===================================================== */}
        {page === "grid" && (
          <div className="content-area">

            <div className="page-intro compact">

              <div>

                <p className="eyebrow">
                  RE3 · OPERATIONAL DRILL-DOWN
                </p>

                <h2>
                  Grid Activity Explorer
                </h2>

                <p>
                  Inspect hourly network activity
                  for an individual grid.
                </p>

              </div>

            </div>

            <div className="search-panel">

              <div className="search-label">

                <span>
                  GRID ID
                </span>

                <small>
                  Valid range: 1–10,000
                </small>

              </div>

              <div className="search-controls">

                <input
                  type="number"
                  min="1"
                  max="10000"
                  value={gridId}
                  onChange={(e) =>
                    setGridId(e.target.value)
                  }
                  placeholder="Enter grid ID"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      loadGrid();
                    }
                  }}
                />

                <button
                  className="primary-button"
                  onClick={() => loadGrid()}
                  disabled={gridLoading}
                >
                  {gridLoading
                    ? "Loading..."
                    : "Load Grid"}

                  <span>
                    →
                  </span>
                </button>

              </div>

            </div>

            {gridLoading && (
              <div className="empty-state">

                <div className="loading-spinner small"></div>

                <strong>
                  Loading grid activity
                </strong>

                <span>
                  Retrieving hourly network records...
                </span>

              </div>
            )}

            {gridError && (
              <div className="error-banner">
                <span>!</span>
                {gridError}
              </div>
            )}

            {gridData && !gridLoading && (
              <>

                <div className="grid-summary">

                  <div>
                    <span>
                      GRID
                    </span>

                    <strong>
                      {gridData.grid_id}
                    </strong>
                  </div>

                  <div>
                    <span>
                      HOURLY RECORDS
                    </span>

                    <strong>
                      {gridData.count}
                    </strong>
                  </div>

                  <div>
                    <span>
                      AS OF
                    </span>

                    <strong>
                      {gridData.as_of}
                    </strong>
                  </div>

                  <div>
                    <span>
                      STATUS
                    </span>

                    <strong className="text-success">
                      AVAILABLE
                    </strong>
                  </div>

                </div>

                <div className="panel table-panel">

                  <div className="panel-header">

                    <div>
                      <span className="eyebrow">
                        ACTIVITY HISTORY
                      </span>

                      <h3>
                        Hourly Network Activity
                      </h3>
                    </div>

                    <span className="record-count">
                      {gridData.count} records
                    </span>

                  </div>

                  <div className="table-wrapper">

                    <table>

                      <thead>
                        <tr>
                          <th>
                            Timestamp
                          </th>

                          <th>
                            Call Activity
                          </th>

                          <th>
                            SMS Activity
                          </th>

                          <th>
                            Internet
                          </th>

                          <th>
                            Total Activity
                          </th>
                        </tr>
                      </thead>

                      <tbody>

                        {(gridData.activity || []).map(
                          (row, index) => (
                            <tr key={index}>

                              <td className="timestamp-cell">
                                {row.timestamp}
                              </td>

                              <td>
                                {(
                                  Number(row.call_in || 0) +
                                  Number(row.call_out || 0)
                                ).toFixed(2)}
                              </td>

                              <td>
                                {(
                                  Number(row.sms_in || 0) +
                                  Number(row.sms_out || 0)
                                ).toFixed(2)}
                              </td>

                              <td>
                                {Number(
                                  row.internet_activity || 0
                                ).toFixed(2)}
                              </td>

                              <td className="strong-cell">
                                {Number(
                                  row.total_activity || 0
                                ).toFixed(2)}
                              </td>

                            </tr>
                          )
                        )}

                      </tbody>

                    </table>

                  </div>

                </div>

              </>
            )}

            {!gridData &&
              !gridLoading &&
              !gridError && (
                <div className="empty-state">

                  <div className="empty-icon">
                    ▦
                  </div>

                  <strong>
                    Select a network grid
                  </strong>

                  <span>
                    Enter a grid ID above to inspect
                    hourly activity.
                  </span>

                </div>
              )}

          </div>
        )}

        {/* =====================================================
            HOTSPOTS & ALERTS
            ===================================================== */}
        {page === "alerts" && (
          <div className="content-area">

            <div className="page-intro compact">

              <div>

                <p className="eyebrow">
                  RE4 · OPERATIONAL MONITORING
                </p>

                <h2>
                  Hotspots & Alerts
                </h2>

                <p>
                  Activity hotspots are prioritized
                  independently from confirmed alert
                  signals.
                </p>

              </div>

            </div>

            <div className="alert-kpis">

              <div className="mini-kpi">
                <span>
                  TOTAL HOTSPOTS
                </span>

                <strong>
                  {hotspots.length}
                </strong>
              </div>

              <div className="mini-kpi high-border">
                <span>
                  HIGH ACTIVITY
                </span>

                <strong>
                  {highHotspotCount}
                </strong>
              </div>

              <div className="mini-kpi attention-border">
                <span>
                  MEDIUM ACTIVITY
                </span>

                <strong>
                  {mediumHotspotCount}
                </strong>
              </div>

              <div className="mini-kpi normal-border">
                <span>
                  LOW ACTIVITY
                </span>

                <strong>
                  {lowHotspotCount}
                </strong>
              </div>

            </div>

            <div className="filter-bar">

              <div className="filter-title">
                <span>
                  HOTSPOT PRIORITY
                </span>
              </div>

              <label>

                <span>
                  Limit
                </span>

                <select
                  value={hotspotLimit}
                  onChange={(e) =>
                    setHotspotLimit(
                      Number(e.target.value)
                    )
                  }
                >
                  <option value={5}>
                    5
                  </option>

                  <option value={10}>
                    10
                  </option>

                  <option value={20}>
                    20
                  </option>
                </select>

              </label>

              <label>

                <span>
                  Priority
                </span>

                <select
                  value={severity}
                  onChange={(e) =>
                    setSeverity(e.target.value)
                  }
                >
                  <option value="ALL">
                    All
                  </option>

                  <option value="HIGH">
                    High
                  </option>

                  <option value="MEDIUM">
                    Medium
                  </option>

                  <option value="LOW">
                    Low
                  </option>
                </select>

              </label>

            </div>

            <section className="section-grid alerts-layout">

              <div className="panel table-panel">

                <div className="panel-header">

                  <div>

                    <span className="eyebrow">
                      PRIORITIZED GRIDS
                    </span>

                    <h3>
                      Network Hotspots
                    </h3>

                  </div>

                  <span className="live-indicator">
                    ACTIVITY RANK
                  </span>

                </div>

                <div className="table-wrapper">

                  <table>

                    <thead>

                      <tr>
                        <th>
                          Rank
                        </th>

                        <th>
                          Grid
                        </th>

                        <th>
                          Activity
                        </th>

                        <th>
                          Priority
                        </th>
                      </tr>

                    </thead>

                    <tbody>

                      {filteredHotspots.map(
                        (item) => {

                          const priority =
                            getHotspotPriority(
                              item.grid_id
                            );

                          const originalRank =
                            hotspots.findIndex(
                              (hotspot) =>
                                Number(
                                  hotspot.grid_id
                                ) ===
                                Number(
                                  item.grid_id
                                )
                            ) + 1;

                          return (
                            <tr
                              key={item.grid_id}
                              className="clickable-row"
                              onClick={() => {
                                setGridId(
                                  String(item.grid_id)
                                );

                                navigate("grid");

                                loadGrid(
                                  item.grid_id
                                );
                              }}
                            >

                              <td>
                                <span className="rank">
                                  #{originalRank}
                                </span>
                              </td>

                              <td>
                                <strong>
                                  Grid {item.grid_id}
                                </strong>
                              </td>

                              <td>
                                {Number(
                                  item.activity ??
                                    item.total_activity ??
                                    0
                                ).toFixed(2)}
                              </td>

                              <td>

                                <span
                                  className={`severity ${priority.toLowerCase()}`}
                                >
                                  <span></span>
                                  {priority}
                                </span>

                              </td>

                            </tr>
                          );
                        }
                      )}

                    </tbody>

                  </table>

                </div>

                {filteredHotspots.length === 0 && (
                  <div className="table-empty">
                    No hotspot records match the
                    selected filter.
                  </div>
                )}

              </div>

              <div className="panel map-panel">

                <div className="panel-header">

                  <div>

                    <span className="eyebrow">
                      GEOSPATIAL VIEW
                    </span>

                    <h3>
                      Network Coverage Map
                    </h3>

                  </div>

                  <span className="map-location">
                    MILAN
                  </span>

                </div>

                {geoData ? (
                  <div className="map-wrapper">

                    <MapContainer
                      center={[45.4642, 9.19]}
                      zoom={11}
                      style={{
                        height: "100%",
                        width: "100%",
                      }}
                    >

                      <TileLayer
                        attribution="&copy; OpenStreetMap contributors"
                        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                      />

                      <GeoJSON
                        data={geoData}

                        style={(feature) => {
                          const id = Number(
                            feature.properties.cellId
                          );

                          const isHotspot =
                            hotspots.some(
                              (item) =>
                                Number(item.grid_id) === id
                            );

                          const priority = isHotspot
                            ? getHotspotPriority(id)
                            : null;

                          return {
                            fillOpacity: isHotspot
                              ? 0.55
                              : 0.08,

                            weight: isHotspot
                              ? 0.7
                              : 0.3,

                            fillColor:
                              priority === "HIGH"
                                ? "#ef4444"
                                : priority === "MEDIUM"
                                ? "#f59e0b"
                                : "#22c55e",
                          };
                        }}

                        onEachFeature={(
                          feature,
                          layer
                        ) => {

                          const selected =
                            Number(
                              feature.properties.cellId
                            );

                          const isHotspot =
                            hotspots.some(
                              (item) =>
                                Number(item.grid_id) ===
                                selected
                            );

                          const priority = isHotspot
                            ? getHotspotPriority(selected)
                            : null;

                          layer.bindTooltip(
                            isHotspot
                              ? `Grid ${selected} · ${priority} ACTIVITY`
                              : `Grid ${selected} · NOT IN TOP HOTSPOTS`,
                            {
                              sticky: true,
                            }
                          );

                          layer.on(
                            "click",
                            () => {

                              setGridId(
                                String(selected)
                              );

                              navigate("grid");

                              loadGrid(selected);
                            }
                          );

                        }}
                      />

                    </MapContainer>

                    <div className="map-legend">

                      <span>
                        <i className="legend-high"></i>
                        High activity
                      </span>

                      <span>
                        <i className="legend-attention"></i>
                        Medium activity
                      </span>

                      <span>
                        <i className="legend-normal"></i>
                        Low activity
                      </span>

                    </div>

                  </div>
                ) : (
                  <div className="map-loading">
                    Loading map...
                  </div>
                )}

              </div>

            </section>

            {/* CONFIRMED ALERTS */}
            <section className="panel confirmed-alert-panel">

              <div className="panel-header">

                <div>

                  <span className="eyebrow">
                    CONFIRMED SIGNALS
                  </span>

                  <h3>
                    Generated Network Alerts
                  </h3>

                </div>

                <span className="badge success">
                  {alerts.length} GENERATED
                </span>

              </div>

              {alerts.length === 0 ? (
                <div className="table-empty">
                  No generated network alerts are
                  currently present. Hotspot priority
                  above is based on activity ranking
                  and does not represent a confirmed
                  network fault.
                </div>
              ) : (
                <div className="table-wrapper">

                  <table>

                    <thead>

                      <tr>
                        <th>
                          Grid
                        </th>

                        <th>
                          Alert Type
                        </th>

                        <th>
                          Severity
                        </th>
                      </tr>

                    </thead>

                    <tbody>

                      {alerts.map(
                        (alert, index) => {

                          const actualSeverity =
                            getActualAlertSeverity(
                              alert.grid_id
                            );

                          return (
                            <tr
                              key={
                                alert.grid_id ??
                                index
                              }
                            >

                              <td>
                                Grid {alert.grid_id}
                              </td>

                              <td>
                                {alert.alert_type ||
                                  alert.severity ||
                                  "Network Alert"}
                              </td>

                              <td>

                                <span
                                  className={`severity ${actualSeverity.toLowerCase()}`}
                                >
                                  <span></span>
                                  {actualSeverity}
                                </span>

                              </td>

                            </tr>
                          );
                        }
                      )}

                    </tbody>

                  </table>

                </div>
              )}

            </section>

          </div>
        )}

        {/* =====================================================
            PREDICTIVE RISK
            ===================================================== */}
        {page === "risk" && (
          <div className="content-area">

            <div className="page-intro compact">

              <div>

                <p className="eyebrow">
                  RE5 · ML PREDICTION LAYER
                </p>

                <h2>
                  Predictive Risk
                </h2>

                <p>
                  Estimate elevated network activity
                  risk for the next hourly interval.
                </p>

              </div>

            </div>

            <div className="search-panel">

              <div className="search-label">

                <span>
                  ANALYZE GRID
                </span>

                <small>
                  Features are retrieved from the
                  authoritative ML2 feature layer.
                </small>

              </div>

              <div className="search-controls">

                <input
                  type="number"
                  min="1"
                  max="10000"
                  value={riskGrid}
                  onChange={(e) =>
                    setRiskGrid(e.target.value)
                  }
                  placeholder="Enter grid ID"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      loadRisk();
                    }
                  }}
                />

                <button
                  className="primary-button"
                  onClick={loadRisk}
                  disabled={riskLoading}
                >
                  {riskLoading
                    ? "Analyzing..."
                    : "Run Risk Analysis"}

                  <span>
                    →
                  </span>
                </button>

              </div>

            </div>

            {riskLoading && (
              <div className="empty-state">

                <div className="loading-spinner small"></div>

                <strong>
                  Running predictive analysis
                </strong>

                <span>
                  Fetching ML2 features and evaluating
                  ML3 risk...
                </span>

              </div>
            )}

            {riskError && (
              <div className="error-banner">
                <span>!</span>
                {riskError}
              </div>
            )}

            {riskResult && !riskLoading && (
              <>

                <section className="risk-hero">

                  <div className="risk-main">

                    <div className="risk-heading">

                      <div>

                        <span className="eyebrow">
                          ML3 PREDICTION
                        </span>

                        <h3>
                          Grid {riskResult.grid_id}
                        </h3>

                        <span className="risk-timestamp">
                          {riskResult.feature_timestamp}
                        </span>

                      </div>

                      <span
                        className={`risk-badge ${String(
                          riskResult.risk_level || "low"
                        ).toLowerCase()}`}
                      >
                        {riskResult.risk_level || "UNKNOWN"}
                      </span>

                    </div>

                    <div className="risk-score-area">

                      <div
                        className={`risk-circle ${String(
                          riskResult.risk_level || "low"
                        ).toLowerCase()}`}
                      >

                        <div>

                          <strong>
                            {(
                              Number(
                                riskResult.risk_score || 0
                              ) * 100
                            ).toFixed(1)}
                            %
                          </strong>

                          <span>
                            PREDICTED RISK
                          </span>

                        </div>

                      </div>

                      <div className="risk-description">

                        <h4>
                          Predicted next-interval risk
                        </h4>

                        <p>
                          The trained ML3 classifier
                          estimates the probability
                          of elevated high-activity
                          risk for the next hourly
                          interval.
                        </p>

                        <div className="model-tag">
                          MODEL ·{" "}
                          {riskResult.model_version || "ML3"}
                        </div>

                      </div>

                    </div>

                  </div>

                  <div className="risk-side">

                    <div className="signal-header">

                      <span className="eyebrow">
                        CURRENT SIGNAL
                      </span>

                      <span
                        className={`signal-dot ${
                          anomaly?.status === "NORMAL"
                            ? "signal-normal"
                            : ""
                        }`}
                      ></span>

                    </div>

                    <div className="current-signal">

                      <strong>
                        ML4 ANOMALY
                      </strong>

                      {anomaly &&
                      anomaly.status === "HIGH_ANOMALY" ? (
                        <>
                          <span className="anomaly-badge high">
                            HIGH ANOMALY
                          </span>

                          <div className="anomaly-score">
                            {anomaly.score !== null
                              ? anomaly.score.toFixed(6)
                              : "—"}

                            <span>
                              robust anomaly score
                            </span>
                          </div>
                        </>
                      ) : anomaly &&
                        anomaly.status === "NORMAL" ? (
                        <>
                          <span className="anomaly-badge normal">
                            NO HIGH ANOMALY
                          </span>

                          <div className="anomaly-score normal-score">
                            {anomaly.score !== null
                              ? anomaly.score.toFixed(6)
                              : "—"}

                            <span>
                              current anomaly score
                            </span>
                          </div>
                        </>
                      ) : (
                        <span className="anomaly-badge normal">
                          SIGNAL UNAVAILABLE
                        </span>
                      )}

                    </div>

                    <div className="signal-note">
                      ML3 predicts future risk.
                      ML4 evaluates the current
                      anomaly signal. These are
                      separate intelligence layers.
                    </div>

                  </div>

                </section>

                <section className="section-grid two-columns">

                  <div className="panel">

                    <div className="panel-header">

                      <div>

                        <span className="eyebrow">
                          MODEL INPUT
                        </span>

                        <h3>
                          ML2 Feature Snapshot
                        </h3>

                      </div>

                    </div>

                    {riskFeatures && (
                      <div className="feature-grid">

                        <div>
                          <span>
                            Average Activity
                          </span>

                          <strong>
                            {Number(
                              riskFeatures.avg_activity
                            ).toFixed(2)}
                          </strong>
                        </div>

                        <div>
                          <span>
                            Activity Growth
                          </span>

                          <strong>
                            {(
                              Number(
                                riskFeatures.activity_growth
                              ) * 100
                            ).toFixed(2)}
                            %
                          </strong>
                        </div>

                        <div>
                          <span>
                            Active Hours
                          </span>

                          <strong>
                            {riskFeatures.active_hours}
                          </strong>
                        </div>

                        <div>
                          <span>
                            Peak Ratio
                          </span>

                          <strong>
                            {Number(
                              riskFeatures.peak_ratio
                            ).toFixed(2)}
                          </strong>
                        </div>

                        <div>
                          <span>
                            Variability
                          </span>

                          <strong>
                            {Number(
                              riskFeatures.variability
                            ).toFixed(2)}
                          </strong>
                        </div>

                        <div>
                          <span>
                            Internet Share
                          </span>

                          <strong>
                            {(
                              Number(
                                riskFeatures.internet_share
                              ) * 100
                            ).toFixed(2)}
                            %
                          </strong>
                        </div>

                      </div>
                    )}

                  </div>

                  <div className="panel explanation-panel">

                    <div className="panel-header">

                      <div>

                        <span className="eyebrow">
                          OPERATIONAL INTERPRETATION
                        </span>

                        <h3>
                          Why this matters
                        </h3>

                      </div>

                    </div>

                    <p>
                      {riskResult.explanation_note ||
                        "Prediction explanation unavailable."}
                    </p>

                    <div className="explanation-callout">

                      <span>
                        i
                      </span>

                      <div>

                        <strong>
                          Decision support signal
                        </strong>

                        <span>
                          The prediction supports NOC
                          investigation and prioritization.
                          It is not proof of congestion or
                          a network fault.
                        </span>

                      </div>

                    </div>

                  </div>

                </section>

              </>
            )}

            {!riskResult &&
              !riskLoading &&
              !riskError && (
                <div className="empty-state">

                  <div className="empty-icon">
                    ◈
                  </div>

                  <strong>
                    Ready for risk analysis
                  </strong>

                  <span>
                    Enter a grid ID and run the ML3
                    predictive model.
                  </span>

                </div>
              )}

          </div>
        )}

        <footer className="footer">

          <span>
            Network Intelligence System
          </span>

          <span>
            ML3 Risk · ML4 Anomaly · FastAPI
          </span>

        </footer>

      </main>
    </div>
  );
}

export default App;