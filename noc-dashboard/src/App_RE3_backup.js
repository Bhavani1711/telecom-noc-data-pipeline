import { useEffect, useState } from "react";
import axios from "axios";
import "./App.css";

function App() {
  const API_URL = process.env.REACT_APP_API_URL;

  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [page, setPage] = useState("overview");

  // RE3
  const [gridId, setGridId] = useState("4857");
  const [gridData, setGridData] = useState(null);
  const [gridLoading, setGridLoading] = useState(false);
  const [gridError, setGridError] = useState("");

  // RE4
  const [hotspots, setHotspots] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [hotspotLimit, setHotspotLimit] = useState(10);
  const [severity, setSeverity] = useState("ALL");

  // RE5
  const [riskGrid, setRiskGrid] = useState("4857");
  const [riskFeatures, setRiskFeatures] = useState({
    avg_activity: 7152.7,
    activity_growth: 0.0331,
    active_hours: 23,
    peak_ratio: 1.5296,
    variability: 8474174.44,
    internet_share: 0.8477,
    feature_timestamp: "2013-11-07 23:00:00",
const [riskFeatures] = useState({const [riskFeatures] = useState({  });
  const [riskResult, setRiskResult] = useState(null);
  const [riskLoading, setRiskLoading] = useState(false);
  const [riskError, setRiskError] = useState("");

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

  useEffect(() => {
    if (page !== "alerts") return;

    axios
      .get(`${API_URL}/network/hotspots?limit=${hotspotLimit}`)
      .then((response) => {
        setHotspots(response.data);
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

  const loadGrid = () => {
    if (!gridId) return;

    setGridLoading(true);
    setGridError("");
    setGridData(null);

    axios
      .get(`${API_URL}/network/grid/${gridId}`)
      .then((response) => {
        setGridData(response.data);
      })
      .catch((err) => {
        if (err.response && err.response.status === 404) {
          setGridError(`Grid ${gridId} was not found.`);
        } else {
          setGridError("Unable to load grid activity.");
        }
      })
      .finally(() => {
        setGridLoading(false);
      });
  };

  const loadRisk = () => {
    setRiskLoading(true);
    setRiskError("");
    setRiskResult(null);

    axios
      .post(`${API_URL}/network/predict-risk`, {
        grid_id: Number(riskGrid),
        ...riskFeatures,
      })
      .then((response) => {
        setRiskResult(response.data);
      })
      .catch(() => {
        setRiskError("Unable to get risk prediction.");
      })
      .finally(() => {
        setRiskLoading(false);
      });
  };

  const getSeverity = (grid) => {
    const alert = alerts.find(
      (item) => Number(item.grid_id) === Number(grid)
    );

    if (!alert) return "NORMAL";

    const alertType = String(
      alert.alert_type || alert.severity || ""
    ).toUpperCase();

    if (
      alertType.includes("HIGH") ||
      alertType.includes("SPIKE")
    ) {
      return "HIGH";
    }

    return "ATTENTION";
  };

  const filteredHotspots = hotspots.filter((item) => {
    if (severity === "ALL") return true;
    return getSeverity(item.grid_id) === severity;
  });

  if (loading) {
    return <div className="app">Loading NOC dashboard...</div>;
  }

  return (
    <div className="app">
      <header>
        <h1>NOC Dashboard</h1>
        <p>Network Operations Overview</p>
      </header>

      <nav className="nav">
        <button onClick={() => setPage("overview")}>
          Overview
        </button>

        <button onClick={() => setPage("grid")}>
          Grid Activity
        </button>

        <button onClick={() => setPage("alerts")}>
          Hotspots & Alerts
        </button>

        <button onClick={() => setPage("risk")}>
          Predictive Risk
        </button>
      </nav>

      {error && <div className="error-banner">{error}</div>}

      {/* OVERVIEW */}
      {page === "overview" && summary && (
        <>
          <div className="status">
            Reporting timestamp: <strong>{summary.as_of}</strong>
          </div>

          <div className="cards">
            <div className="card">
              <h3>Total Activity</h3>
              <p>{summary.total_activity.toLocaleString()}</p>
            </div>

            <div className="card">
              <h3>Peak Hour</h3>
              <p>{summary.peak_hour}</p>
            </div>

            <div className="card">
              <h3>Active Grids</h3>
              <p>{summary.active_grids.toLocaleString()}</p>
            </div>

            <div className="card">
              <h3>Top Grid</h3>
              <p>{summary.top_grid}</p>
            </div>
          </div>
        </>
      )}

      {/* RE3 */}
      {page === "grid" && (
        <div className="page">
          <h2>Grid Explorer</h2>
          <p>Operational drill-down by grid.</p>

          <div className="search-box">
            <input
              type="number"
              min="1"
              max="10000"
              value={gridId}
              onChange={(e) => setGridId(e.target.value)}
              placeholder="Enter grid ID"
            />

            <button onClick={loadGrid}>Load Grid</button>
          </div>

          {gridLoading && <p>Loading grid activity...</p>}

          {gridError && (
            <div className="error-banner">{gridError}</div>
          )}

          {gridData && (
            <>
              <div className="status">
                Grid <strong>{gridData.grid_id}</strong> —{" "}
                {gridData.count} hourly records — as of{" "}
                <strong>{gridData.as_of}</strong>
              </div>

              <table>
                <thead>
                  <tr>
                    <th>Timestamp</th>
                    <th>Call Activity</th>
                    <th>SMS Activity</th>
                    <th>Internet</th>
                    <th>Total Activity</th>
                  </tr>
                </thead>

                <tbody>
                  {gridData.activity.map((row, index) => (
                    <tr key={index}>
                      <td>{row.timestamp}</td>
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
                        {Number(row.internet_activity || 0).toFixed(2)}
                      </td>
                      <td>
                        {Number(row.total_activity || 0).toFixed(2)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </div>
      )}

      {/* RE4 */}
      {page === "alerts" && (
        <div className="page">
          <h2>Hotspots & Alerts</h2>
          <p>Prioritized network attention areas.</p>

          <div className="filters">
            <label>
              Limit:{" "}
              <select
                value={hotspotLimit}
                onChange={(e) =>
                  setHotspotLimit(Number(e.target.value))
                }
              >
                <option value={5}>5</option>
                <option value={10}>10</option>
                <option value={20}>20</option>
              </select>
            </label>

            <label>
              Severity:{" "}
              <select
                value={severity}
                onChange={(e) => setSeverity(e.target.value)}
              >
                <option value="ALL">All</option>
                <option value="NORMAL">Normal</option>
                <option value="ATTENTION">Attention</option>
                <option value="HIGH">High</option>
              </select>
            </label>
          </div>

          <table>
            <thead>
              <tr>
                <th>Rank</th>
                <th>Grid</th>
                <th>Activity</th>
                <th>Status</th>
              </tr>
            </thead>

            <tbody>
              {filteredHotspots.map((item, index) => {
                const status = getSeverity(item.grid_id);

                return (
                  <tr key={item.grid_id}>
                    <td>{index + 1}</td>
                    <td>{item.grid_id}</td>
                    <td>
                      {Number(
                        item.activity || item.total_activity || 0
                      ).toFixed(2)}
                    </td>
                    <td>
                      <strong>{status}</strong>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          {filteredHotspots.length === 0 && (
            <p>No hotspot records match the selected filter.</p>
          )}
        </div>
      )}

      {/* RE5 */}
      {page === "risk" && (
        <div className="page">
          <h2>Predictive Risk</h2>
          <p>
            Model output is shown separately from explanatory narrative.
          </p>

          <div className="search-box">
            <input
              type="number"
              min="1"
              max="10000"
              value={riskGrid}
              onChange={(e) => setRiskGrid(e.target.value)}
              placeholder="Grid ID"
            />

            <button onClick={loadRisk}>
              Get Risk Prediction
            </button>
          </div>

          {riskLoading && <p>Running prediction...</p>}

          {riskError && (
            <div className="error-banner">{riskError}</div>
          )}

          {riskResult && (
            <div className="risk-card">
              <h3>Model Output</h3>

              <p>
                <strong>Grid:</strong> {riskResult.grid_id}
              </p>

              <p>
                <strong>Risk Score:</strong>{" "}
                {riskResult.risk_score}
              </p>

              <p>
                <strong>Risk Level:</strong>{" "}
                {riskResult.risk_level}
              </p>

              <p>
                <strong>Model Version:</strong>{" "}
                {riskResult.model_version}
              </p>

              <hr />

              <h3>Explanation</h3>
              <p>{riskResult.explanation_note}</p>

              <button disabled>
                Explain with AI — Coming Later
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default App;
