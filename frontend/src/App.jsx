import { BrowserRouter, Routes, Route, NavLink, useLocation } from "react-router-dom";
import CitiesPage    from "./pages/CitiesPage";
import NewCityPage   from "./pages/NewCityPage";
import CityDetailPage from "./pages/CityDetailPage";
import ReviewPage    from "./pages/ReviewPage";
import ClusterReviewPage from "./pages/ClusterReviewPage";
import MatchesPage from "./pages/MatchesPage";
import DedupResultsPage from "./pages/DedupResultsPage";

function Layout({ children }) {
  return (
    <div style={{ display: "flex", minHeight: "100vh" }}>
      {/* Sidebar */}
      <nav style={{
        width: 200, flexShrink: 0,
        background: "var(--surface2)",
        borderRight: "1px solid var(--border)",
        padding: "20px 0",
        display: "flex", flexDirection: "column",
      }}>
        <div style={{ padding: "0 20px 20px", borderBottom: "1px solid var(--border)", marginBottom: 8 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{
              width: 28, height: 28, borderRadius: 8,
              background: "var(--accent)", display: "flex",
              alignItems: "center", justifyContent: "center",
              fontSize: 14, fontWeight: 700, color: "#fff",
            }}>B</div>
            <div>
              <div style={{ fontSize: 13, fontWeight: 700, color: "var(--text)", lineHeight: 1.2 }}>Bludot</div>
              <div style={{ fontSize: 11, color: "var(--text-muted)" }}>Pipeline</div>
            </div>
          </div>
        </div>

        <div style={{ padding: "8px 12px", flex: 1 }}>
          <SideNavLink to="/" label="Cities" icon="⊞" />
          <SideNavLink to="/new" label="New City" icon="＋" />
        </div>

        <div style={{ padding: "12px 20px", borderTop: "1px solid var(--border)" }}>
          <span style={{ fontSize: 11, color: "var(--text-muted)", fontFamily: "IBM Plex Mono, monospace" }}>
            v1.0
          </span>
        </div>
      </nav>

      {/* Main content */}
      <main style={{ flex: 1, padding: "28px 32px", overflowY: "auto", maxWidth: "calc(100vw - 200px)" }}>
        {children}
      </main>
    </div>
  );
}

function SideNavLink({ to, label, icon }) {
  return (
    <NavLink
      to={to}
      end={to === "/"}
      style={({ isActive }) => ({
        display: "flex", alignItems: "center", gap: 8,
        padding: "7px 10px", borderRadius: 6,
        fontSize: 13, fontWeight: isActive ? 600 : 400,
        color: isActive ? "var(--accent)" : "var(--text-muted)",
        background: isActive ? "rgba(79,142,247,0.1)" : "transparent",
        textDecoration: "none", marginBottom: 2,
        transition: "all 0.15s",
      })}
    >
      <span style={{ fontSize: 14 }}>{icon}</span>
      {label}
    </NavLink>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/"                           element={<CitiesPage />} />
          <Route path="/new"                        element={<NewCityPage />} />
          <Route path="/city/:cityId"               element={<CityDetailPage />} />
          <Route path="/city/:cityId/review"        element={<ReviewPage />} />
          <Route path="/city/:cityId/cluster-review" element={<ClusterReviewPage />} />
          <Route path="/city/:cityId/matches" element={<MatchesPage />} />
          <Route path="/city/:cityId/dedup-results" element={<DedupResultsPage />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}
