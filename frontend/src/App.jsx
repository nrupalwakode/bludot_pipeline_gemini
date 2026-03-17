import { BrowserRouter as Router, Routes, Route, NavLink } from "react-router-dom";
import CitiesPage from "./pages/CitiesPage";
import NewCityPage from "./pages/NewCityPage";
import CityDetailPage from "./pages/CityDetailPage";
import ReviewPage from "./pages/ReviewPage";
import "./App.css";

export default function App() {
  return (
    <Router>
      <div className="app">
        <nav className="sidebar">
          <div className="sidebar-brand">
            <span className="brand-dot">●</span>
            <span className="brand-text">Bludot<br/>Pipeline</span>
          </div>
          <div className="nav-links">
            <NavLink to="/" end className={({ isActive }) => isActive ? "nav-link active" : "nav-link"}>
              <span className="nav-icon">⊞</span> Cities
            </NavLink>
            <NavLink to="/new" className={({ isActive }) => isActive ? "nav-link active" : "nav-link"}>
              <span className="nav-icon">＋</span> New City
            </NavLink>
          </div>
          <div className="sidebar-footer">v2.0</div>
        </nav>
        <main className="content">
          <Routes>
            <Route path="/" element={<CitiesPage />} />
            <Route path="/new" element={<NewCityPage />} />
            <Route path="/city/:cityId" element={<CityDetailPage />} />
            <Route path="/city/:cityId/review" element={<ReviewPage />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}
