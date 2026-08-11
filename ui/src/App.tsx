import { Link, NavLink, Route, Routes } from "react-router-dom";
import ActivityPage from "./ActivityPage";
import AssetPage from "./AssetPage";
import BackfillPage from "./BackfillPage";
import AssetsPage from "./AssetsPage";
import CommandPalette from "./CommandPalette";
import JobsPage from "./JobsPage";
import JobPage from "./JobPage";
import RunsPage from "./RunsPage";
import RunPage from "./RunPage";

export default function App() {
  return (
    <>
      <header>
        <div className="header-inner">
          <Link to="/" className="wordmark">
            hestan
          </Link>
          <nav>
            <NavLink to="/" end>
              Jobs
            </NavLink>
            <NavLink to="/assets">Assets</NavLink>
            <NavLink to="/runs">Runs</NavLink>
            <NavLink to="/activity">Activity</NavLink>
          </nav>
        </div>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<JobsPage />} />
          <Route path="/jobs/:name" element={<JobPage />} />
          <Route path="/assets" element={<AssetsPage />} />
          {/* a splat, not a param: an asset name carries the separator its
              group is named after, and `sales/orders` is two segments */}
          <Route path="/assets/*" element={<AssetPage />} />
          <Route path="/backfills/:id" element={<BackfillPage />} />
          <Route path="/runs" element={<RunsPage />} />
          <Route path="/runs/:id" element={<RunPage />} />
          <Route path="/activity" element={<ActivityPage />} />
        </Routes>
      </main>
      <CommandPalette />
    </>
  );
}
