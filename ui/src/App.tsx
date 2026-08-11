import { useCallback, useEffect, useState } from "react";
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
import SignIn from "./SignIn";
import { get } from "./api";
import type { Who } from "./identity";
import { OPEN, setToken, token } from "./identity";
import { RoleContext } from "./role";

export default function App() {
  const [who, setWho] = useState<Who | null>(null);
  const [refused, setRefused] = useState(false);

  // the one endpoint outside the guard, and the first thing this asks: whether
  // there is anything to present, and whether what is held is accepted
  const ask = useCallback(async () => {
    try {
      const answer = await get<Who>("/api/whoami");
      setWho(answer);
      setRefused(answer.auth && answer.identity === null && token() !== null);
    } catch {
      // a deployment that cannot say is a deployment that is not there; the
      // pages below say so themselves, one failed fetch at a time
      setWho({ auth: false, identity: null });
    }
  }, []);

  useEffect(() => {
    void ask();
  }, [ask]);

  if (who === null) return null;
  if (who.auth && who.identity === null) {
    return (
      <SignIn
        refused={refused}
        onToken={(typed) => {
          setToken(typed);
          void ask();
        }}
      />
    );
  }

  const identity = who.identity;
  return (
    <RoleContext.Provider value={identity?.role ?? OPEN}>
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
          {identity && (
            <span className="whoami muted">
              {identity.name} · {identity.role}
              {/* only where there is one to forget: an identity a proxy
                  asserted is not this tab's to drop */}
              {token() !== null && (
                <button
                  className="text-btn"
                  onClick={() => {
                    setToken(null);
                    void ask();
                  }}
                >
                  forget token
                </button>
              )}
            </span>
          )}
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
    </RoleContext.Provider>
  );
}
