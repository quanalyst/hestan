import { createContext, useContext, useState } from "react";
import type { ReactNode } from "react";
import { get, usePoll } from "./api";
import { displayName } from "./presentation";
import type { AssetSummary, JobSummary } from "./types";

type Kind = "job" | "asset";
const Names = createContext<(kind: Kind, name: string) => string>((_, name) => name);
// Historical rows retain identifiers; render their current declaration's name.
// Missing registrations remain readable by their original identifier.
export function useRegistryName() { return useContext(Names); }
export default function RegistryNames({ children }: { children: ReactNode }) {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [assets, setAssets] = useState<AssetSummary[]>([]);
  usePoll(() => {
    get<{ jobs: JobSummary[] }>("/api/jobs").then((r) => setJobs(r.jobs)).catch(() => {});
    get<{ assets: AssetSummary[] }>("/api/assets").then((r) => setAssets(r.assets)).catch(() => {});
  }, 10000, []);
  const name = (kind: Kind, id: string) => {
    const item = (kind === "job" ? jobs : assets).find((i) => i.name === id);
    return item ? displayName(item) : id;
  };
  return <Names.Provider value={name}>{children}</Names.Provider>;
}
export function RegisteredName({ kind, name }: { kind: Kind; name: string }) {
  const resolve = useRegistryName();
  return <span title={name}>{resolve(kind, name)}</span>;
}
