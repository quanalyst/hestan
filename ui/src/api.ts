import { useEffect } from "react";
import type { DependencyList } from "react";

export class HttpError extends Error {
  status: number;
  constructor(status: number, path: string, detail?: string | null) {
    super(detail || `${status} ${path}`);
    this.status = status;
  }
}

// the api answers {"error": "..."} on every failure; fall back to the status line
async function detail(res: Response): Promise<string | null> {
  try {
    const data: unknown = await res.json();
    if (data && typeof data === "object" && typeof (data as { error?: unknown }).error === "string")
      return (data as { error: string }).error;
  } catch {
    // non-json error body; the status line is all we have
  }
  return null;
}

export async function get<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new HttpError(res.status, path, await detail(res));
  return res.json() as Promise<T>;
}

export async function post<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });
  if (!res.ok) throw new HttpError(res.status, path, await detail(res));
  return res.json() as Promise<T>;
}

// immediate first call, then every `ms`; null calls once and does not repeat.
// `deps` is the caller's contract: it must name everything `fn` reads, which is
// why exhaustive-deps (which cannot see through it) is off here
export function usePoll(fn: () => void, ms: number | null, deps: DependencyList) {
  useEffect(() => {
    fn();
    if (ms === null) return;
    const id = setInterval(fn, ms);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ms, ...deps]);
}
