import { useEffect } from "react";
import type { DependencyList } from "react";
import { token } from "./identity";

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

// the credential, on every request that has one to send. it goes in a header
// and never in a url: a query string is in the browser's history, in every
// proxy's access log and in the referrer of anything this page links to
function headers(json: boolean): HeadersInit | undefined {
  const held = token();
  if (held === null) return json ? { "content-type": "application/json" } : undefined;
  const sent: Record<string, string> = { authorization: `Bearer ${held}` };
  if (json) sent["content-type"] = "application/json";
  return sent;
}

export async function get<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: headers(false) });
  if (!res.ok) throw new HttpError(res.status, path, await detail(res));
  return res.json() as Promise<T>;
}

async function send<T>(method: string, path: string, body: string | undefined): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: headers(body !== undefined),
    body,
  });
  if (!res.ok) throw new HttpError(res.status, path, await detail(res));
  return res.json() as Promise<T>;
}

export function post<T>(path: string, body?: unknown): Promise<T> {
  return send<T>("POST", path, JSON.stringify(body ?? {}));
}

export function put<T>(path: string, body?: unknown): Promise<T> {
  return send<T>("PUT", path, JSON.stringify(body ?? {}));
}

// no body at all: what a delete carries is its path
export function del<T>(path: string): Promise<T> {
  return send<T>("DELETE", path, undefined);
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
