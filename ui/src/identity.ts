// who this browser is to the deployment, and where the token it presents lives.
//
// **sessionStorage, and here is what that does and does not buy.** it is
// scoped to this tab and this origin, and the browser drops it when the tab
// closes — so a shared machine does not keep a control-plane credential in a
// profile forever, and a second deployment on another origin cannot read it.
//
// what it does not protect against, plainly: any script running on this page
// can read it. that is the whole of the risk, and it is not small — a
// cross-site scripting hole anywhere in this ui, or in anything a browser
// extension injects into it, hands over a token that can launch runs. an
// HttpOnly cookie is the thing that would be out of javascript's reach, and it
// needs a login endpoint, a session table and an expiry to invalidate, which is
// a user store, which hestan does not have and is not going to grow. the token
// is also visible in devtools and in this tab's own request headers, and it
// does not expire: the only revocation is changing the token and restarting the
// deployment.
//
// so: fine for an internal deployment behind a network you trust, where the
// alternative is no authentication at all. not a substitute for an identity
// provider in front of hestan — `Auth::custom` is how you compose one in, and
// then the browser holds that scheme's credential instead of this one.
const KEY = "hestan.token";

export type Role = "viewer" | "operator" | "admin";

export interface Identity {
  name: string;
  role: Role;
}

export interface Who {
  /// whether the deployment checks who is asking at all.
  auth: boolean;
  /// who it makes this browser, or null for nobody it recognizes.
  identity: Identity | null;
}

// storage throws in a browser with cookies-and-storage turned off, and a ui
// that cannot read a token is still a ui that can read an open deployment
export function token(): string | null {
  try {
    return sessionStorage.getItem(KEY);
  } catch {
    return null;
  }
}

export function setToken(value: string | null): void {
  try {
    if (value === null) sessionStorage.removeItem(KEY);
    else sessionStorage.setItem(KEY, value);
  } catch {
    // nowhere to keep it; the request this was typed for still carries it
  }
}

const RANK: Record<Role, number> = { viewer: 0, operator: 1, admin: 2 };

// the same comparison the server makes, so that what the ui offers and what
// the api allows cannot drift into two different answers
export function may(role: Role, needs: Role): boolean {
  return RANK[role] >= RANK[needs];
}

// what an unauthenticated deployment makes everyone: it is loopback, which is
// one process on one machine, and the ui has never asked who was driving it
export const OPEN: Role = "admin";
