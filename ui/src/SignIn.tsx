import { useState } from "react";

// the whole of the sign-in: one field, and what holding a token in a browser
// costs. `identity.ts` is where the choice of sessionStorage is argued;
// this is the short version, said where somebody is about to make it.
export default function SignIn({
  refused,
  onToken,
}: {
  refused: boolean;
  onToken: (token: string) => void;
}) {
  const [typed, setTyped] = useState("");
  return (
    <div className="signin">
      <h1>hestan</h1>
      <p>this deployment checks who is asking.</p>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (typed.trim()) onToken(typed.trim());
        }}
      >
        <input
          className="filter-input"
          type="password"
          value={typed}
          autoFocus
          placeholder="token"
          aria-label="token"
          onChange={(e) => setTyped(e.target.value)}
        />
        <button type="submit" disabled={!typed.trim()}>
          continue
        </button>
      </form>
      {refused && <p className="signin-refused">that token was refused.</p>}
      <p className="muted">
        the token is kept in this tab only, and closing the tab forgets it. anything that can run
        javascript on this page can read it while it is here.
      </p>
    </div>
  );
}
