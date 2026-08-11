import { createContext, useContext } from "react";
import type { Role } from "./identity";
import { OPEN, may } from "./identity";

// the role this browser is driving with, for the controls that ask.
//
// a control a role may not use is **not rendered**. a button that is there and
// answers 403 teaches people that the ui lies about what they can do, and the
// ones who learn that stop reading the rest of it.
export const RoleContext = createContext<Role>(OPEN);

export function useRole(): Role {
  return useContext(RoleContext);
}

/// whether this browser may do something that needs `needs`.
export function useMay(needs: Role): boolean {
  return may(useContext(RoleContext), needs);
}
