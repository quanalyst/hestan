import type { CSSProperties } from "react";
import { shownAndMore } from "./colour";
import type { Stripe } from "./colour";

// the css variable a stripe's angle arrives in; the shade is `styles.css`'s,
// per theme, because this end knows the ground the stripe is drawn on
export const at = (hue: number) => ({ "--h": String(hue) }) as CSSProperties;

// one asset's colour: **one stripe per label, side by side, never blended.**
// averaging two hues produces a third hue, and a third hue stands for a source
// nobody has. past `MAX_STRIPES` the rest become a count, and they are still
// named in the legend.
//
// `aria-hidden`, and deliberately: the names this stands for are written
// beside it, so reading the swatch out loud would say everything twice.
export default function Swatch({ stripes }: { stripes: Stripe[] }) {
  const { shown, more } = shownAndMore(stripes);
  if (shown.length === 0) return null;
  return (
    <span className="swatch" aria-hidden="true" title={stripes.map((s) => s.label).join(", ")}>
      {shown.map((s) => (
        <span key={s.label} className="swatch-stripe" style={at(s.hue)} />
      ))}
      {more > 0 && <span className="swatch-more">+{more}</span>}
    </span>
  );
}
