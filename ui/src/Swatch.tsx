import type { CSSProperties } from "react";
import { shownAndMore } from "./colour";
import type { Stripe } from "./colour";

// six steps of the page's own ink, picked by a label's angle. the palette is
// black, white and grey: what a thing is doing is carried by shape and what it
// came from is carried by how dark its mark is, and neither needs a colour.
//
// six rather than a continuum because two shades a few percent apart are one
// shade, and these are read across a page rather than side by side.
const SHADES = [0.16, 0.32, 0.48, 0.64, 0.82, 1];

// the css variable a mark's shade arrives in, as a fraction of full ink. each
// site decides how much of it to spend: a 4px stripe wants nearly all of it, a
// band behind a name wants a fifth, and `styles.css` is where that is said
export const at = (hue: number) => ({ "--shade": String(SHADES[hue % SHADES.length]) }) as CSSProperties;

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
