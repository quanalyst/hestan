// what a hue is allowed to mean here, and what a swatch is made of.
//
// the palette everywhere else in this ui is grey, and shape carries state
// (`StatusGlyph.tsx`). that is what leaves colour free, and it stays free only
// while it means one thing: **a hue is where an asset belongs or where it came
// from, and never how it is doing.** the moment a hue means "failed" this is
// over.
//
// two consequences the code has to hold up rather than describe:
//
// - a colour is never the only carrier. every hue drawn has its own name
//   written beside it, and the legend names every hue in the view. somebody
//   who cannot tell two hues apart loses speed and loses nothing else.
// - one meaning at a time. the view colours by group or by origin, never both,
//   because two hue meanings at once is noise.
import type { AssetSummary } from "./types";

// group, origin, or no colour at all. off exists because somebody will want it
// and because it is the proof that nothing here is load-bearing
export type HueMode = "group" | "origin" | "off";

export const HUE_MODES = ["group", "origin", "off"] as const;

// colour is on by default, which is a change an existing deployment sees
export const DEFAULT_HUE_MODE: HueMode = "group";

export function hueMode(raw: string | null): HueMode {
  return HUE_MODES.includes(raw as HueMode) ? (raw as HueMode) : DEFAULT_HUE_MODE;
}

// one stripe: the angle it is drawn at and the name it stands for. the name is
// not decoration, it is the other half of the channel
export interface Stripe {
  label: string;
  hue: number;
}

// how many stripes a swatch draws before the rest become a count. three is
// where four 4px bars stop reading as bars, and the ones past it are still in
// the legend and on the asset's own page
export const MAX_STRIPES = 3;

// what one asset is coloured by, in name order.
//
// sorted here rather than trusted from the api: the order is what keeps a
// split swatch from jittering between polls, so it is this end's claim too.
export function stripesFor(a: AssetSummary, mode: HueMode): Stripe[] {
  const all =
    mode === "off"
      ? []
      : mode === "group"
        ? a.group === null || a.group_hue === null
          ? []
          : [{ label: a.group, hue: a.group_hue }]
        : a.provenance.map((o) => ({ label: o.name, hue: o.hue }));
  return [...all].sort((x, y) => x.label.localeCompare(y.label));
}

// the stripes drawn and the count standing for the rest. the swatch and the
// words beside it both go through this, so they can never disagree about how
// many were left out
export function shownAndMore(all: Stripe[]): { shown: Stripe[]; more: number } {
  return all.length <= MAX_STRIPES
    ? { shown: all, more: 0 }
    : { shown: all.slice(0, MAX_STRIPES), more: all.length - MAX_STRIPES };
}

// the group heading a row sits under, in words. it does not take a mode: the
// words are there whether or not anything is coloured, which is what makes
// turning colour off cost nothing
export function groupWords(a: AssetSummary): string {
  return a.group ?? "no group";
}

// and what the row says it descends from. empty is an answer, so it is a
// sentence rather than a blank
export function originWords(a: AssetSummary): string[] {
  return a.provenance.length === 0 ? ["no source"] : a.provenance.map((o) => o.name);
}

// every label a hue is standing for in this view, once each, by name.
//
// without this a colour is decoration: it is the only place a reader can turn
// a hue back into a name, and it is on the same screen as the graph.
export function legendFor(assets: AssetSummary[], mode: HueMode): Stripe[] {
  const held = new Map<string, number>();
  for (const a of assets) {
    for (const stripe of stripesFor(a, mode)) held.set(stripe.label, stripe.hue);
  }
  return [...held]
    .map(([label, hue]) => ({ label, hue }))
    .sort((a, b) => a.label.localeCompare(b.label));
}
