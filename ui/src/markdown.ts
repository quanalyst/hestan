// a deliberate markdown subset, parsed to a tree of nodes and never to html.
// the subset is documented exhaustively in docs/metadata.md; anything outside
// it is left as the literal text it was written as.
//
// this file produces data. MarkdownView.tsx turns the data into react
// elements, which is what makes injection impossible rather than merely
// escaped: no node type here can carry markup, so there is never an html
// string for the renderer to hand to react in the first place.

export type Inline =
  | { kind: "text"; text: string }
  | { kind: "code"; text: string }
  | { kind: "strong"; children: Inline[] }
  | { kind: "em"; children: Inline[] }
  // href is always an http(s) url: see linkHref, which is the only way one is
  // ever made
  | { kind: "link"; href: string; children: Inline[] };

export type Block =
  // 1..6 as written; the renderer shifts them down to fit the page
  | { kind: "heading"; level: number; children: Inline[] }
  | { kind: "paragraph"; children: Inline[] }
  | { kind: "code"; lang: string | null; text: string }
  | { kind: "list"; ordered: boolean; items: Inline[][] }
  | { kind: "rule" };

const FENCE = /^ {0,3}```(.*)$/;
const HEADING = /^ {0,3}(#{1,6})\s+(.*)$/;
const RULE = /^ {0,3}(?:-{3,}|\*{3,}|_{3,})\s*$/;
const BULLET = /^ {0,3}[-*+]\s+(.*)$/;
const ORDERED = /^ {0,3}\d+[.)]\s+(.*)$/;

const CODE_SPAN = /^`([^`]+)`/;
const STRONG = /^\*\*([\s\S]+?)\*\*/;
const EM = /^\*([^*]+)\*/;
const LINK = /^\[([^\]]*)\]\(([^)\s]*)\)/;

// the one place a url becomes an href. http and https only, so a
// `javascript:` target (or a `data:` one, or a protocol-relative `//host`)
// is not a link at all: the construct is left as the text it was written as,
// which shows what it pointed at rather than silently dropping it.
export function linkHref(target: string): string | null {
  const href = target.trim();
  return /^https?:\/\/\S/i.test(href) ? href : null;
}

export function parseInline(src: string): Inline[] {
  const out: Inline[] = [];
  let literal = "";
  const flush = () => {
    if (literal !== "") out.push({ kind: "text", text: literal });
    literal = "";
  };

  let i = 0;
  while (i < src.length) {
    const rest = src.slice(i);
    // code first, so a backtick span holds no markup of its own
    const code = CODE_SPAN.exec(rest);
    if (code) {
      flush();
      out.push({ kind: "code", text: code[1] });
      i += code[0].length;
      continue;
    }
    const strong = STRONG.exec(rest);
    if (strong) {
      flush();
      out.push({ kind: "strong", children: parseInline(strong[1]) });
      i += strong[0].length;
      continue;
    }
    const em = EM.exec(rest);
    if (em) {
      flush();
      out.push({ kind: "em", children: parseInline(em[1]) });
      i += em[0].length;
      continue;
    }
    const link = LINK.exec(rest);
    if (link) {
      const href = linkHref(link[2]);
      if (href === null) {
        // not a link hestan will make, so not a link: the source stands
        literal += link[0];
      } else {
        flush();
        out.push({ kind: "link", href, children: parseInline(link[1]) });
      }
      i += link[0].length;
      continue;
    }
    // an opener that never closes, an unknown construct, a stray `<`: all
    // the same thing: the character it was written as
    literal += src[i];
    i += 1;
  }
  flush();
  return out;
}

// what a list line is, if it is one
function listItem(line: string): { ordered: boolean; text: string } | null {
  const bullet = BULLET.exec(line);
  if (bullet) return { ordered: false, text: bullet[1] };
  const ordered = ORDERED.exec(line);
  return ordered ? { ordered: true, text: ordered[1] } : null;
}

// a line that ends the paragraph above it by starting something else
function opensABlock(line: string): boolean {
  return (
    FENCE.test(line) || HEADING.test(line) || RULE.test(line) || listItem(line) !== null
  );
}

export function parseMarkdown(src: string): Block[] {
  const lines = src.replace(/\r\n?/g, "\n").split("\n");
  const blocks: Block[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    if (line.trim() === "") {
      i += 1;
      continue;
    }

    const fence = FENCE.exec(line);
    if (fence) {
      const lang = fence[1].trim();
      const body: string[] = [];
      i += 1;
      while (i < lines.length && !FENCE.test(lines[i])) body.push(lines[i++]);
      // a fence nobody closed runs to the end rather than swallowing the
      // document into a paragraph
      if (i < lines.length) i += 1;
      blocks.push({ kind: "code", lang: lang === "" ? null : lang, text: body.join("\n") });
      continue;
    }

    // before the bullet rule, so `---` is a rule rather than an empty item
    if (RULE.test(line)) {
      blocks.push({ kind: "rule" });
      i += 1;
      continue;
    }

    const heading = HEADING.exec(line);
    if (heading) {
      blocks.push({
        kind: "heading",
        level: heading[1].length,
        children: parseInline(heading[2].replace(/\s+#*\s*$/, "")),
      });
      i += 1;
      continue;
    }

    const first = listItem(line);
    if (first) {
      const items: Inline[][] = [];
      // one run of same-kind items is one list; switching marker starts
      // another, and there is no nesting in the subset
      let item: { ordered: boolean; text: string } | null = first;
      while (item !== null && item.ordered === first.ordered) {
        items.push(parseInline(item.text));
        i += 1;
        item = i < lines.length ? listItem(lines[i]) : null;
      }
      blocks.push({ kind: "list", ordered: first.ordered, items });
      continue;
    }

    const text: string[] = [];
    while (i < lines.length && lines[i].trim() !== "" && !opensABlock(lines[i])) {
      text.push(lines[i].trim());
      i += 1;
    }
    blocks.push({ kind: "paragraph", children: parseInline(text.join(" ")) });
  }

  return blocks;
}
