// the markdown subset, checked construct by construct, and then checked the
// way that actually matters: rendered through react-dom to the exact string a
// browser would receive, so "no markup and no href gets through" is asserted
// against the output rather than against an intention.
//
// run with `npm test` (vite bundles this for node, node runs it).
import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import Markdown from "../src/Markdown";
import { linkHref, parseInline, parseMarkdown } from "../src/markdown";
import type { Inline } from "../src/markdown";

const html = (source: string) => renderToStaticMarkup(<Markdown source={source} />);

// the text a tree carries, whatever it is wrapped in
function textOf(nodes: Inline[]): string {
  return nodes
    .map((n) =>
      n.kind === "text" || n.kind === "code" ? n.text : textOf(n.children),
    )
    .join("");
}

test("headings shift down to fit the page and keep their inline content", () => {
  assert.deepEqual(parseMarkdown("# one"), [
    { kind: "heading", level: 1, children: [{ kind: "text", text: "one" }] },
  ]);
  assert.equal(parseMarkdown("###### six")[0].kind, "heading");
  assert.match(html("# one"), /<h3 class="md-h">one<\/h3>/);
  assert.match(html("#### four"), /<h6 class="md-h">four<\/h6>/);
  // no space after the hashes is not a heading
  assert.equal(parseMarkdown("#nope")[0].kind, "paragraph");
});

test("paragraphs join their soft-wrapped lines and split on a blank one", () => {
  const blocks = parseMarkdown("one\ntwo\n\nthree");
  assert.equal(blocks.length, 2);
  assert.equal(textOf((blocks[0] as { children: Inline[] }).children), "one two");
  assert.equal(textOf((blocks[1] as { children: Inline[] }).children), "three");
});

test("bold, italic and code spans", () => {
  assert.deepEqual(parseInline("a **b** c"), [
    { kind: "text", text: "a " },
    { kind: "strong", children: [{ kind: "text", text: "b" }] },
    { kind: "text", text: " c" },
  ]);
  assert.deepEqual(parseInline("*i*"), [
    { kind: "em", children: [{ kind: "text", text: "i" }] },
  ]);
  assert.deepEqual(parseInline("`x + 1`"), [{ kind: "code", text: "x + 1" }]);
  assert.match(html("**b** and *i* and `c`"), /<strong>b<\/strong>/);
  assert.match(html("**b** and *i* and `c`"), /<em>i<\/em>/);
  assert.match(html("**b** and *i* and `c`"), /<code class="mono md-code-span">c<\/code>/);
  // a marker inside a code span is text, not a marker
  assert.deepEqual(parseInline("`**not bold**`"), [
    { kind: "code", text: "**not bold**" },
  ]);
});

test("fenced code blocks keep their source and name their language", () => {
  assert.deepEqual(parseMarkdown("```sql\nselect 1\nselect 2\n```"), [
    { kind: "code", lang: "sql", text: "select 1\nselect 2" },
  ]);
  assert.deepEqual(parseMarkdown("```\nplain\n```"), [
    { kind: "code", lang: null, text: "plain" },
  ]);
  // a fence nobody closed runs to the end rather than eating the document
  assert.deepEqual(parseMarkdown("```\nunclosed"), [
    { kind: "code", lang: null, text: "unclosed" },
  ]);
  // and nothing inside one is a construct
  assert.match(html("```\n**x** [a](https://e.test)\n```"), /\*\*x\*\* \[a\]\(https:\/\/e\.test\)/);
});

test("unordered and ordered lists, one run of markers each", () => {
  const [ul, ol] = parseMarkdown("- one\n- two\n\n1. first\n2) second");
  assert.deepEqual(ul, {
    kind: "list",
    ordered: false,
    items: [[{ kind: "text", text: "one" }], [{ kind: "text", text: "two" }]],
  });
  assert.equal((ol as { kind: string; ordered: boolean }).ordered, true);
  assert.match(html("- one\n- two"), /<ul class="md-list"><li>one<\/li><li>two<\/li><\/ul>/);
  assert.match(html("1. one"), /<ol class="md-list"><li>one<\/li><\/ol>/);
  // a bullet run and a numbered run are two lists, not one
  assert.equal(parseMarkdown("- a\n1. b").length, 2);
});

test("horizontal rules, and the ones that are not", () => {
  assert.deepEqual(parseMarkdown("---"), [{ kind: "rule" }]);
  assert.deepEqual(parseMarkdown("***"), [{ kind: "rule" }]);
  assert.deepEqual(parseMarkdown("___"), [{ kind: "rule" }]);
  assert.match(html("a\n\n---\n\nb"), /<hr class="md-rule"\/>/);
  // two dashes is a paragraph, and a bold run is not three asterisks
  assert.equal(parseMarkdown("--")[0].kind, "paragraph");
  assert.equal(parseMarkdown("**bold**")[0].kind, "paragraph");
});

test("links, nested inside the other constructs", () => {
  assert.deepEqual(parseInline("[hestan](https://example.test/a)"), [
    {
      kind: "link",
      href: "https://example.test/a",
      children: [{ kind: "text", text: "hestan" }],
    },
  ]);
  const rendered = html("- **bold with a [link](https://example.test) and `code`**");
  assert.match(
    rendered,
    /<li><strong>bold with a <a href="https:\/\/example\.test" target="_blank" rel="noreferrer">link<\/a> and <code class="mono md-code-span">code<\/code><\/strong><\/li>/,
  );
  // every external link opens in a new tab and leaks no referrer
  assert.match(rendered, /rel="noreferrer"/);
});

test("anything unrecognised is the text it was written as", () => {
  for (const source of ["**unclosed", "[half](", "~~strike~~", "H~2~O", "> quote"]) {
    const rendered = html(source);
    for (const char of source.replace(/[<>&|]/g, "")) {
      assert.ok(rendered.includes(char), `${source} lost ${char}`);
    }
  }
  // a table, a blockquote and a setext underline are outside the subset, so
  // they are paragraphs rather than anything else
  assert.equal(parseMarkdown("> quote")[0].kind, "paragraph");
  assert.equal(parseMarkdown("| a |\n| - |")[0].kind, "paragraph");
});

test("html in the source never reaches the dom as markup", () => {
  const attack = '<img src=x onerror="alert(1)"> and <script>alert(2)</script>';
  const nodes = parseInline(attack);
  assert.ok(
    nodes.every((n) => n.kind === "text"),
    "markup parsed as anything but text",
  );

  // strip the elements the renderer itself produced, and what is left is the
  // attack as inert text: not one angle bracket of it survived as markup
  const rendered = html(attack);
  const text = rendered.replace(/<\/?(?:div|p)(?: class="md")?>/g, "");
  assert.ok(!text.includes("<"), text);
  assert.equal(
    text,
    "&lt;img src=x onerror=&quot;alert(1)&quot;&gt; and &lt;script&gt;alert(2)&lt;/script&gt;",
  );
});

test("a javascript: link is not a link", () => {
  assert.equal(linkHref("javascript:alert(1)"), null);
  assert.deepEqual(parseInline("[x](javascript:alert(1))"), [
    { kind: "text", text: "[x](javascript:alert(1))" },
  ]);

  for (const target of [
    "javascript:alert(1)",
    "JavaScript:alert(1)",
    "data:text/html;base64,PHNjcmlwdD4=",
    "//evil.test",
    "/runs/1",
    "vbscript:msgbox",
    "",
  ]) {
    assert.equal(linkHref(target), null, `${target} was allowed as an href`);
    const rendered = html(`[x](${target})`);
    assert.ok(!rendered.includes("<a "), `${target} became a link: ${rendered}`);
    assert.ok(!rendered.includes("href="), `${target} became an href: ${rendered}`);
  }

  // an http url still works, which is the point of refusing the rest
  assert.equal(linkHref("https://example.test/x"), "https://example.test/x");
  assert.equal(linkHref("http://example.test/x"), "http://example.test/x");
});

test("no react element in the ui is built from an html string", async () => {
  const { readdirSync, readFileSync } = await import("node:fs");
  const dir = new URL("../src/", import.meta.url);
  // the whole subset is safe because nothing in the ui ever hands react an
  // html string, asserted over the source, since one line anywhere would
  // undo it
  const offenders = readdirSync(dir).filter((file) =>
    readFileSync(new URL(file, dir), "utf8").includes("dangerouslySetInnerHTML"),
  );
  assert.deepEqual(offenders, [], "a react element was built from an html string");
});
