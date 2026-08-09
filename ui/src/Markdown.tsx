import { parseMarkdown } from "./markdown";
import type { Block, Inline } from "./markdown";

// the parse tree rendered as react elements. every branch below is an element
// constructor over parsed data — no html string is built anywhere in this file
// or in markdown.ts — so a `<img onerror=...>` in the source is a text child
// react escapes, and the only href that can exist is one linkHref approved.
// the test asserts both, against the rendered output.

function Inlines({ nodes }: { nodes: Inline[] }) {
  return (
    <>
      {nodes.map((n, i) => (
        <InlineView key={i} node={n} />
      ))}
    </>
  );
}

function InlineView({ node }: { node: Inline }) {
  switch (node.kind) {
    case "text":
      return <>{node.text}</>;
    case "code":
      return <code className="mono md-code-span">{node.text}</code>;
    case "strong":
      return (
        <strong>
          <Inlines nodes={node.children} />
        </strong>
      );
    case "em":
      return (
        <em>
          <Inlines nodes={node.children} />
        </em>
      );
    case "link":
      return (
        <a href={node.href} target="_blank" rel="noreferrer">
          <Inlines nodes={node.children} />
        </a>
      );
  }
}

function BlockView({ block }: { block: Block }) {
  switch (block.kind) {
    case "heading": {
      // a metadata block sits inside a page that already has an h1 and an h2,
      // so `#` is an h3 and the rest shift with it
      const Tag = `h${Math.min(block.level + 2, 6)}` as "h3" | "h4" | "h5" | "h6";
      return (
        <Tag className="md-h">
          <Inlines nodes={block.children} />
        </Tag>
      );
    }
    case "paragraph":
      return (
        <p>
          <Inlines nodes={block.children} />
        </p>
      );
    case "code":
      return (
        <>
          {block.lang && <div className="muted md-lang">{block.lang}</div>}
          <pre className="mono md-code">{block.text}</pre>
        </>
      );
    case "list": {
      const items = block.items.map((item, i) => (
        <li key={i}>
          <Inlines nodes={item} />
        </li>
      ));
      return block.ordered ? (
        <ol className="md-list">{items}</ol>
      ) : (
        <ul className="md-list">{items}</ul>
      );
    }
    case "rule":
      return <hr className="md-rule" />;
  }
}

export default function Markdown({ source }: { source: string }) {
  return (
    <div className="md">
      {parseMarkdown(source).map((block, i) => (
        <BlockView key={i} block={block} />
      ))}
    </div>
  );
}
