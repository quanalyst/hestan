import { useEffect } from "react";
import { Link } from "react-router-dom";
import AssetDetail from "./AssetDetail";
import type { AssetSummary } from "./types";
import { assetPath } from "./util";

// the drawer over the assets table: the same body the asset's page draws, in
// the width a quick look gets. the page is where a link goes, so the title is
// one
export default function AssetPanel({
  asset,
  onClose,
}: {
  asset: AssetSummary;
  onClose: () => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <aside className="op-panel">
      <div className="op-panel-head">
        <Link className="mono op-title" to={assetPath(asset.name)}>
          {asset.name}
        </Link>
        <button className="text-btn" onClick={onClose} aria-label="close">
          ×
        </button>
      </div>
      <AssetDetail asset={asset} />
    </aside>
  );
}
