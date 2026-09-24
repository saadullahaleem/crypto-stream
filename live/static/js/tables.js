// The tables of the RisingWave views. Rows change in place and flash when their value changes.

import { emptyRow, flash, setCells } from "./dom.js";
import { formatNumber, formatPrice, formatTime } from "./format.js";

/** `latest`: one row per exchange and product of the selected asset. */
export class LatestTable {
  constructor(body) {
    this.body = body;
    this.rows = new Map(); // "exchange product" -> { row, price }
  }

  clear() {
    this.body.replaceChildren();
    this.rows.clear();
  }

  upsert(change, animate) {
    const key = `${change.exchange} ${change.product_id}`;
    let entry = this.rows.get(key);
    if (!entry) {
      entry = { row: emptyRow(6, [2, 3, 4]), price: null };
      this.rows.set(key, entry);
      this.#sortRows();
    }

    setCells(entry.row, [
      change.exchange,
      change.product_id,
      formatPrice(change.price),
      formatPrice(change.spread),
      formatNumber(change.volume_24h, 1),
      [formatTime(change.ts), "dim"],
    ]);
    if (animate) flash(entry.row, entry.price, change.price);
    entry.price = change.price;
  }

  #sortRows() {
    const sorted = [...this.rows.entries()].sort(([a], [b]) => a.localeCompare(b));
    this.body.replaceChildren(...sorted.map(([, entry]) => entry.row));
  }
}

/** `price_gap`: the largest gaps between exchanges. Drawn at most once a second, because it changes often. */
export class GapTable {
  static MAX_ROWS = 15;
  static REDRAW_MS = 1000;

  constructor(body) {
    this.body = body;
    this.gaps = new Map(); // asset -> gap row
    this.shown = new Map(); // asset -> gap_bps at the last draw
    this.changed = false;
    this.lastDraw = 0;
  }

  clear() {
    this.body.replaceChildren();
    this.gaps.clear();
    this.shown.clear();
    this.changed = false;
  }

  apply(change) {
    if (change.op === "Delete") this.gaps.delete(change.asset);
    else this.gaps.set(change.asset, change);
    this.changed = true;
  }

  /** Draw if something changed and the last draw is old enough. `now` is from requestAnimationFrame. */
  draw(now) {
    if (!this.changed || now - this.lastDraw < GapTable.REDRAW_MS) return;

    const top = [...this.gaps.values()].sort((a, b) => b.gap_bps - a.gap_bps).slice(0, GapTable.MAX_ROWS);
    const rows = top.map((gap) => {
      const row = emptyRow(5, [1, 4]);
      setCells(row, [
        gap.asset,
        gap.exchanges,
        [`${gap.low_exchange} ${formatPrice(gap.low)}`, "dim"],
        [`${gap.high_exchange} ${formatPrice(gap.high)}`, "dim"],
        gap.gap_bps.toFixed(1),
      ]);
      flash(row, this.shown.get(gap.asset), gap.gap_bps);
      return row;
    });

    this.shown = new Map(top.map((gap) => [gap.asset, gap.gap_bps]));
    this.body.replaceChildren(...rows);
    this.changed = false;
    this.lastDraw = now;
  }
}
