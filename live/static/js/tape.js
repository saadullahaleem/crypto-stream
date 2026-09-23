// The trade tape: new orders enter at the top, and older rows move down. No row is sorted again.

import { emptyRow, replay, setCells } from "./dom.js";
import { formatNumber, formatPrice, formatTime, formatValue } from "./format.js";

const MAX_ROWS = 30;
const MAX_HELD = 5000; // trades kept while paused
const COLUMNS = 9;
const NUMERIC_COLUMNS = [4, 5, 6, 7, 8];

/**
 * A taker order that fills several resting orders arrives as several trades with the same exchange,
 * product, side and time. Join them into one order: size and value added up, and the number of fills.
 */
export function groupIntoOrders(trades) {
  const orders = [];
  for (const trade of trades) {
    const key = [trade.exchange, trade.product_id, trade.side, trade.ts].join("|");
    const value = trade.price * trade.last_size;
    const last = orders.at(-1);
    if (last && last.key === key) {
      last.size += trade.last_size;
      last.value += value;
      last.fills += 1;
      last.price = trade.price;
    } else {
      orders.push({ ...trade, key, size: trade.last_size, value, fills: 1 });
    }
  }
  return orders;
}

export class Tape {
  constructor(body) {
    this.body = body;
    this.minValue = 0;
    this.paused = false;
    this.held = [];
  }

  clear() {
    this.body.replaceChildren();
    this.held = [];
  }

  /** Add trades in the order they arrived. `animate` is false for the snapshot of a new page. */
  add(trades, animate) {
    if (this.paused) {
      this.held = this.held.concat(trades).slice(-MAX_HELD);
      return;
    }
    for (const order of groupIntoOrders(trades)) this.#show(order, animate);
    while (this.body.children.length > MAX_ROWS) this.body.lastChild.remove();
  }

  setPaused(paused) {
    this.paused = paused;
    if (!paused) {
      const held = this.held;
      this.held = [];
      this.add(held, true);
    }
  }

  #show(order, animate) {
    const top = this.body.firstChild;

    // More fills of the order on top, from a later message: add them to that row.
    if (top && top.dataset.key === order.key) {
      top.order.size += order.size;
      top.order.value += order.value;
      top.order.fills += order.fills;
      this.#fill(top, top.order);
      if (animate) replay(top, "row--new");
      return;
    }

    if (order.value < this.minValue) return;

    const row = emptyRow(COLUMNS, NUMERIC_COLUMNS);
    row.dataset.key = order.key;
    row.order = order;
    this.#fill(row, order);
    if (animate) row.classList.add("row--new");
    this.body.prepend(row);
  }

  #fill(row, order) {
    setCells(row, [
      [formatTime(order.ts), "dim"],
      order.exchange,
      order.product_id,
      [order.side, order.side],
      formatPrice(order.price),
      formatNumber(order.size, 6),
      order.fills > 1 ? `×${order.fills}` : "",
      formatValue(order.value),
      formatPrice(order.spread),
    ]);
  }
}
