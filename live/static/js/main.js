// Connects to the live server and passes each change to the part of the page that shows it.
//
// The server sends Server-Sent Events, one JSON object each: { kind: "trade" | "latest" | "gap", ...row }.
// Events are collected as they arrive and drawn once per animation frame, so a burst of trades
// costs one redraw, not one for each trade.

import { $ } from "./dom.js";
import { highlightSql } from "./sql.js";
import { GapTable, LatestTable } from "./tables.js";
import { Tape } from "./tape.js";

const tape = new Tape($("#tape"));
const latest = new LatestTable($("#latest"));
const gaps = new GapTable($("#gaps"));

let source = null;
let pending = [];
let tradesThisSecond = 0;

function connect(asset) {
  source?.close();
  tape.clear();
  latest.clear();
  pending = [];

  source = new EventSource(`/events?asset=${encodeURIComponent(asset)}`);
  source.onopen = () => setStatus("live");
  source.onerror = () => setStatus("reconnecting");
  source.onmessage = (message) => {
    const event = JSON.parse(message.data);
    if (event.kind === "trade" && !event.snapshot) tradesThisSecond += 1;
    pending.push(event);
  };
}

function setStatus(text) {
  const status = $("#status");
  status.textContent = text;
  status.className = `status ${text === "live" ? "status--live" : "status--off"}`;
}

function drawFrame(now) {
  const events = pending;
  pending = [];

  const snapshotTrades = [];
  const newTrades = [];
  for (const event of events) {
    if (event.kind === "trade") (event.snapshot ? snapshotTrades : newTrades).push(event);
    else if (event.kind === "latest") latest.upsert(event, !event.snapshot);
    else if (event.kind === "gap") gaps.apply(event);
  }
  if (snapshotTrades.length) tape.add(snapshotTrades, false);
  if (newTrades.length) tape.add(newTrades, true);
  gaps.draw(now);

  requestAnimationFrame(drawFrame);
}

function setUpControls() {
  $("#asset").addEventListener("change", (e) => connect(e.target.value));

  const minValue = $("#min-value");
  tape.minValue = Number(minValue.value);
  minValue.addEventListener("change", (e) => (tape.minValue = Number(e.target.value)));

  const pause = $("#pause");
  pause.addEventListener("click", () => {
    const paused = !tape.paused;
    tape.setPaused(paused);
    pause.setAttribute("aria-pressed", String(paused));
    pause.textContent = paused ? "resume" : "pause";
  });

  setInterval(() => {
    $("#rate").textContent = `${tradesThisSecond} trades/s`;
    tradesThisSecond = 0;
  }, 1000);

  // A hidden tab draws no frames, so events would pile up in memory. Stop the stream while the tab is
  // hidden, and connect again (with a fresh snapshot) when it is visible.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      source?.close();
      setStatus("paused (tab hidden)");
    } else {
      connect($("#asset").value);
    }
  });
}

async function showSql() {
  const views = await (await fetch("/sql")).json();
  for (const [name, sql] of Object.entries(views)) {
    $(`#sql-${name}`).innerHTML = highlightSql(sql);
  }
}

async function start() {
  setUpControls();
  showSql();

  const assets = await (await fetch("/assets")).json();
  const select = $("#asset");
  select.replaceChildren(...assets.map((asset) => new Option(asset, asset)));
  select.value = assets.includes("BTC") ? "BTC" : assets[0];
  connect(select.value);

  requestAnimationFrame(drawFrame);
}

start();
