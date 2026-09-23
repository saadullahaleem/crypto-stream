// Small DOM helpers.

export const $ = (selector) => document.querySelector(selector);

/** A table row with `count` empty cells. Cells whose index is in `numeric` are right-aligned. */
export function emptyRow(count, numeric = []) {
  const row = document.createElement("tr");
  for (let i = 0; i < count; i++) {
    const cell = document.createElement("td");
    if (numeric.includes(i)) cell.className = "num";
    row.append(cell);
  }
  return row;
}

/** Set the text (and optionally the class) of the cells of a row, in order. */
export function setCells(row, values) {
  values.forEach((value, i) => {
    const cell = row.children[i];
    if (Array.isArray(value)) {
      [cell.textContent, cell.className] = value;
    } else {
      cell.textContent = value;
    }
  });
}

/** Play a CSS animation class again, even if the element already has it. */
export function replay(element, className, others = []) {
  element.classList.remove(className, ...others);
  void element.offsetWidth; // forces a reflow, so the browser starts the animation again
  element.classList.add(className);
}

/** Flash a row green (value went up) or red (value went down). */
export function flash(row, before, after) {
  if (before == null || before === after) return;
  replay(row, after > before ? "row--up" : "row--down", ["row--up", "row--down"]);
}
