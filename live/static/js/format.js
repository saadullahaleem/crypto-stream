// Number, price and time formats for the tables.

const locale = "en-US";

/** More decimals for small prices: 84,500.00 / 2.6612 / 0.000011 */
export function formatPrice(price) {
  if (price == null) return "";
  const size = Math.abs(price);
  const decimals = size >= 1000 ? 2 : size >= 1 ? 4 : size >= 0.01 ? 6 : 8;
  return price.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: decimals });
}

export function formatNumber(value, maxDecimals = 4) {
  if (value == null) return "";
  return value.toLocaleString(locale, { maximumFractionDigits: maxDecimals });
}

export function formatValue(dollars) {
  return "$" + dollars.toLocaleString(locale, { maximumFractionDigits: 0 });
}

/** Local time with milliseconds: 18:26:41.073 */
export function formatTime(iso) {
  const time = new Date(iso);
  const clock = time.toLocaleTimeString("en-GB", { hour12: false });
  const millis = String(time.getMilliseconds()).padStart(3, "0");
  return `${clock}.${millis}`;
}
