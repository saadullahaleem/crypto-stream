// Colors for the SQL of a view. Returns HTML.

const KEYWORDS = [
  "CREATE", "MATERIALIZED", "VIEW", "IF", "NOT", "EXISTS", "AS", "SELECT", "FROM",
  "WHERE", "GROUP", "BY", "HAVING", "ORDER", "AND", "OR", "INTERVAL",
];
const FUNCTIONS = ["count", "min", "max", "avg", "sum", "first_value", "last_value", "split_part", "now"];

const keywordPattern = new RegExp(`\\b(${KEYWORDS.join("|")})\\b`, "g");
const functionPattern = new RegExp(`\\b(${FUNCTIONS.join("|")})\\(`, "g");

export function highlightSql(sql) {
  const escaped = sql.replaceAll("&", "&amp;").replaceAll("<", "&lt;");
  return escaped
    .replace(/'[^']*'/g, (text) => `<span class="string">${text}</span>`)
    .replace(keywordPattern, '<span class="keyword">$1</span>')
    .replace(functionPattern, '<span class="function">$1</span>(');
}
