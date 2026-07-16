/**
 * RabbitMQ-style topic pattern matching:
 *   `*` matches exactly one dot-separated segment
 *   `#` matches zero or more segments (supported anywhere, like RabbitMQ)
 */
export function topicMatches(pattern: string, topic: string): boolean {
  return match(pattern.split('.'), topic.split('.'));
}

function match(p: string[], t: string[]): boolean {
  if (p.length === 0) return t.length === 0;
  const [head, ...rest] = p;
  if (head === '#') {
    // '#' consumes 0..n segments
    for (let skip = 0; skip <= t.length; skip++) {
      if (match(rest, t.slice(skip))) return true;
    }
    return false;
  }
  if (t.length === 0) return false;
  if (head === '*' || head === t[0]) return match(rest, t.slice(1));
  return false;
}
