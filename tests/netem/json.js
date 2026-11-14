// How a scenario's result is written: indented, but each sample, mapping and event on one
// line, so that two runs can be read and diffed line by line.
const ONE_PER_LINE = new Set(['samples', 'mappings', 'events', 'alice', 'bob']);

function format(value, indent = '', key = '') {
  if (Array.isArray(value) && ONE_PER_LINE.has(key)) {
    if (!value.length) return '[]';
    return '[\n' + value.map(v => `${indent} ${JSON.stringify(v)}`).join(',\n') + `\n${indent}]`;
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const entries = Object.entries(value).filter(([, v]) => v !== undefined);
    if (!entries.length) return '{}';
    return '{\n' + entries.map(([k, v]) => `${indent} ${JSON.stringify(k)}: ${format(v, indent + ' ', k)}`).join(',\n') +
      `\n${indent}}`;
  }
  return JSON.stringify(value);
}

module.exports = {format: value => format(value) + '\n'};
