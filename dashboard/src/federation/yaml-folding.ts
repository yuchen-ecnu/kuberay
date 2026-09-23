import { isMap, isNode, isSeq, LineCounter, parseDocument } from "yaml";

export interface YamlFold {
  startLine: number;
  endLine: number;
}

/** Locate multi-line YAML collections that can be folded in the preview. */
export function yamlFolds(source: string): YamlFold[] {
  const lineCounter = new LineCounter();
  const document = parseDocument(source, { lineCounter });
  if (document.errors.length || !document.contents) return [];

  const folds = new Map<number, YamlFold>();
  const add = (start: number, end: number) => {
    if (end <= start) return;
    const existing = folds.get(start);
    if (!existing || existing.endLine < end)
      folds.set(start, { startLine: start, endLine: end });
  };
  const lines = (node: { range?: readonly number[] | null }) => {
    if (!node.range) return null;
    return {
      start: lineCounter.linePos(node.range[0]).line - 1,
      end: lineCounter.linePos(node.range[1] - 1).line - 1,
    };
  };

  const visit = (node: unknown) => {
    if (isMap(node)) {
      for (const pair of node.items) {
        const value = pair.value;
        if (
          isNode(pair.key) &&
          (isMap(value) || isSeq(value)) &&
          pair.key.range
        ) {
          const valueLines = lines(value);
          if (valueLines)
            add(
              lineCounter.linePos(pair.key.range[0]).line - 1,
              valueLines.end,
            );
        }
        visit(value);
      }
      return;
    }
    if (isSeq(node)) {
      for (const item of node.items) {
        if (isMap(item) || isSeq(item)) {
          const itemLines = lines(item);
          if (itemLines) add(itemLines.start, itemLines.end);
        }
        visit(item);
      }
    }
  };

  visit(document.contents);
  return [...folds.values()].sort(
    (left, right) =>
      left.startLine - right.startLine || right.endLine - left.endLine,
  );
}
