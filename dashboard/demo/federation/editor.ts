import type { Locator } from "@playwright/test";

export async function editorValue(editor: Locator): Promise<string> {
  return editor.evaluate((element) => {
    const content = element as HTMLElement & {
      cmTile?: { view?: { state?: { doc?: { toString(): string } } } };
    };
    const value = content.cmTile?.view?.state?.doc?.toString();
    if (value === undefined) throw new Error("CodeMirror document unavailable");
    return value;
  });
}
