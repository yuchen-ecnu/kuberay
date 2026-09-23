/** Demo copy is English-only; Kubernetes and Ray diagnostics pass through unchanged. */
export type Locale = "en";
export type MessageKey = string;
export type Translate = (
  key: MessageKey,
  values?: Record<string, string | number>,
) => string;

export function translate(
  _locale: Locale,
  key: MessageKey,
  values: Record<string, string | number> = {},
): string {
  return key.replace(/\{(\w+)\}/g, (match, name) =>
    String(values[name] ?? match),
  );
}

export function translateDiagnostic(message: string, _locale: Locale): string {
  return message;
}
