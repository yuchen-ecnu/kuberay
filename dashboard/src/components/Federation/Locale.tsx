"use client";

import { createContext, useContext, type ReactNode } from "react";
import { translate, type Locale, type Translate } from "@/federation/i18n";

const LocaleContext = createContext<{
  locale: Locale;
  t: Translate;
} | null>(null);

const value = {
  locale: "en" as const,
  t: ((key, values) => translate("en", key, values)) as Translate,
};

export function LocaleProvider({ children }: { children: ReactNode }) {
  return (
    <LocaleContext.Provider value={value}>{children}</LocaleContext.Provider>
  );
}

export function useLocale() {
  const context = useContext(LocaleContext);
  if (!context)
    throw new Error("Federation components require LocaleProvider.");
  return context;
}
