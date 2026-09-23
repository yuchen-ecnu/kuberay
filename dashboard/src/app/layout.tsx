"use client";
// Tailwind global styles have conflicts with Joy UI. For example,
// button background is overridden to be transparent.
// See https://github.com/tailwindlabs/tailwindcss/issues/7500
import { Breadcrumb } from "@/components/Breadcrumb";
import { Box } from "@mui/joy";
import { CssVarsProvider } from "@mui/joy/styles";
import Script from "next/script";
import "./globals.css";
import "@xterm/xterm/css/xterm.css";
import { SnackBarProvider } from "@/components/SnackBarProvider";
import { NamespaceProvider } from "@/components/NamespaceProvider";
import { FirstVisitProvider } from "@/components/FirstVisitContext";
import { usePathname } from "next/navigation";

const favicon =
  "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%232563eb'/%3E%3Ccircle cx='16' cy='16' r='6' fill='white'/%3E%3C/svg%3E";

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const pathname = usePathname();
  if (/\/(?:federation|terminal)\/?$/.test(pathname)) {
    return (
      <html lang="en">
        <head>
          <link rel="icon" href={favicon} />
        </head>
        <body>{children}</body>
      </html>
    );
  }
  return (
    <html lang="en">
      <head>
        <link rel="icon" href={favicon} />
      </head>
      <body>
        <Script strategy="beforeInteractive" src="/dashboard_lib.bundle.js" />
        <CssVarsProvider>
          <NamespaceProvider>
            <FirstVisitProvider>
              <SnackBarProvider>
                <Box component="main" sx={{ px: 4, py: 2 }}>
                  <Breadcrumb />
                  {children}
                </Box>
              </SnackBarProvider>
            </FirstVisitProvider>
          </NamespaceProvider>
        </CssVarsProvider>
      </body>
    </html>
  );
}
