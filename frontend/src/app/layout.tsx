import type { Metadata } from "next";
import localFont from "next/font/local";
import { ReactNode } from "react";

import { QueryProvider } from "@/components/layout/query-provider";

import "./globals.css";

const ibmPlexSans = localFont({
  src: [
    {
      path: "./fonts/ibm-plex-sans-latin-400-normal.woff2",
      weight: "400",
      style: "normal",
    },
    {
      path: "./fonts/ibm-plex-sans-latin-500-normal.woff2",
      weight: "500",
      style: "normal",
    },
    {
      path: "./fonts/ibm-plex-sans-latin-600-normal.woff2",
      weight: "600",
      style: "normal",
    },
  ],
  variable: "--font-sans",
  display: "swap",
});

const ibmPlexMono = localFont({
  src: [
    {
      path: "./fonts/ibm-plex-mono-latin-400-normal.woff2",
      weight: "400",
      style: "normal",
    },
    {
      path: "./fonts/ibm-plex-mono-latin-500-normal.woff2",
      weight: "500",
      style: "normal",
    },
  ],
  variable: "--font-mono",
  display: "swap",
});

export const metadata: Metadata = {
  metadataBase: new URL("https://commoditywatch.co"),
  title: {
    default: "CommodityWatch",
    template: "%s | CommodityWatch",
  },
  description: "Monitor commodity inventories, benchmarks, headlines, and release calendars in one platform shell.",
};

function themeBootstrapScript() {
  return `
    (function() {
      try {
        var saved = window.localStorage.getItem("cw-theme");
        var theme = saved === "dark" ? "dark" : "light";
        document.documentElement.setAttribute("data-theme", theme);
      } catch (error) {
        document.documentElement.setAttribute("data-theme", "light");
      }
    })();
  `;
}

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html className={`${ibmPlexSans.variable} ${ibmPlexMono.variable}`} lang="en" suppressHydrationWarning>
      <body>
        <script dangerouslySetInnerHTML={{ __html: themeBootstrapScript() }} />
        <QueryProvider>{children}</QueryProvider>
      </body>
    </html>
  );
}
