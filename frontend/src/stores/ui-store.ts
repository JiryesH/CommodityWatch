"use client";

import { create } from "zustand";

export type ThemeMode = "light" | "dark";

interface UIState {
  mobileNavOpen: boolean;
  theme: ThemeMode;
  setMobileNavOpen: (open: boolean) => void;
  setTheme: (theme: ThemeMode) => void;
  toggleTheme: () => void;
  syncThemeFromDom: () => void;
}

function applyTheme(theme: ThemeMode) {
  if (typeof document === "undefined") {
    return;
  }

  document.documentElement.setAttribute("data-theme", theme);
  window.localStorage.setItem("cw-theme", theme);
}

export const useUIStore = create<UIState>((set) => ({
  mobileNavOpen: false,
  theme: "light",
  setMobileNavOpen: (open) => set({ mobileNavOpen: open }),
  setTheme: (theme) => {
    applyTheme(theme);
    set({ theme });
  },
  toggleTheme: () =>
    set((state) => {
      const theme = state.theme === "light" ? "dark" : "light";
      applyTheme(theme);
      return { theme };
    }),
  syncThemeFromDom: () => {
    if (typeof document === "undefined") {
      return;
    }

    const theme = document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
    set({ theme });
  },
}));
