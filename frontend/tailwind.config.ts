import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: ["class", '[data-theme="dark"]'],
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    screens: {
      sm: "640px",
      md: "768px",
      lg: "1024px",
      xl: "1280px",
      "2xl": "1536px",
    },
    extend: {
      colors: {
        app: "var(--color-bg-app)",
        canvas: "var(--color-bg-canvas)",
        surface: "var(--color-bg-surface)",
        "surface-alt": "var(--color-bg-surface-alt)",
        elevated: "var(--color-bg-elevated)",
        muted: "var(--color-bg-muted)",
        overlay: "var(--color-bg-overlay)",
        foreground: "var(--color-text-primary)",
        "foreground-secondary": "var(--color-text-secondary)",
        "foreground-muted": "var(--color-text-muted)",
        "foreground-soft": "var(--color-text-soft)",
        border: "var(--color-border-default)",
        "border-subtle": "var(--color-border-subtle)",
        "border-strong": "var(--color-border-strong)",
        accent: "var(--color-accent)",
        "accent-hover": "var(--color-accent-hover)",
        positive: "var(--color-positive)",
        negative: "var(--color-negative)",
        caution: "var(--color-caution)",
        neutral: "var(--color-neutral)",
        info: "var(--color-info)",
        energy: "var(--color-sector-energy)",
        metals: "var(--color-sector-metals)",
        agriculture: "var(--color-sector-agriculture)",
      },
      boxShadow: {
        card: "var(--shadow-card)",
      },
      fontFamily: {
        sans: ["var(--font-sans)", "system-ui", "sans-serif"],
        mono: ["var(--font-mono)", "ui-monospace", "monospace"],
      },
      spacing: {
        1: "var(--space-1)",
        2: "var(--space-2)",
        3: "var(--space-3)",
        4: "var(--space-4)",
        5: "var(--space-5)",
        6: "var(--space-6)",
        8: "var(--space-8)",
        10: "var(--space-10)",
        12: "var(--space-12)",
        16: "var(--space-16)",
      },
      borderRadius: {
        sm: "8px",
        md: "10px",
        lg: "14px",
      },
      minHeight: {
        "chart-desktop": "420px",
        "chart-tablet": "340px",
        "chart-mobile": "280px",
      },
    },
  },
  plugins: [],
};

export default config;
