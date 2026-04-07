# CommodityWatch — Project Folder Instructions

## Current Product Status

CommodityWatch is no longer a headline-only MVP. The shipped product in this repository now includes:

- `/` → dashboard home — multi-module overview (Benchmark Prices, Demand Pulse, Inventory Snapshot, Latest Headlines, Upcoming Releases)
- `/headline-watch/` → HeadlineWatch
- `/price-watch/` → PriceWatch
- `/demand-watch/` → DemandWatch (frontend live; data is currently curated static JS — live API backend in development)
- `/inventory-watch/` → InventoryWatch
- `/calendar-watch/` → CalendarWatch

Two additional modules appear as disabled navigation placeholders and are not yet implemented:
- SupplyWatch
- WeatherWatch

The older headline-first guidance below is still useful for tone, restraint, and audience targeting, but when it conflicts with the current route structure or shipped product surfaces, the current product state above takes precedence.

The `sandbox/commodity-visual-prototype/` directory is archived reference material, not the production PriceWatch implementation.

## What This Project Is

You are working on **CommodityWatch** — a commodity market monitoring web app for traders, analysts, and market professionals. Every file, decision, and output in this folder serves that product.

This is not a general news site. It is not a financial terminal. It is not a portfolio tool. It is a fast, clean, signal-focused headline feed covering three commodity categories: **Energy, Metals, and Agriculture.**

When in doubt about scope: if it isn't directly related to fast professional monitoring of commodity headlines, benchmark prices, or scheduled market events, it does not belong.

---

## The Product in One Sentence

> A fast, signal-focused commodity news headline feed for traders, analysts, and market professionals who need to know what moved — organized by category, stripped of noise, built for people who already know what they're looking at.

---

## Brand Name & Domain

- **Brand name:** CommodityWatch
- **Primary domain:** commoditywatch.co
- **Backup domain:** Not assigned

---

## Target Users

**Primary:** Commodity traders, market analysts, corporate procurement and supply chain professionals with commodity exposure.

**Secondary:** Financial journalists, Gulf/MENA institutional professionals, sophisticated retail investors with commodity exposure.

**Not the audience:** Crypto users, general retail finance app users, long-form readers.

Design, copy, and UX decisions should always optimize for the primary audience. These users are expert. Never explain basic market terminology in the interface.

---

## Brand Voice & Tone

- Confident, not arrogant
- Functional first — labels and UI copy are self-explanatory
- Respect user expertise — never explain what "WTI" or "spot price" means
- Precise — "12 minutes ago" not "recently"
- No hype — never use "powerful," "seamless," "game-changing"
- Economy of words — if a label can be one word, it is one word
- Active voice in headlines

**Tagline:** *"News at spot price."*
**Homepage headline:** *"Every commodity headline. Nothing else."*
**Homepage subhead:** *"No commentary. No filler. Just the commodity headlines that matter."*

---

## Brand Identity

### Color Palette

```
--color-bg:        #F0EFE9   Warm off-white page background
--color-surface:   #FFFFFF   Card backgrounds
--color-primary:   #0F1923   Near-black — headlines, nav, body text
--color-amber:     #E8A020   Brand accent — the signature color
--color-subtext:   #6B7280   Source labels, timestamps, secondary text
--color-border:    #D1CEC8   Dividers, card outlines, subtle rules
--color-up:        #2ECC71   Price-up indicator (use sparingly)
--color-down:      #E74C3C   Price-down indicator (use sparingly)
```

Category tag colors:
```
ENERGY:      #E8A020  (amber)
METALS:      #4A90D9  (steel blue)
AGRICULTURE: #5BA85C  (soft green)
```

### Typography

- **Headlines:** IBM Plex Serif, weight 600
- **UI / Body:** IBM Plex Sans, weight 400/500
- **Timestamps / Tickers:** IBM Plex Mono, weight 400

The amber (`#E8A020`) is CommodityWatch's signature. It should appear on: the logo accent, active filter states, category tags for Energy, headline hover states, and the "Live" badge. Do not introduce additional accent colors.

---

## Product Structure

### Site Map (current shipped product)

```
/                → Dashboard home — multi-module commodity overview
/headline-watch/ → HeadlineWatch — live headline feed
/price-watch/    → PriceWatch — benchmark commodity prices
/demand-watch/   → DemandWatch — demand-side indicator panels
/inventory-watch/→ InventoryWatch — inventory snapshots and detail views
/calendar-watch/ → CalendarWatch — scheduled market event releases
```

Planned but not yet implemented:
```
/supply-watch/   → SupplyWatch (placeholder in nav)
/weather-watch/  → WeatherWatch (placeholder in nav)
```

### Dashboard Homepage Layout

The root `/` is a multi-module overview, not a pure headline feed:

1. Sticky top nav (logo + module tab links)
2. Sector filter bar (All / Energy / Metals / Agriculture)
3. Multi-panel layout: Benchmark Prices (top), Demand Pulse (sidebar), Inventory Snapshot (left), Latest Headlines (bottom), Upcoming Releases (sidebar)
4. Footer

The HeadlineWatch route (`/headline-watch/`) retains the dedicated headline-feed layout with filter pills, load-more, and per-card timestamps.

### Headline Card Structure

Every card contains exactly:
1. Category tag — colored dot + uppercase label (● ENERGY)
2. Headline text — IBM Plex Serif, dominant, links to source
3. Source label + timestamp — IBM Plex Mono, muted, bottom of card

No images. No excerpts. No author bylines in V1.

---

## Shipped Feature Scope

### Implemented

- Live headline feed across all commodity categories (HeadlineWatch)
- Category filter pills — All / Energy / Metals / Agriculture
- Source label + timestamp on every card
- Mobile-responsive layout
- Benchmark commodity price tiles with history (PriceWatch)
- Demand-side indicator panels by vertical (DemandWatch)
- Inventory snapshots and indicator detail views (InventoryWatch)
- Scheduled market event calendar (CalendarWatch)
- Multi-module dashboard homepage
- External links to source articles (open in new tab)

### Not Yet Implemented

| Feature | Status |
|---|---|
| SupplyWatch | Placeholder in nav — not yet built |
| WeatherWatch | Placeholder in nav — not yet built |
| DemandWatch live API | Data is curated static JS; backend publication in development |
| User accounts / login | Not planned for current phase |
| Watchlists | Not planned for current phase |
| Push notifications | Not planned for current phase |
| AI summaries | Not planned for current phase |
| Comment sections | Not planned for current phase |
| Sponsored content | Not planned for current phase |
| Dark mode toggle | Not planned for current phase |
| Search | Not yet implemented |
| Sentiment tags displayed in UI | Not yet implemented |

---

## Technical Defaults

Unless instructed otherwise:

- **Frontend demos:** Single-file HTML (no frameworks, no build tools)
- **Fonts:** IBM Plex Serif + IBM Plex Sans + IBM Plex Mono via Google Fonts
- **CSS:** Custom properties (variables), no Tailwind, no Bootstrap
- **JS:** Vanilla — no React, no Vue, no jQuery
- **No external API calls** in demos
- **Semantic HTML:** `<nav>`, `<main>`, `<article>`, `<footer>`
- **Mobile-first** — single column, horizontal pill scroll, bottom tab bar on mobile

---

## What Good Work Looks Like Here

- Opens fast, scans in under 60 seconds
- Looks like a real, shippable product — not a prototype
- Editorial restraint: no decoration for its own sake
- The amber accent feels like a signature, not a highlight
- A commodity trader opening it immediately understands it was built for them
- Reference aesthetic: *Financial Times front page rebuilt as a mobile web app*

---

## What to Avoid

- Generic "finance app" blue color schemes
- Images or photography in the headline feed
- Dense, anxious card grids — use generous whitespace
- Hiding timestamps or source labels
- Sensationalist copy or marketing hype
- Any feature that adds noise rather than reducing it
- Explaining the product to users who already understand commodity markets
