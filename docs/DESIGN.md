# DESIGN.md — Chain Reaction

> **AML Money Mule Detection & Graph Intelligence Platform**
> Single source of truth for product design, visual system, component behavior, and UI acceptance criteria.
> Version 1.0 · Written to be read by humans and by any coding agent or tool.

---

## 0. How to Use This Document

This file is the **authoritative design reference** for the Chain Reaction frontend. It is deliberately tool-neutral: it contains no instructions specific to any editor, IDE, AI assistant, or framework.

**Rules for any implementer (human or automated):**

1. **Read this file fully before changing any UI.** Do not guess colors, spacing, or behavior.
2. **Tokens are the contract.** Never hardcode hex values, pixel sizes, or font names in components. Reference tokens (Section 14). If a token is missing, add it to the token file first, then use it.
3. **Reuse before creating.** Search the codebase for an existing component or token before writing a new one.
4. **UI changes must not alter data logic.** Restyling a module must not change props, API calls, parsing, or business rules unless explicitly requested.
5. **Precedence when rules conflict:** Accessibility (Section 12) > Semantics of risk color (Section 2.4) > Component spec (Section 8–9) > Screen spec (Section 5–7) > Aesthetic preference.
6. **When something is unspecified,** choose the simplest option consistent with the Design Principles (Section 1) and note the decision in the change description.
7. **Verify against the checklist** in Section 17 before considering any UI task done.

**Terminology used in this file**

| Term | Meaning |
| :--- | :--- |
| **Target account** | The account currently under investigation |
| **Chain** | An ordered multi-hop fund flow, e.g. A → B → C → D |
| **Hop** | One transfer between two accounts in a chain |
| **Decay / haircut** | Amount lost between hops (the mule's fee cut) |
| **Retention %** | Final outflow ÷ initial inflow |
| **Threshold** | GNN mule-probability cutoff for flagging (default `0.5254`) |
| **Tier** | Risk classification: Critical, High, Elevated Review, Standard |
| **Ego-network** | The 2-hop subgraph around the target account |
| **SAR** | Suspicious Activity Report |

---

## 1. Product & Design Intent

### 1.1 Product Purpose

Chain Reaction is an enterprise investigation platform that combines a **2-layer GraphSAGE graph neural network** with **algorithmic temporal motif matching** to uncover money mule accounts and multi-hop layering chains. The frontend turns model output into a fast, explainable, audit-ready investigation cockpit for financial intelligence units.

### 1.2 Users

| Persona | Goal | Pain point addressed |
| :--- | :--- | :--- |
| **Tier-1 Triage Analyst** | Rapidly validate alerts and dismiss false positives | Alert fatigue; 95%+ false-positive rates in legacy tools |
| **Senior Investigator** | Trace full multi-hop flows; write SARs | Manual pivoting across fragmented transaction tables |
| **Risk / Model Audit Officer** | Verify *why* an account was flagged | Black-box models with no explanation |

### 1.3 Design Principles

1. **Signal over decoration.** Every color carries meaning. Lime marks what the analyst can act on or focus on; red/orange/amber mark risk. Nothing is colored purely for style.
2. **Dark-first, graphite-deep.** Analysts work long shifts. A graphite canvas reduces fatigue and lets accent and risk colors read clearly.
3. **Explainability is always visible.** The top 3 behavioral drivers and top 2 counterparty links are visible without scrolling at 1440×900.
4. **Density with breathing room.** Information-rich but organized on a strict 4px grid with clear hierarchy.
5. **Motion explains.** Animation shows flow direction, causality, and focus. It is never ornamental and always respects reduced-motion preferences.
6. **Never color alone.** Every risk indicator pairs color with an icon and a text label.

### 1.4 Visual Personality

| Attribute | Expression |
| :--- | :--- |
| **Sleek** | 1px hairline borders, soft inner glows, minimal chrome, large key numbers |
| **Modern** | 12px rounded panels, subtle layering, crisp monospaced numerals |
| **Authoritative** | Restrained palette, consistent iconography, no playful illustration |
| **Electric** | One high-energy accent (Lime Spark) used sparingly |

---

## 2. Color System

### 2.1 Brand Colors

| Token | Hex | Role |
| :--- | :--- | :--- |
| `lime-500` (**Lime Spark**) | `#B6FF2E` | Primary accent: primary buttons, active-tab indicator, focus rings, selection, key highlights |
| `graphite-800` (**Graphite**) | `#23262F` | Primary surface (cards, panels); label color on lime fills |

**Contrast facts**
- Lime Spark on Graphite ≈ **12.5:1** (AAA).
- Lime Spark on white ≈ **1.3:1** (fails).

**Hard rules**
- Content placed on a lime fill uses **graphite** (`--on-accent`), never white.
- In light mode, lime is used **only as a fill** with graphite content. Never use lime as text, icon stroke, or thin line on a light background.

### 2.2 Graphite Neutral Scale

| Token | Hex | Usage |
| :--- | :--- | :--- |
| `graphite-950` | `#14161B` | App canvas (deepest layer) |
| `graphite-900` | `#1A1D24` | Top bar, sidebar, table headers |
| `graphite-800` | `#23262F` | **Brand graphite** — cards and panels |
| `graphite-700` | `#2D313C` | Raised surfaces, inputs, row hover |
| `graphite-600` | `#3A3F4C` | Borders, dividers |
| `graphite-500` | `#565C6B` | Disabled elements, muted icons |
| `graphite-400` | `#7F8696` | Secondary text, metadata, table headers |
| `graphite-300` | `#A9AEBB` | Secondary body text on dark |
| `graphite-200` | `#D1D4DC` | Primary body text on dark |
| `graphite-100` | `#E8EAEF` | Borders in light mode |
| `graphite-50` | `#F5F6F8` | Light-mode canvas |
| `white` | `#FFFFFF` | Headings on dark; light-mode panels |

### 2.3 Lime Scale

| Token | Hex | Usage |
| :--- | :--- | :--- |
| `lime-50` | `#F6FFE0` | Light-mode tinted backgrounds |
| `lime-200` | `#E4FFA6` | Highlight text on dark |
| `lime-400` | `#CDFF5E` | Primary button hover |
| `lime-500` | `#B6FF2E` | **Brand accent** |
| `lime-600` | `#98DB14` | Primary button pressed |
| `lime-700` | `#75AB0B` | Lime-toned text on light backgrounds (AA on white) |
| `lime-glow` | `rgba(182,255,46,0.16)` | Focus halos, selected-row tint, active-tab glow |

### 2.4 Semantic Risk Colors

Risk colors are intentionally **distinct from lime** so the brand accent never reads as "safe" or "dangerous."

| Token | Dark | Light | Tint (14%) | Meaning | Icon |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `risk-critical` | `#FF4D5E` | `#DC2626` | `rgba(255,77,94,0.14)` | CRITICAL RISK; target mule node; decay amounts | `ShieldAlert` |
| `risk-high` | `#FF8A3D` | `#EA580C` | `rgba(255,138,61,0.14)` | HIGH RISK; chain intermediaries | `AlertTriangle` |
| `risk-elevated` | `#FFC53D` | `#D97706` | `rgba(255,197,61,0.14)` | ELEVATED REVIEW; borderline score | `Eye` |
| `risk-standard` | `#2DD4BF` | `#0D9488` | `rgba(45,212,191,0.14)` | STANDARD; legitimate account | `ShieldCheck` |
| `info` | `#60A5FA` | `#2563EB` | `rgba(96,165,250,0.14)` | Counterparties; informational callouts | `Info` |

> Teal (not green) represents "Standard" so that lime stays unambiguously the **brand and interaction** color.

### 2.5 Color Usage Rules

- **Balance:** ~60% graphite surfaces, ~30% neutral text/borders, ~10% accent and risk colors.
- **One lime moment per region.** A panel contains at most one lime-*filled* element (primary button, active tab indicator, or selected ring).
- **Gradients are rare.** Allowed only for (a) a top-edge sheen on cards (`graphite-700` → transparent) and (b) the probability gauge arc.
- **Tinted backgrounds** for pills/chips always use the 14% tint of the semantic color with full-strength text of the same color.
- **No pure black** (`#000`) anywhere. Deepest allowed color is `graphite-950`.

---

## 3. Typography

| Role | Font | Weights | Notes |
| :--- | :--- | :--- | :--- |
| Headings & UI | **Inter** (fallback `system-ui, sans-serif`) | 400, 500, 600, 700 | Heading tracking `-0.01em` |
| Numbers, money, IDs, timestamps, durations | **JetBrains Mono** (fallback `ui-monospace, monospace`) | 400, 500 | Always `font-variant-numeric: tabular-nums` |

### 3.1 Type Scale

| Token | Size / Line | Weight | Use |
| :--- | :--- | :--- | :--- |
| `display` | 40 / 44 | 700 | Hero probability value |
| `h1` | 28 / 34 | 600 | Account banner title |
| `h2` | 20 / 28 | 600 | Panel and module titles |
| `h3` | 16 / 24 | 600 | Sub-section headers |
| `body` | 14 / 22 | 400 | Narrative text, table cells |
| `caption` | 12 / 16 | 500 | Metadata, labels, table headers (uppercase, `+0.04em` tracking) |
| `mono-lg` | 24 / 28 | 500 | Telemetry and summary values |
| `mono-md` | 16 / 24 | 500 | Account IDs, amounts in cards |
| `mono-sm` | 13 / 20 | 400 | Inline amounts, timestamps |

### 3.2 Data Formatting Rules

| Data | Format | Example |
| :--- | :--- | :--- |
| Currency | `$` + thousands separators + 2 decimals, mono | `$13,725.60` |
| Negative / decay | Leading `-`, critical color, percent in parentheses | `-$1,886.88 (13.7%)` |
| Timestamp (display) | `YYYY-MM-DD HH:mm:ss`, mono, local-agnostic (show source TZ if known) | `2022-09-01 00:10:00` |
| Dwell time | Compact relative | `+10m`, `+8h 15m`, `+2d 6h` |
| Duration (total span) | Hours with 2 decimals if ≥ 1 h; minutes if < 1 h | `78.50 h`, `10 min` |
| Probability | Percent with 1 decimal in prose; 4 decimals in technical views | `70.0%`, `0.7004` |
| Account ID | Mono, uppercase as given, never truncated in headers; middle-ellipsis only in dense lists | `811C2F680` |
| Large counts | Thousands separators; `k`/`M` only in KPI tiles | `515,080` / `515k` |

---

## 4. Layout, Spacing & Shape

### 4.1 Grid & Spacing

- **Base unit:** 4px. Allowed steps: `4, 8, 12, 16, 24, 32, 48, 64`.
- **Max content width:** 1600px; fluid beyond 1280px.
- **Panel padding:** 20px (24px at ≥1440px).
- **Gap between panels:** 16px.
- **Main split:** 45% left / 55% right with a draggable divider (min 35%, max 65%).

### 4.2 Radius & Borders

| Element | Radius | Border |
| :--- | :--- | :--- |
| Panels, cards | 12px | 1px `graphite-600` at 60% opacity |
| Buttons, inputs, selects | 10px | 1px `graphite-600` |
| Pills, badges, chips | 999px | none (tint + text color) |
| Tooltips | 8px | 1px `graphite-600` |
| Node circles | 50% | per node spec |

### 4.3 Elevation

Depth is expressed first through surface color, second through shadow.

| Level | Surface | Shadow (dark) | Use |
| :--- | :--- | :--- | :--- |
| 0 | `graphite-950` | none | App canvas |
| 1 | `graphite-800` | `inset 0 1px 0 rgba(255,255,255,0.04)` | Panels, cards |
| 2 | `graphite-700` | `0 8px 24px rgba(0,0,0,0.35)` | Dropdowns, tooltips, popovers |
| 3 | `graphite-700` | `0 16px 48px rgba(0,0,0,0.50)` | Modals, export dialogs, toasts |

---

## 5. Screen Architecture

```
+-----------------------------------------------------------------------------------+
| TOP BAR     [Logo] Chain Reaction AML     [ Global Search ]     [Status][Theme][User]|
+-----------------------------------------------------------------------------------+
| TELEMETRY   [Accounts] [Transactions] [PR-AUC] [Alert Purity]                      |
+-----------------------------------------------------------------------------------+
| ACCOUNT BANNER  ID · Risk badge · Recommended action · [Generate SAR]              |
+-----------------------------------------------------------------------------------+
| TABS  SAR Risk Card | Chain Stepper | GNNExplainer | Graph View | Benchmarks        |
+--------------------------------------+--------------------------------------------+
| LEFT PANEL (45%)                     | RIGHT PANEL (55%)                          |
|  Risk metrics + gauge                |  Interactive ego-network canvas            |
|  Feature drivers (bars)              |   Red: target · Orange: chain · Blue: other|
|  Multi-hop stepper                   |   Controls (top right) · Legend (bottom L) |
|  SAR narrative + export              |                                            |
+--------------------------------------+--------------------------------------------+
```

### 5.1 Component Tree

- `AppShell` — top bar, search, status, theme toggle
- `GlobalTelemetryBar` — 4 KPI tiles
- `AccountBanner` — sticky
- `TabNavigation`
  - `AnalystCardTab` (SAR Risk Card)
  - `ChainStepperTab`
  - `ExplainerTab`
  - `NetworkGraphTab`
  - `BenchmarkTab`
- `NetworkCanvas` — force-directed graph renderer

### 5.2 Core User Workflows

1. **Triage:** View KPIs → scan alert queue (sorted by GNN probability ≥ threshold or chain participation) → select/search account.
2. **Deep-dive:** Banner risk badge → summary cards (probability gauge, chain role, pass-through retention).
3. **Chain tracing:** Chronological stepper with per-hop dwell, decay, monotonicity; clicking a step focuses that edge/node in the graph.
4. **Explainability:** Feature attribution bars + counterparty importance ranking.
5. **SAR export:** Auto-generated narrative, editable, exportable as `.txt`, `.json`, `.pdf` in ≤ 2 clicks.

---

## 6. Global Shell Components

### 6.1 Top Bar (64px, `graphite-900`)

- **Left:** Logo mark (lime chain-link glyph of three connected nodes) + wordmark "Chain Reaction" (white) + "AML" (`graphite-400`).
- **Center:** Global search, max width 560px. Pill input on `graphite-700`, leading `Search` icon, trailing key hint `/`. Focus: 2px lime ring + `lime-glow` halo.
- **Right:** Status chip (8px pulsing lime dot + "Live · CUDA Model" in mono caption), theme toggle, user avatar.

### 6.2 Global Telemetry Bar

Four equal tiles (2×2 below 1024px).

| Tile | Value style | Accent |
| :--- | :--- | :--- |
| Monitored Accounts | `mono-lg`, white | Neutral sparkline |
| Transactions | `mono-lg`, white | Neutral sparkline |
| Model PR-AUC | `mono-lg`, **lime-500** | Delta chip vs. baseline |
| Alert Purity | `mono-lg`, **lime-500** | Mini lime progress bar |

Model-quality tiles use lime ("our performance"); dataset-scale tiles stay neutral.

### 6.3 Account Banner (sticky, 72px)

`graphite-800` strip with a **3px left border colored by risk tier**.

- **Left:** "ACCOUNT" caption over the ID (`h1`, mono) + copy icon button.
- **Center:** Risk badge pill (Section 8.2).
- **Right:** Recommended action (body, `graphite-300`), primary lime **Generate SAR** button, ghost secondary actions (Freeze, Escalate).

### 6.4 Tab Navigation

- Bar on `graphite-900`. Inactive `graphite-400`, hover `graphite-200`, active white.
- Active indicator: **3px lime underline** that slides between tabs (200ms) with a faint `lime-glow` beneath. In light mode, a graphite underline with a small lime dot.
- Each tab has a leading 16px Lucide icon: `FileText`, `GitCommit`, `BarChart3`, `Share2`, `Gauge`.

---

## 7. Feature Modules

### 7.1 Risk Metrics Card (Left Panel)

- **Probability gauge:** Semicircular arc with `display` value centered. Arc gradient runs `risk-standard → risk-elevated → risk-high → risk-critical`. A **lime tick** marks the decision threshold with label "Threshold 0.5254" and a delta chip (e.g., "+0.175 above").
- **Stat blocks (3):** Hop Count, Chain Role pill, Pass-through Retention % (`mono-lg`).
- Gauge animates fill + count-up over 700ms on load (instant under reduced motion).

### 7.2 Feature Attribution (GNNExplainer)

- Horizontal bars: 20px height, 6px radius, `graphite-700` track.
- **Top 3 features:** lime fill. Others: `graphite-500`.
- Percent label at bar end (mono). Raw value in tooltip.
- Accessible label per bar, e.g., "has_outflow_after_inflow contributes 38 percent."

### 7.3 Counterparty Flow Ranking

- Table: Direction (icon), Counterparty ID (mono), Importance score (mono) + inline bar.
- Inline bar color: **lime** for incoming, **risk-high** for outgoing.
- Selecting a row focuses that node in the graph.

### 7.4 Chain Stepper (Sequential Laundering Motif Stepper)

This is a high-priority module. It verifies chronological fund forwarding (A → B → C → D) with amount decay.

#### 7.4.1 Anatomy

```
┌──────────────────────────────────────────────────────────────────┐
│ Sequential Laundering Motif Stepper            [ Chain 1 · 2 hops ▾ ]│
│ Verifies chronological fund forwarding…                          │
│                                                                  │
│  (◎)  ┌──────────────────────────────────────┐                   │
│   │   │ 100428810           [SOURCE / INITIATOR] │               │
│   │   └──────────────────────────────────────┘                   │
│   ●   $13,725.60 · 🕐 2022-09-01 00:00:00 · ✓                    │
│   │                                                              │
│  (👤) ┌──────────────────────────────────────┐                   │
│   │   │ 8076D5560       [INTERMEDIARY MULE]  │                   │
│   │   └──────────────────────────────────────┘                   │
│   ●   $11,838.72 · 🕐 2022-09-01 00:10:00 · +10m · [-$1,886.88 · 13.7%] ✓
│   │                                                              │
│  (👤) ┌──────────────────────────────────────┐                   │
│       │ 8137EE930           [CASH-OUT SINK]  │                   │
│       └──────────────────────────────────────┘                   │
│                                                                  │
│  ┌──────────┬──────────┬────────────────┬──────────┐             │
│  │ INITIAL  │ FINAL    │ FEE HAIRCUT    │ TOTAL    │             │
│  │ INFLOW   │ OUTFLOW  │ (DECAY)        │ SPAN     │             │
│  │ $13,725  │ $11,838  │ -$1,886 (13.7%)│ 0.17 h   │             │
│  └──────────┴──────────┴────────────────┴──────────┘             │
└──────────────────────────────────────────────────────────────────┘
```

#### 7.4.2 Header

- Title: `h2`, white. Subtitle: `body`, `graphite-400`.
- **Chain selector:** custom-styled control (never a native browser-styled select). 40px tall, `graphite-700` fill, 1px `graphite-600` border, 10px radius, trailing chevron icon. Focus: 2px lime ring + `lime-glow` halo. Menu uses elevation 2; hovered/selected option has `lime-glow` background. Label format: `Chain {n} · {hops} hops · {amount}` with the amount in mono.
- Responsive: below 640px the selector moves under the title at full width.

#### 7.4.3 Stepper Rows

Each account is one row with a **left rail** and a **content card**.

| Element | Spec |
| :--- | :--- |
| Node circle | 36px, 1.5px border, 14% tint fill, 20px Lucide icon |
| Source node | `Target` icon, `risk-critical` |
| Intermediary node | `User` icon, `risk-high` |
| Sink node | `Banknote` (or `LogOut`) icon, `risk-critical`, stronger tint |
| Connector | 2px vertical line in `risk-high`, runs node-to-node; animated dot travels downward (1.6s linear loop; static under reduced motion) |
| Content card | `graphite-800`, 1px `graphite-600` border, 12px radius, 16px padding |
| Account ID | `mono-md`, white, with copy icon button |
| Role pill | Uppercase `caption`, tier-tinted background, tier-colored text, leading icon |
| Selected card | 2px lime ring + `lime-glow` halo |

#### 7.4.4 Hop Metadata (sits on the connector, between cards, never inside a card)

Render in one wrapping row, in this order:

1. **Amount** — `mono-sm`, white.
2. **Clock icon + timestamp** — `mono-sm`, `graphite-300`.
3. **Dwell** — `+10m`, `mono-sm`, `graphite-400` (not shown for the first hop).
4. **Decay chip** — `-$1,886.88 · 13.7%`, `risk-critical` text on 14% tint, pill. Only when decay > 0.
5. **Monotonic check** — lime `Check` icon (16px) only when timestamps are strictly non-decreasing along the chain. If violated, show a `risk-elevated` `AlertTriangle` with tooltip "Timestamp order violated."

#### 7.4.5 Summary Strip

Four-cell grid (`graphite-700` tiles, 12px radius, 16px padding; 2×2 below 640px). Each cell: `caption` label over `mono-lg` value.

| Cell | Value | Color |
| :--- | :--- | :--- |
| INITIAL INFLOW | `$13,725.60` | white |
| FINAL OUTFLOW | `$11,838.72` | white |
| FEE HAIRCUT (DECAY) | `-$1,886.88 (13.7%)` with `TrendingDown` icon | `risk-critical` |
| TOTAL SPAN | `0.17 h` (or minutes if < 1 h) | white |

#### 7.4.6 Behavior

- Clicking or pressing Enter/Space on a step card selects it and triggers the existing "focus node/edge in graph" behavior (camera eases to center over 400ms; matching edge pulses lime).
- Switching chains resets selection and re-renders with a 180ms fade.
- Keyboard: Up/Down arrows move between steps; Tab order follows visual order.

### 7.5 SAR Narrative Box

- Editable text area on `graphite-700`, `body` text, 22px line height, character counter.
- Toolbar: **Copy** (icon morphs to a check for 1.5s), **Download** dropdown (`.txt`, `.json`, `.pdf`), **Add investigator note**.
- Export reachable in ≤ 2 clicks from anywhere in the account view.

### 7.6 Benchmark Tab

- Comparison table (XGBoost vs. GraphSAGE). Winning cell per row gets a lime-tinted background and a small `Trophy` icon.
- PR curve: GraphSAGE = lime line; XGBoost = `graphite-400` dashed; baseline = `graphite-600`.

---

## 8. Component Library

### 8.1 Buttons

| Variant | Default | Hover | Pressed | Disabled |
| :--- | :--- | :--- | :--- | :--- |
| **Primary** | `lime-500` fill, graphite label | `lime-400` | `lime-600` | `graphite-600` fill, `graphite-400` label |
| **Secondary** | `graphite-700` fill, white label, 1px `graphite-600` | `graphite-600` | `graphite-800` | 50% opacity |
| **Ghost** | transparent, `graphite-200` label | `graphite-700` | `graphite-800` | 40% opacity |
| **Destructive** | critical 14% tint, critical label | 22% tint | 30% tint | 50% opacity |

Height 40px (32px compact), 16px horizontal padding, 10px radius. Icon-only buttons are 36px with an accessible name.

### 8.2 Risk Badge

Pill, 24px height, 8px horizontal padding, leading 14px icon, uppercase `caption` text, tier tint background, tier-colored text and icon. `ELEVATED REVIEW` adds a hairline amber border.

| Tier | Label | Icon |
| :--- | :--- | :--- |
| Critical | `CRITICAL RISK` | `ShieldAlert` |
| High | `HIGH RISK` | `AlertTriangle` |
| Elevated | `ELEVATED REVIEW` | `Eye` |
| Standard | `STANDARD` | `ShieldCheck` |

### 8.3 Inputs, Selects, Search

- 44px height (40px for in-module selects), `graphite-700` fill, 1px `graphite-600` border, 10px radius.
- **Focus:** border `lime-500` + 4px `lime-glow` halo.
- **Error:** border `risk-critical`, helper text in critical color.
- **Never ship native browser-styled controls** (white dropdowns, default checkboxes) in dark mode. Use styled equivalents.
- Search autocomplete: 250ms debounce; menu groups **Presets** (lime `Zap` icon) above **Matches** (matched characters white, rest `graphite-400`).

### 8.4 Tables

- Sticky header in `caption` style on `graphite-900`.
- Row height 44px; hover `graphite-700`.
- Selected row: 2px lime left border + `lime-glow` fill.
- Numeric columns right-aligned, mono.

### 8.5 Pills, Chips, Tags

Always 999px radius, tinted background (14%), colored text, optional leading 14px icon. Height 24px (28px if interactive).

### 8.6 Toasts & Callouts

- **Toasts:** top-right, 320px, elevation 3, 3px left border in semantic color; auto-dismiss 5s (errors persist until dismissed).
- **Callouts:** `info` or `risk-elevated` tint, leading icon, one-line message, optional action link.

### 8.7 Tooltips

Elevation 2, 8px radius, 120ms show delay, max width 280px, `mono-sm` for values, `body` for prose. Must be reachable by keyboard focus.

### 8.8 Icons

Lucide, 1.75px stroke. 16px inline, 20px in buttons/tabs/node circles. Monochrome, inherit text color — except risk icons, which take their tier color.

Key set: `ShieldAlert`, `AlertTriangle`, `Eye`, `ShieldCheck`, `GitCommit`, `TrendingDown`, `FileText`, `Share2`, `Search`, `Copy`, `Check`, `Zap`, `Pin`, `Maximize2`, `Target`, `User`, `Banknote`, `Clock`, `Trophy`, `Info`.

---

## 9. Network Graph Specification

### 9.1 Canvas

- Background `graphite-950` with a faint dot grid (`graphite-700` at 40%, 24px spacing). Light mode: `graphite-50` with `graphite-100` dots.
- Canvas/WebGL renderer; target 60fps with up to ~500 visible nodes.
- Frosted-glass control cluster (top right) and legend (bottom left): `graphite-900` at 80% with ~12px backdrop blur.

### 9.2 Nodes

| Type | Radius | Fill | Ring / effect |
| :--- | :--- | :--- | :--- |
| Target | 24px | `risk-critical` | Double ring (inner 2px white @20%, outer 2px critical @40%) + soft red glow |
| Chain | 18px | `risk-high` | 2px ring, orange @50% |
| Counterparty | 12px | `info` @70% | none |
| Low-signal background | 12px | `graphite-400` | none |
| **Selected (any)** | unchanged | unchanged | **2px lime ring + lime-glow halo** |

Labels: `mono-sm` at 11px, white @90%. Always shown for target and chain nodes; others on hover or sufficient zoom.

### 9.3 Edges

| Type | Width | Style | Color |
| :--- | :--- | :--- | :--- |
| Chain flow | 4px | Solid, animated directional pulse | `risk-critical` |
| High attribution (> 0.6) | 2.5px | Solid | `risk-high` |
| Background | 1px | Dashed | `graphite-500` @60% |
| **Focused** | +1px | Solid | `lime-500` with glow |

Curved arcs (`rad 0.08`), arrowheads scaled to width.

### 9.4 Interaction

- **Hover:** non-incident edges drop to 15% opacity; 1-hop neighbors stay 100%; tooltip after 120ms (Account ID, total flow, holding time, GNN score with mini bar).
- **Click:** select, recenter, sync left-panel ranking.
- **Drag-and-pin:** pinned nodes show a small lime pin glyph.
- **Controls:** zoom in/out, fit-to-screen, physics stabilization toggle, layout switch (force-directed vs. hierarchical left-to-right chain layout).
- **Supernodes (> 1,000 counterparties):** cluster background nodes; render top 25 by GNN attribution; show badge `Showing top 25 of 1,240 counterparties (Filtered)` with an "Expand" link.
- **Table alternative:** a keyboard-navigable list of nodes and edges, toggled from the canvas controls.

---

## 10. Motion

| Interaction | Duration | Easing | Notes |
| :--- | :--- | :--- | :--- |
| Hover / focus transitions | 120ms | ease-out | Color and border only |
| Tab underline slide | 200ms | `cubic-bezier(0.2, 0, 0, 1)` | |
| Panel content swap | 180ms | ease-out | Fade + 4px translate |
| Gauge fill / count-up | 700ms | ease-out | |
| Chain pulse / connector dot | 1600ms loop | linear | Direction of fund flow |
| Graph camera focus | 400ms | ease-in-out | Triggered by stepper selection |
| Copy confirmation | 1500ms | n/a | Icon becomes check, then reverts |
| Skeleton shimmer | 1400ms loop | linear | `graphite-700` ↔ `graphite-600` |

**Reduced motion:** when the user prefers reduced motion, stop all loops, jump count-ups to final values, make camera moves instant, and replace pulses with static emphasis (thicker line / solid color).

---

## 11. States & Edge Cases

| State | Behavior |
| :--- | :--- |
| **Initial load** | Skeleton tiles/panels with shimmer; empty gauge arc. First meaningful paint target < 1.5s. |
| **No account selected** | Centered line-art of three linked nodes (one lime), headline "Search an account to begin," preset chips below. |
| **Cold start (isolated / new account)** | Info callout: `Limited Subgraph Connectivity: GNN falls back to 1-hop degree stats. Multi-hop chains: None.` Graph shows a sparse neighborhood with a dashed ring where hop 2 would be. |
| **Invalid account ID** | Input turns critical, toast: `Account ID not found in 515,080 index. Suggested: 811C2F680, 100428660.` Suggestions are clickable chips. |
| **Sub-threshold score with active chain** | Tier = ELEVATED REVIEW (amber) plus callout: `Model assigned borderline score, but account participates in an active layering chain.` Gauge needle sits just below the lime threshold tick. |
| **Supernode** | See Section 9.4. |
| **No chains found** | Chain Stepper shows empty state: `No multi-hop chains detected for this account.` with a muted `GitCommit` icon. |
| **Single chain / many chains** | Selector disabled with single option; with many, selector menu is scrollable (max height 320px). |
| **Timestamp violation** | Warning icon on the affected hop (Section 7.4.4). |
| **API failure** | Panel-level error card: message, **Retry** (secondary button), request ID in mono. Other panels keep working. |
| **Slow response** | Show skeleton after 200ms; never block the entire page. |

### 11.1 Risk Tier Derivation (for UI logic)

| Condition | Tier |
| :--- | :--- |
| Probability ≥ threshold **and** critical signals (e.g., sink role or very high score) | `CRITICAL RISK` |
| Probability ≥ threshold | `HIGH RISK` |
| Probability < threshold **and** participates in an active chain | `ELEVATED REVIEW` |
| Otherwise | `STANDARD` |

> The backend returns `risk_tier`; the UI renders it as provided and must not recompute tiers client-side except for display fallback.

---

## 12. Accessibility

- All text meets **WCAG 2.1 AA** (4.5:1 normal text, 3:1 large text and UI components). Key data on graphite meets AAA.
- Visible focus on every interactive element: 2px lime ring with 2px offset (graphite ring + lime halo in light mode).
- Risk is never conveyed by color alone (icon + label + color).
- Charts expose text alternatives (`aria-label` summaries); the graph has a table alternative.
- Keyboard shortcuts: `/` focus search, `1`–`5` switch tabs, `G` focus graph, `Esc` clear selection. Shortcuts are disabled while typing in inputs.
- Live updates use polite live regions.
- Touch targets ≥ 44px on touch devices.
- Respect reduced-motion and, where feasible, high-contrast preferences.
- Tooltips are available on focus, not only hover.
- Form errors are announced and associated with their inputs.

---

## 13. Responsive Behavior

| Width | Layout |
| :--- | :--- |
| ≥ 1440 | Full split view; 4-tile telemetry row |
| 1024–1439 | 50/50 split; telemetry tiles compress to icon + value |
| 768–1023 | Single column; graph in its own tab; banner actions collapse into a menu |
| < 768 | Read-only triage view: banner, gauge, drivers, vertical stepper, SAR text; graph opens as full-screen sheet |

Graph supports pinch-zoom and two-finger pan on touch.

---

## 14. Design Tokens (Framework-Neutral Contract)

### 14.1 CSS Custom Properties (reference implementation)

```css
:root {
  /* Brand */
  --lime-50:  #F6FFE0;
  --lime-200: #E4FFA6;
  --lime-400: #CDFF5E;
  --lime-500: #B6FF2E;
  --lime-600: #98DB14;
  --lime-700: #75AB0B;
  --lime-glow: rgba(182, 255, 46, 0.16);

  /* Graphite */
  --graphite-950: #14161B;
  --graphite-900: #1A1D24;
  --graphite-800: #23262F;
  --graphite-700: #2D313C;
  --graphite-600: #3A3F4C;
  --graphite-500: #565C6B;
  --graphite-400: #7F8696;
  --graphite-300: #A9AEBB;
  --graphite-200: #D1D4DC;
  --graphite-100: #E8EAEF;
  --graphite-50:  #F5F6F8;

  /* Risk (dark) */
  --risk-critical: #FF4D5E;
  --risk-high:     #FF8A3D;
  --risk-elevated: #FFC53D;
  --risk-standard: #2DD4BF;
  --info:          #60A5FA;
  --risk-critical-tint: rgba(255, 77, 94, 0.14);
  --risk-high-tint:     rgba(255, 138, 61, 0.14);
  --risk-elevated-tint: rgba(255, 197, 61, 0.14);
  --risk-standard-tint: rgba(45, 212, 191, 0.14);
  --info-tint:          rgba(96, 165, 250, 0.14);

  /* Semantic roles */
  --bg:             var(--graphite-950);
  --surface:        var(--graphite-800);
  --surface-raised: var(--graphite-700);
  --surface-nav:    var(--graphite-900);
  --border:         var(--graphite-600);
  --text:           #F8FAFC;
  --text-secondary: var(--graphite-300);
  --text-muted:     var(--graphite-400);
  --accent:         var(--lime-500);
  --accent-hover:   var(--lime-400);
  --accent-pressed: var(--lime-600);
  --on-accent:      var(--graphite-800);
  --focus-ring:     var(--lime-500);

  /* Shape & spacing */
  --radius-panel:   12px;
  --radius-control: 10px;
  --radius-tooltip: 8px;
  --radius-pill:    999px;
  --space-1: 4px;  --space-2: 8px;  --space-3: 12px; --space-4: 16px;
  --space-6: 24px; --space-8: 32px; --space-12: 48px; --space-16: 64px;

  /* Type */
  --font-ui:   'Inter', system-ui, sans-serif;
  --font-mono: 'JetBrains Mono', ui-monospace, monospace;

  /* Motion */
  --dur-fast: 120ms;
  --dur-base: 180ms;
  --dur-tab:  200ms;
  --dur-focus: 400ms;
  --dur-gauge: 700ms;
  --ease-out: cubic-bezier(0.2, 0, 0, 1);
}

:root[data-theme='light'] {
  --bg:             var(--graphite-50);
  --surface:        #FFFFFF;
  --surface-raised: var(--graphite-50);
  --surface-nav:    #FFFFFF;
  --border:         var(--graphite-100);
  --text:           var(--graphite-800);
  --text-secondary: var(--graphite-500);
  --text-muted:     var(--graphite-400);
  --focus-ring:     var(--graphite-800);

  --risk-critical: #DC2626;
  --risk-high:     #EA580C;
  --risk-elevated: #D97706;
  --risk-standard: #0D9488;
  --info:          #2563EB;
}

@media (prefers-reduced-motion: reduce) {
  :root { --dur-fast: 0ms; --dur-base: 0ms; --dur-tab: 0ms; --dur-focus: 0ms; --dur-gauge: 0ms; }
}
```

### 14.2 Token Naming Convention

- Primitive tokens: `{family}-{step}` (e.g., `graphite-700`, `lime-500`).
- Semantic tokens: role-based (`--surface`, `--text-muted`, `--accent`). **Components consume semantic tokens**, which map to primitives per theme.
- Any framework (utility CSS, CSS-in-JS, component libraries, native styling) must map to these same names and values.

### 14.3 Portable Token Export (JSON)

```json
{
  "color": {
    "lime":     { "500": "#B6FF2E", "400": "#CDFF5E", "600": "#98DB14", "700": "#75AB0B" },
    "graphite": { "950": "#14161B", "900": "#1A1D24", "800": "#23262F", "700": "#2D313C",
                  "600": "#3A3F4C", "500": "#565C6B", "400": "#7F8696", "300": "#A9AEBB",
                  "200": "#D1D4DC", "100": "#E8EAEF", "50": "#F5F6F8" },
    "risk": {
      "critical": { "dark": "#FF4D5E", "light": "#DC2626" },
      "high":     { "dark": "#FF8A3D", "light": "#EA580C" },
      "elevated": { "dark": "#FFC53D", "light": "#D97706" },
      "standard": { "dark": "#2DD4BF", "light": "#0D9488" }
    },
    "info": { "dark": "#60A5FA", "light": "#2563EB" }
  },
  "radius": { "panel": 12, "control": 10, "tooltip": 8, "pill": 999 },
  "spacing": [4, 8, 12, 16, 24, 32, 48, 64],
  "font": { "ui": "Inter", "mono": "JetBrains Mono" }
}
```

---

## 15. Data Contracts (UI-Relevant Summary)

The UI consumes these shapes. Field names are stable; do not rename them in the UI layer.

### 15.1 Investigation — `GET /api/v1/accounts/{account_id}/investigation`

| Field | Type | UI usage |
| :--- | :--- | :--- |
| `account_id` | string | Banner ID |
| `risk_tier` | string | Risk badge, banner border color |
| `recommended_action` | string | Banner action text |
| `gnn_assessment.mule_probability` | number 0–1 | Gauge |
| `gnn_assessment.threshold` | number | Gauge threshold tick |
| `gnn_assessment.decision_status` | string | Flag state |
| `gnn_assessment.feature_attributions[]` | `{feature, score, raw_value}` | Attribution bars |
| `gnn_assessment.top_counterparty_flows[]` | `{direction, counterparty, importance_score}` | Counterparty ranking |
| `chain_profile.is_in_chain` | boolean | Chain role / empty state |
| `chain_profile.primary_role` | string | Role pill |
| `chain_profile.active_chains[]` | `{hops, accounts[], initial_amount, final_amount, retention_pct, duration_hours, steps[]}` | Chain Stepper |
| `chain_profile.active_chains[].steps[]` | `{from, to, timestamp, amount}` | Stepper hops |
| `sar_narrative` | string | SAR box |

**Derived values (computed in the UI for display only):**
- Decay amount = `initial_amount − final_amount`
- Decay % = `decay amount ÷ initial_amount`
- Per-hop dwell = difference between consecutive `timestamp` values
- Monotonic = every `timestamp[i] ≥ timestamp[i−1]`

### 15.2 Subgraph — `GET /api/v1/accounts/{account_id}/subgraph?radius=2`

| Field | UI usage |
| :--- | :--- |
| `nodes[].id, role, size` | Node identity, role styling, base size |
| `edges[].source, target, amount, timestamp` | Edge geometry, tooltips |
| `edges[].in_chain` | Chain-flow styling |
| `edges[].importance` | High-attribution styling (> 0.6) |

**Rule:** the UI derives node/edge colors from `role`, `in_chain`, and `importance` using tokens. It must not use color values from the payload.

---

## 16. Content & Microcopy

- Tone: precise, neutral, factual. No exclamation marks, no humor, no emoji.
- Labels: uppercase `caption` for field labels; sentence case for messages.
- Role labels (verbatim): `SOURCE / INITIATOR`, `INTERMEDIARY MULE`, `CASH-OUT SINK`.
- Summary labels (verbatim): `INITIAL INFLOW`, `FINAL OUTFLOW`, `FEE HAIRCUT (DECAY)`, `TOTAL SPAN`.
- Errors state what happened and what to do next; include IDs/request IDs in mono.
- Never describe an account as "guilty" or "criminal"; use "flagged," "high risk," "requires review."

---

## 17. Acceptance Checklist

A UI change is complete only when **all** applicable items pass.

**System**
- [ ] No hardcoded hex values, pixel magic numbers, or font names outside the token files.
- [ ] No native-styled controls visible in dark mode.
- [ ] Works in both dark and light themes via the token mapping.
- [ ] No changes to data fetching, props contracts, or business logic unless requested.

**Color & Brand**
- [ ] At most one lime-filled element per region.
- [ ] Lime never used as text/lines on light backgrounds.
- [ ] Content on lime fills is graphite.
- [ ] Every risk indicator has icon + label + color.

**Typography & Data**
- [ ] All numbers, IDs, timestamps, durations render in the mono font with tabular numerals.
- [ ] Formatting matches Section 3.2.

**Layout & States**
- [ ] 4px spacing grid respected; panel radius 12px, control radius 10px.
- [ ] Loading, empty, error, cold-start, invalid-ID, supernode, and sub-threshold states implemented.
- [ ] Top 3 drivers and top 2 counterparty links visible without scrolling at 1440×900.
- [ ] SAR export reachable in ≤ 2 clicks.

**Chain Stepper**
- [ ] Hops read as a clear top-to-bottom flow with a visible connector.
- [ ] Hop metadata sits on the connector, not inside the account cards.
- [ ] Decay chip, dwell time, and monotonic check display correctly.
- [ ] Summary strip shows four tiles with correct colors.
- [ ] Selecting a step focuses the matching node/edge in the graph.

**Accessibility & Motion**
- [ ] Contrast meets WCAG 2.1 AA.
- [ ] Focus ring visible on all interactive elements; keyboard-only flow works.
- [ ] Reduced-motion behavior verified.
- [ ] Charts and graph have text/table alternatives.

**Responsive**
- [ ] Verified at 1440, 1024, 768, and 390px widths.

---

## 18. Technology Guidance (Non-Binding)

The design is framework-independent. Any stack is acceptable if it can honor the token contract and component specs.

| Concern | Guidance |
| :--- | :--- |
| UI framework | Any component-based framework or server-rendered stack |
| Styling | CSS variables as the theming layer; utility CSS, CSS modules, or CSS-in-JS may sit on top |
| Graph rendering | Canvas/WebGL-based force-graph library (e.g., Cytoscape.js, G6, force-graph) |
| Charts | Any library that supports horizontal bars, gauges, and PR curves with full style control |
| Icons | Lucide |
| Fonts | Inter and JetBrains Mono, with system fallbacks |

If the chosen stack restricts styling of a control (for example, a platform-native select), replace it with a custom accessible component rather than compromising the design.

---

## 19. Change Log

| Version | Date | Notes |
| :--- | :--- | :--- |
| 1.0 | 2026-10-23 | Initial unified design document: brand palette (Lime Spark `#B6FF2E`, Graphite `#23262F`), full token contract, component specs, Chain Stepper redesign, graph spec, accessibility and acceptance criteria. |

*End of DESIGN.md*