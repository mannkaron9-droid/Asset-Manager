# Workspace

## Overview

pnpm workspace monorepo — NBA Elite Betting Bot with a React dashboard, Express API, and Python bot worker.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)
- **Frontend**: React + Vite, Recharts, Tailwind CSS, framer-motion

## Structure

```text
artifacts-monorepo/
├── artifacts/
│   ├── api-server/         # Express API server
│   └── betting-dashboard/  # React dashboard (dark sports theme)
├── bot/
│   ├── bot.py              # Python NBA betting bot (main orchestrator)
│   ├── game_script.py      # 4D game script classifier + role assigner
│   ├── decision_engine.py  # 7-step Edge-Fade decision engine
│   ├── slip_builder.py     # Builds 7-leg Edge-Fade slips
│   └── telegram_formatter.py # Story-based Telegram message formatter
├── bets.json               # Persistent bet tracking
├── bot_status.json         # Bot last-run metadata
├── lib/
│   ├── api-spec/           # OpenAPI spec + Orval codegen config
│   ├── api-client-react/   # Generated React Query hooks
│   ├── api-zod/            # Generated Zod schemas from OpenAPI
│   └── db/                 # Drizzle ORM schema + DB connection
├── scripts/
├── pnpm-workspace.yaml
├── tsconfig.base.json
├── tsconfig.json
└── package.json
```

## API Endpoints

All routes are under `/api`:

- `GET /healthz` — health check
- `GET /bets` — all tracked bets
- `GET /bets/stats` — aggregated stats (win rate, ROI, bankroll history)
- `GET /bot/status` — bot last run + picks today
- `POST /bot/run` — manually trigger bot scan

## Bot Engine — Edge-Fade 7 System (`bot/`)

### Core modules:

| File | Purpose |
|---|---|
| `bot/bot.py` | Main bot loop, data fetching, DB, Telegram |
| `bot/game_script.py` | 4-dimension game script analysis + role assignment |
| `bot/decision_engine.py` | Full 7-step decision engine + EV + slip validation |
| `bot/slip_builder.py` | Orchestrates engine → builds Edge-Fade 7 slip |
| `bot/telegram_formatter.py` | Story-based Telegram message formatter |

### The 7-step decision engine:

1. **Juice Test** — ≤-180 RED, -150/-179 YELLOW, -110/+150 GREEN
2. **Public Pressure Check** — high public % + juiced line → FADE candidate
3. **Game Script Fit** — pace (GRIND/MID/HIGH) × flow (BLOWOUT/CLOSE/MODERATE) × offense × defense
4. **Role Assignment** — SCORER / PLAYMAKER / REBOUNDER / TRAIL_PG per game
5. **EV Check** — true probability > implied probability → +EV → include
6. **Slip Validation** — 6 checks: fade integrity, benefactor connection, role diversity, juice, script alignment, hidden trap
7. **Slip Grader** — A (elite) / B (strong) / C (weak) / D (don't send)

### Slip structure (Edge-Fade 7):
- Legs 1-2: FADES (public stars with juiced, inflated lines)
- Legs 3-7: BENEFACTORS (secondary players who inherit production)
- 1 stat per player, stat diversity (pts + reb + ast)
- Target payout: +250 to +400

### Key principle: Stats don't disappear — they shift.
When Embiid underperforms, Maxey scores more and Harris rebounds more.
The engine maps these relationships automatically.

### Running:
- Run once: `python3 bot/bot.py --once`
- Run scheduled loop: `python3 bot/bot.py`
- Edge-Fade 7 fires daily between 2 PM and 9 PM ET

## Required Secrets

- `BOT_TOKEN` — Telegram bot token
- `ODDS_API_KEY` — The Odds API key
- `BDL_API_KEY` — BallDontLie API key (player stats)
- `FREE_CHANNEL` — Telegram free channel ID (default: -1003721218569)
- `VIP_CHANNEL` — Telegram VIP channel ID (default: -1003858740173)
- `ADMIN_ID` — Telegram admin user ID (default: 6723106141)
- `STRIPE_SECRET_KEY` — Stripe secret key for VIP subscriptions
- `STRIPE_PRICE_ID` — Stripe price ID for $29/month VIP plan

## Deployment

- **Railway service**: `astonishing-kindness`
- **GitHub repo**: `mannkaron9-droid/Asset-Manager` (branch: `main`)
- Railway auto-deploys on every push to `main`

## Data Files

- `bets.json` — persists all picks and results at project root
- `bot_status.json` — tracks last run timestamp + daily pick count

## TypeScript & Composite Projects

Every package extends `tsconfig.base.json` which sets `composite: true`. Run `pnpm run typecheck` from the root to validate the full dependency graph.
