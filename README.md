# Bludot Pipeline v2.0

Automated business record matching pipeline with LLM-assisted matching,
web-based review UI, SQLite database, and Prefect orchestration.

---

## What Changed from v1

| Before | After |
|--------|-------|
| Manual Excel fuzzy lookup (Steps 7, 13) | Fully automated second-pass matcher |
| Manual review in Excel sheets | Web UI with keyboard shortcuts |
| `city_details.py` hand-edited per city | Column mapper in UI, saved to DB |
| Run steps manually one-by-one | Prefect DAG, one command per city |
| ~20 intermediate Excel files | Single SQLite database |
| Binary True/False match logic | Rule filter → Gemini LLM → Human (only UNCERTAIN) |

---

## Architecture

```
City Sheet + Bludot Export
        │
        ▼
  FastAPI + React UI
  ┌─────────────────────────────────┐
  │  Upload → Column Map → Run      │
  │  Pipeline Status Dashboard      │
  │  Human Review Queue             │
  └─────────────────────────────────┘
        │
        ▼
  Prefect Pipeline DAG
  Step 0  → LSH Dedup
  Step 1  → Reformat + Merge
  Step 2  → Candidates + Gemini LLM
  [GATE]  → Human Review (UNCERTAIN only)
  Step 4  → Split Records
  Step 4.1→ Second-pass Matcher + LLM
  [GATE]  → Human Review (if needed)
  Step 5  → Final Business/Custom Sheets
  Step 6  → Contact Sheet
        │
        ▼
  SQLite DB  (upgrades to PostgreSQL via one .env change)
```

### 3-Stage Matching Engine

**Stage 1 — Rule Filter** (fast, zero API cost)
- Street numbers both present + different → DEFINITE_NO_MATCH
- Names completely dissimilar + addresses different → DEFINITE_NO_MATCH
- Everything else → CANDIDATE

**Stage 2 — Gemini 2.5 Flash** (free tier, only for CANDIDATE pairs)
- Returns: MATCH → auto-accept | NO_MATCH → auto-reject | UNCERTAIN → human queue

**Stage 3 — Human Review UI** (only UNCERTAIN pairs)
- Side-by-side record view with scores and LLM reasoning
- Keyboard shortcuts: A = accept, R = reject, ↑↓ = navigate
- Batch submit decisions

---

## Setup

### Prerequisites
- Python 3.11+
- Node.js 20+
- Google Gemini API key (free at https://aistudio.google.com)

### 1. Backend

```bash
cd bludot_pipeline/backend
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Create `.env` in project root:
```
GEMINI_API_KEY=your_key_here
DATABASE_URL=sqlite:///./bludot_pipeline.db   # default, no change needed
```

Start the API:
```bash
uvicorn backend.main:app --reload --port 8000
```

### 2. Frontend

```bash
cd bludot_pipeline/frontend
npm install
npm run dev
```

Open http://localhost:3000

### 3. (Optional) Docker — everything at once

```bash
cp .env.example .env
# Add your GEMINI_API_KEY to .env
docker-compose up
```

- UI:      http://localhost:3000
- API:     http://localhost:8000/docs
- Prefect: http://localhost:4200

---

## Processing a City

1. **New City** → Upload city sheet + bludot export
2. **Map Columns** → Drag/select which source columns map to our schema
3. **Start Pipeline** → Runs automatically through Steps 0–2
4. **Review Queue** → Review only the UNCERTAIN pairs (typically 10–30%)
5. **Resume** → Pipeline continues through Steps 4–6
6. **Download** → Final Excel sheets ready in `cities_and_counties/{name}/results/output/`

---

## Switching to PostgreSQL

Change one line in `.env`:
```
DATABASE_URL=postgresql://user:password@localhost:5432/bludot_pipeline
```

No code changes needed — SQLAlchemy handles everything.

---

## Project Structure

```
bludot_pipeline/
├── backend/
│   ├── main.py                    # FastAPI app + all routes
│   ├── db/
│   │   ├── models.py              # SQLAlchemy ORM models
│   │   └── session.py             # DB engine + session factory
│   ├── core/
│   │   ├── rule_filter.py         # Stage 1: rule-based pre-filter
│   │   ├── llm_judge.py           # Stage 2: Gemini via LangGraph
│   │   └── matching_orchestrator.py  # Ties all 3 stages together
│   ├── pipeline/
│   │   └── pipeline.py            # Prefect DAG
│   └── services/
│       └── export_service.py      # DB → Excel for steps 5/6
├── frontend/
│   └── src/
│       ├── App.jsx
│       ├── pages/
│       │   ├── CitiesPage.jsx     # Cities list + status
│       │   ├── NewCityPage.jsx    # Upload + column mapping wizard
│       │   ├── CityDetailPage.jsx # Pipeline dashboard
│       │   └── ReviewPage.jsx     # Human review UI
│       └── hooks/
│           └── useApi.js          # API client
├── docker-compose.yml
└── README.md
```

---

## Gemini Free Tier

Gemini 2.5 Flash free tier (as of 2025):
- 15 requests/minute
- 1,500 requests/day  
- 1M tokens/day

At ~200 tokens per pair, that's ~5,000 pairs/day free.
For 10–20 cities/month this is more than sufficient.

The pipeline rate-limits itself to 3 req/sec and retries on 429.
