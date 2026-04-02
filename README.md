# Bludot Pipeline

Automated business record matching pipeline. Matches city business license records against the Bludot database.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Setup & Installation](#setup--installation)
3. [Environment Variables](#environment-variables)
4. [Running the App](#running-the-app)
5. [Full Pipeline — Step by Step](#full-pipeline--step-by-step)
   - [Step 0 — Deduplication (LSH)](#step-0--deduplication-lsh)
   - [Human Review 0 — Cluster Review](#human-review-0--cluster-review-soft)
   - [Step 1 — Reformat + Merge Columns](#step-1--reformat--merge-columns)
   - [Step 2 — City vs Bludot Matching](#step-2--city-vs-bludot-matching)
   - [Human Review 1 — Match Review Pass 1](#human-review-1--match-review-pass-1-soft)
   - [Step 3 — Split Records](#step-3--split-records)
   - [Step 4 — Second-Pass Matching](#step-4--second-pass-matching)
   - [Human Review 2 — Match Review Pass 2](#human-review-2--match-review-pass-2-soft)
   - [Step 5 — Generate Final Sheets](#step-5--generate-final-sheets)
6. [File Outputs](#file-outputs)
7. [Frontend Pages](#frontend-pages)
8. [API Endpoints](#api-endpoints)
9. [Key Design Decisions](#key-design-decisions)
10. [Changelog — All Changes Made](#changelog--all-changes-made)
11. [Troubleshooting](#troubleshooting)

---

## Project Structure

```
bludot_pipeline/
├── .env                               ← GROQ_API_KEY goes here
├── docker-compose.yml
├── README.md
├── backend/
│   ├── core/
│   │   ├── step0_dedup.py             ← Step 0: LSH + LLM dedup + bludot concat
│   │   ├── step1_format.py            ← Step 1: Reformat + merge_columns()
│   │   ├── matching_orchestrator.py   ← Step 2 + 4: Rule filter + LLM matching
│   │   ├── llm_judge.py               ← Groq LLM calls (1 batched call per step)
│   │   ├── rule_filter.py             ← Pre-LLM rule filter
│   │   └── step5_6_output.py          ← Step 5: Final Excel output
│   ├── db/
│   │   ├── models.py                  ← SQLAlchemy ORM models
│   │   └── session.py                 ← DB engine + init_db()
│   ├── pipeline/
│   │   └── pipeline.py                ← Orchestrator — runs all steps in order
│   ├── main.py                        ← FastAPI app + all API endpoints
│   ├── requirements.txt
│   └── uploads/                       ← All city data + results live here
│       └── {CityName}/
│           ├── {city_sheet}.xlsx
│           ├── {bludot_export}.xlsx
│           └── results/
│               ├── city_data/
│               │   ├── manual_dedup_records.xlsx
│               │   └── de_duplication_merged.xlsx
│               ├── bludot_data/
│               │   └── bludot_concatenated_records.xlsx
│               └── output/
│                   ├── final_result/
│                   │   ├── additional_city_records_for_{city}.xlsx
│                   │   └── additional_bludot_records_for_{city}.xlsx
│                   └── final_output/
│                       ├── {city}_Business_Matched_Records.xlsx
│                       └── Additional_Matched_Records_Of_{city}.xlsx
└── frontend/
    └── src/
        ├── App.jsx
        ├── App.css
        ├── main.jsx
        ├── hooks/useApi.js
        └── pages/
            ├── CitiesPage.jsx
            ├── NewCityPage.jsx
            ├── CityDetailPage.jsx
            ├── ReviewPage.jsx
            ├── ClusterReviewPage.jsx
            ├── MatchesPage.jsx
            └── DedupResultsPage.jsx
```

---

## Setup & Installation

### Prerequisites
- Docker + Docker Compose
- Groq API key — free at https://console.groq.com

### First Time Setup

```bash
# 1. Navigate to project folder
cd ~/bludot_pipeline

# 2. Create uploads folder (required — avoids permission error on startup)
mkdir -p backend/uploads

# 3. Create .env file with your Groq key
echo "GROQ_API_KEY=gsk_your_key_here" > .env

# 4. Fix any locked folders (if you had an old version running as root)
sudo chown -R $USER:$USER backend/uploads/

# 5. Build and start
docker-compose build --no-cache backend
docker-compose up
```

- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `GROQ_API_KEY` | Yes | Groq API key from console.groq.com (free tier: 14,400 req/day) |
| `DATABASE_URL` | Auto-set | `sqlite:////app/backend/bludot_pipeline.db` |

---

## Running the App

```bash
# Start everything
docker-compose up

# Restart backend only (after copying new .py files)
docker-compose restart backend

# Full rebuild (required after changes to requirements.txt)
docker-compose build --no-cache backend && docker-compose up

# Watch logs
docker-compose logs -f backend
```

---

## Full Pipeline — Step by Step

> All **Human Review** gates are **SOFT** — if nothing needs review the pipeline continues automatically without stopping.

---

### Step 0 — Deduplication (LSH)

**Backend file:** `backend/core/step0_dedup.py`
**Original scripts:** `step0_manual_dedup_records_LSH.py` + `step1.1_bludot_sheets_concat_&_city_fuzzy.py`

**What it does:**
1. Reads the city sheet uploaded via the UI
2. Runs address normalization — strips unit suffixes (`# 1/2`, `STE`, `APT`, `UNIT`, `PMB`, etc.) before comparison so `1011 W 4TH ST` and `1011 W 4TH ST # 1/2` group together correctly
3. Runs LSH (Locality Sensitive Hashing) + TF-IDF to cluster duplicate records based on Business Name + Address1
4. Assigns a `cluster id` to every record
5. **LLM verification of intra-cluster pairs** (Groq, 1 batched call):
   - Same name + same normalized address → **auto-merge** (no LLM)
   - Different street numbers in same cluster → **auto-split** (no LLM)
   - One/both addresses blank → **LLM decides**
   - Similar address (unit suffix difference like `# 1/2`) → **LLM decides**
   - LLM uncertain → **human cluster review**
6. **Cross-cluster near-miss scan** — finds records in different clusters with ≥ 92% name similarity
7. **LLM verification of near-miss pairs** (1 batched call):
   - Same normalized address → **auto-merge** (no LLM)
   - Ambiguous → **LLM decides**
   - LLM uncertain → **human cluster review**
8. Concatenates bludot export (Business + Custom + Contact sheets) into one table

**Output files:**
- `results/city_data/manual_dedup_records.xlsx` — deduplicated city records with `cluster id`
- `results/bludot_data/bludot_concatenated_records.xlsx` — merged bludot sheets

**What to verify after this step:**
- Open `manual_dedup_records.xlsx`
- Check that records with the same name + address are in the same `cluster id`
- Check that records with the same name but different addresses are in **different** `cluster id` values
- If a `cluster id` is wrong — use the **Dedup Results** page in the UI (`/city/{id}/dedup-results`) to change any LLM decision, then save

---

### Human Review 0 — Cluster Review *(soft)*

**UI page:** `/city/{id}/cluster-review`

Shows near-miss pairs the LLM could not decide on. Human actions:
- **Merge All** — confirm duplicates, merge into same cluster
- **Keep Separate** — confirm different businesses

> If 0 uncertain pairs → pipeline continues automatically to Step 1

---

### Step 1 — Reformat + Merge Columns

**Backend file:** `backend/core/step1_format.py`
**Original scripts:** `step1.2_city_de_duplication.py` + `step1.3_deduplication_merge.py`

**What it does:**
1. Reads `manual_dedup_records.xlsx`
2. Deduplicates by `cluster id` — keeps the most complete row per cluster (fewest NaN values)
3. Merges numbered columns using the rules from `step1.3`:
   - **Business Name** (`_1`, `_2`, `_3`…) → picks the **longest** non-empty value
   - **Address1, Address2, City, State, Zipcode, Website** → takes the **first non-empty** value
   - **Phonenumber** → collects all unique phone numbers, deduplicates, joins with `, `
4. Adds `city_index` column (required for matching)

**Output files:**
- `results/city_data/de_duplication_merged.xlsx` — clean merged city records ready for matching

**What to verify:**
- Open `de_duplication_merged.xlsx`
- Check Business Name column has the correct/longest name
- Check phone numbers are deduplicated
- Note which columns contain business names (e.g. `Business Name`, `DBA Name`, `Legal Name`) — you will need these for Step 2 column mapping

---

### Step 2 — City vs Bludot Matching

**Backend file:** `backend/core/matching_orchestrator.py`
**Original scripts:** `step2_city___bludot_fuzzy_match.py`

**What it does:**

Matches every city record against every bludot record using a two-stage approach:

**Stage A — Pre-LLM rule filter (auto-decides ~80% of pairs, no API call):**

| Case | Decision |
|---|---|
| Both street numbers present AND different | AUTO_REJECT |
| Name similarity < 50% | AUTO_REJECT |
| Both addresses blank + name ≥ 90% similar | AUTO_MATCH |
| Street numbers match + name ≥ 88% + address ≥ 75% | AUTO_MATCH |
| Exact name match + one address blank | AUTO_MATCH |

**Stage B — LLM (Groq, 1 batched API call for all remaining ambiguous pairs):**
- All ambiguous pairs sent together in a single call
- LLM returns MATCH / NO_MATCH / UNCERTAIN per pair
- MATCH → confirmed match
- UNCERTAIN → goes to human review queue

**Column mapping:**
When creating a new city in the UI, you map city sheet columns to the standard schema (Business Name, Address1, DBA Name, etc.). If the city has multiple name columns (DBA, Legal Name, Registered Name) or multiple address columns (Street Address, Mailing Address), map all of them — the matching engine considers all mapped name and address columns.

**Output in DB:**
- All `AUTO_MATCH` records stored with `final_decision = AUTO_MATCH`
- All `AUTO_REJECT` records stored with `final_decision = AUTO_NO_MATCH`
- All `UNCERTAIN` records stored with `final_decision = NEEDS_REVIEW`

---

### Human Review 1 — Match Review Pass 1 *(soft)*

**UI page:** `/city/{id}/review`

Shows UNCERTAIN match pairs side-by-side:
- Left side: city record (business name, address)
- Right side: bludot record (name, address, UUID)
- LLM reason shown below

Human actions per pair:
- ✓ **Accept** — confirm as a match
- ✗ **Reject** — mark as not a match
- Bulk submit all decisions at once

> If 0 uncertain pairs → pipeline continues automatically to Step 3

---

### Step 3 — Split Records

**Backend file:** `backend/pipeline/pipeline.py` (`_run_step3`)
**Original scripts:** `step4_final_matched_sheet.py` (`separate_main_spreadsheet`)

**What it does:**
- Identifies all city records matched in pass 1 (AUTO_MATCH + HUMAN_ACCEPTED)
- Identifies all bludot records matched in pass 1
- Prepares the residual records (everything NOT yet matched) for second-pass matching
- The actual split files are written in Step 5

---

### Step 4 — Second-Pass Matching

**Backend file:** `backend/core/matching_orchestrator.py` (pass=2)
**Original scripts:** `step4.1_final_matched_sheet_after_fuzzy_lookup.py`

**What it does:**
- Runs the same two-stage rule + LLM matching as Step 2
- But only on records that were **NOT** matched in pass 1
- Uses `match_pass=2` so pass 1 and pass 2 decisions are kept separate in DB
- LLM results from pass 2 are stored separately and shown in UI as Pass 2

---

### Human Review 2 — Match Review Pass 2 *(soft)*

**UI page:** `/city/{id}/review?pass=2`

Same review UI as pass 1. Shows uncertain pairs from the second matching pass.

> If 0 uncertain pairs → pipeline continues automatically to Step 5

---

### Step 5 — Generate Final Sheets

**Backend file:** `backend/core/step5_6_output.py`
**Original scripts:** `step4_final_matched_sheet.py` + `step4.1_final_matched_sheet_after_fuzzy_lookup.py`

**What it does:**
1. Reads all confirmed matches from DB (AUTO_MATCH + HUMAN_ACCEPTED, both passes)
2. Re-orders matched city and bludot records to align row-by-row (same order as matched pairs)
3. Creates combined sheet with these columns first:
   - UUID, City Business Name, Bludot Name, City Address, Bludot Address, Match Pass, Decision, LLM Reason
   - Then all city record columns
   - Then all bludot record columns
4. Writes additional (unmatched) city records separately
5. Writes additional (unmatched) bludot records separately

**Output files:**
- `results/output/final_output/{city}_Business_Matched_Records.xlsx` — **main output** — all matched pairs side by side
- `results/output/final_output/Additional_Matched_Records_Of_{city}.xlsx` — match metadata (city_index, bludot_index, decision, reason)
- `results/output/final_result/additional_city_records_for_{city}.xlsx` — city records not matched to any bludot record
- `results/output/final_result/additional_bludot_records_for_{city}.xlsx` — bludot records not matched to any city record

**How to access output files:**
After pipeline completes, the files are at:
```
~/bludot_pipeline/backend/uploads/{CityName}/results/output/
```

If folders appear locked, run:
```bash
sudo chown -R $USER:$USER ~/bludot_pipeline/backend/uploads/
```

---

## File Outputs

```
uploads/{CityName}/results/
├── city_data/
│   ├── manual_dedup_records.xlsx         ← After Step 0 (LSH dedup)
│   └── de_duplication_merged.xlsx        ← After Step 1 (column merge)
├── bludot_data/
│   └── bludot_concatenated_records.xlsx  ← After Step 0 (bludot concat)
└── output/
    ├── final_result/
    │   ├── additional_city_records_for_{city}.xlsx    ← Unmatched city records
    │   └── additional_bludot_records_for_{city}.xlsx  ← Unmatched bludot records
    └── final_output/
        ├── {city}_Business_Matched_Records.xlsx       ← MAIN OUTPUT
        └── Additional_Matched_Records_Of_{city}.xlsx  ← Match index
```

---

## Frontend Pages

| Page | URL | Description |
|---|---|---|
| Cities List | `/` | All cities with pipeline status |
| New City | `/city/new` | Upload files + configure column mapping |
| City Detail | `/city/{id}` | Pipeline tracker, stats, files, buttons |
| Dedup Results | `/city/{id}/dedup-results` | View + edit all LLM dedup decisions |
| Cluster Review | `/city/{id}/cluster-review` | Review uncertain near-miss clusters |
| Match Review Pass 1 | `/city/{id}/review` | Side-by-side match review (pass 1) |
| Match Review Pass 2 | `/city/{id}/review?pass=2` | Side-by-side match review (pass 2) |
| Matches Viewer | `/city/{id}/matches` | View all confirmed matched pairs |

---

## API Endpoints

### Cities
| Method | Endpoint | Description |
|---|---|---|
| GET | `/cities/` | List all cities |
| POST | `/cities/` | Create new city |
| GET | `/cities/{id}` | City details |
| GET | `/cities/{id}/status` | Current pipeline step + status |
| GET | `/cities/{id}/stats` | Match counts + breakdown |
| POST | `/cities/{id}/start` | Start pipeline |
| POST | `/cities/{id}/resume` | Resume after human review |

### Column Mapping
| Method | Endpoint | Description |
|---|---|---|
| POST | `/cities/{id}/suggest-mapping` | LLM suggests column mappings |
| POST | `/cities/{id}/column-mapping` | Save column mappings |

### Match Review
| Method | Endpoint | Description |
|---|---|---|
| GET | `/cities/{id}/review` | Get review queue (`?match_pass=2` for pass 2) |
| POST | `/cities/{id}/review/bulk` | Submit bulk decisions |
| POST | `/cities/{id}/review/{candidate_id}` | Submit single decision |

### Dedup Review
| Method | Endpoint | Description |
|---|---|---|
| GET | `/cities/{id}/dedup-review` | Pairs waiting for human decision |
| POST | `/cities/{id}/dedup-review/bulk` | Submit bulk dedup decisions |
| GET | `/cities/{id}/dedup-results` | All dedup pairs + decisions (viewer) |

### Cluster Review
| Method | Endpoint | Description |
|---|---|---|
| GET | `/cities/{id}/cluster-review` | Near-miss cluster groups |
| POST | `/cities/{id}/cluster-review/merge` | Merge clusters |
| POST | `/cities/{id}/cluster-review/keep-separate` | Mark as separate |

### Results
| Method | Endpoint | Description |
|---|---|---|
| GET | `/cities/{id}/matches` | All confirmed matched pairs |

---

## Key Design Decisions

### All LLM calls are batched (1 call per step)
Instead of one API call per pair (which exhausted quotas and hit rate limits), all pairs for a step are sent in a single batched prompt. Groq returns a JSON array of decisions. This means:
- Step 0 dedup = max 2 Groq calls (intra-cluster + cross-cluster near-misses)
- Step 2 matching = 1 Groq call
- Step 4 second-pass = 1 Groq call
- Total per city = max 4 Groq calls

### Pre-LLM rule filter
About 80% of pairs are auto-decided by rules before reaching the LLM:
- Clear duplicates (same name + same address) → auto-merged
- Clear non-duplicates (different street numbers) → auto-rejected
Only genuinely ambiguous pairs (blank addresses, unit suffix differences, partial names) go to Groq.

### All human review gates are soft
No step hard-locks the pipeline. Each gate checks if anything is pending — if not, continues automatically. Human only needs to act when LLM was genuinely uncertain.

### Address normalization
Strips all unit/suite identifiers before comparing:
- `1011 W 4TH ST # 1/2` → `w 4th st`
- `3432 HILLCREST AVE STE 100` → `hillcrest ave`
- `3130 BALFOUR RD STE D PMB 277` → `balfour rd`
- Street ranges collapsed: `2710-3040` → `2710`

---

## Changelog — All Changes Made

### LLM: Gemini → Groq
- Replaced `google-generativeai` + `langgraph` + `langchain-core` with `groq>=0.11.0`
- Removed `prefect` (no longer needed — plain Python orchestration)
- Model: `llama-3.3-70b-versatile` (free tier)
- Added `GROQ_API_KEY` to `.env` and `docker-compose.yml`
- All N pairs → 1 API call instead of N calls

### step0_dedup.py — Dedup improvements
- Added robust address normalization: `# 1/2`, `STE`, `APT`, `UNIT`, `PMB`, `BLDG`, etc. all stripped before LSH comparison
- `DOUGLAS & LINDA OSTROM` at `1011 W 4TH ST` and `1011 W 4TH ST # 1/2` now correctly grouped
- Added LLM verification for intra-cluster suspicious pairs
- Added LLM verification for cross-cluster near-miss pairs
- Decision flow: auto-merge → auto-split → LLM → human review

### step1_format.py — Column merge added
- Integrated full `merge_columns()` logic from original `step1.3_deduplication_merge.py`
- Business Name: picks longest non-empty value across `_1/_2/_3` columns
- Address/City/State/Zip/Website: first non-empty
- Phone: deduplicated unique numbers joined with `, `
- Output: `de_duplication_merged.xlsx` (same filename as original pipeline)

### step5_6_output.py — Fixed 0 matched records
- Was querying wrong field → wrote 0 records to output
- Now correctly reads `AUTO_MATCH` + `HUMAN_ACCEPTED` from DB (both pass 1 + pass 2)
- Reorders records to match pair order (like original `step4.1`)
- Writes combined sheet: UUID + city name + bludot name + addresses as first columns
- Writes `additional_city_records` and `additional_bludot_records` (like original `separate_main_spreadsheet`)

### matching_orchestrator.py — Pre-LLM rule filter
- Added Stage A rule filter that auto-decides ~80% of pairs with no API call
- Only genuinely ambiguous pairs sent to Groq
- All pairs sent in one batched call
- Returns `int` (candidate count) not list — fixed `len()` calls in pipeline.py

### pipeline.py — Full rewrite
- Each step is its own separate function: `_run_step0`, `_run_step1`, `_run_step2`, `_run_step3`, `_run_step4`, `_run_step5`
- All review gates are soft with auto-continue when nothing to review
- Removed `export_service` imports (not needed — step5 handles all file writing)
- Fixed `generate_candidates()` return type (int, not list)
- Pass 2 review correctly detected: `step4_1_review` → URL `?pass=2`

### main.py — Bug fixes
- `/review/bulk` returning 422: route order fixed — `bulk` must come before `/{candidate_id}`
- JSON string keys: fixed `dict[int, bool]` → `dict[str, bool]` in Pydantic models for bulk review
- Added `/cities/{id}/matches` endpoint — returns all confirmed matched pairs
- Added `/cities/{id}/dedup-results` endpoint — returns all dedup pairs with decisions
- Added cluster-review endpoints: `GET`, `POST /merge`, `POST /keep-separate`
- Added `CityRecord` + `BludotRecord` to top-level imports

### ReviewPage.jsx — Pass 2 fix
- Was always loading pass 1 items even when paused at pass 2 step
- Fixed: `effectivePass` stored in state so reloads after submit use correct pass number

### DedupResultsPage.jsx — New page
- View all dedup decisions (DUPLICATE / NOT_DUPLICATE / UNCERTAIN)
- Click any badge to change the LLM decision
- Unsaved changes shown with count + banner
- Save button calls `/dedup-review/bulk` endpoint

### MatchesPage.jsx — New page
- View all confirmed matched pairs for a city
- Filter by AUTO / HUMAN
- Search by name or address

### CityDetailPage.jsx
- Added **⊞ Dedup Results** button (visible once dedup runs)
- Added **✓ View Matches** button (visible when pipeline completed)
- Pass 2 review URL correctly set to `?pass=2`

### docker-compose.yml
- Added `user: "${UID:-1000}:${GID:-1000}"` — container runs as your local user, no locked folders
- Changed `DATABASE_URL` to `sqlite:////app/backend/bludot_pipeline.db` (inside mounted volume)
- Added `./backend/uploads:/app/uploads` volume mount

### requirements.txt
- Removed: `google-generativeai==0.5.4`, `langgraph==0.1.4`, `langchain-core==0.2.5`, `prefect==2.19.3`
- Added: `groq>=0.11.0`

---

## Troubleshooting

**Folders appear locked in file manager**
```bash
sudo chown -R $USER:$USER ~/bludot_pipeline/backend/uploads/
```

**Backend won't start — PermissionError on uploads folder**
```bash
mkdir -p backend/uploads
docker-compose down && docker-compose up
```

**Backend won't start — unable to open database file**
```bash
# DB must be inside the mounted backend volume
# Check docker-compose.yml has:
# DATABASE_URL=sqlite:////app/backend/bludot_pipeline.db
docker-compose down && docker-compose up
```

**ModuleNotFoundError: No module named 'groq'**
```bash
docker-compose build --no-cache backend
docker-compose up
```

**Review page shows "No items to review" even when pipeline is paused**
- Make sure you copied the latest `ReviewPage.jsx` — old version always loaded pass 1
- Check URL: pass 2 review should be `/city/{id}/review?pass=2`

**Output file has 0 matched records**
- Copy latest `step5_6_output.py` to `backend/core/`
- Run a fresh city (delete old one and create new) — old DB data may have wrong field names

**Groq rate limit or quota exhausted**
- Free tier: 14,400 requests/day, 500,000 tokens/day
- All quota-exhausted pairs marked UNCERTAIN → go to human review
- Resume the pipeline after human review — no pairs will be re-sent to Groq
- Quota resets daily at midnight UTC
# bludot_pipeline_gemini
# bludot_pipeline_gemini
