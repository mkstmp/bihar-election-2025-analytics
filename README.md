# Bihar Election 2025 Analytics & LLM Assistant

Interactive FastAPI service plus a lightweight dashboard to explore the mock 2025 Bihar Assembly election data.  DuckDB keeps the CSV-backed dataset in-memory for fast aggregations, while an OpenAI-powered assistant converts natural language questions into SQL and produces narrative answers.

## Key Features
- **Precomputed analytics** – `db.py` builds DuckDB tables/views (party summaries, alliance stats, constituency margins, NOTA trends, etc.) directly from the CSVs in this repo.
- **REST API & dashboard** – `main.py` exposes JSON endpoints that feed the React-less dashboard in `index.html`.
- **Natural-language Q&A** – `llm.py` uses OpenAI’s GPT-4o for SQL generation and answer drafting, with basic safety checks and automatic SQL repair attempts.
- **Caching for hot data** – Frequently requested overview queries are memoized with `functools.lru_cache` to keep responses snappy.

## Project Layout
| Path | Purpose |
| --- | --- |
| `main.py` | FastAPI app, routes, caching, and `/ask` LLM pipeline. |
| `db.py` | DuckDB initialization and derived tables/views. Runs at import time. |
| `llm.py` | SQL/answer prompt templates plus OpenAI client helper functions. |
| `index.html` | Static dashboard that calls the JSON endpoints. |
| `bihar_2025_candidates.csv`, `bihar_2025_ac_totals.csv` | Source datasets loaded into DuckDB. |
| `requirements.txt` | Python dependencies. |
| `Dockerfile` | Production image using Gunicorn + Uvicorn workers. |

## Prerequisites
- Python 3.11+
- OpenAI API key with access to GPT-4o (set as `OPENAI_API_KEY`)
- (Optional) Docker 24+ for containerized runs

## Local Development
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export OPENAI_API_KEY=sk-...
uvicorn main:app --reload --port 8000
```
Visit `http://localhost:8000/` for the dashboard or interact with the API via `/docs`.

### Helpful commands
- `uvicorn main:app --reload --port 8000` – local dev server
- `curl -X POST http://localhost:8000/ask -H "Content-Type: application/json" -d '{"question":"Which party won the most seats?"}'`

## Docker Usage
```bash
docker build -t bihar-llm-app .
docker run -p 8000:8000 -e OPENAI_API_KEY=sk-... bihar-llm-app
```
Gunicorn (4 Uvicorn workers) serves the API inside the container.

## Environment Variables
- `OPENAI_API_KEY` *(required)* – used by `llm.py` when generating SQL or drafting textual answers.
- `PORT` *(optional)* – only needed if your hosting platform expects a specific bind port; update the Gunicorn/uvicorn command accordingly.

## API Overview
All endpoints are JSON unless noted, and they derive data from DuckDB.

| Endpoint | Description |
| --- | --- |
| `GET /` | Returns `index.html`. |
| `GET /analytics/relevant-parties` | Parties that meaningfully compete in head-to-head races. |
| `GET /analytics/opponents?party=JDU` | Frequent opponents for a given party. |
| `GET /analytics/head-to-head?party1=BJP&party2=RJD` | Constituency-wise comparison of two parties. |
| `GET /overview/parties` | Seats, votes, and vote share per party. |
| `GET /overview/alliances` | Alliance-level seats/votes + seat/vote gap. |
| `GET /overview/party_performance?min_seats_won=3&min_vote_share=2` | Extended party metrics (strike rate, avg votes, etc.). |
| `GET /overview/nota` | Aggregated NOTA numbers. |
| `GET /overview/nail_biters?limit=10` | Close contests (≤2% margin). |
| `GET /party/analytics?party_short=BJP` | Positional finishes, vote buckets for one party. |
| `GET /constituency/search?q=Patna` | Quick search for constituencies. |
| `GET /constituency/detail?ac_no=123` | Candidate table for a specific constituency. |
| `POST /ask` | Body: `{"question":"..."}`. Returns `sql`, `answer`, and `rows` using the LLM flow. |

## LLM & Safety Notes
- Models: SQL + answer generation both use `gpt-4o` (configurable in `llm.py`).
- `sql_safe()` enforces read-only queries; anything non-SELECT is rejected.
- On DuckDB errors the server tries one automatic repair pass via GPT before raising `HTTPException`.
- The answer prompt caps table rows at 120 to control token usage; responses mention when truncation occurs.

## Frontend Tips
- The static dashboard calls the JSON routes via Fetch; update the endpoints in `index.html` if the API base URL changes.
- Chart.js powers the few simple charts; no build tooling is required—edit the HTML file directly.

## Deployment Notes
- `Dockerfile` installs system `gcc` headers for DuckDB/Pandas and runs Gunicorn with 4 workers.
- For serverless/Vercel-style deploys, replace the Gunicorn command with `uvicorn` in the start command and ensure `/app` has write access if you persist anything.
- CSVs are loaded into RAM on boot; on small containers consider swapping to Parquet or reducing dataset size.

## Troubleshooting
- **DuckDB init errors**: confirm the CSV paths exist relative to `db.py`.
- **401/429 from OpenAI**: ensure `OPENAI_API_KEY` is set and the account has access to GPT-4o.
- **Slow first request**: DuckDB initialization and CSV ingestion happen when FastAPI imports `db.py`; keep the process warm for best latency.

