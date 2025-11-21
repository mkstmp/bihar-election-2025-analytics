import os
from functools import lru_cache
from typing import List, Dict, Any

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# Import your modules
from db import conn, init_db
from llm import generate_sql, sql_safe, generate_answer_text, client as openai_client

# Initialize DB immediately
init_db()

app = FastAPI(title="Bihar Election 2025 Analytics")

# Enable CORS for production domains (adjust origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- MODELS ----------
class AskRequest(BaseModel):
    question: str

class AskResponse(BaseModel):
    question: str
    sql: str
    answer: str
    rows: List[Dict[str, Any]]

# ---------- UTILS ----------
def df_to_clean_dict(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert DF to list of dicts, handling NaNs."""
    return df.where(pd.notnull(df), None).to_dict(orient="records")

# ---------- CACHED DATA ACCESS ----------
# We use lru_cache because the DB is read-only (static CSVs).
# This drastically reduces load on DuckDB for repeated dashboard views.

@lru_cache(maxsize=128)
def _get_overview_parties():
    return conn.cursor().execute("""
        SELECT party_short, party_canonical, alliance, seats_won, total_votes, vote_share
        FROM party_summary_enriched ORDER BY seats_won DESC, vote_share DESC
    """).df()

@lru_cache(maxsize=1)
def _get_overview_alliances():
    return conn.cursor().execute("""
        SELECT alliance, seats_won, total_votes, vote_share, seat_share, seat_vote_gap
        FROM alliance_summary ORDER BY seats_won DESC
    """).df()

@lru_cache(maxsize=1)
def _get_relevant_parties():
    return conn.cursor().execute("""
        SELECT ps.party_short
        FROM party_summary_enriched ps
        WHERE ps.party_short IN (
            SELECT winner_party_short FROM constituency_margins WHERE winner_party_short IS NOT NULL
            UNION
            SELECT runner_party_short FROM constituency_margins WHERE runner_party_short IS NOT NULL
        )
        AND ps.party_short != 'IND'
        ORDER BY ps.seats_won DESC, ps.total_votes DESC
    """).df()

@lru_cache(maxsize=64)
def _get_opponents(party: str):
    return conn.cursor().execute("""
        WITH relevant_parties AS (
            SELECT DISTINCT winner_party_short as p FROM constituency_margins WHERE winner_party_short IS NOT NULL
            UNION
            SELECT DISTINCT runner_party_short as p FROM constituency_margins WHERE runner_party_short IS NOT NULL
        )
        SELECT t2.party_short, t2.alliance, COUNT(*) as contests
        FROM candidates_enriched t1
        JOIN candidates_enriched t2 ON t1.ac_no = t2.ac_no
        WHERE t1.party_short = ? AND t2.party_short != ?
          AND t2.party_short IN (SELECT p FROM relevant_parties)
          AND t2.party_short != 'IND'
        GROUP BY t2.party_short, t2.alliance
        ORDER BY contests DESC
    """, [party, party]).df()

@lru_cache(maxsize=128)
def _get_head_to_head(party1: str, party2: str):
    return conn.cursor().execute("""
        WITH valid_contests AS (
            SELECT t1.ac_no 
            FROM candidates_enriched t1
            JOIN candidates_enriched t2 ON t1.ac_no = t2.ac_no
            WHERE t1.party_short = ? AND t2.party_short = ?
        )
        SELECT 
            c.ac_no, c.ac_name,
            MAX(CASE WHEN c.party_short = ? THEN c.total_votes END) as p1_votes,
            MAX(CASE WHEN c.party_short = ? THEN c.vote_percent END) as p1_pct,
            MAX(CASE WHEN c.party_short = ? THEN c.total_votes END) as p2_votes,
            MAX(CASE WHEN c.party_short = ? THEN c.vote_percent END) as p2_pct,
            (SELECT party_short FROM candidates_enriched w WHERE w.ac_no = c.ac_no AND w.is_winner = TRUE) as actual_winner
        FROM candidates_enriched c
        JOIN valid_contests vc ON c.ac_no = vc.ac_no
        WHERE c.party_short IN (?, ?)
        GROUP BY c.ac_no, c.ac_name
        ORDER BY c.ac_no
    """, [party1, party2, party1, party1, party2, party2, party1, party2]).df()

# ---------- ROUTES ----------

@app.get("/", response_class=HTMLResponse)
def read_root():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>System Error: index.html not found</h1>"

@app.get("/analytics/relevant-parties")
def get_relevant_parties_endpoint():
    return df_to_clean_dict(_get_relevant_parties())

@app.get("/analytics/opponents")
def get_opponents_endpoint(party: str):
    return df_to_clean_dict(_get_opponents(party))

@app.get("/analytics/head-to-head")
def head_to_head_endpoint(party1: str, party2: str):
    return df_to_clean_dict(_get_head_to_head(party1, party2))

@app.get("/overview/parties")
def overview_parties_endpoint():
    return df_to_clean_dict(_get_overview_parties())

@app.get("/overview/alliances")
def overview_alliances_endpoint():
    return df_to_clean_dict(_get_overview_alliances())

@app.get("/overview/party_performance")
def overview_party_performance(min_seats_won: int = 0, min_vote_share: float = 0.0):
    # Not cached because of variable filters, but could be if needed
    df = conn.cursor().execute("""
        WITH party_contested AS (
            SELECT
                ce.party_short, ce.alliance,
                COUNT(DISTINCT ce.ac_no) AS seats_contested,
                COUNT_IF(ce.is_winner) AS seats_won,
                SUM(ce.total_votes) AS total_votes_contested,
                SUM(act.total_votes) AS total_votes_in_those_acs
            FROM candidates_enriched ce
            JOIN ac_totals act ON act.state = ce.state AND act.ac_no = ce.ac_no
            WHERE ce.party_short IS NOT NULL AND ce.party_short <> 'NOTA'
            GROUP BY ce.party_short, ce.alliance
        ),
        ps AS (SELECT party_short, vote_share FROM party_summary_enriched)
        SELECT
            pc.party_short, pc.alliance, pc.seats_contested, pc.seats_won,
            CASE WHEN pc.seats_contested > 0 THEN pc.seats_won * 100.0 / pc.seats_contested ELSE NULL END AS strike_rate,
            CASE WHEN pc.seats_contested > 0 THEN pc.total_votes_contested * 1.0 / pc.seats_contested ELSE NULL END AS avg_votes_per_seat,
            CASE WHEN pc.total_votes_in_those_acs > 0 THEN pc.total_votes_contested * 100.0 / pc.total_votes_in_those_acs ELSE NULL END AS vote_pct_contested,
            ps.vote_share AS state_vote_share
        FROM party_contested pc
        LEFT JOIN ps ON pc.party_short = ps.party_short
        WHERE pc.seats_won >= ? OR ps.vote_share >= ?
        ORDER BY ps.vote_share DESC
    """, [min_seats_won, min_vote_share]).df()
    return df_to_clean_dict(df)

@app.get("/overview/nota")
def overview_nota():
    df = conn.cursor().execute("SELECT * FROM nota_summary").df()
    return df_to_clean_dict(df)

@app.get("/overview/nail_biters")
def overview_nail_biters(limit: int = 10):
    df = conn.cursor().execute("""
        SELECT * FROM constituency_margins 
        WHERE margin_percent <= 2.0 
        ORDER BY margin_percent ASC LIMIT ?
    """, [limit]).df()
    return df_to_clean_dict(df)

@app.get("/party/analytics")
def party_analytics(party_short: str):
    df = conn.cursor().execute("""
        WITH ranked_candidates AS (
            SELECT 
                ac_no, party_short, vote_percent,
                ROW_NUMBER() OVER (PARTITION BY ac_no ORDER BY total_votes DESC) as rank
            FROM candidates_enriched
        )
        SELECT 
            COUNT_IF(rank = 1) as pos_1,
            COUNT_IF(rank = 2) as pos_2,
            COUNT_IF(rank = 3) as pos_3,
            COUNT_IF(rank = 4) as pos_4,
            COUNT_IF(rank >= 5) as pos_5_plus,
            COUNT_IF(vote_percent >= 50) as vote_gt_50,
            COUNT_IF(vote_percent >= 40 AND vote_percent < 50) as vote_40_50,
            COUNT_IF(vote_percent >= 25 AND vote_percent < 40) as vote_25_40,
            COUNT_IF(vote_percent >= 10 AND vote_percent < 25) as vote_10_25,
            COUNT_IF(vote_percent < 10) as vote_lt_10,
            COUNT(*) as total_seats_contested
        FROM ranked_candidates
        WHERE party_short = ?
    """, [party_short]).df()
    return df_to_clean_dict(df)

@app.get("/constituency/search")
def constituency_search(q: str):
    # Simple sanitization
    q_clean = q.replace("'", "").strip()
    if not q_clean: return []
    return df_to_clean_dict(conn.cursor().execute(f"SELECT DISTINCT ac_no, ac_name FROM candidates WHERE ac_name ILIKE '%{q_clean}%' OR CAST(ac_no AS TEXT) = '{q_clean}' LIMIT 10").df())

@app.get("/constituency/detail")
def constituency_detail(ac_no: int):
    return df_to_clean_dict(conn.cursor().execute("SELECT * FROM candidates_enriched WHERE ac_no = ? ORDER BY total_votes DESC", [ac_no]).df())

# ---------- LLM LOGIC ----------

def repair_sql(question: str, bad_sql: str, error_msg: str) -> str:
    SQL_MODEL = "gpt-4o"
    prompt = f"Fix SQL: {bad_sql} Error: {error_msg} Question: {question} Return SINGLE SELECT query. No Markdown."
    resp = openai_client.chat.completions.create(model=SQL_MODEL, messages=[{"role": "user", "content": prompt}], temperature=0)
    fixed = resp.choices[0].message.content.strip()
    if fixed.startswith("```"): fixed = fixed.strip("`").replace("sql", "").strip()
    return fixed

@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    question = req.question.strip()
    if not question: raise HTTPException(400, "Empty question")
    
    sql = generate_sql(question)
    if not sql_safe(sql): raise HTTPException(400, "Unsafe SQL")

    try:
        df = conn.cursor().execute(sql).df()
    except duckdb.Error as e:
        sql = repair_sql(question, sql, str(e))
        try:
            df = conn.cursor().execute(sql).df()
        except duckdb.Error as e2:
            raise HTTPException(400, f"Error executing SQL: {e2}")

    rows = df_to_clean_dict(df)

    # --- FIX: HANDLE TRUNCATION FOR LARGE LISTS ---
    if len(rows) > 25:
        # If the list is long, pass only a sample to the LLM to prevent it 
        # from attempting to write a huge, truncated list.
        sample_rows = rows[:5]
        answer = generate_answer_text(question, sql, sample_rows)
        # We explicitly tell the user to look at the table for the rest
        answer += f"\n\n**Note:** Found {len(rows)} results. I've summarized the top few above. Please refer to the data table for the full list."
    else:
        # For small lists, let the LLM handle it normally
        answer = generate_answer_text(question, sql, rows)
    # ----------------------------------------------

    # We always return the FULL 'rows' to the frontend so the table renders correctly
    return AskResponse(question=question, sql=sql, answer=answer, rows=rows)